"""记忆作用域迁移。

背景
----
``filtering_settings.memory_scope_mode`` 决定写入与检索时使用的作用域标识
（即 ``session_id``）：

===========  ====================================================
legacy       旧行为，受 ``use_session_filtering`` 控制
session      实际会话 ID（``unified_msg_origin``）
user         ``livingmemory:user:<平台>:<身份>``
global       ``livingmemory:global``
===========  ====================================================

该标识在**写入时**固化进每条记忆（``documents.metadata.session_id``、
``memory_atoms.session_id``），而检索按作用域**精确匹配**。因此一旦切换模式，
既有记忆仍带着旧标识，在新作用域下再也检索不到：

1. 检索侧不会把旧作用域纳入候选（``GLOBAL_MEMORY_SCOPE`` 只是普通字符串，
   没有任何"全局即不过滤"的特判）；
2. 自动迁移 ``_migrate_session_data_if_needed()`` 的触发条件恰好是
   ``not session_id.startswith("livingmemory:")``，把 ``global`` / ``user``
   产生的新作用域排除在外，因此永远不会被触发。

结果是：切换配置后"新记忆能跨会话、旧记忆全部失联"，且没有任何补救入口。

本模块提供**显式**迁移能力——由调用方（用户）指定目标作用域，把既有记录
改写过去。刻意不做任何隐式推断：目标要么是预设关键字，要么是调用方给出的
字面作用域字符串。
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from astrbot.api import logger

# documents 表中的作用域表达式（统一走 json_extract，避免逐行 Python 解析）
_DOC_SCOPE_EXPR = "COALESCE(json_extract(metadata, '$.session_id'), '')"
# memory_atoms 表中的作用域列
_ATOM_SCOPE_EXPR = "COALESCE(session_id, '')"


@dataclass
class ScopeBreakdown:
    """按作用域统计的存量分布。"""

    documents: dict[str, int] = field(default_factory=dict)
    atoms: dict[str, int] = field(default_factory=dict)

    @property
    def total(self) -> int:
        return sum(self.documents.values()) + sum(self.atoms.values())

    @property
    def scopes(self) -> list[str]:
        return sorted(set(self.documents) | set(self.atoms))


@dataclass
class MigrationPlan:
    """一次迁移的影响面（dry-run 也走这里，因此必须只读）。"""

    target: str
    source: str | None = None
    documents: int = 0
    atoms: int = 0
    by_source: dict[str, int] = field(default_factory=dict)

    @property
    def total(self) -> int:
        return self.documents + self.atoms

    @property
    def is_noop(self) -> bool:
        return self.total == 0


@dataclass
class MigrationResult:
    """迁移执行结果。"""

    target: str
    documents: int = 0
    atoms: int = 0
    backup_path: str | None = None

    @property
    def total(self) -> int:
        return self.documents + self.atoms


class ScopeMigrator:
    """把既有记忆的作用域改写到指定目标。

    Args:
        db_connection: 插件主库连接（aiosqlite 风格：``execute`` / ``commit``）。
    """

    def __init__(self, db_connection: Any) -> None:
        if db_connection is None:
            raise ValueError("db_connection 不能为空")
        self._db = db_connection

    # ------------------------------------------------------------------
    # 只读：盘点与计划
    # ------------------------------------------------------------------

    async def survey(self) -> ScopeBreakdown:
        """盘点当前各作用域的存量（只读）。"""
        breakdown = ScopeBreakdown()

        cursor = await self._db.execute(
            f"SELECT {_DOC_SCOPE_EXPR} AS scope, COUNT(*) FROM documents GROUP BY scope"
        )
        for scope, count in await cursor.fetchall():
            breakdown.documents[str(scope)] = int(count)

        cursor = await self._db.execute(
            f"SELECT {_ATOM_SCOPE_EXPR} AS scope, COUNT(*) "
            "FROM memory_atoms GROUP BY scope"
        )
        for scope, count in await cursor.fetchall():
            breakdown.atoms[str(scope)] = int(count)

        return breakdown

    async def plan(self, target: str, source: str | None = None) -> MigrationPlan:
        """计算影响面，**不写库**。

        Args:
            target: 目标作用域（非空）。
            source: 可选，仅迁移该来源作用域的记录；``None`` 表示"除目标外的全部"。
        """
        target = (target or "").strip()
        if not target:
            raise ValueError("target 不能为空")

        plan = MigrationPlan(target=target, source=source)

        for table, scope_expr in (("documents", _DOC_SCOPE_EXPR), ("memory_atoms", _ATOM_SCOPE_EXPR)):
            sql = f"SELECT {scope_expr} AS scope, COUNT(*) FROM {table} WHERE {scope_expr} <> ?"
            params: list[Any] = [target]
            if source is not None:
                sql += f" AND {scope_expr} = ?"
                params.append(source)
            sql += " GROUP BY scope"

            cursor = await self._db.execute(sql, tuple(params))
            rows = await cursor.fetchall()
            counted = 0
            for scope, count in rows:
                counted += int(count)
                key = str(scope)
                plan.by_source[key] = plan.by_source.get(key, 0) + int(count)

            if table == "documents":
                plan.documents = counted
            else:
                plan.atoms = counted

        return plan

    # ------------------------------------------------------------------
    # 写入：备份与执行
    # ------------------------------------------------------------------

    @staticmethod
    async def backup_to(db_connection: Any, target_path: str | Path) -> Path:
        """用 ``VACUUM INTO`` 生成一致性快照（目标文件必须不存在）。"""
        path = Path(target_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            raise FileExistsError(f"备份目标已存在: {path}")
        await db_connection.execute("VACUUM INTO ?", (str(path),))
        return path

    async def execute(
        self, plan: MigrationPlan, *, backup_path: str | Path | None = None
    ) -> MigrationResult:
        """按计划执行迁移。

        幂等性：WHERE 条件包含 ``<> target``，因此重复执行不会重复改写，
        也不会破坏此前记录的 ``old_session_id``。
        """
        result = MigrationResult(target=plan.target)

        if backup_path is not None:
            path = await self.backup_to(self._db, backup_path)
            result.backup_path = str(path)
            logger.info(f"[作用域迁移] 已生成备份: {path}")

        stamp = time.time()

        doc_sql = f"""
            UPDATE documents
            SET metadata = json_set(
                COALESCE(metadata, '{{}}'),
                '$.session_id', ?,
                '$.migrated_at', ?,
                '$.old_session_id', json_extract(COALESCE(metadata, '{{}}'), '$.session_id')
            )
            WHERE {_DOC_SCOPE_EXPR} <> ?
        """
        doc_params: list[Any] = [plan.target, stamp, plan.target]
        if plan.source is not None:
            doc_sql = doc_sql.replace(
                f"WHERE {_DOC_SCOPE_EXPR} <> ?",
                f"WHERE {_DOC_SCOPE_EXPR} <> ? AND {_DOC_SCOPE_EXPR} = ?",
            )
            doc_params.append(plan.source)

        cursor = await self._db.execute(doc_sql, tuple(doc_params))
        result.documents = int(cursor.rowcount or 0)

        atom_sql = f"UPDATE memory_atoms SET session_id = ? WHERE {_ATOM_SCOPE_EXPR} <> ?"
        atom_params: list[Any] = [plan.target, plan.target]
        if plan.source is not None:
            atom_sql += f" AND {_ATOM_SCOPE_EXPR} = ?"
            atom_params.append(plan.source)

        cursor = await self._db.execute(atom_sql, tuple(atom_params))
        result.atoms = int(cursor.rowcount or 0)

        await self._db.commit()

        logger.info(
            f"[作用域迁移] 已迁移 documents={result.documents} "
            f"atoms={result.atoms} -> {plan.target}"
        )
        return result
