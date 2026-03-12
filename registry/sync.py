"""
Registry 结构层同步模块。

职责：
    连接目标数据库，内省所有表的字段信息，写入 schema.registry.json。
    只写结构层（表名 + 字段名），不涉及业务语义（指标定义在 metrics.registry.yaml）。

调用方式：
    CLI：  forge sync [--db <url>]
    Python：from registry.sync import run_sync
            run_sync(database_url, registry_path)

支持的数据库（通过 SQLAlchemy 方言）：
    SQLite     sqlite:///./path/to/file.db
    PostgreSQL postgresql://user:pass@host:5432/dbname
    MySQL      mysql+pymysql://user:pass@host:3306/dbname

输出文件格式（schema.registry.json）：
    {
      "tables": {
        "orders": {
          "columns": {
            "id":     {},
            "status": {"enum": ["completed", "pending", "cancelled"]},
            ...
          }
        }
      }
    }

合并策略（增量，不覆盖元数据）：
    - 新增表/列：插入空元数据 {}
    - 删除的表/列：从 registry 中移除
    - 已有列的 enum 等元数据：完整保留，sync 不覆盖
    metrics.registry.yaml 不受 sync 影响，两个文件独立维护。
"""
from __future__ import annotations

import json
from pathlib import Path

from sqlalchemy import create_engine, inspect


_ENUM_MAX_DISTINCT = 30   # 去重值 ≤ 此数的字符串列自动采样为枚举
_ENUM_SAMPLE_ROWS  = 5000  # 采样行数上限，避免在大表上全表扫描


def _introspect(database_url: str) -> dict[str, dict[str, dict]]:
    """
    内省数据库，返回每个表的列结构及低基数列的枚举值。

    对字符串类型且 DISTINCT 数 ≤ _ENUM_MAX_DISTINCT 的列，
    自动采样所有合法值并写入 enum，无需用户手动维护。

    Args:
        database_url: SQLAlchemy 格式的数据库连接字符串。

    Returns:
        {table_name: {col_name: {"enum": [...]} or {}}}
    """
    from sqlalchemy import text as sa_text

    engine = create_engine(database_url)
    inspector = inspect(engine)
    result: dict[str, dict[str, dict]] = {}

    with engine.connect() as conn:
        for table_name in inspector.get_table_names():
            col_infos = inspector.get_columns(table_name)
            table_cols: dict[str, dict] = {}

            for col in col_infos:
                col_name = col["name"]
                col_type = str(col["type"]).upper()

                # 跳过时间戳、日期类列（类型检测 + 列名启发式，兼容 SQLite TEXT 存储）
                _ts_name = col_name.lower()
                is_ts_col = (
                    any(t in col_type for t in ("DATE", "TIME", "TIMESTAMP")) or
                    _ts_name.endswith("_at") or _ts_name.endswith("_date") or
                    _ts_name in ("created", "updated", "deleted", "timestamp")
                )
                if is_ts_col:
                    table_cols[col_name] = {}
                    continue

                # 对字符串型列尝试自动采样枚举值（跳过 id 类主键/外键）
                is_id_col = col_name == "id" or col_name.endswith("_id")
                if not is_id_col and any(t in col_type for t in ("CHAR", "TEXT", "VARCHAR", "ENUM", "STRING")):
                    try:
                        # 先检查 DISTINCT 数量，避免高基数列（如 name、email）
                        count_sql = sa_text(
                            f"SELECT COUNT(DISTINCT {col_name}) FROM {table_name} "
                            f"LIMIT {_ENUM_SAMPLE_ROWS}"
                        )
                        distinct_count = conn.execute(count_sql).scalar() or 0
                        if 0 < distinct_count <= _ENUM_MAX_DISTINCT:
                            vals_sql = sa_text(
                                f"SELECT DISTINCT {col_name} FROM {table_name} "
                                f"WHERE {col_name} IS NOT NULL ORDER BY {col_name} "
                                f"LIMIT {_ENUM_MAX_DISTINCT}"
                            )
                            vals = [row[0] for row in conn.execute(vals_sql)]
                            table_cols[col_name] = {"enum": vals}
                            continue
                    except Exception:
                        pass  # 采样失败静默跳过，降级为无元数据

                # 整数型 flag/状态列（如 is_vip）：跳过 id 和外键，只采样小基数非 ID 整数
                is_id_col = col_name == "id" or col_name.endswith("_id")
                if not is_id_col and any(t in col_type for t in ("BOOL", "TINYINT", "SMALLINT", "INTEGER", "INT")):
                    try:
                        count_sql = sa_text(
                            f"SELECT COUNT(DISTINCT {col_name}) FROM {table_name} "
                            f"LIMIT {_ENUM_SAMPLE_ROWS}"
                        )
                        distinct_count = conn.execute(count_sql).scalar() or 0
                        if 0 < distinct_count <= _ENUM_MAX_DISTINCT:
                            vals_sql = sa_text(
                                f"SELECT DISTINCT {col_name} FROM {table_name} "
                                f"WHERE {col_name} IS NOT NULL ORDER BY {col_name} "
                                f"LIMIT {_ENUM_MAX_DISTINCT}"
                            )
                            vals = [row[0] for row in conn.execute(vals_sql)]
                            table_cols[col_name] = {"enum": vals}
                            continue
                    except Exception:
                        pass

                table_cols[col_name] = {}

            result[table_name] = table_cols

    engine.dispose()
    return result


def _merge(existing: dict, live_tables: dict[str, dict[str, dict]]) -> dict:
    """
    将数据库实时结构与现有 registry 增量合并，保留用户手动添加的列元数据。

    合并规则：
        - 新增表：使用 _introspect() 采样到的元数据（含 enum）
        - 删除的表：从 registry 移除
        - 新增列：使用 _introspect() 采样到的元数据
        - 删除的列：从该表 columns 中移除
        - 已有列：优先保留用户手动编辑的元数据；若用户未手动添加，则用 live 采样值覆盖

    Args:
        existing:    现有 registry dict（可能是旧数组格式或新 dict 格式）
        live_tables: _introspect() 返回的 {table: {col: {"enum": [...]} or {}}}

    Returns:
        合并后的 registry dict（新格式）
    """
    existing_tables = existing.get("tables", existing)
    merged: dict = {}

    for table_name, live_cols in live_tables.items():
        existing_table = existing_tables.get(table_name, {})

        # 读取现有列元数据（兼容旧数组格式）
        existing_cols_raw = (
            existing_table.get("columns", existing_table)
            if isinstance(existing_table, dict)
            else existing_table
        )
        if isinstance(existing_cols_raw, list):
            existing_col_meta: dict[str, dict] = {col: {} for col in existing_cols_raw}
        else:
            existing_col_meta = dict(existing_cols_raw) if existing_cols_raw else {}

        # 合并策略：用户手动写的 enum 优先；否则用 live 采样值
        merged_cols: dict[str, dict] = {}
        for col_name, live_meta in live_cols.items():
            existing_meta = existing_col_meta.get(col_name)
            if existing_meta and existing_meta.get("enum"):
                # 用户手动标注过，保留不覆盖
                merged_cols[col_name] = existing_meta
            else:
                # 用 live 采样值（可能含 enum，也可能是 {}）
                merged_cols[col_name] = live_meta

        merged[table_name] = {"columns": merged_cols}

    return {"tables": merged}


def run_sync(database_url: str, registry_path: Path) -> dict:
    """
    内省数据库并将结构层增量写入 registry_path。

    与旧版区别：不再完整覆盖，而是保留用户手动标注的列元数据（如 enum 值）。

    Args:
        database_url:   目标数据库连接字符串。
        registry_path:  schema.registry.json 的写入路径（Path 对象）。

    Returns:
        写入磁盘的 registry 字典，与文件内容完全一致。
    """
    live_tables = _introspect(database_url)

    existing: dict = {}
    if registry_path.exists():
        try:
            existing = json.loads(registry_path.read_text())
        except Exception:
            pass

    registry = _merge(existing, live_tables)
    registry_path.write_text(json.dumps(registry, indent=2, ensure_ascii=False))
    return registry
