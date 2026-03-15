"""
SQL 执行器 — 连接数据库执行 SQL 并返回格式化结果。

支持所有 SQLAlchemy 方言（SQLite / PostgreSQL / MySQL 等）。
数据库 URL 从 cfg.DATABASE_URL 读取。

返回值：纯文本表格，直接发送给用户。
"""
from __future__ import annotations

from typing import Any

from config import cfg


def execute(sql: str, max_rows: int = 50) -> str:
    """
    执行 SQL，返回格式化的纯文本结果。

    Args:
        sql:      要执行的 SQL 字符串
        max_rows: 最多返回的行数，超出时附加提示

    Returns:
        格式化后的结果字符串；执行失败时返回错误说明。
    """
    if not cfg.DATABASE_URL:
        return "⚠ 未配置数据库连接（DATABASE_URL），无法执行查询。"

    try:
        from sqlalchemy import create_engine, text
    except ImportError:
        return "⚠ 缺少依赖：请运行 pip install sqlalchemy"

    try:
        engine = create_engine(cfg.DATABASE_URL)
        with engine.connect() as conn:
            result = conn.execute(text(sql))
            rows   = result.fetchmany(max_rows + 1)
            cols   = list(result.keys())
    except Exception as exc:
        return f"⚠ 执行失败：{exc}"

    if not rows:
        return "查询完成，结果为空。"

    truncated = len(rows) > max_rows
    if truncated:
        rows = rows[:max_rows]

    # 计算每列宽度
    col_widths = [len(str(c)) for c in cols]
    for row in rows:
        for i, val in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(val) if val is not None else "NULL"))

    def fmt_row(vals: list[Any]) -> str:
        parts = [str(v if v is not None else "NULL").ljust(col_widths[i]) for i, v in enumerate(vals)]
        return "  ".join(parts)

    sep   = "  ".join("-" * w for w in col_widths)
    lines = [fmt_row(cols), sep] + [fmt_row(list(r)) for r in rows]

    if truncated:
        lines.append(f"（仅显示前 {max_rows} 行）")

    return "\n".join(lines)


def execute_with_data(
    sql: str, max_rows: int = 200
) -> tuple[str, list[str], list[tuple]]:
    """
    执行 SQL，同时返回格式化文本和原始数据（供图表使用）。

    Returns:
        (text, cols, rows)  — text 同 execute()；cols/rows 用于图表生成
    """
    if not cfg.DATABASE_URL:
        return "⚠ 未配置数据库连接（DATABASE_URL），无法执行查询。", [], []

    try:
        from sqlalchemy import create_engine, text as sa_text
    except ImportError:
        return "⚠ 缺少依赖：请运行 pip install sqlalchemy", [], []

    try:
        engine = create_engine(cfg.DATABASE_URL)
        with engine.connect() as conn:
            result = conn.execute(sa_text(sql))
            rows   = result.fetchmany(max_rows + 1)
            cols   = list(result.keys())
    except Exception as exc:
        return f"⚠ 执行失败：{exc}", [], []

    if not rows:
        return "查询完成，结果为空。", cols, []

    truncated = len(rows) > max_rows
    if truncated:
        rows = rows[:max_rows]

    # 格式化文本（同 execute，最多显示 50 行）
    display_rows = rows[:50]
    col_widths = [len(str(c)) for c in cols]
    for row in display_rows:
        for i, val in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(val) if val is not None else "NULL"))

    def fmt_row(vals):
        parts = [str(v if v is not None else "NULL").ljust(col_widths[i]) for i, v in enumerate(vals)]
        return "  ".join(parts)

    sep   = "  ".join("-" * w for w in col_widths)
    lines = [fmt_row(cols), sep] + [fmt_row(list(r)) for r in display_rows]
    if truncated or len(rows) > 50:
        lines.append(f"（显示前 {len(display_rows)} 行，共 {len(rows)} 行）")

    return "\n".join(lines), cols, list(rows)
