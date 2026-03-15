"""
图表生成器 — 根据 SQL 结果自动推荐并生成交互式 ECharts 图表。

策略：
  - 1 文本列 + 1 数值列   → 柱状图 / 饼图（行数 ≤ 8 用饼图，否则用柱状图）
  - 日期/时间列 + 数值列  → 折线图
  - 2+ 数值列             → 柱状图（分组）
  - 其他                  → 柱状图兜底
"""
from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any

# ── 图表存储路径 ──────────────────────────────────────────────────────────────

CHART_DIR = Path(__file__).parent.parent / "web" / "static" / "charts"
CHART_DIR.mkdir(parents=True, exist_ok=True)


# ── 列类型判断 ────────────────────────────────────────────────────────────────

_DATE_KEYWORDS = {"date", "dt", "time", "month", "year", "week", "day", "period"}
_NUM_KEYWORDS  = {
    "amount", "price", "cost", "count", "qty", "quantity", "rate", "ratio",
    "score", "value", "total", "sum", "avg", "revenue", "profit", "margin",
    "gmv", "pct", "percent", "num", "cnt",
}


def _is_date_col(name: str) -> bool:
    n = name.lower()
    return any(k in n for k in _DATE_KEYWORDS)


def _is_numeric(val: Any) -> bool:
    return isinstance(val, (int, float)) and not isinstance(val, bool)


def _col_is_numeric(col: str, sample_vals: list[Any]) -> bool:
    n = col.lower()
    if any(k in n for k in _NUM_KEYWORDS):
        return True
    nums = [v for v in sample_vals if v is not None]
    return bool(nums) and all(_is_numeric(v) for v in nums[:5])


# ── 图表推荐 ──────────────────────────────────────────────────────────────────

def _recommend(cols: list[str], rows: list[tuple]) -> str:
    """返回推荐图表类型：bar / pie / line"""
    if len(cols) < 2:
        return "bar"

    date_cols = [c for c in cols if _is_date_col(c)]
    num_cols   = [
        c for c in cols
        if _col_is_numeric(c, [r[cols.index(c)] for r in rows[:10]])
    ]
    text_cols  = [c for c in cols if c not in date_cols and c not in num_cols]

    if date_cols and num_cols:
        return "line"
    if text_cols and len(num_cols) == 1 and len(rows) <= 8:
        return "pie"
    return "bar"


# ── 图表生成 ──────────────────────────────────────────────────────────────────

def _make_bar(title: str, cols: list[str], rows: list[tuple]) -> str:
    from pyecharts.charts import Bar
    from pyecharts import options as opts

    # 找第一个文本列作 X 轴，其余数值列作 Y 系列
    num_idx  = [i for i, c in enumerate(cols) if _col_is_numeric(c, [r[i] for r in rows[:10]])]
    text_idx = [i for i in range(len(cols)) if i not in num_idx]
    x_idx    = text_idx[0] if text_idx else 0
    y_indices = num_idx if num_idx else [i for i in range(len(cols)) if i != x_idx]

    x_data = [str(r[x_idx]) for r in rows]
    bar = (
        Bar()
        .set_global_opts(
            title_opts=opts.TitleOpts(title=title),
            tooltip_opts=opts.TooltipOpts(trigger="axis"),
            toolbox_opts=opts.ToolboxOpts(is_show=True),
            datazoom_opts=[opts.DataZoomOpts(type_="inside"), opts.DataZoomOpts()],
            xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(rotate=30)),
        )
        .add_xaxis(x_data)
    )
    for yi in y_indices:
        y_data = [r[yi] for r in rows]
        bar.add_yaxis(cols[yi], y_data, label_opts=opts.LabelOpts(is_show=False))

    return bar.render_embed()


def _make_pie(title: str, cols: list[str], rows: list[tuple]) -> str:
    from pyecharts.charts import Pie
    from pyecharts import options as opts

    num_idx  = [i for i, c in enumerate(cols) if _col_is_numeric(c, [r[i] for r in rows[:10]])]
    text_idx = [i for i in range(len(cols)) if i not in num_idx]
    name_idx = text_idx[0] if text_idx else 0
    val_idx  = num_idx[0] if num_idx else (1 if len(cols) > 1 else 0)

    data_pairs = [(str(r[name_idx]), r[val_idx]) for r in rows if r[val_idx] is not None]
    pie = (
        Pie()
        .add(
            cols[val_idx],
            data_pairs,
            radius=["30%", "65%"],
            rosetype="radius",
        )
        .set_global_opts(
            title_opts=opts.TitleOpts(title=title),
            legend_opts=opts.LegendOpts(orient="vertical", pos_left="left"),
            toolbox_opts=opts.ToolboxOpts(is_show=True),
        )
        .set_series_opts(label_opts=opts.LabelOpts(formatter="{b}: {d}%"))
    )
    return pie.render_embed()


def _make_line(title: str, cols: list[str], rows: list[tuple]) -> str:
    from pyecharts.charts import Line
    from pyecharts import options as opts

    date_idx = next((i for i, c in enumerate(cols) if _is_date_col(c)), 0)
    num_indices = [
        i for i, c in enumerate(cols)
        if i != date_idx and _col_is_numeric(c, [r[i] for r in rows[:10]])
    ]
    if not num_indices:
        num_indices = [i for i in range(len(cols)) if i != date_idx]

    x_data = [str(r[date_idx]) for r in rows]
    line = (
        Line()
        .set_global_opts(
            title_opts=opts.TitleOpts(title=title),
            tooltip_opts=opts.TooltipOpts(trigger="axis"),
            toolbox_opts=opts.ToolboxOpts(is_show=True),
            datazoom_opts=[opts.DataZoomOpts(type_="inside"), opts.DataZoomOpts()],
            xaxis_opts=opts.AxisOpts(
                type_="category",
                axislabel_opts=opts.LabelOpts(rotate=30),
            ),
        )
        .add_xaxis(x_data)
    )
    for yi in num_indices:
        line.add_yaxis(
            cols[yi],
            [r[yi] for r in rows],
            is_smooth=True,
            label_opts=opts.LabelOpts(is_show=False),
        )
    return line.render_embed()


def _build_html(chart_embed: str, title: str) -> str:
    return f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{title}</title>
  <style>
    body {{ margin: 0; background: #0f172a; font-family: sans-serif; }}
    h2 {{ color: #94a3b8; text-align: center; padding: 16px 0 0; margin: 0; font-size: 14px; }}
  </style>
</head>
<body>
  <h2>{title}</h2>
  {chart_embed}
</body>
</html>"""


# ── Matplotlib 静态图像 ───────────────────────────────────────────────────────

_COLORS = ["#5470c6", "#91cc75", "#fac858", "#ee6666", "#73c0de",
           "#3ba272", "#fc8452", "#9a60b4", "#ea7ccc", "#48b8d0"]

def _setup_font() -> None:
    import matplotlib.pyplot as plt
    for name in ["STHeiti", "PingFang SC", "Heiti TC", "SimHei", "Arial Unicode MS"]:
        try:
            plt.rcParams["font.sans-serif"] = [name]
            plt.rcParams["axes.unicode_minus"] = False
            return
        except Exception:
            continue


def generate_image(cols: list[str], rows: list[tuple], query_hint: str = "") -> bytes | None:
    """
    生成 matplotlib PNG 字节流，适合上传到飞书后嵌入卡片。
    """
    if not rows or len(cols) < 1:
        return None
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import io

        _setup_font()
        chart_type = _recommend(cols, rows)
        title = (query_hint[:50] + "…" if len(query_hint) > 50 else query_hint) or "查询结果"

        fig, ax = plt.subplots(figsize=(10, 5))
        fig.patch.set_facecolor("#1e293b")
        ax.set_facecolor("#1e293b")
        ax.tick_params(colors="#94a3b8")
        for spine in ax.spines.values():
            spine.set_edgecolor("#334155")

        num_idx  = [i for i, c in enumerate(cols) if _col_is_numeric(c, [r[i] for r in rows[:10]])]
        text_idx = [i for i in range(len(cols)) if i not in num_idx]
        date_idx_list = [i for i, c in enumerate(cols) if _is_date_col(c)]

        if chart_type == "pie":
            name_i = text_idx[0] if text_idx else 0
            val_i  = num_idx[0]  if num_idx  else 1
            labels = [str(r[name_i]) for r in rows]
            values = [r[val_i] or 0 for r in rows]
            ax.pie(values, labels=labels, colors=_COLORS[:len(values)],
                   autopct="%1.1f%%", startangle=140,
                   textprops={"color": "#cbd5e1"})
            ax.axis("equal")

        elif chart_type == "line":
            x_i = date_idx_list[0] if date_idx_list else (text_idx[0] if text_idx else 0)
            x   = [str(r[x_i]) for r in rows]
            for k, yi in enumerate(num_idx or [i for i in range(len(cols)) if i != x_i]):
                y = [r[yi] or 0 for r in rows]
                ax.plot(x, y, marker="o", markersize=4,
                        color=_COLORS[k % len(_COLORS)], label=cols[yi], linewidth=2)
            ax.legend(facecolor="#1e293b", labelcolor="#94a3b8", edgecolor="#334155")
            plt.xticks(rotation=30, ha="right", fontsize=9)

        else:  # bar
            x_i    = text_idx[0] if text_idx else 0
            y_list = num_idx if num_idx else [i for i in range(len(cols)) if i != x_i]
            x_labels = [str(r[x_i]) for r in rows]
            x_pos    = range(len(rows))
            width    = 0.8 / max(len(y_list), 1)
            offset   = -(len(y_list) - 1) * width / 2
            for k, yi in enumerate(y_list):
                y = [r[yi] or 0 for r in rows]
                bars = ax.bar([p + offset + k * width for p in x_pos],
                              y, width=width * 0.9,
                              color=_COLORS[k % len(_COLORS)], label=cols[yi])
            ax.set_xticks(list(x_pos))
            ax.set_xticklabels(x_labels, rotation=30, ha="right", fontsize=9)
            if len(y_list) > 1:
                ax.legend(facecolor="#1e293b", labelcolor="#94a3b8", edgecolor="#334155")

        ax.set_title(title, color="#e2e8f0", fontsize=13, pad=12)
        ax.yaxis.label.set_color("#94a3b8")
        ax.tick_params(axis="y", colors="#94a3b8")
        ax.grid(axis="y", color="#334155", linestyle="--", linewidth=0.5, alpha=0.7)

        plt.tight_layout()
        buf = io.BytesIO()
        fig.savefig(buf, format="png", dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
        plt.close(fig)
        return buf.getvalue()

    except Exception:
        import traceback
        traceback.print_exc()
        return None


# ── 公开接口 ──────────────────────────────────────────────────────────────────

def generate(cols: list[str], rows: list[tuple], query_hint: str = "") -> str | None:
    """
    生成交互图表，返回文件名（不含路径），失败返回 None。

    Args:
        cols:        列名列表
        rows:        数据行（tuple 列表）
        query_hint:  原始查询文本（用作图表标题）

    Returns:
        生成的 HTML 文件名，如 "a1b2c3d4.html"
    """
    if not rows or len(cols) < 1:
        return None

    try:
        chart_type = _recommend(cols, rows)
        title      = (query_hint[:40] + "…" if len(query_hint) > 40 else query_hint) or "查询结果"

        if chart_type == "pie":
            embed = _make_pie(title, cols, rows)
        elif chart_type == "line":
            embed = _make_line(title, cols, rows)
        else:
            embed = _make_bar(title, cols, rows)

        html     = _build_html(embed, title)
        filename = f"{uuid.uuid4().hex[:12]}.html"
        (CHART_DIR / filename).write_text(html, encoding="utf-8")
        return filename

    except Exception:
        import traceback
        traceback.print_exc()
        return None
