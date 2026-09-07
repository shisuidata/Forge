"""Method E — 枚举 schema + 升级提示词（having alias / LIMIT精确 / 排名表 / LAG default）"""

METHOD_ID = "e"
LABEL = "Method E（枚举 schema + 升级提示词）"
MODE = "forge"
NOTES = "D基础上：having必须用alias、LIMIT精确取值、三种排名函数对比表、LAG/LEAD default规则、JOIN字段完整性"

# E 的 SYSTEM_PROMPT 与 D 共用相同文本（在 run_minimax.py 中 METHOD_E_SYSTEM = METHOD_D_SYSTEM）
# 此处为独立存储，便于版本对比。实际上 E 的提示词核心规则已融入 FORGE_DSL_SPEC_V2 中。
# 如果 E 和 D 的提示词看起来一样，是因为 FORGE_DSL_SPEC_V2 在写入时已包含了 E 的所有新规则。

from methods.method_d import SYSTEM_PROMPT  # noqa: F401 — E 和 D 使用同一 system prompt
