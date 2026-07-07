"""
Method Z — large 数据集 × DeepSeek V3（冲刺 80% Case EA）

基线：
  - Method Y：77.5% Case EA / 60.8% Run ACC

本方法验证两处低风险修正：
  1. 普通订单数/订单量/订单占比在未指定状态时，不默认过滤 order_status='已完成'
  2. "加入购物车但该商品未出现在已完成订单中"必须按同一商品反连接订单明细
"""
from pathlib import Path

METHOD_ID = "z"
LABEL = "Method Z（large 数据集，DeepSeek V3，订单状态/加购反连接收窄 retry=2）"
MODE = "forge"
USE_SEMANTIC_LIB = True
RUNS = 3
COMPILE_RETRIES = 2
DATASET = "large"
NOTES = "2026-05-06 Method Y 后收窄普通订单状态过滤和同商品加购反连接"

LLM_PROVIDER = "openai"
MODEL = "deepseek-chat"
BASE_URL = "https://api.deepseek.com/v1"
API_KEY = ""  # 读 env DEEPSEEK_API_KEY

_DATASETS_DIR = Path(__file__).parent.parent.parent / "datasets"

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from build_context import build_registry_context

REGISTRY_CONTEXT = build_registry_context(_DATASETS_DIR / "large")
CASES_FILE = str(_DATASETS_DIR / "large" / "cases.json")
