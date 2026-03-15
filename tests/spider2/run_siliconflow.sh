#!/bin/bash
# 硅基流动三模型横向对比脚本
# 用法: ./tests/spider2/run_siliconflow.sh
# API Key 通过 SF_API_KEY 环境变量传入，或在此设置

SF_API_KEY="${SF_API_KEY:-}"
SF_BASE_URL="https://api.siliconflow.cn/v1"

if [ -z "$SF_API_KEY" ]; then
  echo "❌ 请设置 SF_API_KEY 环境变量"
  exit 1
fi

MODELS=(
  "Qwen/Qwen3.5-397B-A17B"
  "Pro/zai-org/GLM-5"
  "Pro/moonshotai/Kimi-K2.5"
)

METHOD_NAMES=(
  "qwen35_397b"
  "glm5"
  "kimi_k25"
)

for i in "${!MODELS[@]}"; do
  MODEL="${MODELS[$i]}"
  METHOD="${METHOD_NAMES[$i]}"
  echo ""
  echo "=============================="
  echo "  模型: $MODEL"
  echo "  方法: $METHOD"
  echo "=============================="

  LLM_API_KEY="$SF_API_KEY" \
  LLM_BASE_URL="$SF_BASE_URL" \
  LLM_MODEL="$MODEL" \
  LLM_FORMAT="openai" \
  LLM_METHOD="$METHOD" \
  python3 tests/spider2/runner.py --workers 3 "$@"

  echo ""
  echo "▶ 评估 $METHOD ..."
  python3 tests/spider2/evaluate.py --method "$METHOD"
done

echo ""
echo "=============================="
echo "  横向对比汇总"
echo "=============================="
for i in "${!MODELS[@]}"; do
  METHOD="${METHOD_NAMES[$i]}"
  REPORT="tests/spider2/results/$METHOD/eval_report.json"
  if [ -f "$REPORT" ]; then
    ACC=$(python3 -c "import json; r=json.load(open('$REPORT')); print(f\"{r['matched']}/{r['evaluated']} = {r['accuracy']}%\")")
    echo "  ${MODELS[$i]}: $ACC"
  fi
done
