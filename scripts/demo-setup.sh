#!/usr/bin/env bash
# ╔══════════════════════════════════════════════════════════════════════╗
# ║  Forge 首次启动向导                                                   ║
# ║                                                                      ║
# ║  引导用户完成全部配置，生成 Demo 数据或连接自有数据库。                    ║
# ║  后续修改配置可用 forge config 命令。                                   ║
# ║                                                                      ║
# ║  用法：bash scripts/demo-setup.sh                                    ║
# ╚══════════════════════════════════════════════════════════════════════╝
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# ── 颜色 ──────────────────────────────────────────────────────────────
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'
CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

info()  { echo -e "${GREEN}  ✓${NC} $1"; }
warn()  { echo -e "${YELLOW}  !${NC} $1"; }
error() { echo -e "${RED}  ✗${NC} $1"; exit 1; }
ask()   { echo -en "${CYAN}  ?${NC} $1"; }

# ── 工具函数 ──────────────────────────────────────────────────────────
write_yaml() {
    # write_yaml key value — 更新 forge.yaml 中指定的顶层键值
    # 用 python 来安全地修改 YAML
    "$PYTHON" - "$1" "$2" << 'PYEOF'
import sys, yaml
from pathlib import Path
key_path, value = sys.argv[1], sys.argv[2]
p = Path("forge.yaml")
cfg = yaml.safe_load(p.read_text()) if p.exists() else {}
keys = key_path.split(".")
node = cfg
for k in keys[:-1]:
    node = node.setdefault(k, {})
node[keys[-1]] = value
p.write_text(yaml.dump(cfg, allow_unicode=True, sort_keys=False, default_flow_style=False))
PYEOF
}

write_env() {
    # write_env KEY VALUE — 更新 .env 中的键值对
    local key="$1" val="$2"
    if grep -q "^${key}=" .env 2>/dev/null; then
        sed -i.bak "s|^${key}=.*|${key}=${val}|" .env && rm -f .env.bak
    else
        echo "${key}=${val}" >> .env
    fi
}

# ══════════════════════════════════════════════════════════════════════
echo ""
echo -e "${BOLD}╔══════════════════════════════════════╗${NC}"
echo -e "${BOLD}║       Forge 首次启动向导             ║${NC}"
echo -e "${BOLD}╚══════════════════════════════════════╝${NC}"
echo ""

# ── 0. 检查 Python ───────────────────────────────────────────────────
PYTHON="${PYTHON:-python3}"
if ! command -v "$PYTHON" &>/dev/null; then
    error "找不到 $PYTHON，请安装 Python 3.11+"
fi
info "Python $("$PYTHON" -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")"

# ── 1. 安装依赖 ──────────────────────────────────────────────────────
if ! "$PYTHON" -c "import forge.compiler" 2>/dev/null; then
    warn "安装依赖中..."
    pip install -e . -q
fi
info "依赖就绪"

# ── 初始化配置文件 ────────────────────────────────────────────────────
[ ! -f ".env" ] && cp .env.example .env
[ ! -f "forge.yaml" ] && cp forge.yaml.example forge.yaml

# ══════════════════════════════════════════════════════════════════════
echo ""
echo -e "${BOLD}  [1/5] 大模型配置${NC}"
echo ""

echo "  支持的 LLM 提供商："
echo "    1) Anthropic Claude（推荐，原生 Structured Output）"
echo "    2) OpenAI / 兼容接口（DeepSeek、MiniMax、硅基流动、Ollama 等）"
echo ""
ask "选择 [1/2, 默认 1]: "
read -r LLM_CHOICE
LLM_CHOICE="${LLM_CHOICE:-1}"

if [ "$LLM_CHOICE" = "2" ]; then
    write_env "LLM_PROVIDER" "openai"
    write_yaml "llm.provider" "openai"

    ask "API Base URL（如 https://api.deepseek.com/v1）: "
    read -r LLM_URL
    if [ -n "$LLM_URL" ]; then
        write_env "LLM_BASE_URL" "$LLM_URL"
        write_yaml "llm.base_url" "$LLM_URL"
    fi

    ask "模型名称（如 deepseek-chat）: "
    read -r LLM_MODEL
    if [ -n "$LLM_MODEL" ]; then
        write_env "LLM_MODEL" "$LLM_MODEL"
        write_yaml "llm.model" "$LLM_MODEL"
    fi
else
    write_env "LLM_PROVIDER" "anthropic"
    write_yaml "llm.provider" "anthropic"
    write_env "LLM_MODEL" "claude-sonnet-4-6"
    write_yaml "llm.model" "claude-sonnet-4-6"
fi

ask "LLM API Key: "
read -r LLM_KEY
if [ -n "$LLM_KEY" ]; then
    write_env "LLM_API_KEY" "$LLM_KEY"
    # API Key 只写 .env，不写 forge.yaml（安全）
fi
info "大模型配置完成"

# ══════════════════════════════════════════════════════════════════════
echo ""
echo -e "${BOLD}  [2/5] 向量模型配置（用于 Schema RAG 召回）${NC}"
echo ""
echo "  大表场景下（>10 张表），Forge 用向量检索只注入相关表到 prompt，"
echo "  避免 context window 溢出。小表场景可跳过。"
echo ""
echo "  推荐方案："
echo "    1) 硅基流动 BAAI/bge-m3（免费，中文语义优，默认）"
echo "    2) OpenAI text-embedding-3-small"
echo "    3) 其他 OpenAI 兼容接口"
echo "    4) 跳过（小 schema 不需要，自动用 BM25 降级）"
echo ""
ask "选择 [1/2/3/4, 默认 1]: "
read -r EMBED_CHOICE
EMBED_CHOICE="${EMBED_CHOICE:-1}"

case "$EMBED_CHOICE" in
    1)
        write_env "EMBED_BASE_URL" "https://api.siliconflow.cn/v1"
        write_env "EMBED_MODEL" "BAAI/bge-m3"
        write_yaml "embedding.base_url" "https://api.siliconflow.cn/v1"
        write_yaml "embedding.model" "BAAI/bge-m3"
        ask "硅基流动 API Key（https://siliconflow.cn 免费注册）: "
        read -r EMBED_KEY
        [ -n "$EMBED_KEY" ] && write_env "EMBED_API_KEY" "$EMBED_KEY"
        ;;
    2)
        write_env "EMBED_BASE_URL" "https://api.openai.com/v1"
        write_env "EMBED_MODEL" "text-embedding-3-small"
        write_yaml "embedding.base_url" "https://api.openai.com/v1"
        write_yaml "embedding.model" "text-embedding-3-small"
        ask "OpenAI API Key: "
        read -r EMBED_KEY
        [ -n "$EMBED_KEY" ] && write_env "EMBED_API_KEY" "$EMBED_KEY"
        ;;
    3)
        ask "Embedding API Base URL: "
        read -r EMBED_URL
        [ -n "$EMBED_URL" ] && write_env "EMBED_BASE_URL" "$EMBED_URL" && write_yaml "embedding.base_url" "$EMBED_URL"
        ask "模型名称: "
        read -r EMBED_MDL
        [ -n "$EMBED_MDL" ] && write_env "EMBED_MODEL" "$EMBED_MDL" && write_yaml "embedding.model" "$EMBED_MDL"
        ask "API Key: "
        read -r EMBED_KEY
        [ -n "$EMBED_KEY" ] && write_env "EMBED_API_KEY" "$EMBED_KEY"
        ;;
    4)
        info "跳过向量模型，将使用 BM25 关键词检索"
        ;;
esac
info "向量模型配置完成"

# ══════════════════════════════════════════════════════════════════════
echo ""
echo -e "${BOLD}  [3/5] 数据库连接${NC}"
echo ""
echo "    1) 使用内置 Demo 数据库（200 表电商数仓，体验 Forge）"
echo "    2) 连接自己的数据库"
echo ""
ask "选择 [1/2, 默认 1]: "
read -r DB_CHOICE
DB_CHOICE="${DB_CHOICE:-1}"

if [ "$DB_CHOICE" = "2" ]; then
    echo ""
    echo "  连接字符串格式："
    echo "    PostgreSQL: postgresql://user:pass@host:5432/dbname"
    echo "    MySQL:      mysql+pymysql://user:pass@host:3306/dbname"
    echo "    SQLite:     sqlite:///path/to/db.sqlite"
    echo ""
    ask "数据库连接字符串: "
    read -r DB_URL
    write_env "DATABASE_URL" "$DB_URL"
    write_yaml "database.url" "$DB_URL"

    echo ""
    echo "  SQL 方言（用于编译器语法适配）："
    echo "    auto       — 从连接字符串自动推断（默认）"
    echo "    sqlite / mysql / postgresql / bigquery / snowflake"
    echo ""
    ask "SQL 方言 [auto]: "
    read -r DIALECT
    DIALECT="${DIALECT:-auto}"
    write_env "SQL_DIALECT" "$DIALECT"
    write_yaml "database.dialect" "$DIALECT"

    info "数据库配置完成"
    echo ""
    warn "正在同步表结构到 Registry（结构层）..."

    # 为自有数据库创建独立的 registry 目录
    mkdir -p registry/data
    write_yaml "registry.schema_path" "registry/data/schema.registry.json"
    write_yaml "registry.metrics_path" "registry/data/metrics.registry.yaml"
    write_yaml "registry.disambiguations_path" "registry/data/disambiguations.registry.yaml"
    write_yaml "registry.conventions_path" "registry/data/field_conventions.registry.yaml"

    PYTHONPATH="$ROOT" "$PYTHON" -m forge.cli sync --db "$DB_URL" || warn "同步失败，请检查连接字符串"

    # 创建空的语义层文件
    [ ! -f "registry/data/metrics.registry.yaml" ] && echo "# 指标定义 — 参考 docs/registry.md 编写" > registry/data/metrics.registry.yaml
    [ ! -f "registry/data/disambiguations.registry.yaml" ] && echo "# 歧义消除规则 — 参考 docs/registry.md 编写" > registry/data/disambiguations.registry.yaml
    [ ! -f "registry/data/field_conventions.registry.yaml" ] && echo "# 字段使用约定 — 参考 docs/registry.md 编写" > registry/data/field_conventions.registry.yaml

    echo ""
    echo "  结构层（表和字段）已从数据库自动同步。"
    echo "  语义层（指标定义、歧义消除规则）需要你根据业务编写。"
    echo ""
    echo -e "  ${CYAN}详见 docs/registry.md — 构建你自己的语义库${NC}"
    echo ""
else
    # Demo 数据库
    DEMO_DB="tests/datasets/large/database.db"
    if [ ! -f "$DEMO_DB" ] || [ ! -s "$DEMO_DB" ]; then
        warn "正在生成 200 表 Demo 数据库..."
        "$PYTHON" demo/seed_large.py
        if [ -f "demo/large_demo.db" ] && [ -s "demo/large_demo.db" ]; then
            mkdir -p tests/datasets/large
            cp demo/large_demo.db "$DEMO_DB"
        fi
    fi

    DEMO_FULL="$ROOT/$DEMO_DB"
    write_env "DATABASE_URL" "sqlite:///$DEMO_FULL"
    write_yaml "database.url" "sqlite:///$DEMO_FULL"
    write_env "SQL_DIALECT" "sqlite"
    write_yaml "database.dialect" "sqlite"

    # Demo 自带完整的结构层 + 语义层
    write_yaml "registry.schema_path" "tests/datasets/large/schema.registry.json"
    write_yaml "registry.metrics_path" "tests/datasets/large/metrics.registry.yaml"
    write_yaml "registry.disambiguations_path" "tests/datasets/large/disambiguations.registry.yaml"
    write_yaml "registry.conventions_path" "tests/datasets/large/field_conventions.registry.yaml"

    TABLE_COUNT=$("$PYTHON" -c "import json; print(len(json.load(open('$DEMO_DB'.replace('database.db','schema.registry.json'))).get('tables',{})))" 2>/dev/null || echo "?")
    info "Demo 数据库就绪（${TABLE_COUNT} 张表，含完整语义库）"
fi

# ══════════════════════════════════════════════════════════════════════
echo ""
echo -e "${BOLD}  [4/5] 反馈机制（语义库自动维护）${NC}"
echo ""
echo "  开启后，Forge 会："
echo "    • 缓存已确认的查询，相似问题直接复用"
echo "    • 记录用户对歧义问题的澄清（如「复购率的定义」）"
echo "    • 自动将澄清记录合并入语义库，越用越准确"
echo ""
ask "开启反馈机制？[Y/n, 默认 Y]: "
read -r FB_CHOICE
FB_CHOICE="${FB_CHOICE:-Y}"

if [[ "$FB_CHOICE" =~ ^[Nn] ]]; then
    write_env "FEEDBACK_ENABLED" "false"
    write_yaml "feedback.enabled" "false"
    info "反馈机制已关闭（纯查询模式）"
else
    write_env "FEEDBACK_ENABLED" "true"
    write_yaml "feedback.enabled" "true"
    info "反馈机制已开启"
fi

# ══════════════════════════════════════════════════════════════════════
echo ""
echo -e "${BOLD}  [5/5] 飞书 Bot（可选）${NC}"
echo ""
ask "配置飞书 Bot？[y/N, 默认 N]: "
read -r FS_CHOICE
FS_CHOICE="${FS_CHOICE:-N}"

if [[ "$FS_CHOICE" =~ ^[Yy] ]]; then
    ask "飞书 App ID（cli_xxxxxxxxxx）: "
    read -r FS_ID
    [ -n "$FS_ID" ] && write_env "FEISHU_APP_ID" "$FS_ID" && write_yaml "feishu.app_id" "$FS_ID"

    ask "飞书 App Secret: "
    read -r FS_SECRET
    [ -n "$FS_SECRET" ] && write_env "FEISHU_APP_SECRET" "$FS_SECRET" && write_yaml "feishu.app_secret" "$FS_SECRET"

    info "飞书 Bot 配置完成"
else
    info "跳过飞书配置"
fi

# ══════════════════════════════════════════════════════════════════════
echo ""
warn "运行编译器自检..."
if "$PYTHON" -m pytest tests/test_compiler.py -q --tb=line 2>/dev/null; then
    info "编译器自检通过"
else
    warn "部分测试未通过（不影响使用）"
fi

# ── 完成 ─────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}═══════════════════════════════════════${NC}"
echo -e "  ${GREEN}配置完成！${NC}"
echo -e "${BOLD}═══════════════════════════════════════${NC}"
echo ""
echo "  配置文件："
echo "    .env        — API 密钥等敏感信息（不提交 git）"
echo "    forge.yaml  — 所有其他配置（可提交 git）"
echo ""
echo "  启动命令："
echo ""
echo "    # 飞书 Bot"
echo "    PYTHONPATH=. python3 web/feishu.py"
echo ""
echo "    # Web API"
echo "    uvicorn main:app --host 0.0.0.0 --port 8000"
echo ""
echo "  后续修改配置："
echo ""
echo "    forge config                    — 查看当前配置"
echo "    forge config llm.model gpt-4o   — 修改配置项"
echo "    forge sync --db <url>           — 切换数据库并同步 schema"
echo "    forge sync-staging              — 将用户反馈合并入语义库"
echo ""
