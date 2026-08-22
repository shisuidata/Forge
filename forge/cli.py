"""
Forge 命令行入口。

子命令：
    forge compile <file.json>   将 Forge JSON 文件编译为 SQL 并打印到 stdout
    forge compile -             从 stdin 读取 Forge JSON 并编译
    forge sync                  连接数据库，将表结构同步到 schema.registry.json
    forge sync --db <url>       用指定的数据库 URL 覆盖 config 中的 DATABASE_URL
    forge sync --out <path>     将结构层写入指定 schema.registry.json 路径
    forge sync-staging          将 .forge/staging/ 中的歧义确认记录合并入 disambiguations.registry.yaml
    forge doctor                检查当前配置是否满足生产交付条件
    forge poc init <dir>        初始化客户 PoC 工作目录
    forge poc validate <dir>    检查客户 PoC 工作目录
    forge poc report <dir>      汇总客户 PoC 交付报告
    forge config                查看当前配置
    forge config <key> <value>  修改 forge.yaml 中的配置项

典型用法：
    # 编译 JSON 文件
    forge compile query.json

    # 管道调用（配合 LLM 生成的 JSON）
    echo '{"scan":"orders","select":["orders.id"]}' | forge compile -

    # 同步数据库 schema（使用 .env 中的 DATABASE_URL）
    forge sync

    # 指定数据库 URL
    forge sync --db postgresql://user:pass@localhost:5432/mydb

    # 指定输出路径（生产部署初始化 Registry 时常用）
    forge sync --db "$DATABASE_URL" --out registry/data/schema.registry.json

    # 将用户对话中积累的歧义确认记录合并入语义库
    forge sync-staging

    # 检查生产交付 readiness
    forge doctor

    # 查看当前配置
    forge config

    # 修改配置项
    forge config llm.model gpt-4o
    forge config database.url postgresql://user:pass@host/db
    forge config database.dialect postgresql
    forge config feedback.enabled false
"""

import argparse
import json
import sys
from pathlib import Path

from .compiler import compile_query


def _cmd_config(args: argparse.Namespace) -> None:
    """查看或修改配置。"""
    from pathlib import Path
    yaml_path = Path(__file__).parent.parent / "forge.yaml"

    if not args.key:
        # 无参数：展示当前配置
        from config import cfg

        def _mask(s: str) -> str:
            if not s or len(s) < 8:
                return s or "(未设置)"
            return s[:4] + "****" + s[-4:]

        print("Forge 当前配置")
        print("=" * 50)
        print()
        print("大模型 (LLM)")
        print(f"  provider:     {cfg.LLM_PROVIDER}")
        print(f"  model:        {cfg.LLM_MODEL}")
        print(f"  base_url:     {cfg.LLM_BASE_URL or '(默认)'}")
        print(f"  api_key:      {_mask(cfg.LLM_API_KEY)}")
        print()
        print("向量模型 (Embedding)")
        print(f"  base_url:     {cfg.EMBED_BASE_URL}")
        print(f"  model:        {cfg.EMBED_MODEL}")
        print(f"  api_key:      {_mask(cfg.EMBED_API_KEY)}")
        print(f"  top_k:        {cfg.RETRIEVAL_TOP_K}")
        print()
        print("数据库")
        print(f"  url:          {cfg.DATABASE_URL or '(未设置)'}")
        print(f"  dialect:      {cfg.SQL_DIALECT}")
        print()
        print("Registry")
        print(f"  schema:       {cfg.REGISTRY_PATH}")
        print(f"  metrics:      {cfg.METRICS_PATH}")
        print(f"  disambig:     {cfg.DISAMBIGUATIONS_PATH}")
        print()
        print("反馈机制")
        print(f"  enabled:      {cfg.FEEDBACK_ENABLED}")
        print()
        print("飞书 Bot")
        print(f"  app_id:       {cfg.FEISHU_APP_ID or '(未设置)'}")
        print()
        print(f"配置文件: {yaml_path}")
        print(f"环境变量: .env（优先级高于 forge.yaml）")
        return

    # 有参数：修改配置
    key = args.key
    value = args.value
    if value is None:
        # 单参数：只查看某个 key
        try:
            import yaml
            cfg_data = yaml.safe_load(yaml_path.read_text()) if yaml_path.exists() else {}
            keys = key.split(".")
            node = cfg_data
            for k in keys:
                if isinstance(node, dict):
                    node = node.get(k, "(未设置)")
                else:
                    node = "(未设置)"
                    break
            print(f"{key} = {node}")
        except Exception as exc:
            print(f"读取失败: {exc}", file=sys.stderr)
        return

    # key + value：写入 forge.yaml
    try:
        import yaml
        cfg_data = yaml.safe_load(yaml_path.read_text()) if yaml_path.exists() else {}
        keys = key.split(".")
        node = cfg_data
        for k in keys[:-1]:
            node = node.setdefault(k, {})
        node[keys[-1]] = value
        yaml_path.write_text(
            yaml.dump(cfg_data, allow_unicode=True, sort_keys=False, default_flow_style=False)
        )
        print(f"✓ {key} = {value}")
        print(f"  已写入 {yaml_path}")

        # 特殊处理：修改数据库连接后提示同步
        if key == "database.url":
            print()
            print("  提示：运行 forge sync 将新数据库的表结构同步到 Registry")
    except Exception as exc:
        print(f"写入失败: {exc}", file=sys.stderr)
        sys.exit(1)


def _print_doctor(payload: dict) -> None:
    print(f"Forge readiness ({payload['profile']}): {payload['status']}")
    print("=" * 50)
    for check in payload["checks"]:
        mark = {"ok": "OK", "warn": "WARN", "fail": "FAIL"}.get(check["status"], check["status"])
        print(f"[{mark:<4}] {check['name']}: {check['message']}")


def _cmd_poc(args: argparse.Namespace) -> None:
    from . import poc

    target = Path(args.directory)
    if args.poc_command == "init":
        result = poc.init_workspace(target)
    elif args.poc_command == "validate":
        result = poc.validate_workspace(target)
    elif args.poc_command == "report":
        result = poc.write_report(target, Path(args.out) if args.out else None)
    else:
        raise ValueError(f"unknown poc command: {args.poc_command}")

    if args.json_output:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    elif args.poc_command == "validate":
        print(f"PoC workspace: {result['status']} ({result['path']})")
        for issue in result["issues"]:
            print(f"[{issue['level']}] {issue['path']}: {issue['message']}")
    else:
        print(result.get("message") or f"PoC {args.poc_command}: {result['status']}")
        print(result["path"])

    if result.get("status") == "fail" or result.get("ok") is False:
        sys.exit(1)


def main() -> None:
    """CLI 入口函数，由 pyproject.toml 的 [project.scripts] 注册为 forge 命令。"""
    parser = argparse.ArgumentParser(prog="forge", description="Forge DSL compiler & config")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # ── compile 子命令 ────────────────────────────────────────────────────────
    compile_parser = subparsers.add_parser("compile", help="Compile Forge JSON to SQL")
    compile_parser.add_argument(
        "input",
        help="Forge JSON 文件路径，或 - 表示从 stdin 读取",
    )

    # ── sync 子命令 ───────────────────────────────────────────────────────────
    sync_parser = subparsers.add_parser("sync", help="Sync schema from database")
    sync_parser.add_argument(
        "--db",
        metavar="URL",
        default=None,
        help="数据库 URL，优先级高于 config.DATABASE_URL",
    )
    sync_parser.add_argument(
        "--out",
        metavar="PATH",
        default=None,
        help="schema.registry.json 输出路径，默认使用配置中的 REGISTRY_PATH",
    )
    sync_parser.add_argument(
        "--apply",
        action="store_true",
        help="明确应用同步结果；默认只输出 drift proposal，不修改 Registry",
    )

    # ── sync-staging 子命令 ───────────────────────────────────────────────────
    subparsers.add_parser(
        "sync-staging",
        help="将 .forge/staging/ 中的歧义确认记录合并入 disambiguations.registry.yaml",
    )

    # ── doctor 子命令 ───────────────────────────────────────────────────────
    doctor_parser = subparsers.add_parser("doctor", help="检查当前配置是否满足生产交付条件")
    doctor_parser.add_argument(
        "--profile",
        choices=("dev", "poc", "prod"),
        default="prod",
        help="检查口径：dev=开发态，poc=客户 PoC，prod=生产交付",
    )
    doctor_parser.add_argument(
        "--json",
        action="store_true",
        dest="json_output",
        help="输出结构化 JSON，供 smoke/report 自动汇总",
    )

    # ── poc 子命令 ────────────────────────────────────────────────────────────
    poc_parser = subparsers.add_parser("poc", help="客户 PoC 工作目录工具")
    poc_subparsers = poc_parser.add_subparsers(dest="poc_command", required=True)
    for command, help_text in (
        ("init", "初始化客户 PoC 工作目录"),
        ("validate", "检查客户 PoC 工作目录"),
        ("report", "汇总客户 PoC 交付报告"),
    ):
        sub = poc_subparsers.add_parser(command, help=help_text)
        sub.add_argument("directory", help="客户 PoC 工作目录")
        sub.add_argument("--json", action="store_true", dest="json_output", help="输出结构化 JSON")
        if command == "report":
            sub.add_argument("--out", default=None, help="交付报告输出路径，默认写入 <dir>/delivery_report.md")

    # ── config 子命令 ─────────────────────────────────────────────────────────
    config_parser = subparsers.add_parser(
        "config",
        help="查看或修改配置",
        description=(
            "无参数时展示当前配置；\n"
            "一个参数时查看指定 key；\n"
            "两个参数时设置 key = value。\n\n"
            "可配置项：\n"
            "  llm.provider       anthropic | openai\n"
            "  llm.model          模型名称\n"
            "  llm.base_url       OpenAI 兼容接口地址\n"
            "  database.url       数据库连接字符串\n"
            "  database.dialect   auto | sqlite | mysql | postgresql | bigquery | snowflake\n"
            "  embedding.base_url 向量模型 API 地址\n"
            "  embedding.model    向量模型名称\n"
            "  embedding.top_k    每次检索召回的表数量\n"
            "  feedback.enabled   true | false（是否开启语义库自动维护）\n"
            "  feishu.app_id      飞书 App ID\n"
            "  feishu.app_secret  飞书 App Secret"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    config_parser.add_argument("key", nargs="?", default=None, help="配置项（如 llm.model）")
    config_parser.add_argument("value", nargs="?", default=None, help="新值")

    args = parser.parse_args()

    # ── compile 处理 ─────────────────────────────────────────────────────────
    if args.command == "compile":
        if args.input == "-":
            forge = json.load(sys.stdin)
        else:
            with open(args.input) as f:
                forge = json.load(f)
        print(compile_query(forge))

    # ── sync 处理 ────────────────────────────────────────────────────────────
    elif args.command == "sync":
        from config import cfg
        from registry.sync import build_sync_proposal, run_sync

        database_url = args.db or cfg.DATABASE_URL
        if not database_url:
            print(
                "错误：未提供数据库 URL。请设置 DATABASE_URL 环境变量或使用 --db 参数。",
                file=sys.stderr,
            )
            sys.exit(1)

        registry_path = Path(args.out) if args.out else cfg.REGISTRY_PATH
        if args.apply:
            registry = run_sync(database_url, registry_path)
            table_count = len(registry.get("tables", {}))
            print(f"已同步 {table_count} 张表 → {registry_path}")
        else:
            proposal = build_sync_proposal(database_url, registry_path)
            print(json.dumps(proposal, ensure_ascii=False, indent=2))
            print("仅生成 drift proposal；Registry 未修改。使用 --apply 明确应用。", file=sys.stderr)

    # ── sync-staging 处理 ────────────────────────────────────────────────────
    elif args.command == "sync-staging":
        from config import cfg
        from registry.staging_sync import promote_staged

        staging_dir = cfg.STAGING_DIR
        if not staging_dir.exists():
            print(f"Staging 目录为空或不存在：{staging_dir}")
            sys.exit(0)

        pending = list(staging_dir.glob("*.json"))
        if not pending:
            print(f"没有待合并的记录（{staging_dir}）")
            sys.exit(0)

        stats = promote_staged(staging_dir, cfg.DISAMBIGUATIONS_PATH)
        print(
            f"合并完成 → {cfg.DISAMBIGUATIONS_PATH}\n"
            f"  新增: {stats['added']}  更新: {stats['updated']}  跳过: {stats['skipped']}"
        )

    # ── doctor 处理 ─────────────────────────────────────────────────────────
    elif args.command == "doctor":
        from .readiness import readiness_payload

        payload = readiness_payload(args.profile)
        if args.json_output:
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            _print_doctor(payload)
        if payload["status"] == "fail":
            sys.exit(1)

    # ── config 处理 ──────────────────────────────────────────────────────────
    elif args.command == "config":
        _cmd_config(args)

    # ── poc 处理 ─────────────────────────────────────────────────────────────
    elif args.command == "poc":
        _cmd_poc(args)


if __name__ == "__main__":
    main()
