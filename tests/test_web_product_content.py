from pathlib import Path


ROOT = Path(__file__).parents[1]
TEMPLATES = ROOT / "web" / "templates"
ARCHITECTURE_ATLAS = ROOT / "docs" / "architecture-diagrams" / "forge-platform-architecture.html"


def test_user_web_templates_do_not_contain_rejected_promotional_copy():
    rendered_sources = "\n".join([
        *(path.read_text() for path in sorted(TEMPLATES.glob("*.html"))),
        ARCHITECTURE_ATLAS.read_text(),
    ])
    rejected_phrases = (
        "Ask the data, not the dashboard.",
        "从一个业务问题开始",
        "让证据一路走到报告",
        "把任务推进过程放到台面上",
        "INTEGRATION SPIKE",
        "Pi Control Plane → Forge Execution Plane",
        "CANONICAL SCHEMA CONTROL PLANE",
        "AI SQL Agent",
        "可信运行时在线",
        "Forge 不是单一 SQL 生成器",
        "使用越多，组织能力越强",
    )
    for phrase in rejected_phrases:
        assert phrase not in rendered_sources


def test_chat_empty_state_contains_only_task_guidance_and_real_actions():
    chat = (TEMPLATES / "chat.html").read_text()
    assert "新建数据任务" in chat
    assert "查询 SQL 会在执行前等待批准" in chat
    assert all(label in chat for label in ("查询", "口径", "偏好"))


def test_tasks_and_registry_headers_describe_the_page_function():
    tasks = (TEMPLATES / "tasks.html").read_text()
    registry = (TEMPLATES / "registry_studio.html").read_text()
    assert "创建任务、查看跨渠道状态" in tasks
    assert "查看 Canonical Schema 的表格、DDL、ER 和 JSON 投影" in registry
