"""
tests/test_staging_sync.py — registry/staging_sync.py 单元测试

覆盖：
    write_staging_record() 写文件结构
    promote_staged() 基本合并
    promote_staged() 幂等性（confirmed_by_users=true 不覆盖）
    promote_staged() 更新（confirmed_by_users=false 可覆盖）
    promote_staged() 损坏文件跳过
    promote_staged() done 目录归档
"""
import json
import shutil
import tempfile
from pathlib import Path

import pytest
import yaml

from registry.staging_sync import write_staging_record, promote_staged


@pytest.fixture
def tmp_dirs():
    with tempfile.TemporaryDirectory() as tmp:
        staging = Path(tmp) / "staging"
        dis_path = Path(tmp) / "disambiguations.registry.yaml"
        yield staging, dis_path


def _write(staging, key="k1", label="L1", triggers=None, context="ctx", **kwargs):
    return write_staging_record(
        staging_dir=staging,
        key=key,
        label=label,
        triggers=triggers or ["词1"],
        context=context,
        original_question="原始问题",
        clarification_prompt="请问？",
        user_response="回答",
        ambiguity_keys=[],
        **kwargs,
    )


# ── write_staging_record ──────────────────────────────────────────────────────

def test_write_creates_json(tmp_dirs):
    staging, _ = tmp_dirs
    fp = _write(staging)
    assert fp.exists()
    data = json.loads(fp.read_text())
    assert data["key"] == "k1"
    assert data["confirmed_by_users"] is True
    assert "confirmed_at" in data


def test_write_filename_contains_key(tmp_dirs):
    staging, _ = tmp_dirs
    fp = _write(staging, key="my_revenue_rule")
    assert "my_revenue_rule" in fp.name


# ── promote_staged ────────────────────────────────────────────────────────────

def test_promote_adds_new_entry(tmp_dirs):
    staging, dis_path = tmp_dirs
    _write(staging, key="rev", label="营收", triggers=["营收"])
    stats = promote_staged(staging, dis_path)
    assert stats["added"] == 1
    assert stats["updated"] == 0

    content = yaml.safe_load(dis_path.read_text())
    assert "rev" in content
    assert content["rev"]["label"] == "营收"
    assert content["rev"]["confirmed_by_users"] is True


def test_promote_moves_to_done(tmp_dirs):
    staging, dis_path = tmp_dirs
    _write(staging, key="rev")
    promote_staged(staging, dis_path)
    # staging dir should have no .json pending
    assert list(staging.glob("*.json")) == []
    # done dir should have the file
    assert len(list((staging / "done").glob("*.json"))) == 1


def test_promote_idempotent_confirmed(tmp_dirs):
    """已 confirmed_by_users=true 的条目不被覆盖。"""
    staging, dis_path = tmp_dirs
    # 先写一条 confirmed
    existing = {"rev": {"label": "原始", "confirmed_by_users": True, "triggers": ["营收"]}}
    dis_path.write_text(yaml.dump(existing, allow_unicode=True))

    _write(staging, key="rev", label="新版本")
    stats = promote_staged(staging, dis_path)
    assert stats["skipped"] == 1

    content = yaml.safe_load(dis_path.read_text())
    assert content["rev"]["label"] == "原始"   # 未被覆盖


def test_promote_updates_unconfirmed(tmp_dirs):
    """confirmed_by_users=false 的已有条目会被用户确认版本覆盖。"""
    staging, dis_path = tmp_dirs
    existing = {"rev": {"label": "草稿版", "confirmed_by_users": False, "triggers": []}}
    dis_path.write_text(yaml.dump(existing, allow_unicode=True))

    _write(staging, key="rev", label="确认版")
    stats = promote_staged(staging, dis_path)
    assert stats["updated"] == 1

    content = yaml.safe_load(dis_path.read_text())
    assert content["rev"]["label"] == "确认版"
    assert content["rev"]["confirmed_by_users"] is True


def test_promote_skips_malformed(tmp_dirs):
    staging, dis_path = tmp_dirs
    staging.mkdir(parents=True, exist_ok=True)
    (staging / "bad.json").write_text("not json {{{")
    _write(staging, key="ok")
    stats = promote_staged(staging, dis_path)
    assert stats["added"] == 1   # only the valid one


def test_promote_empty_staging(tmp_dirs):
    staging, dis_path = tmp_dirs
    staging.mkdir(parents=True, exist_ok=True)
    stats = promote_staged(staging, dis_path)
    assert stats == {"added": 0, "updated": 0, "skipped": 0}
    assert not dis_path.exists()   # no file created when nothing to write


def test_promote_multiple(tmp_dirs):
    staging, dis_path = tmp_dirs
    for i in range(3):
        _write(staging, key=f"rule_{i}", label=f"Rule {i}")
    stats = promote_staged(staging, dis_path)
    assert stats["added"] == 3
    content = yaml.safe_load(dis_path.read_text())
    assert len(content) == 3
