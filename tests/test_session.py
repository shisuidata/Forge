"""
Tests for agent.session — Session and SessionStore state management.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from agent.session import Message, Session, SessionStore


# ── Message ───────────────────────────────────────────────────────────────────

def test_message_fields():
    m = Message(role="user", content="hello")
    assert m.role == "user"
    assert m.content == "hello"


# ── Session.add / recent ──────────────────────────────────────────────────────

def test_session_add_and_recent():
    s = Session(user_id="u1")
    s.add("user", "query 1")
    s.add("assistant", "answer 1")
    assert len(s.history) == 2
    recent = s.recent(2)
    assert recent[0].role == "user"
    assert recent[1].role == "assistant"


def test_session_recent_returns_last_n():
    s = Session(user_id="u2")
    for i in range(5):
        s.add("user", f"msg {i}")
    last = s.recent(3)
    assert len(last) == 3
    assert last[-1].content == "msg 4"


def test_session_recent_n_larger_than_history():
    s = Session(user_id="u3")
    s.add("user", "only one")
    assert len(s.recent(10)) == 1


def test_session_history_truncated_at_20():
    s = Session(user_id="u4")
    for i in range(25):
        s.add("user", f"turn {i}")
    assert len(s.history) == 20
    # oldest messages dropped; last message preserved
    assert s.history[-1].content == "turn 24"
    assert s.history[0].content == "turn 5"


def test_session_history_exactly_20_not_truncated():
    s = Session(user_id="u5")
    for i in range(20):
        s.add("assistant", f"reply {i}")
    assert len(s.history) == 20


# ── Session pending_sql / pending_forge ───────────────────────────────────────

def test_session_pending_sql_defaults_none():
    s = Session(user_id="u6")
    assert s.pending_sql is None
    assert s.pending_forge is None


def test_session_pending_sql_set_and_clear():
    s = Session(user_id="u7")
    s.pending_sql = "SELECT 1"
    s.pending_forge = {"scan": "orders", "select": ["orders.id"]}
    assert s.pending_sql == "SELECT 1"
    assert s.pending_forge is not None

    s.pending_sql = None
    s.pending_forge = None
    assert s.pending_sql is None
    assert s.pending_forge is None


# ── SessionStore ──────────────────────────────────────────────────────────────

def test_store_get_creates_new_session():
    store = SessionStore()
    s = store.get("alice")
    assert isinstance(s, Session)
    assert s.user_id == "alice"


def test_store_get_returns_same_session():
    store = SessionStore()
    s1 = store.get("bob")
    s1.add("user", "hi")
    s2 = store.get("bob")
    assert s2 is s1
    assert len(s2.history) == 1


def test_store_different_users_isolated():
    store = SessionStore()
    store.get("alice").add("user", "alice msg")
    store.get("bob").add("user", "bob msg")
    assert store.get("alice").history[0].content == "alice msg"
    assert store.get("bob").history[0].content == "bob msg"


def test_store_clear_removes_session():
    store = SessionStore()
    store.get("carol").add("user", "hello")
    store.clear("carol")
    # get after clear returns fresh session
    fresh = store.get("carol")
    assert len(fresh.history) == 0


def test_store_clear_nonexistent_no_error():
    store = SessionStore()
    store.clear("nobody")  # must not raise


def test_store_clear_only_removes_target_user():
    store = SessionStore()
    store.get("alice").add("user", "stay")
    store.get("bob").add("user", "go")
    store.clear("bob")
    assert len(store.get("alice").history) == 1
    assert len(store.get("bob").history) == 0


# ── Session preserves role ordering ───────────────────────────────────────────

def test_session_role_sequence_preserved():
    s = Session(user_id="u8")
    roles = ["user", "assistant", "user", "assistant"]
    for r in roles:
        s.add(r, f"{r} turn")
    for i, msg in enumerate(s.history):
        assert msg.role == roles[i]
