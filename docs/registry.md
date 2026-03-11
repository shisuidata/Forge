# Registry Management

The Registry (`schema.registry.json`) is Forge's knowledge base. It has two layers.

## Structural Layer — auto-generated

Sync from your database:

```bash
forge sync                              # uses DATABASE_URL from .env
forge sync --db postgresql://user:pass@host/db   # override
```

Output format:

```json
{
  "tables": {
    "users": {
      "columns": ["id", "name", "city", "created_at", "is_vip"]
    },
    "orders": {
      "columns": ["id", "user_id", "status", "total_amount", "created_at"]
    }
  }
}
```

Re-run `forge sync` whenever your database schema changes.

## Semantic Layer — defined through conversation

Business users define metrics by talking to the bot:

```
User:  复购率是指下过 2 次及以上订单的用户，除以所有有过下单记录的用户

Forge: 我的理解：
       分子 = 订单数 >= 2 的用户数
       分母 = 订单数 >= 1 的用户数
       是否正确？

User:  对

Forge: ✅ 已保存指标「repurchase_rate」
```

After saving, the metric can be referenced in queries:

```
User: 统计一下复购率
```

Saved format in registry:

```json
{
  "tables": { ... },
  "metrics": {
    "repurchase_rate": {
      "description": "复购率，分子=订单数>=2的用户，分母=有过下单记录的用户"
    }
  }
}
```

## Admin UI

The admin web UI at `/admin/registry` provides:
- Read-only view of synced tables and columns
- Add / edit / delete metric definitions
- Changes take effect immediately (no restart needed)

## Version Control

Commit `schema.registry.json` to version control. This file is your team's shared data contract — changes should be reviewed like code changes.
