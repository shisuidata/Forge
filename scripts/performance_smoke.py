from __future__ import annotations

import argparse
import statistics
import time

from forge.compiler import compile_query
from forge.executor import validate_readonly_sql


QUERIES = [
    {
        "scan": "orders",
        "filter": [{"col": "orders.status", "op": "eq", "val": "completed"}],
        "group": ["orders.user_id"],
        "agg": [{"fn": "sum", "col": "orders.total_amount", "as": "gmv"}],
        "select": ["orders.user_id", "gmv"],
        "sort": [{"col": "gmv", "dir": "desc"}],
        "limit": 20,
    },
    {
        "scan": "orders",
        "joins": [
            {
                "type": "inner",
                "table": "users",
                "on": {"left": "orders.user_id", "right": "users.id"},
            }
        ],
        "filter": [{"col": "orders.created_at", "op": "gte", "val": {"$date": "2025-01-01"}}],
        "group": ["users.city"],
        "agg": [{"fn": "count_distinct", "col": "orders.id", "as": "order_count"}],
        "select": ["users.city", "order_count"],
    },
    {
        "cte": [
            {
                "name": "ranked_orders",
                "query": {
                    "scan": "orders",
                    "window": [
                        {
                            "fn": "row_number",
                            "partition": ["orders.user_id"],
                            "order": [{"col": "orders.created_at", "dir": "desc"}],
                            "as": "rn",
                        }
                    ],
                    "select": ["orders.id", "orders.user_id", "orders.total_amount", "rn"],
                },
            }
        ],
        "scan": "ranked_orders",
        "filter": [{"col": "rn", "op": "eq", "val": 1}],
        "select": ["id", "user_id", "total_amount"],
    },
]


def run_once(iterations: int) -> dict[str, float]:
    compile_latencies: list[float] = []
    validate_latencies: list[float] = []

    for i in range(iterations):
        query = QUERIES[i % len(QUERIES)]
        started = time.perf_counter()
        sql = compile_query(query)
        compile_latencies.append((time.perf_counter() - started) * 1000)

        started = time.perf_counter()
        validate_readonly_sql(sql)
        validate_latencies.append((time.perf_counter() - started) * 1000)

    return {
        "iterations": float(iterations),
        "compile_p50_ms": statistics.median(compile_latencies),
        "compile_p95_ms": statistics.quantiles(compile_latencies, n=20)[18],
        "validate_p50_ms": statistics.median(validate_latencies),
        "validate_p95_ms": statistics.quantiles(validate_latencies, n=20)[18],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Forge local performance smoke test")
    parser.add_argument("--iterations", type=int, default=1000)
    args = parser.parse_args()

    result = run_once(max(1, args.iterations))
    print(
        "iterations={iterations:.0f} "
        "compile_p50_ms={compile_p50_ms:.4f} "
        "compile_p95_ms={compile_p95_ms:.4f} "
        "validate_p50_ms={validate_p50_ms:.4f} "
        "validate_p95_ms={validate_p95_ms:.4f}".format(**result)
    )


if __name__ == "__main__":
    main()
