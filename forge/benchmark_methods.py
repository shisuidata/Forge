"""Production-safe model calls and result comparison for SQL benchmarks."""
from __future__ import annotations

import json
import math
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import yaml

from forge.compiler import compile_query
from forge.lint import lint_conventions


class BenchmarkProviderError(RuntimeError):
    pass


@dataclass(frozen=True)
class BenchmarkMethod:
    id: str
    label: str
    model: str
    base_url: str
    registry_context: str


def build_registry_context(dataset_dir: Path) -> str:
    lines: list[str] = []
    schema_path = dataset_dir / "schema_context.md"
    if schema_path.exists():
        lines.append(schema_path.read_text(encoding="utf-8").strip())

    metrics_path = dataset_dir / "metrics.registry.yaml"
    if metrics_path.exists():
        metrics = yaml.safe_load(metrics_path.read_text(encoding="utf-8")) or {}
        derivatives = {
            key: value
            for key, value in metrics.items()
            if isinstance(value, dict) and value.get("metric_class") == "derivative"
        }
        if derivatives:
            metric_lines = ["## 衍生指标定义（需要多步计算）"]
            for name, metric in derivatives.items():
                metric_lines.append(
                    f"- {name}（{metric.get('label', name)}）= "
                    f"{metric.get('numerator', '')} / {metric.get('denominator', '')}"
                )
                if metric.get("description"):
                    metric_lines.append(f"  含义：{metric['description']}")
                if metric.get("notes"):
                    note = str(metric["notes"]).strip().replace("\n", " | ")
                    metric_lines.append(f"  注意：{note}")
            lines.append("\n".join(metric_lines))

    conventions_path = dataset_dir / "field_conventions.registry.yaml"
    if conventions_path.exists():
        conventions = yaml.safe_load(conventions_path.read_text(encoding="utf-8")) or {}
        convention_lines = ["## 字段使用约定（必须遵守）"]
        for key, rule in conventions.items():
            if not isinstance(rule, dict) or not rule.get("convention"):
                continue
            convention_lines.append(f"【{rule.get('label', key)}】")
            convention_lines.extend(
                f"  {line.strip()}"
                for line in str(rule["convention"]).splitlines()
                if line.strip()
            )
        if len(convention_lines) > 1:
            lines.append("\n".join(convention_lines))
    return "\n\n".join(lines)


def ark_coding_plan_method(root: Path) -> BenchmarkMethod:
    dataset = root / "tests" / "datasets" / "large"
    return BenchmarkMethod(
        id="ai",
        label="Method AI（Ark Coding Plan，Forge JSON + deterministic compiler）",
        model="ark-code-latest",
        base_url="https://ark.cn-beijing.volces.com/api/coding/v3",
        registry_context=build_registry_context(dataset),
    )


def _clean_json(raw: str) -> str:
    text = raw.strip()
    if text.startswith("```"):
        lines = text.splitlines()[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines).strip()
    return text


def call_openai_text(
    api_key: str,
    base_url: str,
    system: str,
    model: str,
    *,
    question: str | None = None,
    messages: list[dict[str, str]] | None = None,
    max_tokens: int = 8192,
    timeout_seconds: float = 120,
    retries: int = 5,
) -> str:
    if not api_key:
        raise BenchmarkProviderError("Coding Plan credential unavailable")
    if messages is None:
        messages = [{"role": "user", "content": str(question or "")}]
    payload = {
        "model": model,
        "max_tokens": max_tokens,
        "messages": [{"role": "system", "content": system}, *messages],
    }
    endpoint = base_url.rstrip("/") + "/chat/completions"
    with httpx.Client(timeout=timeout_seconds) as client:
        for attempt in range(retries):
            try:
                response = client.post(
                    endpoint,
                    headers={"Authorization": f"Bearer {api_key}"},
                    json=payload,
                )
                if response.status_code in {401, 402, 403}:
                    raise BenchmarkProviderError("Coding Plan authentication rejected")
                if response.status_code == 429 or response.status_code >= 500:
                    if attempt < retries - 1:
                        time.sleep(min(40.0, 5.0 * (2 ** attempt)))
                        continue
                response.raise_for_status()
                body = response.json()
                return str(body["choices"][0]["message"]["content"]).strip()
            except BenchmarkProviderError:
                raise
            except (httpx.HTTPError, KeyError, TypeError, ValueError) as exc:
                if attempt < retries - 1:
                    time.sleep(min(40.0, 5.0 * (2 ** attempt)))
                    continue
                raise BenchmarkProviderError("Coding Plan request failed") from exc
    raise BenchmarkProviderError("Coding Plan request exhausted retries")


def run_forge_oai(
    api_key: str,
    base_url: str,
    question: str,
    system: str,
    model: str,
    *,
    max_compile_retries: int = 0,
    timeout_seconds: float = 120,
) -> dict[str, Any]:
    messages = [{"role": "user", "content": question}]
    for attempt in range(1 + max_compile_retries):
        raw = _clean_json(
            call_openai_text(
                api_key,
                base_url,
                system,
                model,
                messages=messages,
                timeout_seconds=timeout_seconds,
            )
        )
        try:
            forge_json = json.loads(raw)
        except json.JSONDecodeError:
            if attempt < max_compile_retries:
                messages.extend(
                    [
                        {"role": "assistant", "content": raw},
                        {
                            "role": "user",
                            "content": "JSON parsing failed. Return one complete valid Forge JSON object only.",
                        },
                    ]
                )
                continue
            return {"forge_json": None, "sql": None, "error_code": "invalid_json", "attempts": attempt + 1}

        warnings = lint_conventions(forge_json, question) if max_compile_retries > 0 else []
        if warnings:
            if attempt < max_compile_retries:
                messages.extend(
                    [
                        {"role": "assistant", "content": raw},
                        {
                            "role": "user",
                            "content": "Convention validation failed:\n"
                            + "\n".join(f"- {warning}" for warning in warnings)
                            + "\nReturn corrected Forge JSON only.",
                        },
                    ]
                )
                continue
            return {"forge_json": forge_json, "sql": None, "error_code": "convention_failed", "attempts": attempt + 1}
        try:
            return {
                "forge_json": forge_json,
                "sql": compile_query(forge_json),
                "error_code": None,
                "attempts": attempt + 1,
            }
        except Exception:
            if attempt < max_compile_retries:
                messages.extend(
                    [
                        {"role": "assistant", "content": raw},
                        {
                            "role": "user",
                            "content": "Forge deterministic compilation failed. Correct the query and return Forge JSON only.",
                        },
                    ]
                )
                continue
            return {"forge_json": forge_json, "sql": None, "error_code": "compile_failed", "attempts": attempt + 1}
    return {"forge_json": None, "sql": None, "error_code": "retry_exhausted", "attempts": 1 + max_compile_retries}


def run_sql_oai(
    api_key: str,
    base_url: str,
    question: str,
    system: str,
    model: str,
    *,
    timeout_seconds: float = 120,
) -> dict[str, Any]:
    return {
        "sql": call_openai_text(
            api_key,
            base_url,
            system,
            model,
            question=question,
            timeout_seconds=timeout_seconds,
        ),
        "error_code": None,
    }


def _numeric_approx(left: Any, right: Any) -> bool:
    try:
        a, b = float(left), float(right)
    except (TypeError, ValueError):
        return str(left).strip().lower() == str(right).strip().lower()
    return math.isclose(a, b, rel_tol=0.001, abs_tol=0.005)


def compare_result_rows(
    reference_rows: list[tuple[Any, ...]], generated_rows: list[tuple[Any, ...]]
) -> bool:
    if len(reference_rows) != len(generated_rows):
        return False
    reference = sorted(reference_rows, key=lambda row: tuple(str(value) for value in row))
    generated = sorted(generated_rows, key=lambda row: tuple(str(value) for value in row))
    for expected, actual in zip(reference, generated):
        if len(expected) != len(actual):
            return False
        if not all(_numeric_approx(left, right) for left, right in zip(expected, actual)):
            return False
    return True


def bird_execution_accuracy(
    gold_rows: list[tuple[Any, ...]], predicted_rows: list[tuple[Any, ...]]
) -> bool:
    """Match BIRD's official execution-accuracy verdict exactly.

    BIRD compares the exact sets of SQLite result tuples. Row order and duplicate
    multiplicity are ignored; tuple value and column order remain exact.
    """
    return set(gold_rows) == set(predicted_rows)
