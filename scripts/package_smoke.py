"""Exercise an installed distribution outside the checkout, using synthetic data only."""
from importlib.resources import files
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import zipfile


def main():
    for archive in map(Path, sys.argv[1:]):
        if archive.suffix == ".whl":
            with zipfile.ZipFile(archive) as bundle:
                names = bundle.namelist()
        else:
            with tarfile.open(archive) as bundle:
                names = [name.partition("/")[2] for name in bundle.getnames()]
        for required in ("forge/schema.json", "forge/templates/poc/registry/schema.registry.json", "web/templates/product_base.html", "web/static/product/product-pages.js"):
            assert required in names, (archive.name, required)
        assert not any("/charts/" in name or name.endswith((".db", ".sqlite", "/forge.yaml", "/.env")) for name in names)

    import forge
    from forge.compiler import compile_query
    assert "SELECT" in compile_query({"scan": "numbers", "select": ["numbers.n"]})
    for package, name in (("forge", "schema.json"), ("web", "templates/product_base.html"), ("web", "static/product/product-pages.js")):
        assert files(package).joinpath(name).is_file(), name
    environment = {key: os.environ[key] for key in ("PATH", "SYSTEMROOT", "LANG", "HOME") if key in os.environ}
    environment["FORGE_DISABLE_DOTENV"] = "true"
    with tempfile.TemporaryDirectory(prefix="forge-installed-smoke-") as temporary:
        root = Path(temporary)
        assert not Path(forge.__file__).resolve().is_relative_to(root)
        import_probe = """
import sqlite3, sqlalchemy, sys
from pathlib import Path
def forbid_runtime_reads(event, args):
    if event == "open" and isinstance(args[0], (str, bytes)):
        path = Path(args[0].decode() if isinstance(args[0], bytes) else args[0])
        assert not set(path.parts) & {".forge", ".runtime", ".env", "forge.yaml"}, "import inspected private runtime data"
sys.addaudithook(forbid_runtime_reads)
attempts = []
def forbidden(*args, **kwargs):
    attempts.append(True)
    raise AssertionError("import attempted a database connection")
sqlite3.connect = forbidden
sqlalchemy.create_engine = forbidden
import main
assert not attempts, "Web import initialized storage"
from agent.memory import memory
assert not memory.__dict__, "Web import initialized legacy memory"
print("import lifecycle passed")
"""
        subprocess.run([sys.executable, "-c", import_probe], cwd=root, env=environment, check=True, timeout=30)
        core_probe = """
import sys
for name in ("web", "agent.agent", "agent.memory"):
    sys.modules[name] = None
from forge.dialects import resolve_dialect
from forge.evaluate import evaluate_query_candidate
from forge.benchmark_service import CandidateEvaluation
assert resolve_dialect("auto", database_url="postgresql+psycopg2://synthetic.invalid/db").resolved == "postgresql"
result = evaluate_query_candidate({"candidate": {"kind": "forge_json", "forge_json": {"scan": "numbers", "select": ["numbers.n"]}}, "question": "List numbers"})
assert result["failure"]["code"] == "dialect_required"
print("core dependency boundary passed")
"""
        subprocess.run([sys.executable, "-c", core_probe], cwd=root, env=environment, check=True, timeout=30)
        cli = str(Path(sys.executable).parent / "forge")
        def run(*args, input=None):
            return subprocess.run([cli, *args], cwd=root, env=environment, input=input, text=True,
                                  check=True, capture_output=True, timeout=90)
        run("--help")
        compiled = run("compile", "-", input=json.dumps({"scan": "numbers", "select": ["numbers.n"]}))
        assert "SELECT" in compiled.stdout
        initialized = json.loads(run("poc", "init", "synthetic-poc", "--json").stdout)
        validated = json.loads(run("poc", "validate", "synthetic-poc", "--json").stdout)
        assert initialized["ok"] is True and validated["status"] == "ok", (initialized, validated)
        result = json.loads(run("quickstart", "--yes", "--json").stdout)
        assert result["status"] == "passed", result
        assert result["enforce"]["status"] == "completed", result
        assert result["explain"]["integrity"] == "verified", result
        print(json.dumps({"distribution": str(forge.__file__), "status": result["status"],
                          "poc": validated["status"], "enforce": result["enforce"], "explain": result["explain"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
