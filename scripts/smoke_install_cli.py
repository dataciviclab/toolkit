from __future__ import annotations

import os
import subprocess
import sys
import tempfile
from pathlib import Path


def _run(cmd: list[str], *, cwd: Path, env: dict[str, str]) -> None:
    printable = " ".join(cmd)
    print(f"$ {printable}")
    subprocess.run(cmd, cwd=cwd, env=env, check=True)


def _venv_paths(venv_dir: Path) -> tuple[Path, Path]:
    if os.name == "nt":
        bindir = venv_dir / "Scripts"
        toolkit_name = "toolkit.exe"
    else:
        bindir = venv_dir / "bin"
        toolkit_name = "toolkit"
    return bindir / "python", bindir / toolkit_name


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]

    with tempfile.TemporaryDirectory(prefix="toolkit-install-smoke-") as tmp:
        tmpdir = Path(tmp)
        venv_dir = tmpdir / "venv"
        out_dir = tmpdir / "out"

        _run([sys.executable, "-m", "venv", str(venv_dir)], cwd=repo_root, env=os.environ.copy())

        venv_python, toolkit_cmd = _venv_paths(venv_dir)
        env = os.environ.copy()
        env["DCL_ROOT"] = str(out_dir)

        _run([str(venv_python), "-m", "pip", "install", "."], cwd=repo_root, env=env)

        if not toolkit_cmd.exists():
            raise FileNotFoundError(f"CLI entry point not found after install: {toolkit_cmd}")

        _run([str(toolkit_cmd), "--help"], cwd=repo_root, env=env)
        _run([str(toolkit_cmd), "run", "--help"], cwd=repo_root, env=env)
        _run([str(toolkit_cmd), "profile", "--help"], cwd=repo_root, env=env)
        _run(
            [str(toolkit_cmd), "run", "all", "--dry-run", "-c", "examples/dataset_min.yml"],
            cwd=repo_root,
            env=env,
        )

        run_dir = out_dir / "data" / "_runs" / "example_min" / "2024"
        run_records = sorted(run_dir.glob("*.json"))
        if not run_records:
            raise RuntimeError(f"Expected dry-run record under {run_dir}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
