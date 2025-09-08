#!/usr/bin/env python3

from paver.easy import task, sh, needs
from pathlib import Path
import shutil
import sys

ROOT = Path(__file__).resolve().parent
PY = sys.executable or "python3"
CLEANER = ROOT / "scripts" / "clean_powerball.py"
RAW_DIR = ROOT / "data" / "raw"
PROC_DIR = ROOT / "data" / "processed"

@task
def setup():
    sh(f'{PY} -m pip install -U -r "{ROOT / "requirements.txt"}"')

@task
def process():
    if not CLEANER.exists():
        raise SystemExit(f"Cleaner not found: {CLEANER}")

    PROC_DIR.mkdir(parents=True, exist_ok=True)
    raws = sorted(RAW_DIR.glob("*.csv"))
    if not raws:
        raise SystemExit(f"No CSVs found in {RAW_DIR}. Put your raw files there.")

    for csv in raws:
        outdir = PROC_DIR / csv.stem
        outdir.mkdir(parents=True, exist_ok=True)
        print(f"→ Processing {csv.name} → {outdir.relative_to(ROOT)}")
        sh(f'{PY} "{CLEANER}" --in "{csv}" --outdir "{outdir}"')

@task
def clean():
    if PROC_DIR.exists():
        print(f"Removing {PROC_DIR} ...")
        shutil.rmtree(PROC_DIR, ignore_errors=True)

    # pycache cleanup (optional)
    for p in ROOT.rglob("__pycache__"):
        shutil.rmtree(p, ignore_errors=True)

    for p in ROOT.rglob("*.pyc"):
        try: p.unlink()
        except: pass

    print("Cleaning complete")
