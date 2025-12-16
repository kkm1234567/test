import subprocess
import sys
from pathlib import Path


# Base directory where the individual exporter scripts live
BASE_DIR = Path(__file__).resolve().parent

# Scripts to run (기본개요 excluded)
SCRIPT_NAMES = [
    "부속지번_건축물_DB를_다른DB로_txt.py",
    "오수정화시설_건축물_DB를_다른DB로_txt.py",
    "전유공용면적_건축물_DB를_다른DB로_txt.py",
    "전유부_건축물_DB를_다른DB로_txt.py",
    "주택가격_건축물_DB를_다른DB로_txt.py",
    "지역지구구역_건축물_DB를_다른DB로_txt.py",
    "총괄표제_건축물_DB를_다른DB로_txt.py",
    "층별개요_건축물_DB를_다른DB로_txt.py",
    "표제부_건축물_DB를_다른DB로_txt.py",
]


def run_scripts():
    python_exe = sys.executable
    for name in SCRIPT_NAMES:
        script_path = BASE_DIR / name
        print(f"=== Running {script_path.name} ===")
        subprocess.run([python_exe, str(script_path)], check=True)
        print(f"=== Completed {script_path.name} ===\n")


if __name__ == "__main__":
    run_scripts()
