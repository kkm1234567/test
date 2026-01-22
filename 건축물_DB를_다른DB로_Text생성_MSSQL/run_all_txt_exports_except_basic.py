import subprocess
import sys
import time
from pathlib import Path

try:
    import psutil  # optional: for peak memory tracking
except Exception:  # pragma: no cover
    psutil = None


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


def run_with_mem(cmd):
    """Run a command and return peak RSS in MB if psutil available, else None."""
    if psutil is None:
        subprocess.run(cmd, check=True)
        return None

    proc = subprocess.Popen(cmd)
    p = psutil.Process(proc.pid)
    peak_rss = 0
    try:
        while True:
            if proc.poll() is not None:
                # one last read after exit
                try:
                    rss = p.memory_info().rss
                    peak_rss = max(peak_rss, rss)
                except Exception:
                    pass
                break
            try:
                rss = p.memory_info().rss
                peak_rss = max(peak_rss, rss)
            except Exception:
                pass
            time.sleep(0.2)
    finally:
        try:
            proc.wait()
        except Exception:
            pass
    return round(peak_rss / (1024 * 1024), 1) if peak_rss else None


def run_scripts():
    python_exe = sys.executable
    for name in SCRIPT_NAMES:
        script_path = BASE_DIR / name
        print(f"=== Running {script_path.name} ===")
        peak = run_with_mem([python_exe, str(script_path)])
        if peak is not None:
            print(f"Peak RSS: {peak} MB")
        elif psutil is None:
            print("(psutil not installed: skipping memory metrics)")
        print(f"=== Completed {script_path.name} ===\n")


if __name__ == "__main__":
    run_scripts()
