import sys
import subprocess
from pathlib import Path

# Base directory of the scripts (this file's directory)
BASE_DIR = Path(__file__).resolve().parent

# Discover all target scripts ending with 'HashAll.py' in the folder
scripts = sorted([
    p for p in BASE_DIR.glob("*HashAll.py")
    if p.name != Path(__file__).name  # exclude this runner itself if named similarly
])

def run_script(script_path: Path) -> int:
    print(f"\n=== 시작: {script_path.name} ===")
    try:
        # Use current Python executable to run each script
        result = subprocess.run([
            sys.executable,
            str(script_path)
        ], cwd=str(BASE_DIR), capture_output=True, text=True)

        # Stream outputs
        if result.stdout:
            print(result.stdout.rstrip())
        if result.stderr:
            # Print stderr after stdout for clarity
            print(result.stderr.rstrip())

        code = result.returncode
        status = "성공" if code == 0 else f"실패 (코드 {code})"
        print(f"=== 종료: {script_path.name} → {status} ===")
        return code
    except Exception as e:
        print(f"오류: {script_path.name} 실행 중 예외 발생 → {e}")
        return 1


def main():
    if not scripts:
        print("실행할 'HashAll.py' 스크립트를 찾지 못했습니다.")
        return

    print("발견된 스크립트 순서:")
    for i, s in enumerate(scripts, 1):
        print(f"  {i}. {s.name}")

    failures = []
    for s in scripts:
        code = run_script(s)
        if code != 0:
            failures.append(s.name)

    print("\n요약 결과:")
    if failures:
        print(f"실패 {len(failures)}개: {', '.join(failures)}")
        sys.exit(1)
    else:
        print("전체 성공")
        sys.exit(0)


if __name__ == "__main__":
    main()
