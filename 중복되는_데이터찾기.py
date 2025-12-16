import os

# 검색 대상 PK
TARGET_PK = "1025145634"

# temp_cur 폴더 경로
BASE_DIR = r"C:\PTR\Prime\Collect\CollectApi\storage\hub_go_kr\tCollectorBuildingUseArea\temp_cur"

# 출력 파일
OUTPUT_PATH = os.path.join(BASE_DIR, "output.txt")

# 결과 모음
matched_lines = []

print("=== PK 검색 시작 ===")

# temp_cur 폴더의 모든 split_*.txt 탐색
for filename in os.listdir(BASE_DIR):
    if not filename.startswith("split_") or not filename.endswith(".txt"):
        continue

    file_path = os.path.join(BASE_DIR, filename)

    print(f"→ 검사 중: {filename}")

    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            parts = line.rstrip("\n").split("|")
            if len(parts) == 0:
                continue

            if parts[0] == TARGET_PK:
                matched_lines.append(line)

# output.txt에 저장
with open(OUTPUT_PATH, "w", encoding="utf-8") as out:
    out.writelines(matched_lines)

print("\n=== 완료 ===")
print(f"총 {len(matched_lines)}개의 라인이 발견되어 저장됨 → {OUTPUT_PATH}")
