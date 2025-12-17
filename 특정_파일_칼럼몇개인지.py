import os

path = r"C:\PTR\Prime\Collect\CollectApi\storage\hub_go_kr\tCollectorBuildingTitle\202009\org.txt"
delimiter = "|"
bad = []

# 첫 줄의 컬럼 수를 기준으로 expected를 결정
with open(path, "r", encoding="utf-8", errors="ignore") as f:
    first_line = f.readline()
    if not first_line:
        print("입력 파일이 비어 있습니다.")
        expected = 0
    else:
        expected = len(first_line.rstrip("\n").split(delimiter))

    # 나머지 줄 검사 (첫 줄도 포함하여 검사해도 되지만, 기준이 되는 줄은 통상 제외)
    for lineno, line in enumerate(f, 2):
        cols = line.rstrip("\n").split(delimiter)
        if len(cols) != expected:
            bad.append((lineno, len(cols), line.strip()))

print(f"기준 컬럼 수: {expected}")
print(f"총 {len(bad)}개 라인에서 컬럼 수 불일치")
for lineno, cnt, text in bad[:20]:  # 너무 많으면 상위 20개만 출력
    print(f"{lineno}: {cnt} cols -> {text}")

# 필요하면 파일로 저장:
# with open("bad_lines.txt", "w", encoding="utf-8") as out:
#     for lineno, cnt, text in bad:
#         out.write(f"{lineno}\t{cnt}\t{text}\n")