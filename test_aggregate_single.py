import xxhash

# --------------------------
# 너의 normalize_key() 그대로 넣기
# --------------------------
def normalize_key(s: str) -> str:
    return s.replace(" ", "").replace("\t", "")

# --------------------------
# 테스트 파일
# --------------------------
INPUT_FILE = r"C:\PTR\Prime\Collect\CollectApi\storage\hub_go_kr\tCollectorBuildingUseArea\temp_cur\output.txt"

# --------------------------
# 설정 (너 로직 그대로)
# --------------------------
areaIndex = 37              # 실제 전유부 면적인 컬럼 index 넣어야 함
keyIndexes = [0]    # 너가 쓰는 key 조합 동일하게 넣기

# --------------------------
# 실행
# --------------------------
agg_map = {}

with open(INPUT_FILE, "r", encoding="utf-8", errors="ignore") as f:
    for raw_line in f:
        raw_line = raw_line.strip()
        if not raw_line:
            continue

        parts = raw_line.split("|")
        group_key = normalize_key(raw_line)

        try:
            area_val = float(parts[areaIndex]) if parts[areaIndex] else 0.0
        except:
            area_val = 0.0

        if group_key not in agg_map:
            agg_map[group_key] = [parts.copy(), area_val]
        else:
            agg_map[group_key][1] += area_val


# --------------------------
# Flush 과정도 동일하게 구현
# --------------------------
results = []

for group_key, (parts, area_sum) in agg_map.items():
    parts_mod = parts.copy()
    parts_mod[areaIndex] = str(area_sum)

    normalized_join = normalize_key("|".join(parts_mod))
    hash_all = xxhash.xxh64(normalized_join).hexdigest()

    key_join = "|".join(parts[i] for i in keyIndexes)
    hash_key = xxhash.xxh64(key_join).hexdigest()

    col0 = parts_mod[0]
    rest = parts_mod[1:]
    final_row = "|".join([col0, hash_all, hash_key, col0] + rest)

    results.append(final_row)


# --------------------------
# 결과 출력
# --------------------------
print("\n=== 결과 총 개수 ===")
print(len(results))

print("\n=== HashAll 중복 체크 ===")
seen = {}
dupes = {}

for r in results:
    parts = r.split("|")
    hash_all = parts[1]

    if hash_all in seen:
        if hash_all not in dupes:
            dupes[hash_all] = []
            dupes[hash_all].append(seen[hash_all])
        dupes[hash_all].append(r)
    else:
        seen[hash_all] = r

print("중복 HashAll 개수:", len(dupes))

if dupes:
    print("\n=== 중복 상세 출력 ===")
    for h, rows in dupes.items():
        print(f"\n🔻 HashAll 충돌: {h}")
        for rr in rows:
            print(rr)
