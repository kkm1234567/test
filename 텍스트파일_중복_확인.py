import os

# TARGET = r"C:\PTR\Prime\Collect\CollectApi\storage\hub_go_kr\tCollectorBuildingUseArea\202511\org.txt"
TARGET = r"C:\PTR\Prime\Collect\CollectApi\storage\hub_go_kr\tCollectorBuildingHousePrice\202511\org.txt"

seen = {}
duplicates = {}

with open(TARGET, "r", encoding="utf-8", errors="ignore") as f:
    for line in f:
        parts = line.strip().split("|")
        if len(parts) < 2:
            continue

        hash_all = parts[1]

        if hash_all in seen:
            # 중복이면 duplicates dict에 저장
            if hash_all not in duplicates:
                duplicates[hash_all] = []
                duplicates[hash_all].append(seen[hash_all])  # 첫 등장 row 저장
            duplicates[hash_all].append(line.strip())         # 중복 row 저장
        else:
            seen[hash_all] = line.strip()

print("\n=== 중복 HashAll 상세 출력 ===")
print("총 중복 HashAll 개수:", len(duplicates))
print("-----------------------------------")

for h, rows in duplicates.items():
    print(f"\n🔻 HashAll: {h}  (총 {len(rows)}회 발견)")
    print("-----------------------------------")
    for r in rows:
        print(r)
