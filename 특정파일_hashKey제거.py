import json
from pathlib import Path

# 파일 경로
json_path = Path(r"C:\PTR\Prime\Collect\CollectApi\storage\realtyprice_kr\t_apt_orffical_price\20260108\all_paths.json")

# JSON 로드
with json_path.open("r", encoding="utf-8") as f:
    data = json.load(f)

def remove_hash_key(obj):
    """
    dict 또는 list 구조에서 hash_key 제거
    """
    if isinstance(obj, list):
        for item in obj:
            if isinstance(item, dict):
                item.pop("hash_key", None)
    elif isinstance(obj, dict):
        obj.pop("hash_key", None)
        for v in obj.values():
            remove_hash_key(v)

remove_hash_key(data)

# 다시 저장 (덮어쓰기)
with json_path.open("w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print("✅ hash_key 제거 완료")
