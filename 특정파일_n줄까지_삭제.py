import os

# ================================================
# 설정
# ================================================
org_path = r"C:\CollectApi\storage\vworld_kr\vworld_land_forest_land_move\202511\org.txt"
temp_path = r"C:\CollectApi\storage\vworld_kr\vworld_land_forest_land_move\202511\org_trimmed.txt"
LINE_LIMIT = 20


# ================================================
# 1) 앞 20줄만 읽어서 임시 파일 생성
# ================================================
print(f"[1/3] 앞 {LINE_LIMIT}줄 추출 중...")

with open(org_path, "r", encoding="utf-8", errors="ignore") as fr, \
     open(temp_path, "w", encoding="utf-8") as fw:

    for i, line in enumerate(fr):
        if i >= LINE_LIMIT:
            break
        fw.write(line)

print(f"[OK] {temp_path} 생성 완료")


# ================================================
# 2) 원본 파일 삭제
# ================================================
print("[2/3] 기존 org.txt 삭제 중...")

try:
    os.remove(org_path)
    print("[OK] 원본 org.txt 삭제 완료")
except Exception as e:
    print("[ERROR] 원본 삭제 실패:", e)
    exit(1)


# ================================================
# 3) 임시 파일을 org.txt 로 이름 변경
# ================================================
print("[3/3] 새 파일을 org.txt 로 교체 중...")

try:
    os.rename(temp_path, org_path)
    print("[완료] org.txt 앞 20줄만 남긴 상태로 성공적으로 교체됨!")
except Exception as e:
    print("[ERROR] 파일 교체 실패:", e)
    exit(1)
