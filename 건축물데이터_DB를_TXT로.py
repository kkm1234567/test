import mysql.connector
import os

# 저장 경로
nameList = [
    # "eais_01_기본개요_bcp",  
    # "eais_02_총괄표제부_bcp",
    # "eais_03_표제부_bcp",    
    # "eais_04_층별개요_bcp",  
    "eais_05_전유부_bcp",    
    # "eais_06_전유공용면적_bcp",
    # "eais_07_오수정화시설_bcp",
    "eais_08_지역지구구역_bcp",
    "eais_09_부속지번_bcp",    
    "eais_10_주택가격_bcp",    
]

for name in nameList:
    output_path = fr"C:\202009\{name}.txt"

    # MariaDB 연결
    conn = mysql.connector.connect(
        host="192.168.11.203",
        user="root",            # 계정 맞게 변경
        password="!@Skdud340",  # 비밀번호 변경
        database="buildledger",
        charset="utf8"
    )

    cursor = conn.cursor()

    # 데이터 조회
    cursor.execute(f"SELECT * FROM {name}")

    # TXT 생성
    with open(output_path, "w", encoding="utf-8") as f:
        for row in cursor:
            # 🔥 strip() 적용 → 주소 같은 필드 앞뒤 여백 제거
            line = "|".join("" if v is None else str(v).strip() for v in row)
            f.write(line + "\n")

    cursor.close()
    conn.close()

    print("TXT 저장 완료 →", output_path)
