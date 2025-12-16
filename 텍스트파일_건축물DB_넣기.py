import mysql.connector
import os

# ----------------------------------------
# 1) EAIS 테이블 ↔ 원본 파일 매핑
# ----------------------------------------
DATA_MAP = {
    "eais_05_전유부_bcp":       "mart_djy_09.txt.utf8",
    "eais_08_지역지구구역_bcp": "mart_djy_10.txt.utf8",
    "eais_09_부속지번_bcp":     "mart_djy_05.txt.utf8",
    "eais_10_주택가격_bcp":     "mart_djy_08.txt.utf8"
}

BASE_DIR = r"C:\Users\guest1\OneDrive - 프롭티어\202509_건축물대장파일\2020-10-28"

# ----------------------------------------
# 2) MariaDB 연결
# ----------------------------------------
conn = mysql.connector.connect(
    host="192.168.11.203",
    user="root",
    password="!@Skdud340",
    database="buildledger",
    charset="utf8",
    allow_local_infile=True   # ★ LOCAL INFILE 허용
)

cursor = conn.cursor()

print("\n=== LOAD DATA 시작 ===\n")

# ----------------------------------------
# 3) 반복문으로 LOAD DATA
# ----------------------------------------
for table_name, filename in DATA_MAP.items():

    file_path = os.path.join(BASE_DIR, filename)

    # 파일 존재 체크
    if not os.path.exists(file_path):
        print(f"❌ 파일 없음 → {file_path}")
        continue

    print(f"➡ 테이블: {table_name}")
    print(f"   파일: {file_path}")

    # LOAD DATA SQL
    sql = f"""
        LOAD DATA LOCAL INFILE '{file_path.replace("\\", "/")}'
        INTO TABLE {table_name}
        CHARACTER SET utf8
        FIELDS TERMINATED BY '|'
        LINES TERMINATED BY '\\n';
    """

    try:
        cursor.execute(sql)
        conn.commit()
        print(f"   ✅ 성공: {cursor.rowcount} rows inserted\n")

    except Exception as e:
        print(f"   ❌ 실패: {e}\n")


# ----------------------------------------
# 4) 종료
# ----------------------------------------
cursor.close()
conn.close()

print("\n=== 모든 LOAD DATA 작업 완료 ===")
