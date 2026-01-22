import mysql.connector

# ----------------------------------------
# 1) 업데이트 대상 테이블 목록
# ----------------------------------------
TARGET_TABLES = [
    "eais_05_전유부_bcp",
    "eais_08_지역지구구역_bcp",
    "eais_09_부속지번_bcp",
    "eais_10_주택가격_bcp"
]

# ----------------------------------------
# 2) MariaDB 연결
# ----------------------------------------
conn = mysql.connector.connect(
    host="192.168.11.203",
    user="root",
    password="!@Skdud340",
    database="buildledger",
    charset="utf8"
)

cursor = conn.cursor()

print("\n=== 건축물 PK 매핑 업데이트 시작 (JOIN 방식) ===\n")

# ----------------------------------------
# 3) 각 테이블에 대해 PK 업데이트
# ----------------------------------------
for table_name in TARGET_TABLES:

    print(f"▶ 테이블 업데이트 시작: {table_name}")

    sql = f"""
        UPDATE {table_name} AS t
        JOIN eais_ledgerno_changed AS m
            ON t.관리_건축물대장_PK = m.LedgerNo_Old
        SET t.관리_건축물대장_PK = m.LedgerNo_New;
    """

    cursor.execute(sql)
    conn.commit()

    print(f"   ✔ 업데이트 완료: {cursor.rowcount} rows affected\n")

# ----------------------------------------
# 4) 종료
# ----------------------------------------
cursor.close()
conn.close()

print("\n=== 모든 PK 업데이트 완료 ===")
