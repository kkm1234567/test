import MySQLdb
import pandas as pd

DST_DB = {
    "host": "dev-prime-dw.cfecisesw67r.ap-northeast-2.rds.amazonaws.com",
    "port": 33306,
    "user": "DB_WO_BUNNY",
    "passwd": "Thrkflxkd72&",
    "db": "db_dw_address",
    "charset": "utf8mb4",
}

CSV_PATH = r"C:\folder\t_land_address_638860e3abde4a4cb5375943bf39b914.txt"

# 1) 파일 총 행 수 확인
print("=== 파일 정보 ===")
try:
    total_lines = sum(1 for line in open(CSV_PATH, 'r', encoding='utf-8'))
    print(f"TXT 파일 총 행 수: {total_lines:,}")
except Exception as e:
    print(f"파일 읽기 오류: {e}")

# 2) DB 현재 행 수 확인
print("\n=== DB 정보 ===")
try:
    conn = MySQLdb.connect(**DST_DB)
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM t_land_address")
    count = cursor.fetchone()[0]
    print(f"DB 현재 행 수: {count:,}")
    
    # 3) DB의 마지막 PRIMARY KEY 10개 확인
    cursor.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='t_land_address' AND COLUMN_KEY='PRI'")
    pk_col = cursor.fetchone()
    if pk_col:
        pk_col = pk_col[0]
        print(f"PRIMARY KEY 컬럼: {pk_col}")
        
        cursor.execute(f"SELECT {pk_col} FROM t_land_address ORDER BY {pk_col} DESC LIMIT 10")
        last_pks = [row[0] for row in cursor.fetchall()]
        print(f"DB 마지막 10개 ID: {last_pks}")
    
    # 4) TXT 파일의 첫 3행과 마지막 3행 미리보기
    print("\n=== TXT 파일 미리보기 ===")
    df = pd.read_csv(CSV_PATH, sep='\t', nrows=5, header=None, encoding='utf-8')
    print(f"파일 첫 5행 ID (0번 컬럼): {df[0].tolist()}")
    
    cursor.close()
    conn.close()
except Exception as e:
    print(f"DB 조회 오류: {e}")
