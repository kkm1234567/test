import MySQLdb
import MySQLdb.cursors
import time
import tempfile
import os

# ---------------------------------------
# 1) 테이블 매핑
# ---------------------------------------
TABLE_MAP = {
    "t_collector_building_sewage_treatment_plant": "t_collector_building_sewage_treatment_plant",
}

# ---------------------------------------
# 2) DB 연결 설정
# ---------------------------------------
SRC_DB = {
    "host": "192.168.10.244",
    "user": "DB_WO_BUNNY",
    "passwd": "Thrkflxkd72&",
    "db": "db_dw_collector_building",
    "charset": "utf8"
}

DST_DB = {
    "host": "dev-prime-dw.cfecisesw67r.ap-northeast-2.rds.amazonaws.com",
    "port": 33306,
    "user": "DB_WO_BUNNY",
    "passwd": "Thrkflxkd72&",
    "db": "db_dw_collector_building",
    "charset": "utf8mb4",
    "local_infile": 1
}

# ---------------------------------------
# 3) CSV + LOAD DATA 초고속 마이그레이션
# ---------------------------------------
def migrate_table_fast(src_conn, dst_conn, src_table, dst_table):
    print(f"\n=== Migrating {src_table} → {dst_table} (CSV+LOAD DATA 방식) ===")
    
    start = time.time()
    
    # 1단계: 전체 건수 확인
    with src_conn.cursor() as c:
        c.execute(f"SELECT COUNT(*) FROM {src_table}")
        total_rows = c.fetchone()[0]
    print(f"총 {total_rows:,} rows 이관 예정")
    
    # 2단계: 임시 CSV 파일 생성
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', newline='', encoding='utf-8') as tmp_file:
        csv_path = tmp_file.name
        print(f"임시 CSV 파일: {csv_path}")
        
        # SELECT INTO OUTFILE 대신 Python에서 CSV 생성
        print("데이터 추출 중...")
        src_cursor = src_conn.cursor(MySQLdb.cursors.SSCursor)
        src_cursor.execute(f"SELECT * FROM {src_table}")
        
        written = 0
        while True:
            rows = src_cursor.fetchmany(50000)  # 대량으로 가져오기
            if not rows:
                break
            
            for row in rows:
                # CSV 형식으로 작성 (탭 구분, NULL은 \N)
                line = '\t'.join(
                    '\\N' if val is None else str(val).replace('\\', '\\\\').replace('\t', '\\t').replace('\n', '\\n')
                    for val in row
                )
                tmp_file.write(line + '\n')
                written += 1
            
            if written % 100000 == 0:
                print(f"  → {written:,} rows 추출됨...")
        
        src_cursor.close()
    
    print(f"✓ CSV 생성 완료: {written:,} rows ({time.time() - start:.2f}초)")
    
    # 3단계: LOAD DATA LOCAL INFILE로 초고속 적재
    print("데이터 적재 중 (LOAD DATA)...")
    load_start = time.time()
    
    try:
        dst_cursor = dst_conn.cursor()
        
        # Windows 경로를 Unix 스타일로 변환
        csv_path_unix = csv_path.replace('\\', '/')
        
        load_sql = f"""
        LOAD DATA LOCAL INFILE '{csv_path_unix}'
        INTO TABLE {dst_table}
        FIELDS TERMINATED BY '\\t'
        LINES TERMINATED BY '\\n'
        """
        
        dst_cursor.execute(load_sql)
        dst_conn.commit()
        
        print(f"✓ 적재 완료! ({time.time() - load_start:.2f}초)")
        print(f"총 소요시간: {time.time() - start:.2f}초")
        
        dst_cursor.close()
        
    except Exception as e:
        print(f"✗ LOAD DATA 에러: {e}")
        raise
    finally:
        # 임시 파일 삭제
        if os.path.exists(csv_path):
            os.remove(csv_path)
            print("임시 파일 삭제됨")

# ---------------------------------------
# 4) main
# ---------------------------------------
if __name__ == "__main__":
    print("=== 초고속 DB 마이그레이션 (CSV+LOAD DATA 방식) ===")
    
    src_conn = MySQLdb.connect(**SRC_DB)
    dst_conn = MySQLdb.connect(**DST_DB)
    
    for src_table, dst_table in TABLE_MAP.items():
        migrate_table_fast(src_conn, dst_conn, src_table, dst_table)
    
    src_conn.close()
    dst_conn.close()
    
    print("\n=== 모든 테이블 이관 완료 ===")
