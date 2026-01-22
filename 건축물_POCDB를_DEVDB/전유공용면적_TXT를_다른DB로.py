import MySQLdb
import MySQLdb.cursors
import time
import tempfile
import os

# ---------------------------------------
# 1) 테이블 매핑
# ---------------------------------------
TABLE_MAP = {
    "t_collector_building_use_area": "t_collector_building_use_area",
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
    "local_infile": 1,
    "connect_timeout": 600,
    "read_timeout": 600,
    "write_timeout": 600
}

CHUNK_SIZE = 200000  # CSV 파일당 최대 row 수

# ---------------------------------------
# 3) CSV + LOAD DATA 초고속 마이그레이션 (분할 처리)
# ---------------------------------------
def migrate_table_fast(src_conn, dst_conn, src_table, dst_table):
    print(f"\n=== Migrating {src_table} → {dst_table} (CSV+LOAD DATA 분할 방식) ===")
    
    start = time.time()
    
    # 1단계: 전체 건수 확인
    with src_conn.cursor() as c:
        c.execute(f"SELECT COUNT(*) FROM {src_table}")
        total_rows = c.fetchone()[0]
    print(f"총 {total_rows:,} rows 이관 예정")
    
    # 2단계: 데이터 추출 및 분할 LOAD DATA
    print("데이터 추출 및 적재 중 (분할 처리)...")
    src_cursor = src_conn.cursor(MySQLdb.cursors.SSCursor)
    src_cursor.execute(f"SELECT * FROM {src_table}")
    
    dst_cursor = dst_conn.cursor()
    
    # 타임아웃 설정 늘리기
    try:
        dst_cursor.execute("SET SESSION wait_timeout=28800")
        dst_cursor.execute("SET SESSION interactive_timeout=28800")
        dst_cursor.execute("SET SESSION net_read_timeout=600")
        dst_cursor.execute("SET SESSION net_write_timeout=600")
    except:
        pass  # RDS에서 설정 불가능할 수 있음
    
    total_inserted = 0
    chunk_num = 0
    
    while True:
        chunk_num += 1
        
        # CHUNK_SIZE만큼 CSV 파일 생성
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', newline='', encoding='utf-8') as tmp_file:
            csv_path = tmp_file.name
            
            written = 0
            while written < CHUNK_SIZE:
                rows = src_cursor.fetchmany(10000)
                if not rows:
                    break
                
                for row in rows:
                    line = '\t'.join(
                        '\\N' if val is None else str(val).replace('\\', '\\\\').replace('\t', '\\t').replace('\n', '\\n').replace('\r', '\\r')
                        for val in row
                    )
                    tmp_file.write(line + '\n')
                    written += 1
        
        if written == 0:
            os.remove(csv_path)
            break
        
        # LOAD DATA 실행
        try:
            csv_path_unix = csv_path.replace('\\', '/')
            
            load_sql = f"""
            LOAD DATA LOCAL INFILE '{csv_path_unix}'
            INTO TABLE {dst_table}
            FIELDS TERMINATED BY '\\t'
            LINES TERMINATED BY '\\n'
            """
            
            chunk_start = time.time()
            dst_cursor.execute(load_sql)
            dst_conn.commit()
            chunk_time = time.time() - chunk_start
            
            total_inserted += written
            progress = (total_inserted / total_rows * 100) if total_rows > 0 else 0
            elapsed = time.time() - start
            rate = total_inserted / elapsed if elapsed > 0 else 0
            
            print(f"  → Chunk {chunk_num}: {written:,} rows ({chunk_time:.1f}초) | 누적: {total_inserted:,}/{total_rows:,} ({progress:.1f}%) | {rate:.0f} rows/sec")
            
        except Exception as e:
            print(f"✗ LOAD DATA 에러 (Chunk {chunk_num}): {e}")
            os.remove(csv_path)
            raise
        finally:
            if os.path.exists(csv_path):
                os.remove(csv_path)
        
        if written < CHUNK_SIZE:
            break
    
    src_cursor.close()
    dst_cursor.close()
    
    elapsed = time.time() - start
    print(f"\n✓ 완료! 총 {total_inserted:,} rows 이관됨 ({elapsed:.2f}초, {total_inserted/elapsed:.0f} rows/sec)")

# ---------------------------------------
# 4) main
# ---------------------------------------
if __name__ == "__main__":
    print("=== 초고속 DB 마이그레이션 (CSV+LOAD DATA 분할 방식) ===")
    
    src_conn = MySQLdb.connect(**SRC_DB)
    dst_conn = MySQLdb.connect(**DST_DB)
    
    for src_table, dst_table in TABLE_MAP.items():
        migrate_table_fast(src_conn, dst_conn, src_table, dst_table)
    
    src_conn.close()
    dst_conn.close()
    
    print("\n=== 모든 테이블 이관 완료 ===")
