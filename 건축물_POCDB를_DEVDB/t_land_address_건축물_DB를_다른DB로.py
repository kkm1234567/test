import MySQLdb
import MySQLdb.cursors
import time
import tempfile
import os
import uuid

# ---------------------------------------
# 1) 테이블 매핑
# ---------------------------------------
TABLE_MAP = {
    "t_road_name_land_address": "t_road_name_land_address",
}

# ---------------------------------------
# 2) DB 연결 설정
# ---------------------------------------
SRC_DB = {
    "host": "192.168.10.244",
    "user": "DB_WO_BUNNY",
    "passwd": "Thrkflxkd72&",
    "db": "db_dw_address",
    "charset": "utf8mb4",
    "use_unicode": True,
}

DST_DB = {
    "host": "dev-prime-dw.cfecisesw67r.ap-northeast-2.rds.amazonaws.com",
    "port": 33306,
    "user": "DB_WO_BUNNY",
    "passwd": "Thrkflxkd72&",
    "db": "db_dw_address",
    "charset": "utf8mb4",
    "local_infile": 1,
    "connect_timeout": 600000,
    "read_timeout": 600000,
    "write_timeout": 600000    
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
    
    # geometry 컬럼 자동 식별 (type에 geometry/point/polygon/linestring 등 포함되면 변환)
    src_cursor = src_conn.cursor()
    src_cursor.execute(f"SHOW COLUMNS FROM {src_table}")
    columns_raw = src_cursor.fetchall()  # (Field, Type, Null, Key, Default, Extra)
    src_cursor.close()

    columns = [col[0] for col in columns_raw]
    geom_cols = [
        col[0]
        for col in columns_raw
        if any(t in str(col[1]).lower() for t in ["geometry", "point", "polygon", "linestring", "multipoint", "multilinestring", "multipolygon"])
    ]

    # SELECT 쿼리 생성 (geometry 컬럼은 WKT로 변환)
    select_cols = []
    for col in columns:
        if col in geom_cols:
            select_cols.append(f"ST_AsText({col}) AS {col}")
        else:
            select_cols.append(col)
    select_sql = f"SELECT {', '.join(select_cols)} FROM {src_table}"

    print("데이터 추출 및 분할 적재 중...")
    src_cursor = src_conn.cursor(MySQLdb.cursors.SSCursor)
    src_cursor.execute(select_sql)

    dst_cursor = dst_conn.cursor()
    CHUNK_SIZE = 200000
    total_inserted = 0
    chunk_num = 0
    chunk_files = []  # 청크 파일 경로 저장
    
    while True:
        chunk_num += 1
        csv_path = f"C:/folder/{src_table}_{uuid.uuid4().hex}_chunk{chunk_num}.csv"
        chunk_files.append(csv_path)  # 청크 파일 경로 추가
        
        with open(csv_path, mode='w', newline='', encoding='utf-8') as tmp_file:
            written = 0
            while written < CHUNK_SIZE:
                rows = src_cursor.fetchmany(10000)
                if not rows:
                    break
                for row in rows:
                    def _fmt(val):
                        if val is None:
                            return '\\N'
                        if isinstance(val, (bytes, bytearray, memoryview)):
                            # geometry가 바이너리로 반환될 때 WKT 텍스트로 디코딩
                            val = bytes(val).decode('utf-8', 'replace')
                        return str(val).replace('\\', '\\\\').replace('\t', '\\t').replace('\n', '\\n').replace('\r', '\\r')

                    line = '\t'.join(_fmt(val) for val in row)
                    tmp_file.write(line + '\n')
                    written += 1
                    total_inserted += 1
        
        if written == 0:
            chunk_files.pop()  # 빈 파일은 제거
            break
        if written < CHUNK_SIZE:
            break
    
    src_cursor.close()
    dst_cursor.close()
    
    # 청크 파일들을 하나로 합치기
    if chunk_files:
        merged_file = f"C:/folder/{src_table}_merged.csv"
        print(f"청크 파일들을 하나로 병합 중... → {merged_file}")
        
        with open(merged_file, 'w', newline='', encoding='utf-8') as outfile:
            for chunk_file in chunk_files:
                with open(chunk_file, 'r', encoding='utf-8') as infile:
                    outfile.write(infile.read())
        
        print(f"병합 완료! 청크 파일 삭제 중...")
        # 청크 파일들 삭제
        for chunk_file in chunk_files:
            try:
                os.remove(chunk_file)
            except Exception as e:
                print(f"파일 삭제 실패: {chunk_file} - {e}")
        
        print(f"청크 파일 {len(chunk_files)}개 삭제 완료")
    
    elapsed = time.time() - start
    print(f"\n✓ 완료! 총 {total_inserted:,} rows 이관됨 ({elapsed:.2f}초)")

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
