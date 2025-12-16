import pyodbc
import time
import xxhash

# ---------------------------------------
# 1) 대상 테이블 (이미 데이터가 존재하는 테이블)
# ---------------------------------------
# 층별개요: 건축물대장_층별개요, 층별개요고유키, 동일데이터중복수
# 주택가격: 건축물대장_주택가격, 층별개요고유키, 동일데이터중복수
DST_TABLE = "건축물대장_층별개요"  # 또는 "건축물대장_층별개요"
HASH_COLUMN = "층별개요고유키"  # hash를 저장할 컬럼
DUP_COUNT_COLUMN = "동일데이터중복수"  # 중복 수를 저장할 컬럼

# ---------------------------------------
# 2) MSSQL DB 연결 설정 (로컬 서버 - Windows 인증)
# ---------------------------------------
MSSQL_SERVER = "localhost"  # 또는 실제 서버명 (예: ".\SQLEXPRESS")
MSSQL_DATABASE = "EAIS_202104"

# Windows 인증 사용 (현재 로그인한 사용자로 자동 인증 - 비밀번호 불필요)

BATCH_SIZE = 5000

# ---------------------------------------
# 3) MSSQL 연결 함수
# ---------------------------------------
def get_mssql_connection():
    """MSSQL 연결 생성 (Windows 인증)"""
    conn_str = (
        f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        f'SERVER={MSSQL_SERVER};'
        f'DATABASE={MSSQL_DATABASE};'
        f'Trusted_Connection=yes;'
    )
    
    print(f"  → 연결 문자열: {conn_str}")
    return pyodbc.connect(conn_str)


# ---------------------------------------
# 4) Hash 및 중복 제거 함수
# ---------------------------------------
def deduplicate_with_hash(conn, table_name, hash_col, dup_count_col):
    """
    MSSQL 테이블의 모든 로우를 읽어 hash를 계산하고,
    중복 제거 후 재삽입합니다.
    """
    print(f"\n=== Processing {table_name} with HashAll deduplication ===")

    cursor = conn.cursor()

    # MSSQL에서 컬럼 정보 가져오기
    print(f"\n[단계 1] {table_name} 테이블 스키마 읽기...")
    cursor.execute(f"""
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = ? 
        ORDER BY ORDINAL_POSITION
    """, table_name)
    
    col_names = [row[0] for row in cursor.fetchall()]
    col_count = len(col_names)
    print(f"  → 총 {col_count}개 컬럼: {col_names}")

    # hash에서 제외할 컬럼들
    exclude_cols = {
        hash_col,           # hash를 저장할 컬럼 (층별개요고유키)
        dup_count_col,      # 중복 개수 (동일데이터중복수)
        "생성일자",          # 생성 일자
        "CreateDateTime",   # 생성 일시
    }
    
    hash_col_indices = [i for i, col in enumerate(col_names) if col not in exclude_cols]
    print(f"  → hash 계산에 사용할 컬럼 ({len(hash_col_indices)}개): {[col_names[i] for i in hash_col_indices]}")

    # 전체 row 수 확인
    cursor.execute(f"SELECT COUNT(*) FROM [{table_name}]")
    total_rows = cursor.fetchone()[0]
    print(f"\n[단계 2] 총 {total_rows:,} rows 읽어서 hash 계산 중...")

    # 모든 데이터 읽기
    select_cols_sql = ", ".join([f"[{c}]" for c in col_names])
    select_sql = f"SELECT {select_cols_sql} FROM [{table_name}]"
    
    read_cursor = conn.cursor()
    read_cursor.execute(select_sql)
    
    # INSERT SQL 준비
    placeholders = ", ".join(["?"] * col_count)
    col_list_sql = ", ".join([f"[{c}]" for c in col_names])
    insert_sql = f"INSERT INTO [{table_name}] ({col_list_sql}) VALUES ({placeholders})"

    # Dictionary: hash -> {'row': data, 'count': n}
    hash_dict = {}
    processed = 0
    start = time.time()

    print("  → 데이터 읽기 및 hash 계산 중...")
    while True:
        rows = read_cursor.fetchmany(BATCH_SIZE)
        if not rows:
            break

        for row in rows:
            # hash에 사용할 데이터만 추출
            hash_data = []
            for idx in hash_col_indices:
                val = row[idx]
                if val is None:
                    hash_data.append('')
                elif isinstance(val, bytes):
                    hash_data.append(val.decode('utf-8', errors='replace'))
                else:
                    hash_data.append(str(val))
            
            # hash 계산
            hash_str = '|'.join(hash_data)
            hash_value = xxhash.xxh64(hash_str.encode('utf-8')).hexdigest()
            
            if hash_value in hash_dict:
                # 중복 발견
                hash_dict[hash_value]['count'] += 1
            else:
                # 새로운 hash
                final_row = list(row)
                hash_col_idx = col_names.index(hash_col)
                final_row[hash_col_idx] = hash_value
                # 중복수는 1로 초기화
                dup_col_idx = col_names.index(dup_count_col)
                final_row[dup_col_idx] = 1
                
                hash_dict[hash_value] = {
                    'row': tuple(final_row),
                    'count': 1
                }
            
            processed += 1
            if processed % 100000 == 0:
                print(f"     처리중: {processed:,}/{total_rows:,} rows (고유 hash: {len(hash_dict):,}개)")

    read_cursor.close()
    
    print(f"  → 총 {processed:,} rows 처리 완료")
    print(f"  → 고유 hash {len(hash_dict):,}개 발견")
    print(f"  → 중복 제거로 {processed - len(hash_dict):,}건 감소 예상")
    
    print(f"\n[단계 3] {table_name} 테이블 TRUNCATE...")
    cursor.execute(f"TRUNCATE TABLE [{table_name}]")
    conn.commit()
    print("  → TRUNCATE 완료")

    print(f"\n[단계 4] 중복 제거된 데이터 재삽입...")
    batch = []
    inserted = 0
    
    for hash_value, data in hash_dict.items():
        row_list = list(data['row'])
        # 중복수 컬럼 업데이트
        dup_col_idx = col_names.index(dup_count_col)
        row_list[dup_col_idx] = data['count']
        batch.append(tuple(row_list))
        
        if len(batch) >= BATCH_SIZE:
            cursor.executemany(insert_sql, batch)
            conn.commit()
            inserted += len(batch)
            print(f"  → {inserted:,}/{len(hash_dict):,} unique records 삽입 완료")
            batch = []

    if batch:
        cursor.executemany(insert_sql, batch)
        conn.commit()
        inserted += len(batch)

    elapsed = time.time() - start
    print(f"\n[완료] 원본 {processed:,} rows → 중복 제거 후 {inserted:,} unique rows 삽입 ({elapsed:.2f}초)")
    print(f"중복 제거율: {((processed - inserted) / processed * 100):.2f}%")

    cursor.close()

# ---------------------------------------
# 5) main
# ---------------------------------------
if __name__ == "__main__":
    print("=== MSSQL Hash 기반 중복 제거 시작 ===")
    print(f"대상 서버: {MSSQL_SERVER}")
    print(f"대상 DB: {MSSQL_DATABASE}")
    print(f"대상 테이블: {DST_TABLE}")
    print(f"Hash 컬럼: {HASH_COLUMN}")
    print(f"중복수 컬럼: {DUP_COUNT_COLUMN}")
    
    try:
        print("\nMSSQL DB 연결 중...")
        conn = get_mssql_connection()
        print("  → 연결 성공!")

        deduplicate_with_hash(conn, DST_TABLE, HASH_COLUMN, DUP_COUNT_COLUMN)

        conn.close()
        print("\n=== 중복 제거 완료 ===")
        
    except Exception as e:
        print(f"\n오류 발생: {e}")
        import traceback
        traceback.print_exc()
