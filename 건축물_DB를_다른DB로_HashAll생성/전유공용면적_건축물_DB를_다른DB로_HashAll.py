import MySQLdb
import MySQLdb.cursors
import time
import xxhash
import pickle

DST_TABLE = "tCollectorBuildingUseArea"
PK_COLS = ("BuildingLedgerCd", "BuildingUseAreaCd")
DUP_COUNT_COL = "DuplicationRecodeCnt"
BATCH_SIZE = 5000
CHUNK_SIZE = 50000  # 한 번에 메모리에 로드할 행 수

DST_DB = dict(
    host="192.168.10.244",
    user="DB_WO_BUNNY",
    password="Thrkflxkd72&",
    database="dbDwCollectorBuilding",
    charset="utf8mb4",
    connect_timeout=300,
    read_timeout=600,
    write_timeout=600,
)


def deduplicate_with_hash(dst_conn):
    print(f"\n=== Processing {DST_TABLE} with Chunked Deduplication ===")

    cur = dst_conn.cursor()
    cur.execute(f"DESCRIBE {DST_TABLE}")
    desc = cur.fetchall()
    col_names = [d[0] for d in desc]
    col_count = len(col_names)
    print(f"컬럼 수: {col_count}")

    missing_pk = [c for c in PK_COLS if c not in col_names]
    if missing_pk:
        raise SystemExit(f"PK 컬럼이 테이블에 없습니다: {missing_pk}")

    pk2_idx = col_names.index(PK_COLS[1])
    dup_idx = col_names.index(DUP_COUNT_COL) if DUP_COUNT_COL and DUP_COUNT_COL in col_names else None

    select_cols_sql = ",".join([f"`{c}`" for c in col_names])
    select_sql = f"SELECT {select_cols_sql} FROM {DST_TABLE}"
    
    placeholders = ",".join(["%s"] * col_count)
    dst_col_sql = ",".join([f"`{c}`" for c in col_names])
    insert_sql = f"INSERT INTO {DST_TABLE} ({dst_col_sql}) VALUES ({placeholders})"

    skip_cols = {PK_COLS[1], "CreateDateTime", "OriginDocumentMonth"}
    if DUP_COUNT_COL:
        skip_cols.add(DUP_COUNT_COL)

    # 임시 테이블 생성: hash_value별 중복 개수 저장
    temp_table = f"{DST_TABLE}_temp_dedup"
    print(f"\n임시 테이블 생성: {temp_table}")
    cur.execute(f"DROP TABLE IF EXISTS {temp_table}")
    cur.execute(f"""
        CREATE TEMPORARY TABLE {temp_table} (
            hash_value VARCHAR(16) PRIMARY KEY,
            dedup_count INT DEFAULT 1,
            row_blob LONGBLOB
        ) ENGINE=MEMORY DEFAULT CHARSET=utf8mb4
    """)
    dst_conn.commit()

    # Phase 1: 청크 단위로 읽으면서 hash 계산 및 임시 테이블에 저장
    read_cursor = dst_conn.cursor(MySQLdb.cursors.SSCursor)
    read_cursor.execute(select_sql)
    
    processed = 0
    start = time.time()
    
    print("\nPhase 1: Chunked hashing and storing to temp table...")
    
    while True:
        chunk_rows = read_cursor.fetchmany(CHUNK_SIZE)
        if not chunk_rows:
            break
        
        # 청크 내에서 hash 계산 및 저장
        for row in chunk_rows:
            hash_parts = []
            for col_name, val in zip(col_names, row):
                if col_name in skip_cols:
                    continue
                hash_parts.append("" if val is None else str(val))
            
            hash_value = xxhash.xxh64("|".join(hash_parts).encode("utf-8")).hexdigest()
            
            # row 데이터 pickle로 저장 (첫 번째 나온 행만 저장)
            final_row = list(row)
            final_row[pk2_idx] = hash_value
            if dup_idx is not None:
                final_row[dup_idx] = 1
            
            row_blob = pickle.dumps(tuple(final_row))
            
            # 임시 테이블에 INSERT
            # 이미 있으면 count만 증가, 없으면 새로 저장
            try:
                cur.execute(f"""
                    INSERT INTO {temp_table} (hash_value, dedup_count, row_blob)
                    VALUES (%s, 1, %s)
                    ON DUPLICATE KEY UPDATE dedup_count = dedup_count + 1
                """, (hash_value, row_blob))
            except Exception as e:
                print(f"Error inserting hash {hash_value}: {e}")
            
            processed += 1
        
        # 배치 커밋
        dst_conn.commit()
        print(f"  → {processed:,} rows processed and stored to temp table")
    
    read_cursor.close()
    
    # 임시 테이블에서 통계 조회
    cur.execute(f"SELECT COUNT(*), SUM(dedup_count) FROM {temp_table}")
    unique_count, total_count = cur.fetchone()
    print(f"\nPhase 1 완료: {total_count:,} rows processed, {unique_count:,} unique records")
    
    # Phase 2: 원본 테이블 삭제 및 최종 데이터 삽입
    print("Phase 2: Clearing destination table and inserting deduplicated data...")
    
    cur.execute(f"DELETE FROM {DST_TABLE}")
    dst_conn.commit()
    print(f"  → Table {DST_TABLE} cleared")
    
    # Phase 3: 임시 테이블에서 읽어서 최종 삽입
    cur.execute(f"SELECT row_blob, dedup_count FROM {temp_table} ORDER BY hash_value")
    rows = cur.fetchall()
    
    batch = []
    inserted = 0
    
    for row_blob, dedup_count in rows:
        row_tuple = pickle.loads(row_blob)
        row_list = list(row_tuple)
        if dup_idx is not None:
            row_list[dup_idx] = dedup_count
        batch.append(tuple(row_list))
        
        if len(batch) >= BATCH_SIZE:
            cur.executemany(insert_sql, batch)
            dst_conn.commit()
            inserted += len(batch)
            print(f"  → {inserted:,}/{unique_count:,} unique records inserted")
            batch.clear()
    
    if batch:
        cur.executemany(insert_sql, batch)
        dst_conn.commit()
        inserted += len(batch)
    
    # 임시 테이블 정리
    cur.execute(f"DROP TABLE {temp_table}")
    dst_conn.commit()
    
    elapsed = time.time() - start
    print(f"\n완료! 원본 {total_count:,} rows → 중복 제거 후 {inserted:,} unique rows 삽입 ({elapsed:.2f}초)")
    if total_count:
        print(f"중복 제거율: {((total_count - inserted) / total_count * 100):.2f}%")
    
    cur.close()


if __name__ == "__main__":
    print("DB 연결 중...")
    dst_conn = MySQLdb.connect(**DST_DB)
    deduplicate_with_hash(dst_conn)
    dst_conn.close()
    print("\n=== 중복 제거 완료 ===")
    dst_conn.close()
    print("\n=== 중복 제거 완료 ===")
