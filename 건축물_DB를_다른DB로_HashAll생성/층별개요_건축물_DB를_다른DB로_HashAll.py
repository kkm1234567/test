import MySQLdb
import MySQLdb.cursors
import time
import xxhash

# ---------------------------------------
# 1) 대상 테이블 (이미 데이터가 존재하는 테이블)
# ---------------------------------------
DST_TABLE = "tCollectorBuildingFloor"

# ---------------------------------------
# 2) DB 연결 설정
# ---------------------------------------
SRC_DB = dict(
    host="192.168.11.203",
    user="root",
    password="!@Skdud340",
    database="buildledger",
    charset="utf8"
)

DST_DB = dict(
    host="192.168.10.244",
    user="DB_WO_BUNNY",
    password="Thrkflxkd72&",
    database="dbDwCollectorBuilding",
    charset="utf8mb4",
    connect_timeout=36000,
    read_timeout=36000,
    write_timeout=36000,
)

BATCH_SIZE = 1000

# ---------------------------------------
# 3) Hash 및 중복 제거 함수
# ---------------------------------------
def deduplicate_with_hash(dst_conn, dst_table):
    print(f"\n=== Processing {dst_table} with HashAll deduplication ===")

    dst_cursor = dst_conn.cursor()

    # Discover destination table schema
    dst_cursor.execute(f"DESCRIBE {dst_table}")
    dst_desc = dst_cursor.fetchall()
    dst_col_names = [d[0] for d in dst_desc]
    dst_col_count = len(dst_col_names)
    
    print(f"대상 테이블 컬럼 수: {dst_col_count}, 컬럼: {dst_col_names}")

    # Count existing rows
    dst_cursor.execute(f"SELECT COUNT(*) FROM {dst_table}")
    total_rows = dst_cursor.fetchone()[0]
    print(f"총 {total_rows:,} rows 읽어서 hash 계산 및 중복 제거 중...")

    # Read all data from destination table
    select_cols_sql = ",".join([f"`{c}`" for c in dst_col_names])
    select_sql = f"SELECT {select_cols_sql} FROM {dst_table}"
    
    read_cursor = dst_conn.cursor(MySQLdb.cursors.SSCursor)
    read_cursor.execute(select_sql)
    
    # Prepare INSERT SQL
    placeholders = ",".join(["%s"] * dst_col_count)
    dst_col_list_sql = ",".join([f"`{c}`" for c in dst_col_names])
    insert_sql = f"INSERT INTO {dst_table} ({dst_col_list_sql}) VALUES ({placeholders})"

    print("SQL 준비 완료")

    # Dictionary to store: temp_hash -> (row_data, hash_str, count)
    hash_dict = {}
    processed = 0
    start = time.time()

    print("Phase 1: Reading destination table data and grouping by content...")
    while True:
        row = read_cursor.fetchone()
        if row is None:
            break

        # Calculate temporary hash from all columns (excluding BuildingFloorCd, DuplicationRecodeCnt, CreateDateTime, OriginDocumentMonth)
        hash_data = []
        for col_name, val in zip(dst_col_names, row):
            if col_name not in ('BuildingFloorCd', 'DuplicationRecodeCnt', 'CreateDateTime', 'OriginDocumentMonth'):
                hash_data.append(str(val) if val is not None else '')
        
        # Create temporary hash for grouping (without count)
        hash_str = '|'.join(hash_data)
        temp_hash = xxhash.xxh64(hash_str.encode('utf-8')).hexdigest()
        
        # Store in dictionary: if hash exists, increment count; otherwise store row
        if temp_hash in hash_dict:
            hash_dict[temp_hash]['count'] += 1
        else:
            hash_dict[temp_hash] = {
                'row': row,
                'hash_str': hash_str,  # 원본 hash_str 저장 (나중에 count 포함해서 최종 hash 생성용)
                'count': 1
            }
        
        processed += 1
        if processed % 100000 == 0:
            print(f"  → Processed {processed:,}/{total_rows:,} rows, unique groups: {len(hash_dict):,}")

    read_cursor.close()
    
    print(f"Phase 1 완료: {processed:,} rows processed, {len(hash_dict):,} unique records")
    print(f"Phase 2: Clearing destination table and inserting deduplicated data...")
    
    # Clear destination table
    dst_cursor.execute(f"DELETE FROM {dst_table}")
    dst_conn.commit()
    print(f"  → Table {dst_table} cleared")

    # Insert deduplicated data with final hash (including count)
    batch = []
    inserted = 0
    building_floor_cd_idx = dst_col_names.index('BuildingFloorCd')
    dup_count_idx = dst_col_names.index('DuplicationRecodeCnt')
    
    for temp_hash, data in hash_dict.items():
        row_list = list(data['row'])
        final_count = data['count']
        
        # 최종 hash 생성: 원본 데이터 + DuplicationRecodeCnt 포함
        final_hash_str = data['hash_str'] + '|' + str(final_count)
        final_hash = xxhash.xxh64(final_hash_str.encode('utf-8')).hexdigest()
        
        # BuildingFloorCd에 최종 hash 저장
        row_list[building_floor_cd_idx] = final_hash
        # DuplicationRecodeCnt에 실제 중복 횟수 저장
        row_list[dup_count_idx] = final_count
        
        batch.append(tuple(row_list))
        
        if len(batch) >= BATCH_SIZE:
            dst_cursor.executemany(insert_sql, batch)
            dst_conn.commit()
            inserted += len(batch)
            print(f"  → {inserted:,}/{len(hash_dict):,} unique records inserted")
            batch.clear()

    if batch:
        dst_cursor.executemany(insert_sql, batch)
        dst_conn.commit()
        inserted += len(batch)

    print(f"완료! 원본 {processed:,} rows → 중복 제거 후 {inserted:,} unique rows 삽입 ({time.time() - start:.2f}초)")
    print(f"중복 제거율: {((processed - inserted) / processed * 100):.2f}%")

    dst_cursor.close()

# ---------------------------------------
# 4) main
# ---------------------------------------
if __name__ == "__main__":
    print("DB 연결 중...")

    dst_conn = MySQLdb.connect(**DST_DB)

    deduplicate_with_hash(dst_conn, DST_TABLE)

    dst_conn.close()

    print("\n=== 중복 제거 완료 ===")
