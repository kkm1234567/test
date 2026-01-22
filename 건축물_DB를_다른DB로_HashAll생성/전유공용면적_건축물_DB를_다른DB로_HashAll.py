import MySQLdb
import MySQLdb.cursors
import time
import xxhash
import gc

DST_TABLE = "tCollectorBuildingUseArea"
PK_COLS = ("BuildingLedgerCd", "BuildingUseAreaCd")
DUP_COUNT_COL = "DuplicationRecodeCnt"
BATCH_SIZE = 5000
CHUNK_SIZE = 100000  # 청크 크기

DST_DB = dict(
    host="192.168.10.244",
    user="DB_WO_BUNNY",
    password="Thrkflxkd72&",
    database="dbDwCollectorBuilding",
    charset="utf8mb4",
    connect_timeout=3600,
    read_timeout=3600,
    write_timeout=3600,
)


def deduplicate_with_hash(dst_conn):
    print(f"\n=== Processing {DST_TABLE} with chunked two-pass deduplication ===")

    cur = dst_conn.cursor()
    cur.execute(f"DESCRIBE {DST_TABLE}")
    desc = cur.fetchall()
    col_names = [d[0] for d in desc]
    col_count = len(col_names)
    print(f"대상 테이블 컬럼 수: {col_count}")

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

    # Pass 1: hash -> count (lightweight)
    print("\nPass 1: Counting duplicate hashes...")
    read_cursor = dst_conn.cursor(MySQLdb.cursors.SSCursor)
    read_cursor.execute(select_sql)

    hash_count = {}
    total_rows = 0

    while True:
        row = read_cursor.fetchone()
        if row is None:
            break

        hash_parts = []
        for col_name, val in zip(col_names, row):
            if col_name in skip_cols:
                continue
            hash_parts.append("" if val is None else str(val))

        hash_value = xxhash.xxh64("|".join(hash_parts).encode("utf-8")).hexdigest()
        hash_count[hash_value] = hash_count.get(hash_value, 0) + 1
        total_rows += 1

        if total_rows % 500000 == 0:
            print(f"  → {total_rows:,} rows counted, {len(hash_count):,} unique hashes")

    read_cursor.close()
    unique_count = len(hash_count)
    print(f"Pass 1 완료: {total_rows:,} rows, {unique_count:,} unique hashes")

    # nothing to do: keep table as-is
    if total_rows == 0 or unique_count == 0:
        print("원본이 없거나 고유 레코드가 없어 DELETE/INSERT를 생략합니다.")
        cur.close()
        return

    # Pass 2: chunked read, insert uniques with counts
    print("\nPass 2: Clearing destination table and inserting deduplicated data...")
    cur.execute(f"DELETE FROM {DST_TABLE}")
    dst_conn.commit()
    print(f"  → Table {DST_TABLE} cleared")

    read_cursor = dst_conn.cursor(MySQLdb.cursors.SSCursor)
    read_cursor.execute(select_sql)

    processed_rows = {}
    batch = []
    inserted = 0
    start = time.time()

    print("Pass 2: Reading and inserting in chunks...")

    while True:
        chunk_rows = read_cursor.fetchmany(CHUNK_SIZE)
        if not chunk_rows:
            break

        for row in chunk_rows:
            hash_parts = []
            for col_name, val in zip(col_names, row):
                if col_name in skip_cols:
                    continue
                hash_parts.append("" if val is None else str(val))

            hash_value = xxhash.xxh64("|".join(hash_parts).encode("utf-8")).hexdigest()

            if hash_value in processed_rows:
                continue
            processed_rows[hash_value] = True

            final_row = list(row)
            final_row[pk2_idx] = hash_value
            if dup_idx is not None:
                final_row[dup_idx] = hash_count[hash_value]

            batch.append(tuple(final_row))

            if len(batch) >= BATCH_SIZE:
                cur.executemany(insert_sql, batch)
                dst_conn.commit()
                inserted += len(batch)
                print(f"  → {inserted:,}/{unique_count:,} unique records inserted")
                batch.clear()

        print(f"  → Processed {len(processed_rows):,}/{unique_count:,} unique records")

    if batch:
        cur.executemany(insert_sql, batch)
        dst_conn.commit()
        inserted += len(batch)

    read_cursor.close()

    hash_count.clear()
    processed_rows.clear()
    gc.collect()

    elapsed = time.time() - start
    print(f"\n완료! 원본 {total_rows:,} rows → 중복 제거 후 {inserted:,} unique rows 삽입 ({elapsed:.2f}초)")
    if total_rows:
        print(f"중복 제거율: {((total_rows - inserted) / total_rows * 100):.2f}%")

    cur.close()


if __name__ == "__main__":
    print("DB 연결 중...")
    dst_conn = MySQLdb.connect(**DST_DB)
    deduplicate_with_hash(dst_conn)
    dst_conn.close()
    print("\n=== 중복 제거 완료 ===")
