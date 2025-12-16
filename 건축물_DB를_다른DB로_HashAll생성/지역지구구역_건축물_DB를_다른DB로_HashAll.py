import MySQLdb
import MySQLdb.cursors
import time
import xxhash

DST_TABLE = "tCollectorBuildingRegionDistrictSection"
PK_COLS = ("BuildingLedgerCd", "BuildingRegionDistrictSectionCd")
DUP_COUNT_COL = "DuplicationRecodeCnt"
BATCH_SIZE = 5000

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
    print(f"\n=== Processing {DST_TABLE} with HashAll deduplication ===")

    cur = dst_conn.cursor()
    cur.execute(f"DESCRIBE {DST_TABLE}")
    desc = cur.fetchall()
    col_names = [d[0] for d in desc]
    col_count = len(col_names)
    print(f"대상 테이블 컬럼 수: {col_count}, 컬럼: {col_names}")

    missing_pk = [c for c in PK_COLS if c not in col_names]
    if missing_pk:
        raise SystemExit(f"PK 컬럼이 테이블에 없습니다: {missing_pk}")

    pk2_idx = col_names.index(PK_COLS[1])
    dup_idx = col_names.index(DUP_COUNT_COL) if DUP_COUNT_COL and DUP_COUNT_COL in col_names else None

    select_cols_sql = ",".join([f"`{c}`" for c in col_names])
    select_sql = f"SELECT {select_cols_sql} FROM {DST_TABLE}"
    read_cursor = dst_conn.cursor(MySQLdb.cursors.SSCursor)
    read_cursor.execute(select_sql)

    placeholders = ",".join(["%s"] * col_count)
    dst_col_sql = ",".join([f"`{c}`" for c in col_names])
    insert_sql = f"INSERT INTO {DST_TABLE} ({dst_col_sql}) VALUES ({placeholders})"

    skip_cols = {PK_COLS[1], "CreateDateTime", "OriginDocumentMonth"}
    if DUP_COUNT_COL:
        skip_cols.add(DUP_COUNT_COL)

    hash_dict = {}
    processed = 0
    start = time.time()

    print("Phase 1: hashing rows...")
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

        final_row = list(row)
        final_row[pk2_idx] = hash_value
        if dup_idx is not None:
            final_row[dup_idx] = 1

        if hash_value in hash_dict:
            hash_dict[hash_value]["count"] += 1
        else:
            hash_dict[hash_value] = {"row": tuple(final_row), "count": 1}

        processed += 1
        if processed % 100000 == 0:
            print(f"  → {processed:,} rows processed, unique hashes: {len(hash_dict):,}")

    read_cursor.close()

    print(f"Phase 1 완료: {processed:,} rows processed, {len(hash_dict):,} unique records")
    print("Phase 2: Clearing destination table and inserting deduplicated data...")

    cur.execute(f"DELETE FROM {DST_TABLE}")
    dst_conn.commit()
    print(f"  → Table {DST_TABLE} cleared")

    batch = []
    inserted = 0
    for data in hash_dict.values():
        row_list = list(data["row"])
        if dup_idx is not None:
            row_list[dup_idx] = data["count"]
        batch.append(tuple(row_list))

        if len(batch) >= BATCH_SIZE:
            cur.executemany(insert_sql, batch)
            dst_conn.commit()
            inserted += len(batch)
            print(f"  → {inserted:,}/{len(hash_dict):,} unique records inserted")
            batch.clear()

    if batch:
        cur.executemany(insert_sql, batch)
        dst_conn.commit()
        inserted += len(batch)

    elapsed = time.time() - start
    print(f"완료! 원본 {processed:,} rows → 중복 제거 후 {inserted:,} unique rows 삽입 ({elapsed:.2f}초)")
    if processed:
        print(f"중복 제거율: {((processed - inserted) / processed * 100):.2f}%")

    cur.close()


if __name__ == "__main__":
    print("DB 연결 중...")
    dst_conn = MySQLdb.connect(**DST_DB)
    deduplicate_with_hash(dst_conn)
    dst_conn.close()
    print("\n=== 중복 제거 완료 ===")
