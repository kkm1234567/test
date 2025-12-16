import MySQLdb
import MySQLdb.cursors
import time
import traceback

import xxhash

# ---------------------------------------
# 1) 테이블 매핑
# ---------------------------------------
TABLE_MAP = {
    "eais_10_주택가격_bcp": "tCollectorBuildingHousePrice",
}

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
    charset="utf8mb4"
)

BATCH_SIZE = 5000
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

# ---------------------------------------
# 3) SRC 컬럼 정의 (원본에서 가져올 컬럼 순서)
# ---------------------------------------
# 원본에서 이 칼럼들은 제외하고 나머지 순서대로 대상에 매핑합니다
SOURCE_EXCLUDE = []

# exact SOURCE_COLS as provided by the user (preserve order)
SOURCE_COLS = [
    "관리_건축물대장_PK",
    "대장_구분_코드",
    "대장_구분_코드_명",
    "대장_종류_코드",
    "대장_종류_코드_명",
    "대지_위치",
    "도로명_대지_위치",
    "건물_명",
    "시군구_코드",
    "법정동_코드",
    "대지_구분_코드",
    "번",
    "지",
    "특수지_명",
    "블록",
    "로트",
    "외필지_수",
    "새주소_도로_코드",
    "새주소_법정동_코드",
    "새주소_지상지하_코드",
    "새주소_본_번",
    "새주소_부_번",
    "기준_일자",
    "주택가격",
    "생성_일자",
]

# columns we will keep from source, in order (exclude specified ones)
KEEP_COLS = [c for c in SOURCE_COLS if c not in SOURCE_EXCLUDE]

# NOTE: Static `SELECT_COLS` and `dest_cols` removed — column counts
# will be discovered at runtime. The migration will use the destination
# table's column count (DESCRIBE) to determine how many source columns
# to keep, and will set the last two destination columns to NULL.

# ---------------------------------------
# 4) Streaming Migration 함수
# ---------------------------------------
def migrate_table(src_conn, dst_conn, src_table, dst_table):
    print(f"\n=== Migrating {src_table} → {dst_table} ===")

    # prepare SELECT column list (wrapped with backticks for non-ASCII names)
    select_cols_sql = ",".join([f"`{c}`" for c in SOURCE_COLS])
    select_sql = f"SELECT {select_cols_sql} FROM {src_table}"

    # NOTE: we will open the streaming SRC cursor *after* we determine a
    # resume point from the destination table so we can skip rows already
    # inserted (avoids re-inserting duplicates when restarting the script).

    dst_cursor = dst_conn.cursor()

    # helper to execute a batch with reconnect/retry on 2006/2013
    def execute_batch_with_retry(dst_conn_local, dst_cursor_local, sql, params_batch):
        last_exc = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                # ensure connection is alive, try pinging with reconnect
                try:
                    dst_conn_local.ping(True)
                except Exception:
                    # will attempt a fresh reconnect below
                    pass

                # ensure cursor is tied to current connection
                try:
                    dst_cursor_local = dst_conn_local.cursor()
                except Exception:
                    dst_cursor_local = dst_conn_local.cursor()

                dst_cursor_local.executemany(sql, params_batch)
                dst_conn_local.commit()
                return dst_conn_local, dst_cursor_local
            except MySQLdb.OperationalError as e:
                last_exc = e
                code = e.args[0] if e.args else None
                if code in (2006, 2013):
                    print(f"WARN: dst executemany/commit failed (code={code}), attempting reconnect (attempt {attempt}/{MAX_RETRIES})")
                    try:
                        dst_conn_local.close()
                    except Exception:
                        pass
                    try:
                        dst_conn_local = MySQLdb.connect(**DST_DB)
                        dst_cursor_local = dst_conn_local.cursor()
                    except Exception as conn_e:
                        print("ERROR: reconnect attempt failed:", conn_e)
                        if attempt < MAX_RETRIES:
                            time.sleep(RETRY_DELAY)
                            continue
                        else:
                            raise
                    if attempt < MAX_RETRIES:
                        time.sleep(RETRY_DELAY)
                        continue
                    else:
                        raise
                else:
                    raise

        # if we exit loop without return, raise last exception
        if last_exc:
            raise last_exc

    # INSERT SQL: destination column list removed — destination column
    # count and order are discovered dynamically (DESCRIBE)

    # discover destination table column count at runtime (DESCRIBE)
    # retry/ping/reconnect on transient OperationalError (server gone away)
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            try:
                dst_conn.ping(True)
            except Exception:
                pass
            dst_cursor.execute(f"DESCRIBE {dst_table}")
            dst_desc = dst_cursor.fetchall()
            break
        except MySQLdb.OperationalError as e:
            code = e.args[0] if e.args else None
            if code in (2006, 2013):
                print(f"WARN: DESCRIBE failed (code={code}), reconnecting attempt {attempt}/{MAX_RETRIES}")
                try:
                    dst_conn.close()
                except Exception:
                    pass
                try:
                    dst_conn = MySQLdb.connect(**DST_DB)
                    dst_cursor = dst_conn.cursor()
                except Exception as conn_e:
                    print("ERROR: reconnect attempt failed:", conn_e)
                    if attempt < MAX_RETRIES:
                        time.sleep(RETRY_DELAY)
                        continue
                    else:
                        raise
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
                    continue
                else:
                    raise
            else:
                raise
    dst_col_count = len(dst_desc)
    dst_col_names = [d[0] for d in dst_desc]
    # build destination metadata for runtime validation/fallbacks
    # dst_desc rows: (Field, Type, Null, Key, Default, Extra)
    dst_meta = {d[0]: {'Type': d[1], 'Null': d[2], 'Default': d[4]} for d in dst_desc}

    # Build explicit destination->source mapping. If a destination column has
    # no matching source (e.g. we intentionally exclude it), map to None so it
    # will be inserted as NULL. Update mapping if destination schema differs.
    # mapping exactly follows the destination column list provided by the user
    mapping = {
        'BuildingLedgerCd': '관리_건축물대장_PK',
        'BuildingHousePriceCd': None,
        'LedgerType': '대장_구분_코드',
        'LedgerTypeName': '대장_구분_코드_명',
        'LedgerClassType': '대장_종류_코드',
        'LedgerClassTypeName': '대장_종류_코드_명',
        'LotLocation': '대지_위치',
        'RoadNameLotLocation': '도로명_대지_위치',
        'BuildingName': '건물_명',
        'SiGuGunCd': '시군구_코드',
        'LegalDongCd': '법정동_코드',
        'LotType': '대지_구분_코드',
        'MainLotNum': '번',
        'SubLotNum': '지',
        'SpecialLotName': '특수지_명',
        'Block': '블록',
        'Lot': '로트',
        'EtcLotCnt': '외필지_수',
        'NewAddressRoadNameCd': '새주소_도로_코드',
        'NewAddressLegalDongCd': '새주소_법정동_코드',
        'NewAddressFloorCd': '새주소_지상지하_코드',
        'NewAddressMainLotNum': '새주소_본_번',
        'NewAddressSubLotNum': '새주소_부_번',
        'HousePrice': '주택가격',
        'DuplicationRecodeCnt': None,
        'CreateDate': '생성_일자',
        'CreateDateTime': None,
        'OriginDocumentDay': '기준_일자',
        'OriginDocumentMonth': None,
    }

    # Determine resume point from destination: find max BuildingLedgerCd
    try:
        dst_cursor.execute(f"SELECT MAX(BuildingLedgerCd) FROM {dst_table}")
        max_dst_pk = dst_cursor.fetchone()[0]
    except Exception:
        max_dst_pk = None

    where_clause = ""
    select_sql_with_where = select_sql
    # If destination already has rows, resume after the max primary key value
    if max_dst_pk is not None:
        print(f"INFO: resuming - destination max BuildingLedgerCd = {max_dst_pk}")
        # Count remaining rows using a parameterized query to avoid SQL injection
        with src_conn.cursor() as c:
            c.execute(f"SELECT COUNT(*) FROM {src_table} WHERE `{SOURCE_COLS[0]}` > %s", (max_dst_pk,))
            total_rows = c.fetchone()[0]
        where_clause = f"WHERE `{SOURCE_COLS[0]}` > %s"
        select_sql_with_where = select_sql + f" {where_clause} ORDER BY `{SOURCE_COLS[0]}` ASC"
    else:
        # no resume needed; full table
        with src_conn.cursor() as c:
            c.execute(f"SELECT COUNT(*) FROM {src_table}")
            total_rows = c.fetchone()[0]
        select_sql_with_where = select_sql + f" ORDER BY `{SOURCE_COLS[0]}` ASC"

    print(f"총 {total_rows:,} rows 이관 예정")

    # If the user-requested positional mapping (exclude SOURCE_EXCLUDE then
    # append two NULLs) yields the exact destination column count, prefer
    # that positional mapping. Otherwise fall back to the explicit name
    # mapping above (safer, handles schema differences).
    if len(KEEP_COLS) + 2 == dst_col_count:
        print("INFO: using positional mapping (KEEP_COLS order) as requested")
        order_mapping = {}
        ordered_src = KEEP_COLS + [None, None]
        for dst_col, src_col in zip(dst_col_names, ordered_src):
            order_mapping[dst_col] = src_col
        mapping = order_mapping
    else:
        print("INFO: positional mapping doesn't match destination column count; using explicit mapping dict")

    # ensure mapping covers destination columns (missing keys -> abort)
    missing = [c for c in dst_col_names if c not in mapping]
    if missing:
        print("FATAL: missing mapping entries for destination columns:", missing)
        print("Please update the 'mapping' dict to map destination columns to source column names (or None)")
        raise SystemExit("Missing mapping entries")

    placeholders = ",".join(["%s"] * dst_col_count)

    # Use explicit destination column list in INSERT to avoid depending
    # on server column ordering. Columns are wrapped with backticks.
    dst_col_list_sql = ",".join([f"`{c}`" for c in dst_col_names])
    insert_sql = f"INSERT INTO {dst_table} ({dst_col_list_sql}) VALUES ({placeholders})"

    print("Insert SQL 준비 완료")
    print("Insert SQL:", insert_sql)

    # ===== Pass 1: 해시 카운트 계산 (메모리 경량: 해시 -> 카운트만 저장) =====
    print("\nPass 1: 해시 카운트 계산 중...")
    # 해시 계산에서 제외할 대상 컬럼들 (목적지 기준)
    skip_cols_dst = {"BuildingHousePriceCd", "DuplicationRecodeCnt", "CreateDateTime", "OriginDocumentMonth"}
    # 목적지 컬럼 인덱스 추출
    try:
        idx_hash = dst_col_names.index("BuildingHousePriceCd")
    except ValueError:
        idx_hash = None
    try:
        idx_dup = dst_col_names.index("DuplicationRecodeCnt")
    except ValueError:
        idx_dup = None

    # SRC 스트리밍 커서 (Pass 1)
    src_cursor_pass1 = src_conn.cursor(MySQLdb.cursors.SSCursor)
    if max_dst_pk is not None:
        src_cursor_pass1.execute(select_sql_with_where, (max_dst_pk,))
    else:
        src_cursor_pass1.execute(select_sql_with_where)

    hash_count = {}
    rows_counted = 0
    while True:
        row = src_cursor_pass1.fetchone()
        if row is None:
            break
        src_values = dict(zip(SOURCE_COLS, row))
        # 목적지 행을 동일하게 구성 (해시/중복수는 제외 값 계산을 위해 임시 구성)
        new_row_list_tmp = []
        for dst_col in dst_col_names:
            src_col = mapping.get(dst_col)
            val = None if src_col is None else src_values.get(src_col)
            if val is None and dst_meta.get(dst_col, {}).get('Null') == 'NO':
                default = dst_meta.get(dst_col, {}).get('Default')
                if default is not None:
                    val = default
                else:
                    t = dst_meta.get(dst_col, {}).get('Type', '').lower()
                    val = 0 if any(x in t for x in ('int','decimal','numeric','float','double')) else ''
            new_row_list_tmp.append(val)
        # 해시 파츠 구성 (skip 대상 제외)
        hash_parts = []
        for col_name, val in zip(dst_col_names, new_row_list_tmp):
            if col_name in skip_cols_dst:
                continue
            hash_parts.append('' if val is None else str(val))
        hash_value = xxhash.xxh64('|'.join(hash_parts).encode('utf-8')).hexdigest()
        hash_count[hash_value] = hash_count.get(hash_value, 0) + 1
        rows_counted += 1
        if rows_counted % 1000000 == 0:
            print(f"  → {rows_counted:,} rows counted, {len(hash_count):,} unique hashes")

    src_cursor_pass1.close()
    print(f"Pass 1 완료: {rows_counted:,} rows, {len(hash_count):,} unique hashes")

    # ===== Pass 2: 실제 이관 (해시키/중복수 삽입 포함) =====
    print("\nPass 2: 데이터 이관 중...")
    # SRC 스트리밍 커서 (Pass 2)
    src_cursor = src_conn.cursor(MySQLdb.cursors.SSCursor)
    if max_dst_pk is not None:
        src_cursor.execute(select_sql_with_where, (max_dst_pk,))
    else:
        src_cursor.execute(select_sql_with_where)

    batch = []
    inserted = 0
    start = time.time()

    # Build rows by taking SOURCE_COLS in order, excluding SOURCE_EXCLUDE,
    # then append two NULLs for the final destination columns.
    expected = dst_col_count
    # columns we will keep from source, in order
    keep_cols = [c for c in SOURCE_COLS if c not in SOURCE_EXCLUDE]

    while True:
        row = src_cursor.fetchone()
        if row is None:
            break

        # build new_row by mapping each destination column to a source column
        # value (mapping value None => insert NULL)
        src_values = dict(zip(SOURCE_COLS, row))
        new_row_list = []
        for dst_col in dst_col_names:
            src_col = mapping.get(dst_col)
            if src_col is None:
                val = None
            else:
                val = src_values.get(src_col)

            # if value is None but destination column is NOT NULL, apply fallback
            if val is None and dst_meta.get(dst_col, {}).get('Null') == 'NO':
                default = dst_meta.get(dst_col, {}).get('Default')
                if default is not None:
                    val = default
                else:
                    t = dst_meta.get(dst_col, {}).get('Type', '').lower()
                    # numeric types -> 0, otherwise empty string
                    if any(x in t for x in ('int', 'decimal', 'numeric', 'float', 'double')):
                        val = 0
                    else:
                        val = ''

            new_row_list.append(val)
        # 해시 계산 및 컬럼 반영
        hash_parts = []
        for col_name, val in zip(dst_col_names, new_row_list):
            if col_name in skip_cols_dst:
                continue
            hash_parts.append('' if val is None else str(val))
        hash_value = xxhash.xxh64('|'.join(hash_parts).encode('utf-8')).hexdigest()
        if idx_hash is not None:
            new_row_list[idx_hash] = hash_value
        if idx_dup is not None:
            new_row_list[idx_dup] = hash_count.get(hash_value, 1)

        new_row = tuple(new_row_list)
        # final validation (should match dst_col_count)
        if len(new_row) != expected:
            raise ValueError(f"Built row length {len(new_row)} != expected {expected}")

        batch.append(new_row)

        if len(batch) >= BATCH_SIZE:
            # validate batch rows have expected column count
            expected = dst_col_count
            for i, r in enumerate(batch):
                if len(r) != expected:
                    print(f"ERROR: row length mismatch in batch (index {i}) - got {len(r)}, expected {expected}")
                    print("Sample row:", r)
                    raise ValueError(f"Row length {len(r)} != expected {expected}")

            # use retrying executor that may reconnect and return updated conn/cursor
            try:
                dst_conn, dst_cursor = execute_batch_with_retry(dst_conn, dst_cursor, insert_sql, batch)
            except Exception:
                print("FATAL: failed to write batch after retries")
                traceback.print_exc()
                raise
            inserted += len(batch)
            print(f"  → {inserted:,}/{total_rows:,} inserted")
            batch.clear()

    if batch:
        expected = dst_col_count
        for i, r in enumerate(batch):
            if len(r) != expected:
                print(f"ERROR: row length mismatch in final batch (index {i}) - got {len(r)}, expected {expected}")
                print("Sample row:", r)
                raise ValueError(f"Row length {len(r)} != expected {expected}")

        try:
            dst_conn, dst_cursor = execute_batch_with_retry(dst_conn, dst_cursor, insert_sql, batch)
        except Exception:
            print("FATAL: failed to write final batch after retries")
            traceback.print_exc()
            raise
        inserted += len(batch)

    print(f"완료! 총 {inserted:,} rows 이관됨 ({time.time() - start:.2f}초)")

    src_cursor.close()
    dst_cursor.close()

    # return dst_conn in case caller should keep updated connection
    return dst_conn

# ---------------------------------------
# 5) main
# ---------------------------------------
if __name__ == "__main__":
    print("DB 연결 중...")

    src_conn = MySQLdb.connect(**SRC_DB)
    dst_conn = MySQLdb.connect(**DST_DB)

    for src_table, dst_table in TABLE_MAP.items():
        dst_conn = migrate_table(src_conn, dst_conn, src_table, dst_table)

    src_conn.close()
    dst_conn.close()

    print("\n=== 모든 테이블 이관 완료 ===")
