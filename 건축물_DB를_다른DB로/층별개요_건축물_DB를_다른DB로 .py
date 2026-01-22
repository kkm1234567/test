import MySQLdb
import MySQLdb.cursors
import time
from datetime import datetime

# ---------------------------------------
# 1) 테이블 매핑
# ---------------------------------------
TABLE_MAP = {
    "eais_04_층별개요_bcp": "tCollectorBuildingFloor",
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

# ---------------------------------------
# 3) SRC 컬럼 정의 (원본에서 가져올 컬럼 순서)
# ---------------------------------------
# 원본에서 이 칼럼들은 제외하고 나머지 순서대로 대상에 매핑합니다
SOURCE_EXCLUDE = []

# exact SOURCE_COLS as provided by the user (preserve order)
SOURCE_COLS = [
    "관리_건축물대장_PK",
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
    "새주소_도로_코드",
    "새주소_법정동_코드",
    "새주소_지상지하_코드",
    "새주소_본_번",
    "새주소_부_번",
    "동_명",
    "층_구분_코드",
    "층_구분_코드_명",
    "층_번호",
    "층_번호_명",
    "구조_코드",
    "구조_코드_명",
    "기타_구조",
    "주_용도_코드",
    "주_용도_코드_명",
    "기타_용도",
    "면적",
    "주_부속_구분_코드",
    "주_부속_구분_코드_명",
    "면적_제외_여부",
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

    # 전체 건수
    with src_conn.cursor() as c:
        c.execute(f"SELECT COUNT(*) FROM {src_table}")
        total_rows = c.fetchone()[0]

    print(f"총 {total_rows:,} rows 이관 예정")

    # SRC 스트리밍 커서: select full SOURCE_COLS (we will map explicitly)
    src_cursor = src_conn.cursor(MySQLdb.cursors.SSCursor)
    # wrap column names with backticks to handle non-ASCII names
    select_cols_sql = ",".join([f"`{c}`" for c in SOURCE_COLS])
    select_sql = f"SELECT {select_cols_sql} FROM {src_table}"
    src_cursor.execute(select_sql)

    dst_cursor = dst_conn.cursor()

    # INSERT SQL: destination column list removed — destination column
    # count and order are discovered dynamically (DESCRIBE)

    # discover destination table column count at runtime (DESCRIBE)
    dst_cursor.execute(f"DESCRIBE {dst_table}")
    dst_desc = dst_cursor.fetchall()
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
        'BuildingFloorCd': None,
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
        'NewAddressRoadNameCd': '새주소_도로_코드',
        'NewAddressLegalDongCd': '새주소_법정동_코드',
        'NewAddressFloorCd': '새주소_지상지하_코드',
        'NewAddressMainLotNum': '새주소_본_번',
        'NewAddressSubLotNum': '새주소_부_번',
        'DongName': '동_명',
        'FloorCd': '층_구분_코드',
        'FloorCdName': '층_구분_코드_명',
        'FloorNum': '층_번호',
        'FloorName': '층_번호_명',
        'BuildingStructureCd': '구조_코드',
        'BuildingStructureCdName': '구조_코드_명',
        'EtcBuildingStructure': '기타_구조',
        'MainUseCd': '주_용도_코드',
        'MainUseCdName': '주_용도_코드_명',
        'EtcUse': '기타_용도',
        'Area': '면적',
        'MainSubFlag': '주_부속_구분_코드',
        'MainSubFlagName': '주_부속_구분_코드_명',
        'AreaExclusionFlag': '면적_제외_여부',
        'DuplicationRecodeCnt': None,
        'CreateDate': '생성_일자',
        'CreateDateTime': None,  # 동적으로 처리
        'OriginDocumentMonth': None,  # 동적으로 처리
    }

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

        # CreateDateTime, OriginDocumentMonth 설정
        try:
            idx_create_dt = dst_col_names.index('CreateDateTime')
            new_row_list[idx_create_dt] = datetime.now()
        except ValueError:
            pass
        try:
            idx_origin_month = dst_col_names.index('OriginDocumentMonth')
            new_row_list[idx_origin_month] = '202009'
        except ValueError:
            pass

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

            dst_cursor.executemany(insert_sql, batch)
            dst_conn.commit()
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

        dst_cursor.executemany(insert_sql, batch)
        dst_conn.commit()
        inserted += len(batch)

    print(f"완료! 총 {inserted:,} rows 이관됨 ({time.time() - start:.2f}초)")

    src_cursor.close()
    dst_cursor.close()

# ---------------------------------------
# 5) main
# ---------------------------------------
if __name__ == "__main__":
    print("DB 연결 중...")

    src_conn = MySQLdb.connect(**SRC_DB)
    dst_conn = MySQLdb.connect(**DST_DB)

    for src_table, dst_table in TABLE_MAP.items():
        migrate_table(src_conn, dst_conn, src_table, dst_table)

    src_conn.close()
    dst_conn.close()

    print("\n=== 모든 테이블 이관 완료 ===")
