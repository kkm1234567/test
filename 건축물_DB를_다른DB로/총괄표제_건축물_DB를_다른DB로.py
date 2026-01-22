import MySQLdb
import MySQLdb.cursors
import time
from datetime import datetime

# ---------------------------------------
# 1) 테이블 매핑
# ---------------------------------------
TABLE_MAP = {
    "eais_02_총괄표제부_bcp": "tCollectorBuildingOverallTitle",
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
SOURCE_EXCLUDE = ["대지_위치", "도로명_대지_위치", "옥내_기계식_대수"]

SOURCE_COLS = [
    "관리_건축물대장_PK",
    "대장_구분_코드",
    "대장_구분_코드_명",
    "대장_종류_코드",
    "대장_종류_코드_명",
    "신_구_대장_구분_코드",
    "신_구_대장_구분_코드_명",
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
    "대지_면적",
    "건축_면적",
    "건폐_율",
    "연면적",
    "용적_률_산정_연면적",
    "용적_률",
    "주_용도_코드",
    "주_용도_코드_명",
    "기타_용도",
    "세대_수",
    "가구_수",
    "주_건축물_수",
    "부속_건축물_수",
    "부속_건축물_면적",
    "총_주차_수",
    "옥내_기계식_대수",
    "옥내_기계식_면적",
    "옥외_기계식_대수",
    "옥외_기계식_면적",
    "옥내_자주식_대수",
    "옥내_자주식_면적",
    "옥외_자주식_대수",
    "옥외_자주식_면적",
    "허가_일",
    "착공_일",
    "사용승인_일",
    "허가번호_년",
    "허가번호_기관_코드",
    "허가번호_기관_코드_명",
    "허가번호_구분_코드",
    "허가번호_구분_코드_명",
    "호_수",
    "에너지효율_등급",
    "에너지절감_율",
    "에너지_EPI점수",
    "친환경_건축물_등급",
    "친환경_건축물_인증점수",
    "지능형_건축물_등급",
    "지능형_건축물_인증점수",
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

    # Build explicit destination->source mapping. If a destination column has
    # no matching source (e.g. we intentionally exclude it), map to None so it
    # will be inserted as NULL. Update mapping if destination schema differs.
    mapping = {
        'BuildingLedgerCd': '관리_건축물대장_PK',
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
        'NewDocumentFlagCd': '신_구_대장_구분_코드',
        'NewDocumentFlagCdName': '신_구_대장_구분_코드_명',
        'SpecialLotName': '특수지_명',
        'Block': '블록',
        'Lot': '로트',
        'EtcLotCnt': '외필지_수',
        'NewAddressRoadNameCd': '새주소_도로_코드',
        'NewAddressLegalDongCd': '새주소_법정동_코드',
        'NewAddressFloorCd': '새주소_지상지하_코드',
        'NewAddressMainLotNum': '새주소_본_번',
        'NewAddressSubLotNum': '새주소_부_번',
        'LotArea': '대지_면적',
        'BuildingArea': '건축_면적',
        'BuildingCoverageRatio': '건폐_율',
        'GrossFloorArea': '연면적',
        'FarCalculationGfa': '용적_률_산정_연면적',
        'FloorAreaRatio': '용적_률',
        'MainUseCd': '주_용도_코드',
        'MainUseCdName': '주_용도_코드_명',
        'EtcUse': '기타_용도',
        'UnitCnt': '세대_수',
        'HouseholdCnt': '가구_수',
        'MainBuildingCnt': '주_건축물_수',
        'SubBuildingCnt': '부속_건축물_수',
        'SubBuildingArea': '부속_건축물_면적',
        'TotalParkingCnt': '총_주차_수',
        'IndoorMechanicalParkingCnt': None,  # '옥내_기계식_대수' intentionally excluded
        'IndoorMechanicalParkingArea': '옥내_기계식_면적',
        'OutdoorMechanicalParkingCnt': '옥외_기계식_대수',
        'OutdoorMechanicalParkingArea': '옥외_기계식_면적',
        'IndoorSelfParkingCnt': '옥내_자주식_대수',
        'IndoorSelfParkingArea': '옥내_자주식_면적',
        'OutdoorSelfParkingCnt': '옥외_자주식_대수',
        'OutdoorSelfParkingArea': '옥외_자주식_면적',
        'PermitDate': '허가_일',
        'CommencementDate': '착공_일',
        'UsePermitDate': '사용승인_일',
        'PermitNumYear': '허가번호_년',
        'PermitNumAgencyCd': '허가번호_기관_코드',
        'PermitNumAgencyCdName': '허가번호_기관_코드_명',
        'PermitNumCd': '허가번호_구분_코드',
        'PermitNumCdName': '허가번호_구분_코드_명',
        'HoCnt': '호_수',
        'EnergyEfficiencyGrade': '에너지효율_등급',
        'EnergySavingRatio': '에너지절감_율',
        'EPIScore': '에너지_EPI점수',
        'GreenBuildingGrade': '친환경_건축물_등급',
        'GreenBuildingScore': '친환경_건축물_인증점수',
        'SmartBuildingGrade': '지능형_건축물_등급',
        'SmartBuildingScore': '지능형_건축물_인증점수',
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

    # using INSERT without column names — this requires destination table
    # column order/structure to match the values provided below.
    insert_sql = f"INSERT INTO {dst_table} VALUES ({placeholders})"

    print("Insert SQL 준비 완료")

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
                new_row_list.append(None)
            else:
                new_row_list.append(src_values.get(src_col))

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
