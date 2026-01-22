import MySQLdb
import MySQLdb.cursors
import time
from datetime import datetime

# ---------------------------------------
# 1) 테이블 매핑
# ---------------------------------------
TABLE_MAP = {
    "eais_01_기본개요_bcp": "tCollectorBuilding",
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
# 3) SRC → DEST 컬럼 정의
#    (관리_상위_건축물대장_PK 제외)
# ---------------------------------------
SELECT_COLS = [
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
    "관리_상위_건축물대장_PK",
    "특수지_명",
    "블록",
    "로트",
    "외필지_수",
    "새주소_도로_코드",
    "새주소_법정동_코드",
    "새주소_지상지하_코드",
    "새주소_본_번",
    "새주소_부_번",
    "지역_코드",
    "지구_코드",
    "구역_코드",
    "지역_코드_명",
    "지구_코드_명",
    "구역_코드_명",
    "생성_일자"
]

DEST_COL_COUNT = len(SELECT_COLS)  # 29

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

    # SRC 스트리밍 커서
    src_cursor = src_conn.cursor(MySQLdb.cursors.SSCursor)
    select_sql = f"""
        SELECT {",".join(SELECT_COLS)}
        FROM {src_table}
    """
    src_cursor.execute(select_sql)

    dst_cursor = dst_conn.cursor()

    # INSERT SQL
    # destination columns must match the order and count of SELECT_COLS + 2 extra columns
    dest_cols = [
        "BuildingLedgerCd",
        "LedgerType",
        "LedgerTypeName",
        "LedgerClassType",
        "LedgerClassTypeName",
        "LotLocation",
        "RoadNameLotLocation",
        "BuildingName",
        "SiGuGunCd",
        "LegalDongCd",
        "LotType",
        "MainLotNum",
        "SubLotNum",
        "ParentBuildingLedgerCd",
        "SpecialLotName",
        "Block",
        "Lot",
        "EtcLotCnt",
        "NewAddressRoadNameCd",
        "NewAddressLegalDongCd",
        "NewAddressFloorCd",
        "NewAddressMainLotNum",
        "NewAddressSubLotNum",
        "RegionCd",
        "DistrictCd",
        "SectionCd",
        "RegionCdName",
        "DistrictCdName",
        "SectionCdName",
        "CreateDate",
        "CreateDateTime",
        "OriginDocumentDate"
    ]

    column_sql = ",".join(dest_cols)
    placeholders = ",".join(["%s"] * len(dest_cols))

    insert_sql = f"""
        INSERT INTO {dst_table} ({column_sql})
        VALUES ({placeholders})
    """

    print("Insert SQL 준비 완료")

    batch = []
    inserted = 0
    start = time.time()

    while True:
        row = src_cursor.fetchone()
        if row is None:
            break

        # row는 원본 29개 컬럼, CreateDateTime과 OriginDocumentDate 추가 필요
        row_list = list(row)
        
        # CreateDateTime: 현재 시간
        row_list.append(datetime.now())
        
        # OriginDocumentDate: 202009로 고정
        row_list.append('202009')
        
        batch.append(tuple(row_list))

        if len(batch) >= BATCH_SIZE:
            # validate batch rows have expected column count
            expected = len(dest_cols)
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
        expected = len(dest_cols)
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
