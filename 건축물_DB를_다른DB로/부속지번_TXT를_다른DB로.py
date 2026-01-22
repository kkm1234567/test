# -*- coding: utf-8 -*-
"""
부속지번 TXT 파일을 MySQL DB로 이관하는 스크립트
TXT 파일 형식: PK|HashAll|HashKey|BuildingLedgerCd|data...|CreateDate|DupCnt
"""

import MySQLdb
import time
import traceback
from datetime import datetime

# ---------------------------------------
# 1) 설정
# ---------------------------------------
TXT_FILE = r"C:\PTR\Prime\Collect\CollectApi\storage\hub_go_kr\tCollectorBuildingSubLot\202009\org.txt"
DST_TABLE = "tCollectorBuildingSubLot"
ORIGIN_MONTH = "202009"

DST_DB = dict(
    host="192.168.10.244",
    user="DB_WO_BUNNY",
    password="Thrkflxkd72&",
    database="dbDwCollectorBuilding",
    charset="utf8mb4"
)

BATCH_SIZE = 5000
MAX_RETRIES = 3
RETRY_DELAY = 5

# ---------------------------------------
# 2) 대상 테이블 컬럼 (37개)
# ---------------------------------------
DST_COLUMNS = [
    "BuildingLedgerCd",        # parts[3]
    "BuildingSubLotCd",        # parts[2] (HashKey)
    "LedgerType",              # parts[4]
    "LedgerTypeName",          # parts[5]
    "LedgerClassType",         # parts[6]
    "LedgerClassTypeName",     # parts[7]
    "LotLocation",             # parts[8]
    "RoadNameLotLocation",     # parts[9]
    "BuildingName",            # parts[10]
    "SiGuGunCd",               # parts[11]
    "LegalDongCd",             # parts[12]
    "LotType",                 # parts[13]
    "MainLotNum",              # parts[14]
    "SubLotNum",               # parts[15]
    "SpecialLotName",          # parts[16]
    "Block",                   # parts[17]
    "Lot",                     # parts[18]
    "NewAddressRoadNameCd",    # parts[19]
    "NewAddressLegalDongCd",   # parts[20]
    "NewAddressFloorCd",       # parts[21]
    "NewAddressMainLotNum",    # parts[22]
    "NewAddressSubLotNum",     # parts[23]
    "SubLedgerTypeCd",         # parts[24]
    "SubLedgerTypeCdName",     # parts[25]
    "SubSiGuGunCd",            # parts[26]
    "SubLegalDongCd",          # parts[27]
    "SubLotType",              # parts[28]
    "SubMainLotNum",           # parts[29]
    "SubSubLotNum",            # parts[30]
    "SubSpecialLotName",       # parts[31]
    "SubBlock",                # parts[32]
    "SubLot",                  # parts[33]
    "SubEtcLot",               # parts[34]
    "DuplicationRecodeCnt",    # parts[37]
    "CreateDate",              # parts[36]
    "CreateDateTime",          # 현재 시간
    "OriginDocumentMonth",     # ORIGIN_MONTH
]

# ---------------------------------------
# 3) TXT 파싱 함수
# ---------------------------------------
def parse_line(line):
    """
    TXT 한 줄을 파싱하여 DB INSERT용 튜플 반환
    
    형식: PK|HashAll|HashKey|BuildingLedgerCd|data...|CreateDate|DupCnt
    
    parts[0] = PK (사용 안함)
    parts[1] = HashAll -> BuildingSubLotCd
    parts[2] = HashKey (사용 안함)
    parts[3] = BuildingLedgerCd
    parts[4:35] = 31개 데이터 필드 (LedgerType ~ SubEtcLot)
    parts[35] = CreateDate
    parts[36] = DuplicationRecodeCnt
    """
    parts = line.rstrip('\n').split('|')
    
    # 최소 필드 수 체크 (총 37개 파트)
    if len(parts) < 37:
        return None
    
    building_ledger_cd = parts[3]
    building_sub_lot_cd = parts[1]  # HashAll 사용
    
    # 데이터 필드 (31개): parts[4] ~ parts[34]
    data_fields = parts[4:35]
    
    # CreateDate와 DuplicationRecodeCnt
    create_date = parts[35]
    dup_cnt = parts[36]
    
    # DuplicationRecodeCnt 변환
    try:
        dup_cnt_int = int(dup_cnt) if dup_cnt else 0
    except ValueError:
        dup_cnt_int = 0
    
    # 현재 시간
    now = datetime.now()
    
    # 최종 row 구성 (37개 컬럼)
    row = (
        building_ledger_cd,      # BuildingLedgerCd
        building_sub_lot_cd,     # BuildingSubLotCd
        *data_fields,            # 31개 데이터 필드
        dup_cnt_int,             # DuplicationRecodeCnt
        create_date,             # CreateDate
        now,                     # CreateDateTime
        ORIGIN_MONTH,            # OriginDocumentMonth
    )
    
    return row

# ---------------------------------------
# 4) 메인 이관 함수
# ---------------------------------------
def migrate_txt_to_db():
    print(f"=== {TXT_FILE} → {DST_TABLE} 이관 시작 ===")
    
    # DB 연결
    dst_conn = MySQLdb.connect(**DST_DB)
    dst_cursor = dst_conn.cursor()
    
    # INSERT SQL 생성
    col_list = ",".join([f"`{c}`" for c in DST_COLUMNS])
    placeholders = ",".join(["%s"] * len(DST_COLUMNS))
    insert_sql = f"INSERT INTO {DST_TABLE} ({col_list}) VALUES ({placeholders})"
    
    print(f"INSERT SQL: {insert_sql}")
    print(f"컬럼 수: {len(DST_COLUMNS)}")
    
    # 파일 읽기 및 이관
    batch = []
    inserted = 0
    skipped = 0
    start = time.time()
    
    # 총 라인 수 계산
    print("파일 라인 수 계산 중...")
    with open(TXT_FILE, 'r', encoding='utf-8') as f:
        total_lines = sum(1 for _ in f)
    print(f"총 {total_lines:,} 라인")
    
    with open(TXT_FILE, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            if not line.strip():
                skipped += 1
                continue
            
            row = parse_line(line)
            if row is None:
                skipped += 1
                continue
            
            # 컬럼 수 검증
            if len(row) != len(DST_COLUMNS):
                print(f"WARN: 라인 {line_num} 컬럼 수 불일치 - {len(row)} vs {len(DST_COLUMNS)}")
                skipped += 1
                continue
            
            batch.append(row)
            
            if len(batch) >= BATCH_SIZE:
                try:
                    dst_cursor.executemany(insert_sql, batch)
                    dst_conn.commit()
                    inserted += len(batch)
                    print(f"  → {inserted:,}/{total_lines:,} inserted ({100*inserted/total_lines:.1f}%)")
                except Exception as e:
                    print(f"ERROR at batch ending line {line_num}: {e}")
                    traceback.print_exc()
                    # 개별 삽입 시도
                    for i, r in enumerate(batch):
                        try:
                            dst_cursor.execute(insert_sql, r)
                            dst_conn.commit()
                            inserted += 1
                        except Exception as e2:
                            print(f"  개별 삽입 실패: {e2}")
                            skipped += 1
                batch.clear()
    
    # 남은 배치 처리
    if batch:
        try:
            dst_cursor.executemany(insert_sql, batch)
            dst_conn.commit()
            inserted += len(batch)
        except Exception as e:
            print(f"ERROR at final batch: {e}")
            traceback.print_exc()
    
    elapsed = time.time() - start
    print(f"\n=== 완료 ===")
    print(f"총 {inserted:,} rows 이관됨")
    print(f"스킵: {skipped:,} rows")
    print(f"소요 시간: {elapsed:.2f}초")
    
    dst_cursor.close()
    dst_conn.close()

# ---------------------------------------
# 5) main
# ---------------------------------------
if __name__ == "__main__":
    migrate_txt_to_db()
