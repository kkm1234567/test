# -*- coding: utf-8 -*-
"""
주택가격 TXT 파일을 MySQL DB로 이관하는 스크립트
TXT 파일 형식: PK|HashAll|HashKey|BuildingLedgerCd|data...|공시기준일|주택가격|CreateDate|DupCnt
"""

import MySQLdb
import time
import traceback
from datetime import datetime

# ---------------------------------------
# 1) 설정
# ---------------------------------------
TXT_FILE = r"C:\PTR\Prime\Collect\CollectApi\storage\hub_go_kr\tCollectorBuildingHousePrice\202009\org.txt"
DST_TABLE = "tCollectorBuildingHousePrice"
ORIGIN_MONTH = "202009"

DST_DB = dict(
    host="192.168.10.244",
    user="DB_WO_BUNNY",
    password="Thrkflxkd72&",
    database="dbDwCollectorBuilding",
    charset="utf8mb4",
    connect_timeout=100000,
    read_timeout=100000,
    write_timeout=100000,
)

BATCH_SIZE = 5000  # 서버 wait_timeout이 30초라서 줄임
MAX_RETRIES = 3
RETRY_DELAY = 3

# ---------------------------------------
# 2) 대상 테이블 컬럼 (29개)
# ---------------------------------------
DST_COLUMNS = [
    "BuildingLedgerCd",        # parts[3]
    "BuildingHousePriceCd",    # parts[2] (HashKey)
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
    "EtcLotCnt",               # parts[19]
    "NewAddressRoadNameCd",    # parts[20]
    "NewAddressLegalDongCd",   # parts[21]
    "NewAddressFloorCd",       # parts[22]
    "NewAddressMainLotNum",    # parts[23]
    "NewAddressSubLotNum",     # parts[24]
    "HousePrice",              # parts[26]
    "DuplicationRecodeCnt",    # parts[28]
    "CreateDate",              # parts[27]
    "CreateDateTime",          # 현재 시간
    "OriginDocumentDay",       # parts[25]
    "OriginDocumentMonth",     # ORIGIN_MONTH
]

# ---------------------------------------
# 3) TXT 파싱 함수
# ---------------------------------------
def parse_line(line):
    """
    TXT 한 줄을 파싱하여 DB INSERT용 튜플 반환
    
    샘플: 1005129106|60932f08441f9369|c61733e49c361627|1005129106|2|집합|4|전유부|서울특별시...|20190101|372000000|20190528|1
    
    parts[0] = PK (미사용)
    parts[1] = HashAll -> BuildingHousePriceCd
    parts[2] = HashKey (미사용)
    parts[3] = BuildingLedgerCd
    parts[4:25] = 21개 데이터 필드 (LedgerType ~ NewAddressSubLotNum, 빈필드)
    parts[25] = 공시기준일 (OriginDocumentDay)
    parts[26] = 주택가격 (HousePrice)
    parts[27] = CreateDate
    parts[28] = DuplicationRecodeCnt
    """
    parts = line.rstrip('\n').split('|')
    
    # 최소 필드 수 체크 (총 29개 파트)
    if len(parts) < 29:
        return None
    
    building_ledger_cd = parts[3]
    building_house_price_cd = parts[1]  # HashAll 사용
    
    # 데이터 필드 (21개): parts[4:25]
    data_fields = parts[4:25]
    
    # 공시기준일, 주택가격, CreateDate, DuplicationRecodeCnt
    origin_doc_day = parts[25]
    house_price = parts[26]
    create_date = parts[27]
    dup_cnt = parts[28]
    
    # 숫자 변환
    try:
        dup_cnt_int = int(dup_cnt) if dup_cnt else 0
    except ValueError:
        dup_cnt_int = 0
    
    try:
        house_price_int = int(house_price) if house_price else 0
    except ValueError:
        house_price_int = 0
    
    # EtcLotCnt (parts[19]) 숫자 변환
    try:
        etc_lot_cnt = int(data_fields[15]) if data_fields[15] else 0
    except ValueError:
        etc_lot_cnt = 0
    
    # NewAddressMainLotNum, NewAddressSubLotNum 숫자 변환
    try:
        new_addr_main = int(data_fields[19]) if data_fields[19] else 0
    except ValueError:
        new_addr_main = 0
    
    try:
        new_addr_sub = int(data_fields[20]) if data_fields[20] else 0
    except ValueError:
        new_addr_sub = 0
    
    # 현재 시간
    now = datetime.now()
    
    # 최종 row 구성 (29개 컬럼)
    row = (
        building_ledger_cd,      # BuildingLedgerCd
        building_house_price_cd, # BuildingHousePriceCd
        data_fields[0],          # LedgerType
        data_fields[1],          # LedgerTypeName
        data_fields[2],          # LedgerClassType
        data_fields[3],          # LedgerClassTypeName
        data_fields[4],          # LotLocation
        data_fields[5],          # RoadNameLotLocation
        data_fields[6],          # BuildingName
        data_fields[7],          # SiGuGunCd
        data_fields[8],          # LegalDongCd
        data_fields[9],          # LotType
        data_fields[10],         # MainLotNum
        data_fields[11],         # SubLotNum
        data_fields[12],         # SpecialLotName
        data_fields[13],         # Block
        data_fields[14],         # Lot
        etc_lot_cnt,             # EtcLotCnt
        data_fields[16],         # NewAddressRoadNameCd
        data_fields[17],         # NewAddressLegalDongCd
        data_fields[18],         # NewAddressFloorCd
        new_addr_main,           # NewAddressMainLotNum
        new_addr_sub,            # NewAddressSubLotNum
        house_price_int,         # HousePrice
        dup_cnt_int,             # DuplicationRecodeCnt
        create_date,             # CreateDate
        now,                     # CreateDateTime
        origin_doc_day,          # OriginDocumentDay
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
                batch_success = False
                for attempt in range(1, MAX_RETRIES + 1):
                    try:
                        dst_conn.ping(True)  # 연결 확인 및 재연결
                        dst_cursor.executemany(insert_sql, batch)
                        dst_conn.commit()
                        inserted += len(batch)
                        batch_success = True
                        elapsed = time.time() - start
                        speed = inserted / elapsed if elapsed > 0 else 0
                        print(f"  → {inserted:,}/{total_lines:,} inserted ({100*inserted/total_lines:.1f}%) - {speed:.0f} rows/sec")
                        break
                    except MySQLdb.OperationalError as e:
                        code = e.args[0] if e.args else None
                        if code in (2006, 2013) and attempt < MAX_RETRIES:
                            print(f"WARN: 연결 끊김 (code={code}), 재연결 시도 {attempt}/{MAX_RETRIES}")
                            time.sleep(RETRY_DELAY)
                            try:
                                dst_conn = MySQLdb.connect(**DST_DB)
                                dst_cursor = dst_conn.cursor()
                            except Exception:
                                pass
                            continue
                        print(f"ERROR at batch ending line {line_num}: {e}")
                        traceback.print_exc()
                        break
                
                if not batch_success:
                    # 개별 삽입 시도
                    for r in batch:
                        try:
                            dst_conn.ping(True)
                            dst_cursor.execute(insert_sql, r)
                            dst_conn.commit()
                            inserted += 1
                        except Exception as e2:
                            skipped += 1
                
                batch.clear()
    
    # 남은 배치 처리
    if batch:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                dst_conn.ping(True)
                dst_cursor.executemany(insert_sql, batch)
                dst_conn.commit()
                inserted += len(batch)
                break
            except MySQLdb.OperationalError as e:
                code = e.args[0] if e.args else None
                if code in (2006, 2013) and attempt < MAX_RETRIES:
                    print(f"WARN: final batch 연결 끊김, 재연결 시도 {attempt}/{MAX_RETRIES}")
                    time.sleep(RETRY_DELAY)
                    try:
                        dst_conn = MySQLdb.connect(**DST_DB)
                        dst_cursor = dst_conn.cursor()
                    except Exception:
                        pass
                    continue
                print(f"ERROR at final batch: {e}")
                traceback.print_exc()
                break
    
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
