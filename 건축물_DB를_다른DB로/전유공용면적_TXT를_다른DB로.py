# -*- coding: utf-8 -*-
"""
전유공용면적 TXT 파일을 MySQL DB로 이관하는 스크립트
TXT 파일 형식: PK|HashAll|HashKey|BuildingLedgerCd|data...|CreateDate|DupCnt
"""

import MySQLdb
import time
import os
from datetime import datetime

# ---------------------------------------
# 1) 설정
# ---------------------------------------
TXT_FILE = r"C:\PTR\Prime\Collect\CollectApi\storage\hub_go_kr\tCollectorBuildingUseArea\202009\org.txt"
DST_TABLE = "tCollectorBuildingUseArea"
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

BATCH_SIZE = 1000  # 서버 wait_timeout이 30초라서 작게 설정
MAX_RETRIES = 3
RETRY_DELAY = 3

# ---------------------------------------
# 2) 파일 → DB 삽입 함수
# ---------------------------------------
def load_txt_to_db(txt_file, dst_table):
    print(f"\n=== Loading {txt_file} → {dst_table} ===")
    
    if not os.path.exists(txt_file):
        print(f"파일 없음: {txt_file}")
        return
    
    dst_conn = MySQLdb.connect(**DST_DB)
    dst_cursor = dst_conn.cursor()
    
    # 대상 테이블 스키마 확인
    dst_cursor.execute(f"DESCRIBE {dst_table}")
    dst_desc = dst_cursor.fetchall()
    dst_col_names = [d[0] for d in dst_desc]
    dst_col_count = len(dst_col_names)
    
    print(f"대상 테이블 컬럼 수: {dst_col_count}")
    
    # INSERT SQL 준비
    placeholders = ",".join(["%s"] * dst_col_count)
    dst_col_list_sql = ",".join([f"`{c}`" for c in dst_col_names])
    insert_sql = f"INSERT INTO {dst_table} ({dst_col_list_sql}) VALUES ({placeholders})"
    
    print(f"INSERT SQL 준비 완료")
    
    """
    파일 형식 (40개 파트):
    [0] PK (미사용)
    [1] HashAll → BuildingUseAreaCd
    [2] HashKey (미사용)
    [3] BuildingLedgerCd
    [4]~[37] 데이터필드 (34개)
    [38] CreateDate
    [39] DuplicationRecodeCnt
    
    DB 컬럼 (43개):
    [0]  BuildingLedgerCd
    [1]  BuildingUseAreaCd (HashAll)
    [2]~[35] 데이터필드 (34개)
    [36] Area (면적 - 소수점 처리 필요)
    [37] DuplicationRecodeCnt
    [38] CreateDate
    [39] CreateDateTime
    [40] OriginDocumentMonth
    """
    
    # 파일 라인 수 확인
    with open(txt_file, 'r', encoding='utf-8') as f:
        total_lines = sum(1 for _ in f)
    print(f"총 {total_lines:,} 라인")
    
    batch = []
    inserted = 0
    errors = 0
    start = time.time()
    now = datetime.now()
    
    with open(txt_file, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.rstrip('\r\n')
            if not line:
                continue
            
            parts = line.split('|')
            
            if len(parts) < 43:
                if errors < 5:
                    print(f"라인 {line_num}: 필드 부족 ({len(parts)}개)")
                errors += 1
                continue
            
            try:
                hash_all = parts[1]            # HashAll
                building_ledger_cd = parts[3]  # BuildingLedgerCd
                
                # 데이터 필드: parts[4]~parts[40] (37개)
                data_fields = parts[4:41]
                
                # CreateDate: parts[41]
                create_date = parts[41]
                
                # DuplicationRecodeCnt: parts[42]
                dup_cnt = parts[42]
                try:
                    dup_cnt_int = int(dup_cnt) if dup_cnt else 0
                except ValueError:
                    dup_cnt_int = 0
                
                # 숫자 변환이 필요한 필드들
                # data_fields 인덱스 기준:
                # [18] NewAddressMainLotNum (parts[22])
                # [19] NewAddressSubLotNum (parts[23])  
                # [24] FloorNum (parts[28])
                # [36] Area (parts[40])
                
                # NewAddressMainLotNum - data_fields[18]
                try:
                    new_addr_main = int(data_fields[18]) if data_fields[18] else 0
                except ValueError:
                    new_addr_main = 0
                
                # NewAddressSubLotNum - data_fields[19]
                try:
                    new_addr_sub = int(data_fields[19]) if data_fields[19] else 0
                except ValueError:
                    new_addr_sub = 0
                
                # FloorNum - data_fields[24]
                try:
                    floor_num = int(data_fields[24]) if data_fields[24] else 0
                except ValueError:
                    floor_num = 0
                
                # Area - data_fields[36] (소수점 처리)
                try:
                    area_str = data_fields[36]
                    if area_str.startswith('.'):
                        area_str = '0' + area_str
                    area_val = int(float(area_str) * 100) if area_str else 0
                except ValueError:
                    area_val = 0
                
                # DB row 구성 (43개):
                # DB 컬럼 순서에 맞춰 매핑
                row = [
                    building_ledger_cd,      # [0] BuildingLedgerCd
                    hash_all,                # [1] BuildingUseAreaCd
                    data_fields[0],          # [2] LedgerType
                    data_fields[1],          # [3] LedgerTypeName
                    data_fields[2],          # [4] LedgerClassType
                    data_fields[3],          # [5] LedgerClassTypeName
                    data_fields[4],          # [6] LotLocation
                    data_fields[5],          # [7] RoadNameLotLocation
                    data_fields[6],          # [8] BuildingName
                    data_fields[7],          # [9] SiGuGunCd
                    data_fields[8],          # [10] LegalDongCd
                    data_fields[9],          # [11] LotType
                    data_fields[10],         # [12] MainLotNum
                    data_fields[11],         # [13] SubLotNum
                    data_fields[12],         # [14] SpecialLotName
                    data_fields[13],         # [15] Block
                    data_fields[14],         # [16] Lot
                    data_fields[15],         # [17] NewAddressRoadNameCd
                    data_fields[16],         # [18] NewAddressLegalDongCd
                    data_fields[17],         # [19] NewAddressFloorCd
                    new_addr_main,           # [20] NewAddressMainLotNum (int) - data_fields[18]
                    new_addr_sub,            # [21] NewAddressSubLotNum (int) - data_fields[19]
                    data_fields[20],         # [22] DongName
                    data_fields[21],         # [23] HoName
                    data_fields[22],         # [24] FloorCd
                    data_fields[23],         # [25] FloorCdName
                    floor_num,               # [26] FloorNum (int) - data_fields[24]
                    data_fields[25],         # [27] UseAreaType
                    data_fields[26],         # [28] UseAreaTypeName
                    data_fields[27],         # [29] MainSubFlag
                    data_fields[28],         # [30] MainSubFlagName
                    data_fields[29],         # [31] FloorNumName
                    data_fields[30],         # [32] BuildingStructureCd
                    data_fields[31],         # [33] BuildingStructureCdName
                    data_fields[32],         # [34] EtcBuildingStructure
                    data_fields[33],         # [35] MainUseCd
                    data_fields[34],         # [36] MainUseCdName
                    data_fields[35],         # [37] EtcUse
                    area_val,                # [38] Area (int) - data_fields[36]
                    dup_cnt_int,             # [39] DuplicationRecodeCnt
                    create_date,             # [40] CreateDate
                    now,                     # [41] CreateDateTime
                    ORIGIN_MONTH,            # [42] OriginDocumentMonth
                ]
                
                if len(row) != dst_col_count:
                    if errors < 5:
                        print(f"라인 {line_num}: 컬럼 수 불일치 (row={len(row)}, expected={dst_col_count})")
                    errors += 1
                    continue
                
                batch.append(tuple(row))
                
                if len(batch) >= BATCH_SIZE:
                    batch_success = False
                    for attempt in range(1, MAX_RETRIES + 1):
                        try:
                            dst_conn.ping(True)
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
                            print(f"ERROR at line {line_num}: {e}")
                            break
                    
                    if not batch_success:
                        errors += len(batch)
                    
                    batch.clear()
                    
            except Exception as e:
                if errors < 5:
                    print(f"라인 {line_num} 오류: {e}")
                errors += 1
    
    # 남은 batch 처리
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
                break
    
    elapsed = time.time() - start
    print(f"\n완료! {inserted:,} rows 삽입 ({elapsed:.2f}초)")
    print(f"오류: {errors:,}건")
    
    dst_cursor.close()
    dst_conn.close()

# ---------------------------------------
# 3) main
# ---------------------------------------
if __name__ == "__main__":
    print("=== 전유공용면적 TXT → DB 이관 시작 ===")
    load_txt_to_db(TXT_FILE, DST_TABLE)
    print("\n=== 완료 ===")
