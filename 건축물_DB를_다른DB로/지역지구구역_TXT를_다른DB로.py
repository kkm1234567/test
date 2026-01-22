import MySQLdb
import time
import os

# ---------------------------------------
# 1) 설정
# ---------------------------------------
DST_TABLE = "tCollectorBuildingRegionDistrictSection"

TXT_FILE = r"C:\PTR\Prime\Collect\CollectApi\storage\hub_go_kr\tCollectorBuildingRegionDistrictSection\202009\org.txt"

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

BATCH_SIZE = 5000
ORIGIN_DOCUMENT_MONTH = "202009"

# ---------------------------------------
# 2) 파일 → DB 삽입 함수
# ---------------------------------------
def load_txt_to_db(dst_conn, txt_file, dst_table):
    print(f"\n=== Loading {txt_file} → {dst_table} ===")
    
    if not os.path.exists(txt_file):
        print(f"파일 없음: {txt_file}")
        return
    
    dst_cursor = dst_conn.cursor()
    
    # 대상 테이블 스키마 확인
    dst_cursor.execute(f"DESCRIBE {dst_table}")
    dst_desc = dst_cursor.fetchall()
    dst_col_names = [d[0] for d in dst_desc]
    dst_col_count = len(dst_col_names)
    
    print(f"대상 테이블 컬럼 수: {dst_col_count}")
    print(f"컬럼 목록: {dst_col_names}")
    
    # INSERT SQL 준비
    placeholders = ",".join(["%s"] * dst_col_count)
    dst_col_list_sql = ",".join([f"`{c}`" for c in dst_col_names])
    insert_sql = f"INSERT INTO {dst_table} ({dst_col_list_sql}) VALUES ({placeholders})"
    
    print(f"INSERT SQL: {insert_sql[:100]}...")
    
    """
    파일 형식 (22개 필드):
    [0]  관리PK (사용안함)
    [1]  HashAll → BuildingRegionDistrictSectionCd
    [2]  HashKey (사용안함)
    [3]  관리건축물PK → BuildingLedgerCd
    [4]~[19] 데이터필드 (16개): 대지위치~기타
    [20] 생성일자 → CreateDate
    [21] DupCnt → DuplicationRecodeCnt
    
    DB 컬럼 (22개):
    [0]  BuildingLedgerCd
    [1]  BuildingRegionDistrictSectionCd (HashAll)
    [2]~[17] 데이터필드 (16개)
    [18] DuplicationRecodeCnt
    [19] CreateDate
    [20] CreateDateTime
    [21] OriginDocumentMonth
    """
    
    # 파일 라인 수 확인
    with open(txt_file, 'r', encoding='utf-8') as f:
        total_lines = sum(1 for _ in f)
    print(f"총 {total_lines:,} 라인")
    
    batch = []
    inserted = 0
    errors = 0
    start = time.time()
    
    from datetime import datetime
    now = datetime.now()
    
    with open(txt_file, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.rstrip('\r\n')
            if not line:
                continue
            
            parts = line.split('|')
            
            if len(parts) < 22:
                if errors < 5:
                    print(f"라인 {line_num}: 필드 부족 ({len(parts)}개)")
                errors += 1
                continue
            
            try:
                # 파일 필드 매핑:
                # [0] 관리PK (사용안함)
                # [1] HashAll
                # [2] HashKey (사용안함)
                # [3] 관리건축물PK
                # [4]~[19] 데이터필드 (16개)
                # [20] 생성일자
                # [21] DupCnt
                
                hash_all = parts[1]            # HashAll
                building_ledger_cd = parts[3]  # 관리건축물PK
                
                # 데이터 필드: parts[4]~parts[19] (16개)
                data_fields = parts[4:20]
                
                # 생성일자: parts[20]
                create_date = parts[20] if len(parts) > 20 else ''
                
                # DuplicationCnt: parts[21]
                dup_cnt = parts[21] if len(parts) > 21 else parts[-1]
                
                # DB row 구성 (22개):
                # [0]  BuildingLedgerCd
                # [1]  BuildingRegionDistrictSectionCd (HashAll)
                # [2]~[17] data_fields (16개)
                # [18] DuplicationRecodeCnt
                # [19] CreateDate
                # [20] CreateDateTime
                # [21] OriginDocumentMonth
                row = [
                    building_ledger_cd,    # BuildingLedgerCd
                    hash_all,              # BuildingRegionDistrictSectionCd
                    *data_fields,          # 16개 데이터 필드
                    dup_cnt,               # DuplicationRecodeCnt
                    create_date,           # CreateDate
                    now,                   # CreateDateTime
                    ORIGIN_DOCUMENT_MONTH  # OriginDocumentMonth
                ]
                
                # 컬럼 수 검증
                if len(row) != dst_col_count:
                    if errors < 5:
                        print(f"라인 {line_num}: 컬럼 수 불일치 (row={len(row)}, expected={dst_col_count})")
                        print(f"  parts 개수: {len(parts)}, data_fields: {len(data_fields)}")
                    errors += 1
                    continue
                
                batch.append(tuple(row))
                
                if len(batch) >= BATCH_SIZE:
                    dst_cursor.executemany(insert_sql, batch)
                    dst_conn.commit()
                    inserted += len(batch)
                    print(f"  → {inserted:,}/{total_lines:,} inserted")
                    batch.clear()
                    
            except Exception as e:
                if errors < 5:
                    print(f"라인 {line_num} 오류: {e}")
                errors += 1
    
    if batch:
        dst_cursor.executemany(insert_sql, batch)
        dst_conn.commit()
        inserted += len(batch)
    
    elapsed = time.time() - start
    print(f"\n완료! {inserted:,} rows 삽입 ({elapsed:.2f}초)")
    print(f"오류: {errors:,}건")
    
    dst_cursor.close()

# ---------------------------------------
# 3) main
# ---------------------------------------
if __name__ == "__main__":
    print("DB 연결 중...")
    dst_conn = MySQLdb.connect(**DST_DB)
    
    load_txt_to_db(dst_conn, TXT_FILE, DST_TABLE)
    
    dst_conn.close()
    print("\n=== 완료 ===")
