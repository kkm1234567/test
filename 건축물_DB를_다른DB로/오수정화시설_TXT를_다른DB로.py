import MySQLdb
import time
import os

# ---------------------------------------
# 1) 설정
# ---------------------------------------
DST_TABLE = "tCollectorBuildingSewageTreatmentPlant"

TXT_FILE = r"C:\PTR\Prime\Collect\CollectApi\storage\hub_go_kr\tCollectorBuildingSewageTreatmentPlant\202009\org.txt"

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
    파일 형식:
    [0]관리_건축물대장_PK | [1]HashAll | [2]HashKey | [3]관리건축물PK | [4]대지_위치 | [5]도로명_대지_위치 | ... | [마지막]DuplicationCnt
    
    DB 컬럼 순서 (예상):
    BuildingLedgerCd                ← [3] 관리건축물PK
    BuildingSewageTreatmentPlantCd  ← [1] HashAll
    LotLocation                     ← [4] 대지_위치
    ...
    DuplicationRecodeCnt            ← 마지막 필드
    CreateDateTime                  ← 현재 시간
    OriginDocumentMonth             ← '202009'
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
            
            # 파일 필드 매핑:
            # [0] 관리_건축물대장_PK (중복)
            # [1] HashAll
            # [2] HashKey (사용 안함)
            # [3] 관리건축물PK (실제 사용)
            # [4]~ 나머지 데이터
            # [마지막] DuplicationRecodeCnt
            
            if len(parts) < 10:  # 최소 필드 수 체크
                if errors < 5:
                    print(f"라인 {line_num}: 필드 부족 ({len(parts)}개)")
                errors += 1
                continue
            
            try:
                # 파일 필드:
                # [0] 관리PK (중복, 사용안함)
                # [1] HashAll
                # [2] HashKey (사용안함)
                # [3] 관리건축물PK
                # [4]~[30] 데이터 필드들 (대장구분코드 ~ 용량루베)
                # [31] 생성일자 (CreateDate)
                # [32] DuplicationCnt
                
                hash_all = parts[1]            # HashAll
                building_ledger_cd = parts[3]  # 관리건축물PK
                
                # 데이터 필드: parts[4]~parts[30] (27개)
                data_fields = parts[4:31]
                
                # 생성일자: parts[31]
                create_date = parts[31] if len(parts) > 31 else ''
                
                # DuplicationCnt: parts[32] (마지막)
                dup_cnt = parts[32] if len(parts) > 32 else parts[-1]
                
                # DB row 구성:
                # 0: BuildingLedgerCd
                # 1: BuildingSewageTreatmentPlantCd (HashAll)
                # 2~28: data_fields (27개)
                # 29: DuplicationRecodeCnt
                # 30: CreateDate
                # 31: CreateDateTime
                # 32: OriginDocumentMonth
                row = [
                    building_ledger_cd,    # BuildingLedgerCd
                    hash_all,              # BuildingSewageTreatmentPlantCd
                    *data_fields,          # 27개 데이터 필드
                    dup_cnt,               # DuplicationRecodeCnt
                    create_date,           # CreateDate
                    now,                   # CreateDateTime
                    ORIGIN_DOCUMENT_MONTH  # OriginDocumentMonth
                ]
                
                # 컬럼 수 맞추기
                if len(row) != dst_col_count:
                    if errors < 5:
                        print(f"라인 {line_num}: 컬럼 수 불일치 (row={len(row)}, expected={dst_col_count})")
                        print(f"  parts 개수: {len(parts)}")
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
    
    # 남은 batch 처리
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
