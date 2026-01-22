import MySQLdb
import MySQLdb.cursors
import time
import xxhash

# ---------------------------------------
# 1) 대상 테이블
# ---------------------------------------
DST_TABLE = "tCollectorBuildingFloor"

# ---------------------------------------
# 2) DB 연결 설정
# ---------------------------------------
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

# ---------------------------------------
# 3) normalize_key 함수 (텍스트 파일 로직과 동일)
# ---------------------------------------
_CONTROL_MAP = dict.fromkeys(range(0, 32), None)
_CONTROL_MAP[127] = None

def normalize_key(line: str) -> str:
    if line and line[0] == "\ufeff":
        line = line.lstrip("\ufeff")
    return line.translate(_CONTROL_MAP).strip()

# ---------------------------------------
# 4) 1줄 테스트 함수
# ---------------------------------------
def test_hash_one_row(dst_conn, dst_table):
    print(f"\n=== Testing Hash Logic with 1 row from {dst_table} ===")

    dst_cursor = dst_conn.cursor()

    # Discover destination table schema
    dst_cursor.execute(f"DESCRIBE {dst_table}")
    dst_desc = dst_cursor.fetchall()
    dst_col_names = [d[0] for d in dst_desc]
    
    print(f"컬럼 수: {len(dst_col_names)}")
    print(f"컬럼 목록: {dst_col_names}")

    # Read just 1 row
    select_cols_sql = ",".join([f"`{c}`" for c in dst_col_names])
    select_sql = f"SELECT {select_cols_sql} FROM {dst_table} LIMIT 1"
    
    dst_cursor.execute(select_sql)
    row = dst_cursor.fetchone()
    
    if row is None:
        print("데이터 없음!")
        return
    
    print(f"\n=== 원본 데이터 (1행) ===")
    for col_name, val in zip(dst_col_names, row):
        print(f"  {col_name}: {val}")

    # =========================================
    # 방법 1: 기존 DB 로직 (4개 컬럼 제외)
    # =========================================
    excluded_cols = ('BuildingFloorCd', 'DuplicationRecodeCnt', 'CreateDateTime', 'OriginDocumentMonth')
    
    hash_data_db = []
    print(f"\n=== [방법1 - DB로직] 4개 컬럼 제외 ===")
    for col_name, val in zip(dst_col_names, row):
        if col_name not in excluded_cols:
            str_val = str(val) if val is not None else ''
            hash_data_db.append(str_val)
    
    test_count = 1
    hash_str_db = '|'.join(hash_data_db) + '|' + str(test_count)
    hash_db = xxhash.xxh64(hash_str_db.encode('utf-8')).hexdigest()
    print(f"  hash_str 길이: {len(hash_str_db)}")
    print(f"  hash: {hash_db}")

    # =========================================
    # 방법 2: 텍스트 파일 로직 (모든 컬럼 + normalize_key)
    # 텍스트 파일에서 오는 순서대로 컬럼 값을 사용
    # =========================================
    print(f"\n=== [방법2 - 텍스트로직] 모든 컬럼 + normalize_key ===")
    
    # 텍스트 파일의 원본 필드 순서 (33개 컬럼 - BuildingFloorCd, DuplicationRecodeCnt 등은 DB에서 추가됨)
    # 원본 TXT 컬럼 순서대로 가져와야 함
    txt_col_order = [
        'BuildingLedgerCd',      # 관리_건축물대장_PK
        'SiteLocation',          # 대지_위치
        'RoadNameSiteLocation',  # 도로명_대지_위치
        'BuildingName',          # 건물_명
        'SiGunGuCd',             # 시군구_코드
        'LegalDongCd',           # 법정동_코드
        'SiteClsCd',             # 대지_구분_코드
        'Bun',                   # 번
        'Ji',                    # 지
        'SpecialSiteName',       # 특수지_명
        'BlockName',             # 블록
        'LotName',               # 로트
        'NewAddressRoadCd',      # 새주소_도로_코드
        'NewAddressLegalDongCd', # 새주소_법정동_코드
        'NewAddressAboveGroundCd', # 새주소_지상지하_코드
        'NewAddressMainNo',      # 새주소_본_번
        'NewAddressSubNo',       # 새주소_부_번
        'DongName',              # 동_명칭
        'FloorClsCd',            # 층_구분_코드
        'FloorClsCdName',        # 층_구분_코드_명
        'FloorNo',               # 층_번호
        'FloorNoName',           # 층_번호_명
        'StructureCd',           # 구조_코드
        'StructureCdName',       # 구조_코드_명
        'OtherStructure',        # 기타_구조
        'MainUseCd',             # 주_용도_코드
        'MainUseCdName',         # 주_용도_코드_명
        'OtherUse',              # 기타_용도
        'Area',                  # 면적
        'MainSubClsCd',          # 주부속_구분_코드
        'MainSubClsCdName',      # 주부속_구분_코드_명
        'AreaExcludeYn',         # 면적_제외_여부
        'CreateDate',            # 생성_일자
    ]
    
    # DB row에서 해당 컬럼값 추출 (텍스트 파일 순서대로)
    parts = []
    for txt_col in txt_col_order:
        if txt_col in dst_col_names:
            idx = dst_col_names.index(txt_col)
            val = row[idx]
            parts.append(str(val) if val is not None else '')
        else:
            parts.append('')
            print(f"  경고: {txt_col} 컬럼 없음")
    
    # 텍스트 파일 로직: parts + [str(dup_cnt)] → join → normalize_key → hash
    parts_for_hash = parts + [str(test_count)]
    hash_str_txt = '|'.join(parts_for_hash)
    hash_str_txt_normalized = normalize_key(hash_str_txt)
    hash_txt = xxhash.xxh64(hash_str_txt_normalized.encode('utf-8')).hexdigest()
    
    print(f"  hash_str 길이: {len(hash_str_txt)}")
    print(f"  normalized 길이: {len(hash_str_txt_normalized)}")
    print(f"  hash: {hash_txt}")
    
    # =========================================
    # 비교
    # =========================================
    print(f"\n=== 비교 ===")
    print(f"  방법1 (DB로직):     {hash_db}")
    print(f"  방법2 (텍스트로직): {hash_txt}")
    print(f"  일치 여부: {hash_db == hash_txt}")
    
    if hash_db != hash_txt:
        print(f"\n=== 차이점 분석 ===")
        print(f"  hash_str_db:  '{hash_str_db[:100]}...'")
        print(f"  hash_str_txt: '{hash_str_txt_normalized[:100]}...'")

    dst_cursor.close()
    print("\n=== 테스트 완료 ===")

# ---------------------------------------
# 4) main
# ---------------------------------------
if __name__ == "__main__":
    print("DB 연결 중...")
    dst_conn = MySQLdb.connect(**DST_DB)
    
    test_hash_one_row(dst_conn, DST_TABLE)
    
    dst_conn.close()
