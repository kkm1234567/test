import MySQLdb
import MySQLdb.cursors
import os
import time

# -----------------------------------------------
# MariaDB 연결 설정
# -----------------------------------------------
SRC_DB = dict(
    host="192.168.11.203",
    user="root",
    password="!@Skdud340",
    database="buildledger",
    charset="utf8"
)

# 출력 경로
OUTPUT_DIR = r"C:\PTR\Prime\Collect\CollectApi\storage\hub_go_kr\202009"
BATCH_SIZE = 5000

# 테이블 매핑: MariaDB 테이블 → txt 파일명
TABLE_MAP = {
    "eais_01_기본개요_bcp": "건축물대장_기본개요.txt",
    "eais_02_총괄표제부_bcp": "건축물대장_총괄표제.txt",
    "eais_03_표제부_bcp": "건축물대장_표제부.txt",
    "eais_04_층별개요_bcp": "건축물대장_층별개요.txt",
    "eais_05_부속지번_bcp": "건축물대장_부속지번.txt",
    "eais_06_전유부_bcp": "건축물대장_전유부.txt",
    "eais_07_전유공용면적_bcp": "건축물대장_전유공용면적.txt",
    "eais_08_주택가격_bcp": "건축물대장_주택가격.txt",
    "eais_09_오수정화시설_bcp": "건축물대장_오수정화시설.txt",
    "eais_10_지역지구구역_bcp": "건축물대장_지역지구구역.txt",
}


def export_table_to_txt(src_conn, table_name, output_file):
    """MariaDB 테이블을 txt 파일로 내보냄"""
    
    print(f"\n=== Exporting {table_name} → {output_file} ===")
    
    output_path = os.path.join(OUTPUT_DIR, output_file)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # 테이블 행 개수 확인
    cursor = src_conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
    total_rows = cursor.fetchone()[0]
    print(f"총 {total_rows:,} rows 내보내는 중...")
    
    # 테이블 컬럼 정보 조회
    cursor.execute(f"DESCRIBE `{table_name}`")
    columns = [row[0] for row in cursor.fetchall()]
    print(f"컬럼 수: {len(columns)}, 컬럼: {columns}")
    
    # Streaming cursor로 모든 행 읽기
    read_cursor = src_conn.cursor(MySQLdb.cursors.SSCursor)
    select_sql = f"SELECT * FROM `{table_name}`"
    read_cursor.execute(select_sql)
    
    start = time.time()
    total_written = 0
    
    # txt 파일에 쓰기
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        fetch_size = BATCH_SIZE
        while True:
            rows = read_cursor.fetchmany(fetch_size)
            if not rows:
                break
            
            for row in rows:
                # NULL 처리, pipe로 구분
                line = "|".join("" if v is None else str(v) for v in row)
                f.write(line + "\n")
                total_written += 1
            
            if total_written % 100000 == 0:
                print(f"  → {total_written:,}/{total_rows:,} rows written")
    
    read_cursor.close()
    elapsed = time.time() - start
    
    print(f"완료! {total_written:,} rows 저장됨 ({elapsed:.2f}초)")
    print(f"파일: {output_path}")
    
    return total_written


def main():
    """모든 테이블을 txt로 내보냄"""
    
    print("=" * 60)
    print("MariaDB → TXT Export Started")
    print("=" * 60)
    print(f"출력 경로: {OUTPUT_DIR}")
    
    try:
        print("\nMariaDB 연결 중...")
        src_conn = MySQLdb.connect(**SRC_DB)
        print("  → 연결 성공!")
        
        total_exported = 0
        for table_name, output_file in TABLE_MAP.items():
            try:
                count = export_table_to_txt(src_conn, table_name, output_file)
                total_exported += count
            except Exception as e:
                print(f"ERROR: {table_name} 내보내기 실패 - {e}")
                import traceback
                traceback.print_exc()
        
        src_conn.close()
        
        print("\n" + "=" * 60)
        print(f"모든 테이블 내보내기 완료!")
        print(f"총 {total_exported:,} rows 저장됨")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n오류 발생: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
