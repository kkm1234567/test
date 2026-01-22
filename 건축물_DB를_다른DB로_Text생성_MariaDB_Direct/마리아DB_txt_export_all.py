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
OUTPUT_DIR = r"C:\PTR\Prime\Collect\CollectApi\storage\hub_go_kr\tCollectorBuildingExclusiveUse\202009"
BATCH_SIZE = 5000

# 테이블 매핑: MariaDB 테이블 → txt 파일명
TABLE_MAP = {
    # "eais_01_기본개요_bcp": "건축물대장_기본개요.txt",
    # "eais_02_총괄표제부_bcp": "건축물대장_총괄표제.txt",
    # "eais_03_표제부_bcp": "건축물대장_표제부.txt",
    # "eais_04_층별개요_bcp": "건축물대장_층별개요.txt",
    # "eais_09_부속지번_bcp": "건축물대장_부속지번.txt",
    "eais_05_전유부_bcp": "건축물대장_전유부.txt",
    # "eais_06_전유공용면적_bcp": "건축물대장_전유공용면적.txt",
    # "eais_10_주택가격_bcp": "건축물대장_주택가격.txt",
    # "eais_07_오수정화시설_bcp": "건축물대장_오수정화시설.txt",
    # "eais_08_지역지구구역_bcp": "건축물대장_지역지구구역.txt",
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
    

    # 테이블 컬럼 정보 조회 및 숫자형 컬럼 인덱스 추출
    cursor.execute(f"DESCRIBE `{table_name}`")
    desc_rows = cursor.fetchall()
    columns = [row[0] for row in desc_rows]
    print(f"컬럼 수: {len(columns)}, 컬럼: {columns}")

    # 숫자형 타입 패턴
    numeric_types = ("int", "float", "double", "decimal", "numeric", "real", "tinyint", "smallint", "mediumint", "bigint")
    numeric_indexes = [i for i, row in enumerate(desc_rows) if any(t in row[1].lower() for t in numeric_types)]

    target_col = "도로명_대지_위치"
    target_idx = columns.index(target_col) if target_col in columns else None
    
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
                row_list = ["" if v is None else str(v) for v in row]

                # '도로명_대지_위치' 값은 항상 앞에 공백을 하나 둔다
                if target_idx is not None:
                    val = row_list[target_idx]
                    if not val.startswith(" "):
                        row_list[target_idx] = " " + val

                # 모든 숫자형 컬럼에 대해 float 변환 후 소수점 4자리까지, 불필요한 0 제거
                for idx in numeric_indexes:
                    try:
                        num_val = float(row_list[idx])
                        row_list[idx] = ('{:.4f}'.format(num_val)).rstrip('0').rstrip('.')
                    except Exception:
                        pass  # 변환 실패시 원본값 유지

                line = "|".join(row_list)
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
