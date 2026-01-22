import MySQLdb
import MySQLdb.cursors
import os
import time

SRC_DB = dict(
    host="192.168.11.203",
    user="root",
    password="!@Skdud340",
    database="buildledger",
    charset="utf8"
)
OUTPUT_DIR = r"C:\PTR\Prime\Collect\CollectApi\storage\hub_go_kr\t_collector_building_house_price\202009"
BATCH_SIZE = 5000
TABLE_NAME = "eais_10_주택가격_bcp"
OUTPUT_FILE = "건축물대장_주택가격.txt"

def export_table_to_txt():
    print(f"\n=== Exporting {TABLE_NAME} → {OUTPUT_FILE} ===")
    output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    conn = MySQLdb.connect(**SRC_DB)
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM `{TABLE_NAME}`")
    total_rows = cursor.fetchone()[0]
    print(f"총 {total_rows:,} rows 내보내는 중...")
    cursor.execute(f"DESCRIBE `{TABLE_NAME}`")
    desc_rows = cursor.fetchall()
    columns = [row[0] for row in desc_rows]
    print(f"컬럼 수: {len(columns)}, 컬럼: {columns}")
    numeric_types = ("int", "float", "double", "decimal", "numeric", "real", "tinyint", "smallint", "mediumint", "bigint")
    numeric_indexes = [i for i, row in enumerate(desc_rows) if any(t in row[1].lower() for t in numeric_types)]
    target_col = "도로명_대지_위치"
    target_idx = columns.index(target_col) if target_col in columns else None
    read_cursor = conn.cursor(MySQLdb.cursors.SSCursor)
    select_sql = f"SELECT * FROM `{TABLE_NAME}`"
    read_cursor.execute(select_sql)
    start = time.time()
    total_written = 0
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        fetch_size = BATCH_SIZE
        while True:
            rows = read_cursor.fetchmany(fetch_size)
            if not rows:
                break
            for row in rows:
                row_list = ["" if v is None else str(v) for v in row]
                if target_idx is not None:
                    val = row_list[target_idx]
                    if not val.startswith(" "):
                        row_list[target_idx] = " " + val
                for idx in numeric_indexes:
                    try:
                        num_val = float(row_list[idx])
                        row_list[idx] = ('{:.4f}'.format(num_val)).rstrip('0').rstrip('.')
                    except Exception:
                        pass
                line = "|".join(row_list)
                f.write(line + "\n")
                total_written += 1
            if total_written % 100000 == 0:
                print(f"  → {total_written:,}/{total_rows:,} rows written")
    read_cursor.close()
    elapsed = time.time() - start
    print(f"완료! {total_written:,} rows 저장됨 ({elapsed:.2f}초)")
    print(f"파일: {output_path}")
    conn.close()

if __name__ == "__main__":
    export_table_to_txt()
