
import os
import time
import pyodbc

MSSQL_SERVER = "localhost"
MSSQL_DATABASE = "EAIS_202104"
TABLE_NAME = "건축물대장_기본개요"
OUTPUT_DIR = r"C:\PTR\Prime\Collect\CollectApi\storage\hub_go_kr\t_collector_building\202104"
OUTPUT_FILE = "건축물대장_기본개요.txt"
BATCH_SIZE = 5000
LOG_EVERY = 100000

def get_connection():
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={MSSQL_SERVER};"
        f"DATABASE={MSSQL_DATABASE};"
        "Trusted_Connection=yes;"
    )
    return pyodbc.connect(conn_str)

def export_to_txt():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)

    conn = get_connection()
    # 컬럼 정보용 커서
    schema_cursor = conn.cursor()
    schema_cursor.execute(
        """
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_CATALOG = ? AND TABLE_SCHEMA = 'dbo' AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION;
        """,
        MSSQL_DATABASE,
        TABLE_NAME,
    )
    desc_rows = schema_cursor.fetchall()
    columns = [row[0] for row in desc_rows]
    numeric_types = ("int", "float", "decimal", "numeric", "real", "smallint", "bigint", "tinyint")
    numeric_indexes = [i for i, row in enumerate(desc_rows) if row[1].lower() in numeric_types]
    target_col = "도로명대지위치"
    target_idx = columns.index(target_col) if target_col in columns else None
    col_list = ", ".join([f"[{c}]" for c in columns])
    select_sql = f"SELECT {col_list} FROM [{MSSQL_DATABASE}].[dbo].[{TABLE_NAME}]"
    print(f"쿼리 실행 중... -> {select_sql}")
    # 데이터 select용 커서
    data_cursor = conn.cursor()
    data_cursor.execute(select_sql)
    data_cursor.arraysize = BATCH_SIZE
    total_rows = 0
    start = time.time()

    with open(output_path, "w", encoding="utf-8", newline="") as f:
        lines = []
        import psutil
        process = psutil.Process(os.getpid())
        while True:
            rows = data_cursor.fetchmany(BATCH_SIZE)
            if not rows:
                break
            for row in rows:
                row_list = ["" if v is None else str(v) for v in row]
                # '도로명대지위치' 값은 항상 앞에 공백을 하나 둔다
                if target_idx is not None:
                    val = row_list[target_idx]
                    if not val.startswith(" "):
                        row_list[target_idx] = " " + val
                # 모든 numeric 컬럼들 처리 (float 변환 후 소수점 아래 0 제거, 0은 '0'으로, 4자리까지)
                for idx in numeric_indexes:
                    try:
                        num_val = row_list[idx]
                        if num_val != "":
                            num_val_f = float(num_val)
                            if num_val_f == 0:
                                row_list[idx] = "0"
                            else:
                                num_val_str = ('{:.4f}'.format(num_val_f)).rstrip('0').rstrip('.')
                                row_list[idx] = num_val_str
                    except Exception as e:
                        print(f"숫자 변환 오류: {num_val} ({e})")
                lines.append("|".join(row_list) + "\n")
            mem_gb = process.memory_info().rss / (1024 ** 3)
            if mem_gb >= 10 * 0.95 or len(lines) > 1000000:
                f.writelines(lines)
                lines.clear()
            total_rows += len(rows)
            if total_rows % LOG_EVERY == 0:
                print(f"  → {total_rows:,} rows processed, 메모리 사용량: {mem_gb:.2f} GB")
        if lines:
            f.writelines(lines)

    elapsed = time.time() - start
    print(f"완료: {total_rows:,} rows written to {output_path} ({elapsed:.2f}초)")
    data_cursor.close()
    schema_cursor.close()
    conn.close()

if __name__ == "__main__":
    export_to_txt()
