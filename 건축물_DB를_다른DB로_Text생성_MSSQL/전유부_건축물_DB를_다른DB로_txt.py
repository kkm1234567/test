import os
import time
import pyodbc


MSSQL_SERVER = "localhost"
MSSQL_DATABASE = "EAIS_202104"
TABLE_NAME = "건축물대장_전유부"
OUTPUT_DIR = r"C:\PTR\Prime\Collect\CollectApi\storage\hub_go_kr\202104"
OUTPUT_FILE = "건축물대장_전유부.txt"
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
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_CATALOG = ? AND TABLE_SCHEMA = 'dbo' AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION;
        """,
        MSSQL_DATABASE,
        TABLE_NAME,
    )
    columns = [row[0] for row in cursor.fetchall()]

    target_col = "도로명대지위치"
    target_idx = columns.index(target_col) if target_col in columns else None

    col_list = ", ".join([f"[{c}]" for c in columns])
    select_sql = f"SELECT {col_list} FROM [{MSSQL_DATABASE}].[dbo].[{TABLE_NAME}]"

    print(f"쿼리 실행 중... -> {select_sql}")
    cursor.execute(select_sql)
    cursor.arraysize = BATCH_SIZE

    total_rows = 0
    start = time.time()

    with open(output_path, "w", encoding="utf-8", newline="") as f:
        while True:
            rows = cursor.fetchmany(BATCH_SIZE)
            if not rows:
                break

            for row in rows:
                row_list = ["" if v is None else str(v) for v in row]

                # '도로명대지위치' 값은 항상 앞에 공백을 하나 둔다
                if target_idx is not None:
                    val = row_list[target_idx]
                    if not val.startswith(" "):
                        row_list[target_idx] = " " + val

                f.write("|".join(row_list) + "\n")

            total_rows += len(rows)
            if total_rows % LOG_EVERY == 0:
                print(f"  → {total_rows:,} rows written")

    elapsed = time.time() - start
    print(f"완료: {total_rows:,} rows written to {output_path} ({elapsed:.2f}초)")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    export_to_txt()
