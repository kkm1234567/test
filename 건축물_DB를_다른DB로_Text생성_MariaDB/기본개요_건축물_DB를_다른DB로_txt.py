import os
import time
import pyodbc


# ---------------------------------------
# 설정
# ---------------------------------------
MSSQL_SERVER = "localhost"  # 필요 시 ".\\SQLEXPRESS" 등으로 변경
MSSQL_DATABASE = "EAIS_202104"
TABLE_NAME = "건축물대장_기본개요"
OUTPUT_DIR = r"C:\PTR\Prime\Collect\CollectApi\storage"
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
    cursor = conn.cursor()
    # DB에 정의된 컬럼 순서를 그대로 사용 (ORDER BY ORDINAL_POSITION)
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

    col_list = ", ".join([f"[{c}]" for c in columns])
    select_sql = f"SELECT {col_list} FROM [{MSSQL_DATABASE}].[dbo].[{TABLE_NAME}]"

    print(f"쿼리 실행 중... -> {select_sql}")
    cursor.execute(select_sql)

    total_rows = 0
    start = time.time()

    with open(output_path, "w", encoding="utf-8", newline="") as f:
        while True:
            rows = cursor.fetchmany(BATCH_SIZE)
            if not rows:
                break

            lines = []
            for row in rows:
                # None을 빈 문자열로 치환 후 | 로 연결
                line = "|".join(["" if v is None else str(v) for v in row])
                lines.append(line)

            f.write("\n".join(lines))
            f.write("\n")

            total_rows += len(rows)
            if total_rows % LOG_EVERY == 0:
                print(f"  → {total_rows:,} rows written")

    elapsed = time.time() - start
    print(f"완료: {total_rows:,} rows written to {output_path} ({elapsed:.2f}초)")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    export_to_txt()
