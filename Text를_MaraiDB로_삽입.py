import pymysql

SRC_DB = dict(
    host="192.168.11.203",
    user="root",
    password="!@Skdud340",
    database="price",
    charset="utf8mb4",
)

FILE_PATH = r"C:\매매\아파트_매매_20260119204929.txt"
TABLE_NAME = "실거래가아파트_매매_web_test"

def load_file_to_db():
    infile = FILE_PATH.replace("\\", "/")  # LOAD DATA는 /가 안전

    conn = pymysql.connect(
        **SRC_DB,
        autocommit=True,
        local_infile=1,
    )

    try:
        with conn.cursor() as cur:
            sql = f"""
LOAD DATA LOCAL INFILE '{infile}'
INTO TABLE `{SRC_DB["database"]}`.`{TABLE_NAME}`
CHARACTER SET utf8mb4
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\\n';
"""
            cur.execute(sql)

        print("✅ LOAD DATA 완료")
    finally:
        conn.close()

if __name__ == "__main__":
    load_file_to_db()
