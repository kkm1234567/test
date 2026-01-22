import MySQLdb

DB = dict(
    host="192.168.11.203",
    user="root",
    password="!@Skdud340",
    database="vwrold",
    charset="utf8mb4",
    local_infile=1,   # ✅ 중요
)

INPUT_FILE = r"C:\PTR\Prime\Collect\CollectApi\storage\vworld_kr\t_land_plan\202411\org.txt"
TABLE_NAME = "vworld_t_land_plan_org"

def load_data():
    conn = MySQLdb.connect(**DB)
    cur = conn.cursor()

    # (선택) 로컬 인파일 허용 확인/세팅
    cur.execute("SET SESSION sql_mode=''")  # strict 때문에 실패하는 경우 완화(필요시만)

    sql = f"""
    LOAD DATA LOCAL INFILE %s
    INTO TABLE {TABLE_NAME}
    CHARACTER SET utf8mb4
    FIELDS TERMINATED BY '|'
    LINES TERMINATED BY '\\n'
    (
      column0, column1, column2, column3, column4, column5, column6, column7, column8,
      column9, column10, column11, column12, column13, column14, column15, column16,
      @c17
    )
    SET column17 = NULLIF(@c17, '');
    """

    cur.execute(sql, (INPUT_FILE,))
    conn.commit()

    print("DONE. inserted rows:", cur.rowcount)

    cur.close()
    conn.close()

if __name__ == "__main__":
    load_data()
