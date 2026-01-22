import pyodbc
import os

# ----------------------------------------
# 1) MSSQL 연결 설정
# ----------------------------------------
MSSQL_SERVER = "localhost"
MSSQL_DATABASE = "EAIS_202104"
MSSQL_DRIVER = "{ODBC Driver 17 for SQL Server}"

# 필요시 사용자명/비밀번호 추가
conn_str = f"Driver={MSSQL_DRIVER};Server={MSSQL_SERVER};Database={MSSQL_DATABASE};Trusted_Connection=yes;"

# ----------------------------------------
# 2) 파일 설정
# ----------------------------------------
TXT_FILE = r"C:\Users\guest1\Downloads\건축물대장_층별개요\건축물대장_층별개요.txt"

# ----------------------------------------
# 3) 파일 읽고 insert
# ----------------------------------------
if not os.path.exists(TXT_FILE):
    print(f"❌ 파일 없음: {TXT_FILE}")
    exit(1)

try:
    conn = pyodbc.connect(conn_str, timeout=10)
    cursor = conn.cursor()
    print("✅ MSSQL 연결 성공")
except Exception as e:
    print(f"❌ MSSQL 연결 실패: {e}")
    exit(1)

# 테이블 구조 확인 (컬럼명 + 타입 조회)
try:
    cursor.execute(f"""
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME='건축물대장_층별개요' 
        ORDER BY ORDINAL_POSITION
    """)
    col_info = cursor.fetchall()
    columns = [row[0] for row in col_info]
    col_types = {row[0]: row[1] for row in col_info}
    print(f"\n테이블 컬럼 ({len(columns)}개): {columns}")
    print(f"\n컬럼 타입: {col_types}")
except Exception as e:
    print(f"❌ 컬럼 조회 실패: {e}")
    exit(1)

# 데이터 타입 변환 함수
def convert_value(value, col_name, col_type):
    """값을 해당 데이터 타입으로 변환"""
    # 빈 문자열 처리
    if value == '' or value is None:
        if col_type in ('numeric', 'decimal', 'int', 'bigint', 'float', 'real'):
            return None  # NULL로 처리
        return None if value is None else ''
    
    # numeric/decimal 타입: 숫자 변환
    if col_type in ('numeric', 'decimal'):
        try:
            return float(value)
        except ValueError:
            print(f"⚠️  {col_name}를 numeric으로 변환 실패: '{value}'")
            return None
    
    # int 타입: 정수 변환
    elif col_type in ('int', 'bigint', 'smallint', 'tinyint'):
        try:
            return int(value)
        except ValueError:
            print(f"⚠️  {col_name}를 int으로 변환 실패: '{value}'")
            return None
    
    # float/real 타입: 실수 변환
    elif col_type in ('float', 'real'):
        try:
            return float(value)
        except ValueError:
            print(f"⚠️  {col_name}를 float으로 변환 실패: '{value}'")
            return None
    
    # varchar/nvarchar: 문자열 유지
    else:
        return value if value else None

# 파일에서 데이터 읽기
inserted = 0
failed = 0
batch_size = 1000
batch = []

with open(TXT_FILE, 'r', encoding='utf-8') as f:
    for line_num, line in enumerate(f, 1):
        line = line.rstrip('\n')
        if not line:
            continue
        
        # pipe로 구분
        fields = line.split('|')
        
        # 컬럼 수 확인
        if len(fields) != len(columns):
            print(f"⚠️  Line {line_num}: 필드 수 불일치 ({len(fields)} != {len(columns)})")
            print(f"   Line: {line[:100]}...")
            failed += 1
            continue
        
        # 각 필드를 해당 데이터 타입으로 변환
        converted_fields = []
        for col_name, value in zip(columns, fields):
            col_type = col_types.get(col_name, 'nvarchar')
            converted_val = convert_value(value, col_name, col_type)
            converted_fields.append(converted_val)
        
        batch.append(tuple(converted_fields))
        
        # batch가 다 찼으면 insert
        if len(batch) >= batch_size:
            try:
                placeholders = ','.join(['?' for _ in columns])
                col_str = ','.join(f"[{col}]" for col in columns)
                insert_sql = f"INSERT INTO [건축물대장_층별개요] ({col_str}) VALUES ({placeholders})"
                
                cursor.executemany(insert_sql, batch)
                conn.commit()
                inserted += len(batch)
                print(f"✅ {inserted:,} rows inserted")
                batch.clear()
            except Exception as e:
                print(f"❌ Insert 실패 (batch): {e}")
                print(f"   Sample row: {batch[0] if batch else 'N/A'}")
                failed += len(batch)
                batch.clear()
                conn.rollback()

# 남은 batch insert
if batch:
    try:
        placeholders = ','.join(['?' for _ in columns])
        col_str = ','.join(f"[{col}]" for col in columns)
        insert_sql = f"INSERT INTO [건축물대장_층별개요] ({col_str}) VALUES ({placeholders})"
        
        cursor.executemany(insert_sql, batch)
        conn.commit()
        inserted += len(batch)
        print(f"✅ {inserted:,} rows inserted")
    except Exception as e:
        print(f"❌ Insert 실패 (final batch): {e}")
        failed += len(batch)
        conn.rollback()

cursor.close()
conn.close()

print(f"\n=== 완료 ===")
print(f"총 {inserted:,}개 insert")
print(f"실패: {failed}개")
