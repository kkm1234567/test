import MySQLdb
import os
import pandas as pd
from typing import List
import time


# ---------------------------------------
# 1) 대상 테이블 및 CSV 경로
# ---------------------------------------
# 출력/입력 파일 경로 (탭 구분 TXT)
CSV_PATH = r"C:\folder\t_road_name_address_merged.txt"
TABLE_NAME = "t_road_name_address"

# 대용량 파일 처리를 위한 배치 크기 (크게 설정)
BATCH_SIZE = 5000  # 한 번에 처리할 행 수

# 체크포인트 파일 (마지막 성공한 누적 행 번호 저장)
CHECKPOINT_FILE = os.path.join(os.path.dirname(__file__), "t_road_name_address_checkpoint.txt")

# ---------------------------------------
# 2) DB 연결 설정 (DST_DB만)
# ---------------------------------------
DST_DB = {
    "host": "dev-prime-dw.cfecisesw67r.ap-northeast-2.rds.amazonaws.com",
    "port": 33306,
    "user": "DB_WO_BUNNY",
    "passwd": "Thrkflxkd72&",
    "db": "db_dw_address",
    "charset": "utf8mb4",
    "local_infile": 1,
    "connect_timeout": 60,
    "read_timeout": 300,
    "write_timeout": 300
}


# 재시도 함수
def execute_with_retry(cursor, sql, max_retries=1):
    """SQL 실행 (재시도 없음, 그냥 실행)"""
    try:
        cursor.execute(sql)
        return True
    except Exception:
        return False


# DB 연결 생성 함수
def create_connection():
    """새로운 DB 연결 생성"""
    try:
        conn = MySQLdb.connect(**DST_DB)
        return conn
    except Exception as e:
        print(f"[DB 연결 실패] {e}")
        return None


def load_checkpoint():
    """체크포인트 파일에서 마지막 처리 행 불러오기"""
    if not os.path.exists(CHECKPOINT_FILE):
        return 0
    try:
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            return int(f.read().strip() or 0)
    except:
        return 0


def save_checkpoint(current_row: int):
    """현재 처리 행 번호를 체크포인트 파일에 저장"""
    try:
        with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
            f.write(str(current_row))
    except:
        pass


# 컬럼 정보 조회
def get_table_columns(table_name):
    """테이블 컬럼 정보 조회"""
    conn = create_connection()
    if not conn:
        return None, None
    
    try:
        cursor = conn.cursor()
        cursor.execute(f"SHOW COLUMNS FROM {table_name}")
        columns_info = cursor.fetchall()
        columns = [row[0] for row in columns_info]
        geom_cols = [row[0] for row in columns_info if row[1].lower().startswith(('geometry', 'point'))]
        return columns, geom_cols
    finally:
        cursor.close()
        conn.close()


# 데이터 변환 함수
def convert_row_to_values(row, columns, geom_cols):
    """행 데이터를 SQL VALUES로 변환"""
    values = []
    for col in columns:
        val = row[col] if col in row and pd.notna(row[col]) else None
        
        if col in geom_cols:
            # GEOMETRY/POINT 컬럼 처리
            if val and val != '\\N':
                values.append(f"ST_GeomFromText('{val}')")
            else:
                values.append("NULL")
        else:
            # 일반 컬럼 처리
            if val is None or val == '\\N' or (isinstance(val, float) and pd.isna(val)):
                values.append("NULL")
            else:
                # SQL 인젝션 방지
                val_str = str(val).replace("'", "''")
                values.append(f"'{val_str}'")
    
    return values


# 배치 삽입 함수
def insert_batch(batch_df, columns, geom_cols, table_name, conn):
    """배치 데이터 삽입 (연결 재사용)"""
    cursor = conn.cursor()
    success_count = 0
    error_count = 0
    
    try:
        # 5000행을 1000행씩 5번에 나눠서 INSERT (연결 끊김 방지)
        micro_batch_size = 1000
        for start_idx in range(0, len(batch_df), micro_batch_size):
            end_idx = min(start_idx + micro_batch_size, len(batch_df))
            micro_batch = batch_df.iloc[start_idx:end_idx]
            
            values_list = []
            for idx, row in micro_batch.iterrows():
                try:
                    values = convert_row_to_values(row, columns, geom_cols)
                    values_list.append(f"({', '.join(values)})")
                except Exception as e:
                    error_count += 1
                    if error_count <= 3:
                        print(f"[Row {idx}] 에러: {str(e)[:100]}")
            
            # 각 마이크로 배치를 별도로 INSERT & COMMIT
            if values_list:
                try:
                    sql = f"INSERT IGNORE INTO {table_name} ({', '.join(columns)}) VALUES {', '.join(values_list)}"
                    cursor.execute(sql)
                    conn.commit()
                    success_count += len(values_list)
                except Exception as e:
                    print(f"[마이크로배치 실패] {str(e)[:100]}")
                    try:
                        conn.rollback()
                    except:
                        pass
                    error_count += len(values_list)
    
    except Exception as e:
        print(f"[배치 커밋 실패] {e}")
        try:
            conn.rollback()
        except:
            pass
        error_count += success_count  # 롤백 시 모두 에러 처리
        success_count = 0
    
    finally:
        try:
            cursor.close()
        except:
            pass
    
    return success_count, error_count


# 주 함수
# ---------------------------------------
# 3) CSV → DST_DB 적재 함수
# ---------------------------------------
def load_csv_to_dst_db(csv_path, table_name, start_row=None):
    # start_row가 None이면 체크포인트에서 읽기
    if start_row is None:
        start_row = load_checkpoint()
    effective_start = start_row
    print(f"\n=== TXT(탭) → {table_name} (DST_DB) 적재 ===")
    print(f"시작 행: {effective_start}")
    
    if not os.path.exists(csv_path):
        print(f"[오류] TXT 파일이 존재하지 않습니다: {csv_path}")
        return
    
    # 컬럼 정보 조회
    columns, geom_cols = get_table_columns(table_name)
    if not columns:
        print("[오류] 테이블 컬럼 정보를 조회할 수 없습니다")
        return

    conn = create_connection()
    if not conn:
        print("[오류] DB 연결 실패로 적재를 중단합니다")
        return
    
    print(f"테이블 컬럼: {columns}")
    print(f"Geometry/Point 컬럼: {geom_cols}")
    
    # 파일 읽기 및 배치 처리
    total_success = 0
    total_error = 0
    batch_count = 0
    current_row = 0
    
    try:
        for chunk in pd.read_csv(csv_path, sep='\t', dtype=str, chunksize=BATCH_SIZE, 
                                encoding='utf-8', header=None, names=columns):
            batch_count += 1
            chunk = chunk.reset_index(drop=True)
            
            # 시작 행 이전 배치는 건너뛰기
            if effective_start > 0 and current_row + len(chunk) <= effective_start:
                current_row += len(chunk)
                continue
            
            # 시작 행이 이 배치 중간에 있으면 해당 행부터 시작
            if effective_start > 0 and current_row < effective_start < current_row + len(chunk):
                skip_in_chunk = effective_start - current_row
                chunk = chunk.iloc[skip_in_chunk:].reset_index(drop=True)
                current_row += skip_in_chunk  # 건너뛴 만큼 current_row 업데이트
                print(f"[배치 {batch_count}] {skip_in_chunk}행 건너뛰고 시작")
            
            if len(chunk) == 0:
                current_row += len(chunk)
                continue
            
            print(f"[배치 {batch_count}] {len(chunk)}행 처리 중... (누적 행: {current_row + len(chunk)})")
            
            # 배치 삽입
            success, error = insert_batch(chunk, columns, geom_cols, table_name, conn)
            total_success += success
            total_error += error
            current_row += len(chunk)
            
            # 배치 후 체크포인트 저장 (다음 재시작 시 이 행부터 시작)
            save_checkpoint(current_row)
            
            if (success + error) > 0:
                print(f"✓ 배치 {batch_count} 완료: {success}행 성공, {error}행 실패 (누계: {total_success}행)")
            
            # 휴식 제거로 속도 향상; DB 부하 시 재도입 고려
    except Exception as e:
        print(f"[에러] 파일 읽기 중 오류 발생: {e}")
        import traceback
        traceback.print_exc()
    finally:
        try:
            conn.close()
        except:
            pass
    
    print(f"\n✓ 적재 완료")
    print(f"  - 성공한 행: {total_success}")
    print(f"  - 실패한 행: {total_error}")
    print(f"  - 마지막 처리 행: {current_row}")


# ---------------------------------------
# 4) main
# ---------------------------------------
if __name__ == "__main__":
    print("=" * 50)
    print("CSV → DST_DB 적재 시작")
    print("=" * 50)
    # start_row=None이면 체크포인트에서 이어서 시작 (29995000행부터 이어짐)
    load_csv_to_dst_db(CSV_PATH, TABLE_NAME, start_row=None)
    print("\n✓ 모든 작업 완료!")
