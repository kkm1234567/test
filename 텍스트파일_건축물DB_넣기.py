import mysql.connector
import os
import tempfile
import shutil
import re

# ----------------------------------------
# 1) EAIS 테이블 ↔ 원본 파일 매핑
# ----------------------------------------
DATA_MAP = {
    # 테이블: 파일명
    "eais_01_기본개요_bcp": "건축물대장_기본개요.txt",
}

# 각 테이블의 첫 번째 PK 컬럼명 (LedgerNo 업데이트용)
# - eais_ledgerno_changed.LedgerNo_Old와 조인하여 LedgerNo_New로 업데이트
TABLE_PK_COLUMN = {
    "eais_01_기본개요_bcp": "관리_건축물대장_PK",  # ★ 실제 컬럼명으로 수정 필요
}

# 각 파일의 원본 인코딩 설정(필요 시 수정)
# - 텍스트가 멀쩡하지만 DB에서 깨질 경우, 파일 인코딩이 UTF-8이 아닐 가능성이 큼
# - 한국 공공데이터는 cp949(=euckr)인 경우가 많음
FILE_CHARSET = {
    # 예: 해당 파일이 cp949라면 아래처럼 지정
    "건축물대장_기본개요.txt": "euckr",  # cp949/euckr 추정. UTF-8이면 "utf8"로 변경
}

# 로딩 시 사용할 줄바꿈 문자 (정규화 후 \n으로 통일)
LINE_TERMINATOR = "\\n"

BASE_DIR = r"C:\Users\guest1\OneDrive - 프롭티어\hub_go_kr\202009"

# ----------------------------------------
# 2) MariaDB 연결
# ----------------------------------------
conn = mysql.connector.connect(
    host="192.168.11.203",
    user="root",
    password="!@Skdud340",
    database="buildledger",
    charset="utf8",
    allow_local_infile=True   # ★ LOCAL INFILE 허용
)

cursor = conn.cursor()

print("\n=== LOAD DATA 시작 ===\n")

# ----------------------------------------
# 3) 반복문으로 LOAD DATA
# ----------------------------------------
for table_name, filename in DATA_MAP.items():

    file_path = os.path.join(BASE_DIR, filename)

    # 파일 존재 체크
    if not os.path.exists(file_path):
        print(f"❌ 파일 없음 → {file_path}")
        continue

    print(f"➡ 테이블: {table_name}")
    print(f"   파일: {file_path}")

    # 파일 인코딩 추정/설정
    file_charset = FILE_CHARSET.get(filename, "utf8")

    # 일부 MariaDB/MySQL에서는 cp949가 "euckr"로 동작
    # - 파일이 cp949/euckr라면 CHARACTER SET euckr 사용을 권장
    # - UTF-8 파일이면 utf8/utf8mb4 사용

    # CRLF 처리: 윈도우 파일은 \r\n 일 가능성 높음
    line_term = LINE_TERMINATOR

    # BOM 문제 또는 미지원 인코딩 대응을 위해, 필요 시 임시로 UTF-8 변환
    # - DB가 해당 file_charset을 지원하지 않거나 깨지는 경우에만 사용
    use_transcoding = False
    transcoded_path = None

    # 안전 모드: euckr로 로딩을 먼저 시도하고 오류/깨짐 시 UTF-8로 변환 재시도할 수 있도록 구조화
    def make_sql(load_path: str, charset: str, field_term: str) -> str:
        return f"""
            LOAD DATA LOCAL INFILE '{load_path.replace("\\", "/")}'
            INTO TABLE {table_name}
            CHARACTER SET {charset}
            FIELDS TERMINATED BY '{field_term}'
            LINES TERMINATED BY '{line_term}'
            IGNORE 0 LINES;
        """

    # 구분자 자동 감지: 첫 몇 줄에서 탭(`\t`) vs 파이프(`|`)를 비교
    detected_field_term = '|'
    try:
        with open(file_path, "r", encoding=file_charset, errors="replace") as src:
            sample_lines = [src.readline() for _ in range(5)]
            cnt_pipe = sum(l.count('|') for l in sample_lines if l)
            cnt_tab = sum(l.count('\t') for l in sample_lines if l)
            if cnt_tab > cnt_pipe:
                detected_field_term = '\t'
    except Exception:
        pass

    # 사전 정제: 이스케이프된 파이프 `\|`를 빈 문자열로 치환
    # - 원본 파일에서 `\|`는 "빈 필드" 또는 "이스케이프된 파이프"를 의미
    # - `|\|` → `||` 로 치환하여 빈 컬럼으로 처리
    # - 원본 파일은 그대로 두고, 임시 변환 파일을 생성하여 로드
    # - ★ 정규화 파일은 UTF-8로 저장 (인코딩 오류 방지)
    fd_proc, temp_proc_path = tempfile.mkstemp(prefix="normalize_", suffix=".txt")
    os.close(fd_proc)
    
    print(f"   🔍 감지된 구분자: {'탭(TAB)' if detected_field_term == chr(9) else '파이프(|)'}")
    
    sample_shown = False
    normalize_success = False
    try:
        with open(file_path, "r", encoding=file_charset, errors="replace") as src, \
             open(temp_proc_path, "w", encoding="utf-8", newline="") as dst:  # ★ UTF-8로 저장
            for idx, line in enumerate(src):
                # ★ 먼저 개행문자 제거 (마지막 컬럼에 포함되지 않도록)
                line = line.rstrip('\r\n')
                
                original = line
                fixed = line
                
                # 핵심 치환: 파이프 뒤에 백슬래시+파이프 → 파이프 두 개 (빈 필드)
                # `|\|` → `||`
                fixed = re.sub(r'\|\\\|', '||', fixed)
                
                # 추가: 단독 `\|` (라인 시작 등) → 빈 문자열
                fixed = re.sub(r'\\\|', '', fixed)
                
                # 디버깅: 첫 5줄 중 변경된 라인 샘플 출력
                if idx < 5 and original != fixed and not sample_shown:
                    print(f"   📝 샘플 변환 (라인 {idx+1}):")
                    print(f"      원본: {original[:100].strip()}...")
                    print(f"      변환: {fixed[:100].strip()}...")
                    sample_shown = True
                
                # ★ 개행은 \n만 사용 (일관성)
                dst.write(fixed + '\n')
        normalize_success = True
        print(f"   ✅ 정규화 완료 (UTF-8 임시 파일 생성)")
    except Exception as norm_err:
        print(f"   ⚠ 정규화 실패: {norm_err}. 원본으로 시도합니다.")
        # 정규화 실패 시 원본 경로로 대체
        temp_proc_path = file_path

    # 1차 시도: 정규화된 파일(UTF-8) 또는 원본 파일로 로딩
    # 정규화 성공 시 UTF-8, 실패 시 원본 charset 사용
    load_charset = "utf8" if normalize_success else file_charset
    sql = make_sql(temp_proc_path, load_charset, detected_field_term)

    try:
        cursor.execute(sql)
        conn.commit()
        inserted_count = cursor.rowcount
        print(f"   ✅ 성공: {inserted_count:,} rows inserted")
        
        # ----------------------------------------
        # LedgerNo 업데이트: eais_ledgerno_changed 조인
        # ----------------------------------------
        pk_column = TABLE_PK_COLUMN.get(table_name)
        if pk_column:
            print(f"   🔄 LedgerNo 업데이트 시작 ({pk_column} → LedgerNo_New)...")
            update_sql = f"""
                UPDATE {table_name} t
                INNER JOIN eais_ledgerno_changed c
                    ON t.{pk_column} = c.LedgerNo_Old
                SET t.{pk_column} = c.LedgerNo_New;
            """
            try:
                cursor.execute(update_sql)
                conn.commit()
                updated_count = cursor.rowcount
                print(f"   ✅ LedgerNo 업데이트 완료: {updated_count:,} rows updated\n")
            except Exception as upd_err:
                print(f"   ⚠ LedgerNo 업데이트 실패: {upd_err}\n")
        else:
            print(f"   ⚠ PK 컬럼 미설정 → LedgerNo 업데이트 스킵\n")

    except Exception as e:
        print(f"   ⚠ 첫 시도 실패(CHARSET={file_charset}): {e}")

        # 2차 시도: (정규화된 파일을 기반으로) UTF-8 변환 후 재로딩 시도
        try:
            use_transcoding = True
            fd, temp_path = tempfile.mkstemp(prefix="loaddata_", suffix=".txt")
            os.close(fd)

            with open(temp_proc_path, "r", encoding=file_charset, errors="replace") as src, \
                 open(temp_path, "w", encoding="utf-8", newline="") as dst:
                shutil.copyfileobj(src, dst)

            transcoded_path = temp_path
            sql2 = make_sql(transcoded_path, "utf8", detected_field_term)
            cursor.execute(sql2)
            conn.commit()
            print(f"   ✅ 재시도 성공(UTF-8 변환): {cursor.rowcount} rows inserted\n")
        except Exception as e2:
            print(f"   ❌ 재시도 실패: {e2}\n")
        finally:
            if use_transcoding and transcoded_path and os.path.exists(transcoded_path):
                try:
                    os.remove(transcoded_path)
                except OSError:
                    pass
            if temp_proc_path != file_path and os.path.exists(temp_proc_path):
                try:
                    os.remove(temp_proc_path)
                except OSError:
                    pass


# ----------------------------------------
# 4) 종료
# ----------------------------------------
cursor.close()
conn.close()

print("\n=== 모든 LOAD DATA 작업 완료 ===")
