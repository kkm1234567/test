dir = r"C:\orgs\t_collector_building_overall_title_org.txt"
target_idx_list = [27, 28, 29, 30, 31, 32, 40, 43, 45, 47, 49, 60]


# 각 인덱스별로 결과를 저장할 딕셔너리
results = {}
for idx in target_idx_list:
    results[idx] = {
        'max_value': float('-inf'),
        'max_line': None,
        'max_decimal_places': 0,
        'max_integer_places': 0,
        'longest_decimal_value': None,
        'longest_decimal_line': None
    }

line_number = 0
sample_printed = False
headers = []

try:
    with open(dir, 'r', encoding='utf-8') as f:
        for line in f:
            line_number += 1
            line = line.strip()
            if not line:
                continue
            
            # 다양한 구분자 시도 (파이프, 탭, 쉼표)
            if '|' in line:
                parts = line.split('|')
            elif '\t' in line:
                parts = line.split('\t')
            else:
                parts = line.split(',')

            # 헤더 저장 (1행)
            if line_number == 1:
                headers = [h.strip() for h in parts]
                print(f"총 컬럼 수: {len(parts)}\n")
                continue  # 헤더 라인은 샘플 출력/계산에서 제외

            # 2~4행 샘플 출력 (헤더 포함, 정돈된 포맷)
            if 2 <= line_number <= 4:
                formatted = []
                for idx, val in enumerate(parts):
                    clean_val = val.strip()
                    if clean_val == "":
                        clean_val = "(blank)"
                    header_name = headers[idx] if idx < len(headers) else ""
                    if header_name:
                        formatted.append(f"{idx:>3} | {header_name} | {clean_val}")
                    else:
                        formatted.append(f"{idx:>3} | {clean_val}")
                print(f"라인 {line_number}")
                print("\n".join(formatted))
                print()
            
            # 컬럼 정보 출력 (한번만)
            if line_number == 1:
                headers = [h.strip() for h in parts]
                print(f"총 컬럼 수: {len(parts)}\n")
            
            # 모든 target_idx에 대해 처리
            for target_idx in target_idx_list:
                if len(parts) >= abs(target_idx):
                    try:
                        value_str = parts[target_idx].strip()
                        value = float(value_str)
                        
                        # 최대값 추적
                        if value > results[target_idx]['max_value']:
                            results[target_idx]['max_value'] = value
                            results[target_idx]['max_line'] = line_number
                        
                        # 소수점 자릿수 계산
                        if '.' in value_str:
                            parts_num = value_str.split('.')
                            integer_part = parts_num[0].lstrip('-')  # 음수 부호 제거
                            decimal_places = len(parts_num[1])
                            integer_places = len(integer_part) if integer_part and integer_part != '0' else 1
                            
                            if decimal_places > results[target_idx]['max_decimal_places']:
                                results[target_idx]['max_decimal_places'] = decimal_places
                                results[target_idx]['longest_decimal_value'] = value_str
                                results[target_idx]['longest_decimal_line'] = line_number
                            
                            if integer_places > results[target_idx]['max_integer_places']:
                                results[target_idx]['max_integer_places'] = integer_places
                        else:
                            # 정수인 경우
                            integer_part = value_str.lstrip('-')
                            integer_places = len(integer_part) if integer_part else 1
                            if integer_places > results[target_idx]['max_integer_places']:
                                results[target_idx]['max_integer_places'] = integer_places
                                
                    except ValueError:
                        continue
    
    # 각 인덱스별 결과 출력
    for target_idx in target_idx_list:
        res = results[target_idx]
        header_name = headers[target_idx] if target_idx < len(headers) else "N/A"
        print(f"\n{'='*70}")
        print(f"인덱스 {target_idx}: {header_name}")
        print(f"{'='*70}")
        print(f"최대값: {res['max_value']}")
        print(f"최대값이 있는 라인: {res['max_line']}")
        print(f"\n소수점 자릿수가 가장 긴 값: {res['longest_decimal_value']}")
        print(f"소수점 자릿수: {res['max_decimal_places']}")
        print(f"해당 라인: {res['longest_decimal_line']}")
        print(f"\n정수 부분 최대 자릿수: {res['max_integer_places']}")
        
        # DB NUMERIC 설정 계산 (+3 여유를 소수점에 추가)
        recommended_scale = res['max_decimal_places'] + 3  # 소수점에 여유 3 추가
        recommended_precision = res['max_integer_places'] + recommended_scale
        
        print(f"\n권장 DB 설정: numeric({recommended_precision}, {recommended_scale})")
        print(f"  - 전체 자릿수 (precision): {recommended_precision}")
        print(f"  - 소수점 자릿수 (scale): {recommended_scale}")
        print(f"\n상세:")
        print(f"  - 정수 부분: {res['max_integer_places']}자리")
        print(f"  - 소수점: {res['max_decimal_places']}자리 + 여유 3자리 = {recommended_scale}자리")
        print(f"  - 합계: {res['max_integer_places']} + {recommended_scale} = {recommended_precision}")
    
except FileNotFoundError:
    print(f"파일을 찾을 수 없습니다: {dir}")
except Exception as e:
    print(f"에러 발생: {e}")