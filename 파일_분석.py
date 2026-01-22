import os

TXT_FILE = r"C:\Users\guest1\Downloads\건축물대장_층별개요\건축물대장_층별개요.txt"

if not os.path.exists(TXT_FILE):
    print(f"❌ 파일 없음: {TXT_FILE}")
    exit(1)

print("첫 10개 라인 분석:\n")

with open(TXT_FILE, 'r', encoding='utf-8') as f:
    for i in range(10):
        line = f.readline()
        if not line:
            break
        line = line.rstrip('\n')
        fields = line.split('|')
        print(f"라인 {i+1}: {len(fields)}개 필드")
        print(f"  내용: {line[:80]}...")
        if i == 0:
            print(f"  필드: {fields}")
        print()
