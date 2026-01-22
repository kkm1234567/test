
from minio import Minio
import re


bucket = "ptr.prime.collect"
host = "192.168.10.169"
port = 9001
client = Minio(
    "192.168.10.169:9000",
    access_key="OqguAfPHIx7oAHxdFSM5",
    secret_key="8s7sBRdeygaLeT8CDR5lEltRXUKlK30QiAaZl5Wy",
    secure=False,
)


def camel_to_snake(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

# hub_go_kr 경로의 모든 오브젝트 나열
objects = client.list_objects(bucket, prefix="hub_go_kr/", recursive=True)

for obj in objects:
    obj_name = obj.object_name
    # 파일명만 추출
    base_name = obj_name.split("/")[-1]
    # 카멜케이스 감지 (대문자가 포함된 경우)
    if re.search(r'[A-Z]', base_name):
        snake_name = camel_to_snake(base_name)
        target_object = obj_name.replace(base_name, snake_name)
        print(f"{obj_name} -> {target_object}")
        try:
            # 원본 파일 다운로드
            response = client.get_object(bucket, obj_name)
            data = response.read()
            response.close()
            
            # 새로운 이름으로 업로드
            client.put_object(
                bucket,
                target_object,
                __import__('io').BytesIO(data),
                len(data)
            )
            
            # 원본 삭제
            client.remove_object(bucket, obj_name)
            print(f"✓ 완료: {obj_name}")
        except Exception as e:
            print(f"✗ 오류: {obj_name} - {str(e)}")