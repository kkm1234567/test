from minio import Minio
import io

bucket = "ptr.prime.collect"
client = Minio(
    "192.168.10.169:9000",
    access_key="OqguAfPHIx7oAHxdFSM5",
    secret_key="8s7sBRdeygaLeT8CDR5lEltRXUKlK30QiAaZl5Wy",
    secure=False,
)

source_object = "hub_go_kr/dbDwCollectorBuilding"
target_object = "hub_go_kr/db_dw_collector_building"

try:
    # 원본 파일 다운로드
    print(f"다운로드 중: {source_object}")
    response = client.get_object(bucket, source_object)
    data = response.read()
    response.close()
    
    # 새로운 이름으로 업로드
    print(f"업로드 중: {target_object}")
    client.put_object(
        bucket,
        target_object,
        io.BytesIO(data),
        len(data)
    )
    
    # 원본 삭제
    print(f"삭제 중: {source_object}")
    client.remove_object(bucket, source_object)
    
    print(f"✓ 완료: {source_object} -> {target_object}")
except Exception as e:
    print(f"✗ 오류: {str(e)}")
