from minio import Minio

bucket = "ptr.prime.collect"
client = Minio(
    "192.168.10.169:9000",
    access_key="OqguAfPHIx7oAHxdFSM5",
    secret_key="8s7sBRdeygaLeT8CDR5lEltRXUKlK30QiAaZl5Wy",
    secure=False,
)

print("hub_go_kr 경로의 최상위 오브젝트:")
objects = client.list_objects(bucket, prefix="hub_go_kr/", recursive=False)
for obj in objects:
    print(f"  {obj.object_name}")
