from minio import Minio
from io import BytesIO

class MinioClient:
    def __init__(self):
        self.bucket = "ptr.prime.collect"
        self.client = Minio(
            "192.168.10.169:9000",
            access_key="OqguAfPHIx7oAHxdFSM5",
            secret_key="8s7sBRdeygaLeT8CDR5lEltRXUKlK30QiAaZl5Wy",
            secure=False,
        )

    def ensure_hub2021_folder(self):
        object_name = "Hub2021Data/.keep"

        # 이미 존재하면 스킵
        try:
            self.client.stat_object(self.bucket, object_name)
            print("✔ Hub2021Data 폴더 이미 존재")
            return
        except Exception:
            print("ℹ Hub2021Data 폴더 없음 → 생성 시도")

        # 0바이트 텍스트 업로드
        data = BytesIO(b"")

        self.client.put_object(
            bucket_name=self.bucket,
            object_name=object_name,
            data=data,
            length=0,
            content_type="text/plain",
        )

        print("✅ Hub2021Data 폴더 유지용 빈 파일 생성 완료")


if __name__ == "__main__":
    print("▶ MinIO Hub2021Data 폴더 생성 체크 시작")

    m = MinioClient()
    m.ensure_hub2021_folder()

    print("▶ 작업 종료")
