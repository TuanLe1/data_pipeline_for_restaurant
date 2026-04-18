import boto3
import logging
import os, json
from botocore.client import Config

logger = logging.getLogger(__name__)

class S3Loader:
    def __init__(self, config: dict):
        s3_conf = config.get("S3-Config", {})
        self.bucket = s3_conf.get("bucket_name")
        
        self.s3_client = boto3.client(
            's3',
            endpoint_url=s3_conf.get("endpoint"),
            aws_access_key_id=s3_conf.get("access_key"),
            aws_secret_access_key=s3_conf.get("secret_key"),
            config=Config(signature_version='s3v4'),
            region_name=s3_conf.get("region")
        )

    def upload_file(self, local_path: str, s3_path: str):
        """Đẩy file từ Disk lên MinIO"""
        try:
            self.s3_client.upload_file(local_path, self.bucket, s3_path)
            logger.info(f"📤 Uploaded to S3: s3://{self.bucket}/{s3_path}")
        except Exception as e:
            logger.error(f"❌ S3 Upload Error: {e}")
            raise e
        
    def upload_jsonl_from_memory(self, data: list, s3_path: str):
        """Đẩy mảng Python trực tiếp lên S3 dưới dạng JSONLines (No Disk)"""
        try:
            # Chuyển list dict thành string JSONL (mỗi dòng 1 record)
            jsonl_content = "\n".join([json.dumps(r, ensure_ascii=False) for r in data])
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=s3_path,
                Body=jsonl_content.encode('utf-8'),
                ContentType='application/x-jsonlines'
            )
            logger.info(f"⚡ RAM -> S3 success: {s3_path}")
        except Exception as e:
            logger.error(f"❌ S3 Upload Error: {e}")
            raise e