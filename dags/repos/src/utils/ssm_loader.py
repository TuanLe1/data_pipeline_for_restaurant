import boto3
import logging
import json
import os

logger = logging.getLogger(__name__)

class SSMParameterLoader:
    def __init__(self, region_name="ap-southeast-2"):
        self.region = region_name
        try:
            self.client = boto3.client('ssm', region_name=self.region)
        except Exception as e:
            logger.error(f"Failed to connect to AWS SSM: {e}")
            self.client = None

    def get_parameter(self, param_name, required=True):
        if not self.client:
            if required: raise Exception("AWS SSM Client not initialized")
            return None

        try:
            response = self.client.get_parameter(
                Name=param_name,
                WithDecryption=True 
            )
            return response['Parameter']['Value']
        except Exception as e:
            # Code cũ của bạn bắt exception chưa chuẩn, tôi fix nhẹ để an toàn hơn
            logger.error(f"Error fetching {param_name}: {e}")
            if required: raise e
            return None

    def merge_ssm_config(self, config: dict, param_path="/uratei_etl_config"):
        logger.info(f"Fetching config JSON from SSM: {param_path}...")
        try:
            json_str = self.get_parameter(param_path, required=True)
            ssm_values = json.loads(json_str)
            config.update(ssm_values)
            # print("ssm_values value:", ssm_values) # Comment print để log sạch hơn
            logger.info("Successfully merged secrets from AWS SSM.")
        except Exception as e:
            logger.critical(f"Failed to load SSM config: {e}")
            raise e
        return config

# 👇👇👇 ĐÃ SỬA: Đưa hàm này ra ngoài (Sát lề trái), KHÔNG nằm trong Class nữa
def load_config(config_path=None):
    """
    Hàm helper chuẩn để load config
    """
    # 1. Xác định đường dẫn file
    if not config_path:
        # Ưu tiên đường dẫn trong Docker Airflow
        docker_path = "/opt/airflow/config/config.json"
        
        if os.path.exists(docker_path):
            config_path = docker_path
        else:
            # Fallback: Tìm tương đối
            current_dir = os.path.dirname(__file__)
            config_path = os.path.abspath(os.path.join(current_dir, "../../../configs/config.json"))

    logger.info(f"📂 Loading config from: {config_path}")

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"❌ Config file not found at: {config_path}")

    # 2. Đọc File JSON
    with open(config_path, 'r') as f:
        config = json.load(f)

    # 3. Merge với SSM
    try:
        loader = SSMParameterLoader()
        config = loader.merge_ssm_config(config)
    except Exception as e:
        logger.warning(f"⚠️ SSM Merge failed (using local config only): {e}")
    
    return config