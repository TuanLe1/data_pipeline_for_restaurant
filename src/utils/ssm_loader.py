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
        except self.client.exceptions.ParameterNotFound:
            if required:
                logger.error(f"Parameter not found: {param_name}")
                raise FileNotFoundError(f"Missing required SSM parameter: {param_name}")
            return None
        except Exception as e:
            logger.error(f"Error fetching {param_name}: {e}")
            if required: raise e
            return None

    def merge_ssm_config(self, config: dict, param_path="/uratei_etl_config"):
        logger.info(f"Fetching config JSON from SSM: {param_path}...")
         
        try:
            json_str = self.get_parameter(param_path, required=True)
            ssm_values = json.loads(json_str)
            config.update(ssm_values)
            print("ssm_values value:", ssm_values)
            
            logger.info("Successfully merged secrets from AWS SSM.")
            
        except json.JSONDecodeError as e:
            logger.critical(f"Invalid JSON format in SSM parameter {param_path}: {e}")
            raise e
        except Exception as e:
            logger.critical(f"Failed to load SSM config: {e}")
            raise e
        
        return config