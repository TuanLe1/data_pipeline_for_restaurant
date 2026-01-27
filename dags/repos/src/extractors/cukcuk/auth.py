import hashlib
import hmac
import json
import datetime
import requests
import logging

logger = logging.getLogger(__name__)

class CukCukAuth:
    def __init__(self, config: dict):
        self.config = config
        self.app_id = config.get("AppID")
        self.secret_key = config.get("secret_key")
        self.domain = config.get("Domain")
        
        base_url = config.get("CukCuk-Base-URL", "https://graphapi.cukcuk.com/api").rstrip("/")
        token_endpoint = config.get("Get-Token-URL", "/Account/Login")
        
        self.login_url = f"{base_url}{token_endpoint}"
        
        logger.info(f"Auth URL configured: {self.login_url}") 

    def _generate_signature(self, login_time_str: str) -> str:
        raw_data = {
            "AppID": self.app_id,
            "Domain": self.domain,
            "LoginTime": login_time_str
        }
        
        json_str = json.dumps(raw_data, separators=(',', ':'))
        
        signature = hmac.new(
            self.secret_key.encode('ascii'), 
            json_str.encode('utf-8'), 
            hashlib.sha256
        ).hexdigest()
        
        return signature

    def get_token(self) -> str:
        """
        Call API to get Access Token
        """
        now_utc = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z")
        signature = self._generate_signature(now_utc)
        
        payload = {
            "AppID": self.app_id,
            "Domain": self.domain,
            "LoginTime": now_utc,
            "SignatureInfo": signature
        }
        
        headers = {'Content-Type': 'application/json'}
        
        try:
            response = requests.post(self.login_url, json=payload, headers=headers, timeout=15)
            
            if response.status_code == 200:
                resp_json = response.json()
                token = resp_json.get("Data", {}).get("AccessToken")
                if token:
                    logger.info("✅ Authentication successful. Token retrieved.")
                    return token
                else:
                    logger.error(f"Auth failed. No token in response. Body: {resp_json}")
            else:
                logger.error(f"❌ Auth failed. Status: {response.status_code}")
                
        except Exception as e:
            logger.error(f"Error during authentication: {e}")
            
        raise Exception("Failed to retrieve Access Token from CukCuk")