import hashlib
import hmac
import json
import datetime
import requests


APP_ID = ""
SECRET_KEY = ""     # Ví dụ: 123456...
DOMAIN = ""             # Ví dụ: cukcuk.vn
BASE_URL = "https://graphapi.cukcuk.com/api/Account/Login"
# =======================================

def get_token():
    now_utc = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z")
    raw_data = {"AppID": APP_ID, "Domain": DOMAIN, "LoginTime": now_utc}
    json_str = json.dumps(raw_data, separators=(',', ':'))
    signature = hmac.new(SECRET_KEY.encode('ascii'), json_str.encode('utf-8'), hashlib.sha256).hexdigest()
    
    payload = {
        "AppID": APP_ID,
        "Domain": DOMAIN,
        "LoginTime": now_utc,
        "SignatureInfo": signature
    }
    
    try:
        resp = requests.post(BASE_URL, json=payload)
        print(f"Status: {resp.status_code}")
        if resp.status_code == 200:
            token = resp.json().get('Data', {}).get('AccessToken')
            print("\n=== COPY TOKEN BÊN DƯỚI ===")
            print(token)
            return token
        else:
            print("Lỗi:", resp.text)
    except Exception as e:
        print(e)

get_token()