import logging
import asyncio
import aiohttp
from aiohttp import TCPConnector, ClientTimeout
import random
from typing import List, Dict, Optional, Union, Any
from datetime import datetime, timezone
import dateutil.parser

from .auth import CukCukAuth

logger = logging.getLogger(__name__)

class CukCukExtractor:
    _cached_token = None

    def __init__(self, config: dict):
        self.config = config
        self.base_url = config.get("CukCuk-Base-URL", "https://graphapi.cukcuk.com/api")
        self.company_code = config.get("CompanyCode", "uraeteicambodia")
        self.branch_map = config.get("Branch-Name_Mapper", {})
        self.branch_id = list(self.branch_map.keys())[0] if self.branch_map else ""

        self.semaphore = asyncio.Semaphore(5)
        self.max_retries = 5
        self.base_delay = 1.0
        self.auth_handler = CukCukAuth(config)

    def _get_valid_token(self):
        if CukCukExtractor._cached_token:
            return CukCukExtractor._cached_token
        try:
            token = self.auth_handler.get_token()
            CukCukExtractor._cached_token = token
            return token
        except Exception as e:
            logger.critical(f"Auth Failed: {e}")
            raise e

    async def _call_api_async(self, session: aiohttp.ClientSession, url: str, method: str = "GET", 
                             json_body: dict = None, params: dict = None) -> Dict:
        token = self._get_valid_token()
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
            "CompanyCode": self.company_code
        }
        RETRY_CODES = [429, 500, 502, 503, 504, 520, 525]

        async with self.semaphore:
            await asyncio.sleep(random.uniform(0.1, 0.3))
            last_exception = None
            for attempt in range(self.max_retries):
                try:
                    async with session.request(method, url, json=json_body, params=params, headers=headers) as response:
                        if response.status == 200:
                            return await response.json()
                        elif response.status == 401:
                            CukCukExtractor._cached_token = None
                            token = self._get_valid_token()
                            headers["Authorization"] = f"Bearer {token}"
                            continue
                        elif response.status in RETRY_CODES:
                            sleep_time = self.base_delay * (2 ** attempt) + random.uniform(0, 1)
                            await asyncio.sleep(sleep_time)
                        else:
                            text = await response.text()
                            raise Exception(f"API Error {response.status}: {text[:200]}")
                except Exception as e:
                    last_exception = e
                    await asyncio.sleep(self.base_delay * (2 ** attempt))
            
            raise Exception(f"Failed {url} after retries. Last error: {last_exception}")

    def _parse_date_safe(self, date_val: Union[str, datetime]) -> datetime:
        if isinstance(date_val, datetime):
            return date_val.replace(tzinfo=timezone.utc) if date_val.tzinfo is None else date_val
        if not date_val:
            return datetime.min.replace(tzinfo=timezone.utc)
        try:
            dt = dateutil.parser.parse(date_val)
            return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt
        except:
            return datetime.min.replace(tzinfo=timezone.utc)

    # =========================================================================
    # 1. EXTRACT ORDERS (Tách Header & Detail)
    # =========================================================================
    async def extract_orders(self, from_date: str, to_date: datetime = None) -> Dict[str, List[Dict]]:
        all_headers = []
        all_details = []
        limit = 100
        endpoint = self.config.get("Get-OrderHeader-URL", "/v1/orders/paging")
        url = f"{self.base_url}{endpoint}"
        detail_endpoint = self.config.get("Get-OrderDetail-URL", "/v1/orders/")
        
        cutoff_date = self._parse_date_safe(to_date) if to_date else None
        logger.info(f"📦 [ASYNC] Extracting ORDERS & DETAILS from: {from_date}")

        async with aiohttp.ClientSession(connector=TCPConnector(limit=0), timeout=ClientTimeout(total=900)) as session:
            current_page = 1
            is_finished = False
            
            while not is_finished:
                header_tasks = []
                for i in range(5): # BATCH_SIZE = 5
                    payload = {"Page": current_page + i, "Limit": limit, "BranchId": self.branch_id, "LastSyncDate": from_date}
                    header_tasks.append(self._call_api_async(session, url, method="POST", json_body=payload))
                
                header_results = await asyncio.gather(*header_tasks)
                
                valid_headers_in_batch = []
                for res in header_results:
                    data_page = res.get("Data", [])
                    if not data_page:
                        is_finished = True
                        break
                    
                    for item in data_page:
                        item_dt = self._parse_date_safe(item.get("Date"))
                        if not cutoff_date or item_dt < cutoff_date:
                            valid_headers_in_batch.append(item)
                    
                    if len(data_page) < limit: is_finished = True

                # Fetch Details for batch
                if valid_headers_in_batch:
                    detail_tasks = []
                    for order in valid_headers_in_batch:
                        oid = order.get("Id")
                        detail_tasks.append(self._call_api_async(session, f"{self.base_url}{detail_endpoint}{oid}"))
                    
                    details_results = await asyncio.gather(*detail_tasks)

                    for order, raw_detail_resp in zip(valid_headers_in_batch, details_results):
                        # 1. Add Header
                        all_headers.append(order)
                        
                        # 2. Add Details
                        content = raw_detail_resp.get("Data", [])
                        line_items = content.get("OrderDetails", []) if isinstance(content, dict) else (content if isinstance(content, list) else [])
                        
                        for det in line_items:
                            det.update({
                                "order_id": order.get("Id"), 
                                "report_date": order.get("Date")[:10] if order.get("Date") else None
                            })
                            all_details.append(det)
                
                if not is_finished: current_page += 5
        
        return {"headers": all_headers, "details": all_details}

    # =========================================================================
    # 2. EXTRACT INVOICES (Tách Header, Detail, Payment)
    # =========================================================================
    async def extract_invoices(self, from_date: str, to_date: datetime = None) -> Dict[str, List[Dict]]:
        all_headers = []
        all_details = []
        all_payments = []
        limit = 100
        url = f"{self.base_url}{self.config.get('Get-SAInvoiceHeader-URL', '/v1/sainvoices/paging')}"
        detail_base = f"{self.base_url}{self.config.get('Get-SAInvoiceDetail-URL', '/v1/sainvoices/')}"
        
        cutoff_date = self._parse_date_safe(to_date) if to_date else None
        logger.info(f"🧾 [ASYNC] Extracting INVOICES, DETAILS & PAYMENTS from: {from_date}")

        async with aiohttp.ClientSession(connector=TCPConnector(limit=0), timeout=ClientTimeout(total=900)) as session:
            current_page = 1
            is_finished = False
            
            while not is_finished:
                header_tasks = [self._call_api_async(session, url, method="POST", 
                               json_body={"Page": current_page + i, "Limit": limit, "IncludeInactive": True, "LastSyncDate": from_date}) 
                               for i in range(5)]
                
                header_results = await asyncio.gather(*header_tasks)
                
                valid_headers_in_batch = []
                for res in header_results:
                    data_page = res.get("Data", [])
                    if not data_page:
                        is_finished = True
                        break
                    for item in data_page:
                        item_dt = self._parse_date_safe(item.get("RefDate") or item.get("CreatedDate"))
                        if not cutoff_date or item_dt < cutoff_date:
                            valid_headers_in_batch.append(item)
                    if len(data_page) < limit: is_finished = True

                if valid_headers_in_batch:
                    detail_tasks = [self._call_api_async(session, f"{detail_base}{h.get('RefId')}") for h in valid_headers_in_batch]
                    results = await asyncio.gather(*detail_tasks)

                    for header, res in zip(valid_headers_in_batch, results):
                        all_headers.append(header)
                        
                        detail_data = res.get("Data", {})
                        ref_id = header.get("RefId")
                        report_date = header.get("ReportDate")

                        # Phẳng hóa Details
                        for d in detail_data.get("SAInvoiceDetails", []):
                            d.update({"ref_id": ref_id, "report_date": report_date})
                            all_details.append(d)
                        
                        # Phẳng hóa Payments
                        for p in detail_data.get("SAInvoicePayments", []):
                            p.update({"ref_id": ref_id, "report_date": report_date})
                            all_payments.append(p)

                if not is_finished: current_page += 5
        
        return {"headers": all_headers, "details": all_details, "payments": all_payments}

    # =========================================================================
    # 3. MASTER DATA (PRODUCTS / CUSTOMERS)
    # =========================================================================
    async def _extract_master_data(self, endpoint_key: str, default_url: str, name: str) -> List[Dict]:
        all_items = []
        url = f"{self.base_url}{self.config.get(endpoint_key, default_url)}"
        logger.info(f"📚 [ASYNC] Extracting {name}...")

        async with aiohttp.ClientSession(connector=TCPConnector(limit=0), timeout=ClientTimeout(total=900)) as session:
            current_page = 1
            is_finished = False
            while not is_finished:
                tasks = [self._call_api_async(session, url, method="POST", 
                         json_body={"Page": current_page + i, "Limit": 100, "IncludeInactive": True, 
                                    "BranchId": "00000000-0000-0000-0000-000000000000", "LastSyncDate": "2010-01-01T00:00:00Z"}) 
                         for i in range(5)]
                results = await asyncio.gather(*tasks)
                for res in results:
                    data = res.get("Data", [])
                    if data:
                        all_items.extend(data)
                        if len(data) < 100: is_finished = True
                    else:
                        is_finished = True
                if not is_finished: current_page += 5
        return all_items

    async def extract_products(self) -> List[Dict]:
        return await self._extract_master_data("Get-Product-URL", "/v1/inventoryitems/paging", "PRODUCTS")

    async def extract_customers(self) -> List[Dict]:
        return await self._extract_master_data("Get-Customer-URL", "/v1/customers/paging", "CUSTOMERS")