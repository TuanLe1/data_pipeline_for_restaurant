import logging
import asyncio
import aiohttp
from aiohttp import TCPConnector, ClientTimeout
import random
import math
from typing import List, Dict, Optional, Union, Any
from datetime import datetime, timezone
import dateutil.parser

from .auth import CukCukAuth

logger = logging.getLogger(__name__)

class CukCukExtractor:
    _cached_token = None
    _token_expiry = None

    def __init__(self, config: dict):
        self.config = config
        self.base_url = config.get("CukCuk-Base-URL", "https://graphapi.cukcuk.com/api")
        self.company_code = config.get("CompanyCode", "uraeteicambodia")
        self.branch_map = config.get("Branch-Name_Mapper", {})
        self.branch_id = list(self.branch_map.keys())[0] if self.branch_map else ""

        # Rate Limit & Network Config
        self.semaphore = asyncio.Semaphore(5)
        self.max_retries = 5
        self.base_delay = 1.0

        # Initialize Auth Handler (Login is deferred)
        self.auth_handler = CukCukAuth(config)

    def _get_valid_token(self):
        """Lazy Login: Check cached token or login if needed"""
        if CukCukExtractor._cached_token:
            return CukCukExtractor._cached_token

        logger.info("Authenticating with CukCuk (One-time)...")
        try:
            token = self.auth_handler.get_token()
            CukCukExtractor._cached_token = token
            return token
        except Exception as e:
            logger.critical(f"Auth Failed: {e}")
            raise e

    async def _call_api_async(self, session: aiohttp.ClientSession, url: str, method: str = "GET", 
                              json_body: dict = None, params: dict = None) -> Optional[Dict]:
        """Generic Async API Call with Auto-Auth & Timeout Handling"""
        
        token = self._get_valid_token()
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
            "CompanyCode": self.company_code
        }

        async with self.semaphore:
            await asyncio.sleep(random.uniform(0.1, 0.3))
            for attempt in range(self.max_retries):
                try:
                    async with session.request(method, url, json=json_body, params=params, headers=headers) as response:
                        if response.status == 200:
                            return await response.json()
                        
                        elif response.status == 401:
                            # Token expired! Clear cache and retry immediately
                            logger.warning("Token expired (401). Refreshing...")
                            CukCukExtractor._cached_token = None
                            token = self._get_valid_token()
                            headers["Authorization"] = f"Bearer {token}"
                            continue # Retry the loop with new token

                        elif response.status in [429, 500, 502, 503, 504]:
                            sleep_time = self.base_delay * (2 ** attempt) + random.uniform(0, 1)
                            logger.warning(f"API {response.status}. Retrying in {sleep_time:.1f}s... ({url})")
                            await asyncio.sleep(sleep_time)
                        else:
                            # 4xx errors (except 429) usually cannot be retried
                            text = await response.text()
                            logger.error(f"API Error {response.status}: {text[:200]}")
                            return None
                
                except (asyncio.TimeoutError, aiohttp.ServerTimeoutError) as e:
                    sleep_time = self.base_delay * (2 ** attempt)
                    logger.warning(f"Timeout Error (Attempt {attempt+1}): {e}. Retrying...")
                    await asyncio.sleep(sleep_time)

                except aiohttp.ClientError as e:
                    sleep_time = self.base_delay * (2 ** attempt)
                    logger.warning(f"⚠️ Network Error: {e}. Retrying...")
                    await asyncio.sleep(sleep_time)
            
            logger.error(f"Failed {url} after {self.max_retries} attempts.")
            return None

    def _parse_date_safe(self, date_val: Union[str, datetime]) -> datetime:
        """Parse date to UTC datetime"""
        if isinstance(date_val, datetime):
            return date_val.replace(tzinfo=timezone.utc) if date_val.tzinfo is None else date_val
        
        if not date_val:
            return datetime.min.replace(tzinfo=timezone.utc)

        try:
            dt = dateutil.parser.parse(date_val)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except:
            return datetime.min.replace(tzinfo=timezone.utc)

    # =========================================================================
    # 1. EXTRACT ORDERS
    # =========================================================================
    async def extract_orders(self, from_date: str, to_date: datetime = None) -> List[Dict]:
        all_orders = []
        limit = 100
        endpoint = self.config.get("Get-OrderHeader-URL", "/v1/orders/paging")
        url = f"{self.base_url}{endpoint}"
        detail_endpoint = self.config.get("Get-OrderDetail-URL", "/v1/orders/")
        
        cutoff_date = self._parse_date_safe(to_date) if to_date else None
        logger.info(f"[ASYNC] Start extracting ORDERS from: {from_date} (Batch Strategy)")

        connector = TCPConnector(limit=0, ttl_dns_cache=300)
        timeout = ClientTimeout(total=600) 

        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            current_page = 1
            is_finished = False
            BATCH_SIZE = 5
            
            while not is_finished:
                header_tasks = []
                for i in range(BATCH_SIZE):
                    payload = {
                        "Page": current_page + i, "Limit": limit, 
                        "BranchId": self.branch_id, "LastSyncDate": from_date
                    }
                    header_tasks.append(self._call_api_async(session, url, method="POST", json_body=payload))
                
                header_results = await asyncio.gather(*header_tasks)
                
                valid_orders_in_batch = []
                batch_has_empty_page = False
                items_count_in_batch = 0
                
                for res in header_results:
                    if not res: continue
                    data_page = res.get("Data", [])
                    
                    if not data_page:
                        batch_has_empty_page = True
                        continue
                        
                    items_count_in_batch += len(data_page)
                    if len(data_page) < limit: 
                        is_finished = True # Found last page
                    
                    # Client-side Date Filter
                    for item in data_page:
                        if cutoff_date:
                            item_dt = self._parse_date_safe(item.get("Date"))
                            if item_dt < cutoff_date:
                                valid_orders_in_batch.append(item)
                        else:
                            valid_orders_in_batch.append(item)
                # Fetch Details for valid orders
                if valid_orders_in_batch:
                    detail_tasks = []
                    for order in valid_orders_in_batch:
                        oid = order.get("Id")
                        detail_url = f"{self.base_url}{detail_endpoint}{oid}"
                        detail_tasks.append(self._call_api_async(session, detail_url))
                    
                    details_results = await asyncio.gather(*detail_tasks)

                    # Merge logic
                    for order, raw_detail_resp in zip(valid_orders_in_batch, details_results):
                        if raw_detail_resp:
                            content = raw_detail_resp.get("Data", [])
                            if isinstance(content, dict):
                                line_items = content.get("OrderDetails", [])
                            else:
                                line_items = content if isinstance(content, list) else []
                            
                            parent_info = {
                                "OrderId": order.get("Id"), "OrderDate": order.get("Date"),
                                "BranchId": order.get("BranchId"), "BranchName": order.get("BranchName")
                            }
                            for item in line_items: item.update(parent_info)
                            order["line_items"] = line_items
                        else:
                            order["line_items"] = []
                        all_orders.append(order)
                
                # Check stop condition
                if batch_has_empty_page or items_count_in_batch == 0:
                    is_finished = True
                
                if not is_finished:
                    current_page += BATCH_SIZE
        
        return all_orders

    # =========================================================================
    # 2. EXTRACT INVOICES
    # =========================================================================
    async def extract_invoices(self, from_date: str, to_date: datetime = None) -> List[Dict]:
        all_items = []
        limit = 100
        endpoint = self.config.get("Get-SAInvoiceHeader-URL", "/v1/sainvoices/paging")
        url = f"{self.base_url}{endpoint}"
        detail_base_url = f"{self.base_url}{self.config.get('Get-SAInvoiceDetail-URL', '/v1/sainvoices/')}"
        
        cutoff_date = self._parse_date_safe(to_date) if to_date else None
        logger.info(f"🧾 [ASYNC] Syncing INVOICES from {from_date} (Batch Strategy)...")

        connector = TCPConnector(limit=0, ttl_dns_cache=300)
        timeout = ClientTimeout(total=600)

        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            current_page = 1
            is_finished = False
            BATCH_SIZE = 5 
            
            while not is_finished:
                # 1. Fetch Header Pages
                header_tasks = []
                for i in range(BATCH_SIZE):
                    payload = { "Page": current_page + i, "Limit": limit, "IncludeInactive": True, "LastSyncDate": from_date }
                    header_tasks.append(self._call_api_async(session, url, method="POST", json_body=payload))
                
                header_results = await asyncio.gather(*header_tasks)
                
                valid_items_in_batch = []
                batch_has_empty_page = False
                items_count_in_batch = 0

                for res in header_results:
                    if not res: continue
                    data_page = res.get("Data", [])
                    
                    if not data_page: 
                        batch_has_empty_page = True
                        continue
                    
                    items_count_in_batch += len(data_page)
                    if len(data_page) < limit: is_finished = True

                    for item in data_page:
                        if cutoff_date:
                            item_dt = self._parse_date_safe(item.get("RefDate") or item.get("CreatedDate"))
                            if item_dt < cutoff_date:
                                valid_items_in_batch.append(item)
                        else:
                            valid_items_in_batch.append(item)

                # 2. Fetch Details
                if valid_items_in_batch:
                    detail_tasks = []
                    for item in valid_items_in_batch:
                        rid = item.get("RefId")
                        detail_tasks.append(self._call_api_async(session, f"{detail_base_url}{rid}"))
                    
                    results = await asyncio.gather(*detail_tasks)

                    for item, res in zip(valid_items_in_batch, results):
                        detail_data = res.get("Data", {}) if res else {}
                        item["invoice_details"] = detail_data.get("SAInvoiceDetails", [])
                        item["invoice_payments"] = detail_data.get("SAInvoicePayments", [])
                        all_items.append(item)

                if batch_has_empty_page or items_count_in_batch == 0:
                    is_finished = True
                
                if not is_finished:
                    current_page += BATCH_SIZE
        
        return all_items

    # =========================================================================
    # 3. MASTER DATA (PRODUCTS / CUSTOMERS)
    # =========================================================================
    async def _extract_master_data(self, endpoint_key: str, default_url: str, name: str) -> List[Dict]:
        """
        Generic Master Data Extractor with Direct Batching
        1. Paginate through data with given endpoint
        2. Collect all items into a list
        3. Return the list of items
        """
        all_items = []
        limit = 100
        endpoint = self.config.get(endpoint_key, default_url)
        url = f"{self.base_url}{endpoint}"
        
        base_payload = {
            "Limit": limit, "IncludeInactive": True,
            "BranchId": "00000000-0000-0000-0000-000000000000", "LastSyncDate": "2010-01-01T00:00:00Z"
        }
        
        logger.info(f"📚 [ASYNC] Extracting {name} (Direct Batching)...")

        connector = TCPConnector(limit=0, ttl_dns_cache=300)
        timeout = ClientTimeout(total=600)
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            current_page = 1
            is_finished = False
            BATCH_SIZE = 20 
            
            while not is_finished:
                tasks = []
                # Create concurrent tasks for the next batch of pages
                for i in range(BATCH_SIZE):
                    p_load = base_payload.copy()
                    p_load["Page"] = current_page + i
                    tasks.append(self._call_api_async(session, url, method="POST", json_body=p_load))
                
                # Fire all requests in the batch simultaneously
                results = await asyncio.gather(*tasks)
                
                items_in_batch = 0
                batch_has_empty_page = False

                for res in results:
                    if res and res.get("Success"):
                        data = res.get("Data", [])
                        if data:
                            all_items.extend(data)
                            items_in_batch += len(data)
                            # If any page in this batch is not full, it's the last page.
                            if len(data) < limit: 
                                is_finished = True
                        else:
                            batch_has_empty_page = True
                
                if batch_has_empty_page or items_in_batch == 0:
                    is_finished = True
                
                if not is_finished:
                    current_page += BATCH_SIZE

        logger.info(f"{name}: Extracted {len(all_items)} items.")
        return all_items

    async def extract_products(self) -> List[Dict]:
        return await self._extract_master_data("Get-Product-URL", "/v1/inventoryitems/paging", "PRODUCTS")

    async def extract_customers(self) -> List[Dict]:
        return await self._extract_master_data("Get-Customer-URL", "/v1/customers/paging", "CUSTOMERS")