from typing import List, Optional
import httpx
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import asyncio
from src.tools.models import Brand
from src.tools.config import settings
from sqlmodel import Session, select
import logging
from src.tools.database import bulk_create, ensure_partition_exists
import random
from src.tools.database import create_monthly_partitions

class ScraperService:
    def __init__(self):
        self.proxy_index = 0
        self.request_count = 0
        self.last_request_time = datetime.now()
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Connection": "keep-alive",
        }
        
    def get_next_proxy(self) -> str:
        proxy = f"socks5://{settings.PROXY_USERNAME}:{settings.PROXY_PASSWORD}@{settings.PROXY_IPS[self.proxy_index]}:{settings.PROXY_PORTS[self.proxy_index]}"
        self.proxy_index = (self.proxy_index + 1) % len(settings.PROXY_IPS)
        return proxy

    async def make_request(self, url: str, max_retries: int = 3) -> Optional[httpx.Response]:
        for attempt in range(max_retries):
            try:
                proxy = self.get_next_proxy()
                # Thêm delay ngẫu nhiên để tránh bị phát hiện
                await asyncio.sleep(random.uniform(1, 3))

                async with httpx.AsyncClient(
                    timeout=httpx.Timeout(60.0),  # Tăng timeout
                    verify=False,  # Tắt SSL verification
                    follow_redirects=True,
                    proxies={
                        "http://": proxy,
                        "https://": proxy
                    }
                ) as client:
                    response = await client.get(
                        url,
                        headers=self.headers
                    )
                    response.raise_for_status()
                    return response

            except httpx.HTTPError as e:
                logging.error(f"HTTP error occurred (attempt {attempt + 1}/{max_retries}): {str(e)}")
                if attempt == max_retries - 1:
                    return None
                await asyncio.sleep(random.uniform(2, 5))  # Delay trước khi thử lại

            except Exception as e:
                logging.error(f"Error occurred (attempt {attempt + 1}/{max_retries}): {str(e)}")
                if attempt == max_retries - 1:
                    return None
                await asyncio.sleep(random.uniform(2, 5))  # Delay trước khi thử lại

    async def scrape_by_date_range(self, start_date: datetime, end_date: datetime, session: Session) -> List[Brand]:
        current_page = 1
        brands = []
        
        while True:
            # Rate limiting
            if self.request_count >= settings.REQUEST_LIMIT:
                time_diff = datetime.now() - self.last_request_time
                if time_diff.total_seconds() < 60:
                    await asyncio.sleep(60 - time_diff.total_seconds())
                self.request_count = 0
                self.last_request_time = datetime.now()
            
            # Format dates and create URL
            start_str = start_date.strftime("%d.%m.%Y")
            end_str = end_date.strftime("%d.%m.%Y")
            url = f"https://vietnamtrademark.net/search?fd={start_str}%20-%20{end_str}&p={current_page}"
            
            response = await self.make_request(url)
            if not response:
                logging.error(f"Failed to get response for page {current_page}, retrying...")
                await asyncio.sleep(random.uniform(5, 10))  # Delay dài hơn trước khi thử lại
                continue
                
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract data from table
            rows = soup.select("table.table tbody tr")
            if not rows:
                break

            for row in rows:
                try:
                    # -- Parse ngày nộp đơn
                    date_text_tag = row.select_one("td:nth-child(7)")
                    if not date_text_tag:
                        logging.warning("Missing application_date, skipping row.")
                        continue

                    date_text = date_text_tag.text.strip()
                    application_date = datetime.strptime(date_text, "%d.%m.%Y")

                    # -- Đảm bảo partition tồn tại
                    ensure_partition_exists(application_date)

                    # -- Parse từng trường (an toàn với kiểm tra None)
                    brand_name_tag = row.select_one("td:nth-child(4) label")
                    brand_name = brand_name_tag.text.strip() if brand_name_tag else ""

                    image_tag = row.select_one("td.mau-nhan img")
                    image_url = image_tag["src"] if image_tag and image_tag.has_attr("src") else None

                    product_group_tag = row.select_one("td:nth-child(5) span")
                    product_group = product_group_tag.text.strip() if product_group_tag else ""

                    status_tag = row.select_one("td.trang-thai span.badge")
                    status = status_tag.text.strip() if status_tag else ""

                    application_number_tag = row.select_one("td:nth-child(8) a")
                    application_number = application_number_tag.text.strip() if application_number_tag else ""

                    applicant_tag = row.select_one("td:nth-child(9)")
                    applicant = applicant_tag.text.strip() if applicant_tag else ""

                    representative_tag = row.select_one("td:nth-child(10)")
                    representative = representative_tag.text.strip() if representative_tag else ""

                    # -- Các trường bắt buộc: status, application_date, application_number
                    if not all([status, application_number]):
                        logging.warning(f"Missing required field (status or application_number), skipping. Row: {row}")
                        continue

                    # -- Tạo đối tượng Brand
                    brand = Brand(
                        brand_name=brand_name,
                        image_url=image_url,
                        product_group=product_group,
                        status=status,
                        application_date=application_date,
                        application_number=application_number,
                        applicant=applicant,
                        representative=representative
                    )
                    brands.append(brand)

                except Exception as e:
                    logging.error(f"Error parsing row: {str(e)}\nRow HTML: {str(row)}")
                    continue

            current_page += 1
            self.request_count += 1
            await asyncio.sleep(settings.REQUEST_DELAY)
        
        # Sử dụng bulk_create để thêm nhiều bản ghi cùng lúc
        if brands:
            bulk_create(session, brands)
        return brands
    
    async def check_pending_brands(self, session: Session):
        # Get all brands with "Đang giải quyết" status
        statement = select(Brand).where(Brand.status == "Đang giải quyết")
        pending_brands = session.exec(statement).all()
        
        for brand in pending_brands:
            url = f"https://vietnamtrademark.net/search?q={brand.application_number}"
            response = await self.make_request(url)
            
            if not response:
                continue
                
            try:
                soup = BeautifulSoup(response.text, 'html.parser')
                new_status = soup.select_one("td.trang-thai span.badge").text.strip()
                
                if new_status != brand.status:
                    brand.status = new_status
                    brand.updated_at = datetime.utcnow()
                    session.add(brand)
                    logging.info(f"Updated status for {brand.application_number}: {brand.status} -> {new_status}")
            except Exception as e:
                logging.error(f"Error checking brand {brand.application_number}: {str(e)}")
                continue
        
        session.commit()

