from typing import List, Optional
import httpx
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import asyncio
from .models import Brand
from .config import settings
from sqlmodel import Session, select
import logging
from .database import bulk_create, ensure_partition_exists

class ScraperService:
    def __init__(self):
        self.proxy_index = 0
        self.request_count = 0
        self.last_request_time = datetime.now()
        
    def get_next_proxy(self) -> str:
        proxy = f"http://{settings.PROXY_USERNAME}:{settings.PROXY_PASSWORD}@{settings.PROXY_IPS[self.proxy_index]}:{settings.PROXY_PORTS[self.proxy_index]}"
        self.proxy_index = (self.proxy_index + 1) % len(settings.PROXY_IPS)
        return proxy
    
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
            
            async with httpx.AsyncClient(proxies={"http://": self.get_next_proxy()}) as client:
                response = await client.get(url)
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Extract data from table
                rows = soup.select("table.table tbody tr")
                if not rows:
                    break
                    
                for row in rows:
                    application_date = datetime.strptime(row.select_one("td:nth-child(7)").text, "%d.%m.%Y")
                    
                    # Đảm bảo partition tồn tại trước khi thêm dữ liệu
                    ensure_partition_exists(application_date)
                    
                    brand = Brand(
                        brand_name=row.select_one("td:nth-child(4) label").text,
                        image_url=row.select_one("td.mau-nhan img")["src"],
                        product_group=row.select_one("td:nth-child(5) span").text,
                        status=row.select_one("td.trang-thai span.badge").text,
                        application_date=application_date,
                        application_number=row.select_one("td:nth-child(8) a").text,
                        applicant=row.select_one("td:nth-child(9)").text,
                        representative=row.select_one("td:nth-child(10)").text
                    )
                    brands.append(brand)
                
                current_page += 1
                self.request_count += 1
                await asyncio.sleep(settings.REQUEST_DELAY)
        
        # Sử dụng bulk_create để thêm nhiều bản ghi cùng lúc
        bulk_create(session, brands)
        return brands
    
    async def check_pending_brands(self, session: Session):
        # Get all brands with "Đang giải quyết" status
        statement = select(Brand).where(Brand.status == "Đang giải quyết")
        pending_brands = session.exec(statement).all()
        
        for brand in pending_brands:
            url = f"https://vietnamtrademark.net/search?q={brand.application_number}"
            
            async with httpx.AsyncClient(proxies={"http://": self.get_next_proxy()}) as client:
                response = await client.get(url)
                soup = BeautifulSoup(response.text, 'html.parser')
                
                new_status = soup.select_one("td.trang-thai span.badge").text
                if new_status != brand.status:
                    brand.status = new_status
                    brand.updated_at = datetime.utcnow()
                    session.add(brand)
                    logging.info(f"Updated status for {brand.application_number}: {brand.status} -> {new_status}")
        
        session.commit()

