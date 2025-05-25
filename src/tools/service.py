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

class ScraperService: # sẽ tạo một class là ScraperService
    def __init__(self): # tạo một cái contructor
        self.proxy_index = 0 # gán cho proxy_index = 0
        self.request_count = 0 # biến đếm số lần gửi request
        self.last_request_time = datetime.now() # đếm cái reuest cuối cùng
        self.headers = { # phần header để cấu hình code
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Connection": "keep-alive",
        }
        
    def get_next_proxy(self) -> str: #hàm này sẽ sử lý thông tin cảu proxy
        # nó sẽ tạo một cái link tương ứng như sau sockes5://name:pass@IP: in dexsố mấy
        proxy = f"socks5://{settings.PROXY_USERNAME}:{settings.PROXY_PASSWORD}@{settings.PROXY_IPS[self.proxy_index]}:{settings.PROXY_PORTS[self.proxy_index]}"
        # nó sẽ lấy cái giá trị proxy_index đầu tiền sẽ công một và chia lấy dữ với độ dài của proxyip chỗ này éo hiểu lắm
        self.proxy_index = (self.proxy_index + 1) % len(settings.PROXY_IPS)
        return proxy# trả về một cái return

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
        brands_to_add = []
        stop_scraping_due_to_duplicate = False  # Cờ để dừng scrape khi gặp trùng lặp

        while True:  # Vòng lặp qua các trang
            if self.request_count >= settings.REQUEST_LIMIT:
                time_diff = datetime.now() - self.last_request_time
                if time_diff.total_seconds() < 60:
                    await asyncio.sleep(60 - time_diff.total_seconds())
                self.request_count = 0
                self.last_request_time = datetime.now()

            start_str = start_date.strftime("%d.%m.%Y")
            end_str = end_date.strftime("%d.%m.%Y")
            url = f"https://vietnamtrademark.net/search?fd={start_str}%20-%20{end_str}&p={current_page}"

            logging.info(f"Scraping URL: {url}")
            response = await self.make_request(url)
            if not response:
                logging.error(f"Failed to get response for page {current_page}, stopping for this date range.")
                break  # Dừng nếu không thể lấy dữ liệu trang

            soup = BeautifulSoup(response.text, 'html.parser')
            rows = soup.select("table.table tbody tr")
            if not rows:
                logging.info(f"No more data found on page {current_page} or page is empty.")
                break  # Dừng nếu không có dòng nào trên trang (kết thúc dữ liệu)

            for row in rows:  # Vòng lặp qua các dòng trên một trang
                try:
                    date_text_tag = row.select_one("td:nth-child(7)")
                    if not date_text_tag:
                        logging.warning("Missing application_date tag, skipping row.")
                        continue
                    date_text = date_text_tag.text.strip()
                    parsed_application_date = datetime.strptime(date_text, "%d.%m.%Y")

                    ensure_partition_exists(parsed_application_date)

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

                    if not all([status, application_number]):
                        logging.warning(
                            f"Missing required field (status or application_number). Skipping. Row: {row.prettify()}")
                        continue

                    # --- BẮT ĐẦU KIỂM TRA TRÙNG LẶP ---
                    stmt = select(Brand).where(Brand.application_number == application_number)
                    existing_brand = session.exec(stmt).first()

                    if existing_brand:
                        message = f"Dữ liệu này đã có trong database (Số đơn: {application_number}). Dừng quá trình scrape cho khoảng ngày này."
                        print(f"Thông báo: {message}")
                        logging.info(message)
                        stop_scraping_due_to_duplicate = True  # Đặt cờ để dừng
                        break  # Thoát khỏi vòng lặp xử lý các dòng (for row in rows)
                    # --- KẾT THÚC KIỂM TRA TRÙNG LẶP ---

                    brand = Brand(
                        brand_name=brand_name,
                        image_url=image_url,
                        product_group=product_group,
                        status=status,
                        application_date=parsed_application_date,
                        application_number=application_number,
                        applicant=applicant,
                        representative=representative
                    )
                    brands_to_add.append(brand)

                except ValueError as ve:
                    logging.error(f"Error parsing date ('{date_text}') in row: {str(ve)}\nRow HTML: {row.prettify()}")
                    continue
                except Exception as e:
                    logging.error(f"Error parsing row: {str(e)}\nRow HTML: {row.prettify()}")
                    continue

            if stop_scraping_due_to_duplicate:
                break  # Thoát khỏi vòng lặp xử lý các trang (while True)

            current_page += 1
            self.request_count += 1
            await asyncio.sleep(settings.REQUEST_DELAY)

        # Kết thúc vòng lặp while (do hết dữ liệu, lỗi hoặc tìm thấy trùng lặp)
        # Thêm tất cả các brand đã thu thập được (trước khi dừng hoặc khi hoàn tất)
        if brands_to_add:
            logging.info(f"Adding {len(brands_to_add)} new brand(s) to the database.")
            bulk_create(session, brands_to_add)
        else:
            logging.info("No new brands to add for this date range.")

        return brands_to_add

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

