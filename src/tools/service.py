import os
import uuid
from urllib.parse import urlparse, unquote
import httpx
from typing import List, Optional, Callable, Dict, Any    
from datetime import datetime, timezone, date as date_type
from bs4 import BeautifulSoup
# from datetime import datetime, timezone    (Đã có ở trên)
import asyncio
from src.tools.models import Brand
from src.tools.config import settings
from sqlmodel import Session, select
from src.tools.database import bulk_create
import random
import logging

logger_service = logging.getLogger(__name__)

class ScraperService:
    def __init__(self, media_dir: str):    
        self.media_dir = media_dir    
        self.proxy_index = 0
        self.request_count = 0
        self.last_request_time = datetime.now()
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Connection": "keep-alive",
            "DNT": "1",
            "Upgrade-Insecure-Requests": "1"
        }


    def get_next_proxy(self) -> Optional[str]:
        if not settings.PROXY_IPS or not settings.PROXY_PORTS:
            logging.debug("proxy hoặc ip rỗng . chạy không có proxy.")
            return None

        has_auth = settings.PROXY_USERNAME and settings.PROXY_PASSWORD

        if not (len(settings.PROXY_IPS) == len(settings.PROXY_PORTS)):
            logging.error("lỗi config proxy: độ dài của proxy không trùng .")
            return None

        proxy_ip = settings.PROXY_IPS[self.proxy_index]
        proxy_port = settings.PROXY_PORTS[self.proxy_index]

        if has_auth:
            proxy_str = f"socks5://{settings.PROXY_USERNAME}:{settings.PROXY_PASSWORD}@{proxy_ip}:{proxy_port}"
        else:
            proxy_str = f"socks5://{proxy_ip}:{proxy_port}"

        self.proxy_index = (self.proxy_index + 1) % len(settings.PROXY_IPS)
        logging.debug(f"dùng proxy số : {proxy_ip}:{proxy_port}")
        return proxy_str

    async def download_image(self,image_url_original: str) -> str | None:
        if not image_url_original:
            logging.warning("download_image gọi với một link ảnh gốc rỗng.")
            return None


        full_save_folder_on_disk = self.media_dir    

        try:
            os.makedirs(full_save_folder_on_disk, exist_ok=True)
        except OSError as e:
            logging.error(f"không thể tạo một thư mục  {full_save_folder_on_disk}: {e}")
            return None

        try:
            ssl_verify = getattr(settings, 'SSL_VERIFY_DOWNLOAD', True)
            download_timeout = getattr(settings, 'DOWNLOAD_TIMEOUT', 30.0)

            async with httpx.AsyncClient(verify=ssl_verify, timeout=download_timeout,
                                         follow_redirects=True) as client:
                logging.info(f"Đang cố gắng tải xuống hình ảnh từ: {image_url_original}")
                img_response = await client.get(image_url_original, headers=self.headers)
                img_response.raise_for_status()
                parsed_url = urlparse(image_url_original)
                path_component = unquote(parsed_url.path)
                original_filename_from_url = os.path.basename(path_component)
                _, ext_from_url = os.path.splitext(original_filename_from_url)

                logging.debug(f"DEBUG download_image: URL gốc: '{image_url_original}'")
                logging.debug(f"DEBUG download_image: Tên tệp gốc từ URL: '{original_filename_from_url}'")
                logging.debug(f"DEBUG download_image: Mở rộng từ URL: '{ext_from_url}' (repr: {repr(ext_from_url)})")

                content_type = img_response.headers.get("content-type", "").lower()
                logging.debug(f"DEBUG download_image: Tiêu đề Content-Type: '{content_type}'")

                determined_ext = None
                if "image/jpeg" in content_type or "image/jpg" in content_type:
                    determined_ext = ".jpg"
                elif "image/png" in content_type:
                    determined_ext = ".png"
                elif "image/gif" in content_type:
                    determined_ext = ".gif"
                elif "image/webp" in content_type:
                    determined_ext = ".webp"
                elif "image/svg+xml" in content_type:
                    determined_ext = ".svg"

                if not determined_ext:
                    logging.warning(
                        f"Không thể xác định phần mở rộng chuẩn từ Content-Type '{content_type}' for {image_url_original}. "
                        f"Phần mở rộng gốc từ URL là '{ext_from_url}'. Mặc định là .jpg như một giải pháp dự phòng.")
                    determined_ext = ".jpg"

                logging.debug(
                    f"DEBUG download_image: Phần mở rộng cuối cùng được chọn: '{determined_ext}' (repr: {repr(determined_ext)})")

                unique_filename_base = str(uuid.uuid4())
                unique_filename = f"{unique_filename_base}{determined_ext}"
                logging.debug(
                    f"DEBUG download_image: Tên tệp duy nhất được tạo:'{unique_filename}' (repr: {repr(unique_filename)})")
                save_path_on_disk = os.path.join(full_save_folder_on_disk, unique_filename)

                with open(save_path_on_disk, "wb") as f:
                    f.write(img_response.content)

                # Lấy tên của thư mục con cuối cùng từ self.media_dir
                image_subfolder_name = os.path.basename(self.media_dir)    
                relative_url_path = os.path.join(image_subfolder_name, unique_filename).replace("\\", "/")    

                logging.info(
                    f"Hình ảnh đã được tải xuống thành công: {save_path_on_disk}. Phần URL tương đối: {relative_url_path}")
                logging.debug(
                    f"DEBUG download_image: Đường dẫn URL tương đối cần trả về: '{relative_url_path}' (repr: {repr(relative_url_path)})")
                return relative_url_path

        except httpx.HTTPStatusError as e_http:
            error_text = e_http.response.text if hasattr(e_http, 'response') and e_http.response and hasattr(
                e_http.response, 'text') else str(e_http)
            status_code_text = e_http.response.status_code if hasattr(e_http, 'response') and hasattr(e_http.response,
                                                                                                      'status_code') else 'N/A'
            logging.error(f"HTTP lỗi  {status_code_text} tải image {image_url_original}: {error_text}")
            return None
        except httpx.RequestError as e_req:
            logging.error(f"Yêu cầu tải xuống hình ảnh lỗi {image_url_original}: {str(e_req)}")
            return None
        except Exception as e:
            logging.error(f"Lỗi chung khi tải hình ảnh {image_url_original}: {str(e)}", exc_info=True)
            return None

    async def make_request(self, url: str, max_retries: Optional[int] = None) -> Optional[httpx.Response]:
        effective_max_retries = max_retries if max_retries is not None else settings.MAX_REQUEST_RETRIES
        current_proxy = self.get_next_proxy()
        proxies_config = {"http://": current_proxy, "https://": current_proxy} if current_proxy else None

        for attempt in range(effective_max_retries):
            try:
                min_delay_req = settings.MIN_REQUEST_DELAY
                max_delay_req = settings.MAX_REQUEST_DELAY
                if attempt > 0:
                    retry_delay = random.uniform(min_delay_req + 1,
                                                 max_delay_req + 2)
                    logging.info(f"Thử lại {url} sau {retry_delay:.2f} giây...")
                    await asyncio.sleep(retry_delay)

                async with httpx.AsyncClient(
                        timeout=httpx.Timeout(settings.REQUEST_TIMEOUT),
                        verify=settings.SSL_VERIFY_REQUEST,
                        follow_redirects=True,
                        proxies=proxies_config
                ) as client:
                    logging.debug(
                        f"Making request to {url} (Attempt {attempt + 1}/{effective_max_retries}) with proxy {current_proxy or 'None'}")
                    response = await client.get(url, headers=self.headers)
                    response.raise_for_status()
                    return response

            except httpx.HTTPStatusError as e_http:
                error_text = e_http.response.text if hasattr(e_http, 'response') and e_http.response and hasattr(
                    e_http.response, 'text') else str(e_http)
                status_code = e_http.response.status_code if hasattr(e_http, 'response') and hasattr(e_http.response,
                                                                                                     'status_code') else None
                logging.warning(
                    f"Lỗi trạng thái HTTP (Cố gắng {attempt + 1}/{effective_max_retries}) for {url}: {status_code or 'N/A'} - {error_text}")

                if status_code in [403, 401, 429]:
                    logging.error(
                        f"Lỗi HTTP nghiêm trọng {status_code} for {url}. Thay đổi proxy và thử lại nếu có thể.")
                    current_proxy = self.get_next_proxy()
                    proxies_config = {"http://": current_proxy, "https://": current_proxy} if current_proxy else None
                    if attempt == effective_max_retries - 1: logging.error(
                        f"Thử lại lần cuối thất bại với lỗi {status_code} cho {url}."); return None
                    await asyncio.sleep(random.uniform(5, 10))
                    continue
                if attempt == effective_max_retries - 1: logging.error(
                    f"Thử lại lần cuối thất bại cho {url} với lỗi {status_code}."); return None
                await asyncio.sleep(random.uniform(2, 5))

            except httpx.RequestError as e_req:
                logging.warning(
                    f"Yêu cầu Lỗi (Cố gắng {attempt + 1}/{effective_max_retries}) for {url}: {str(e_req)}")
                current_proxy = self.get_next_proxy()
                proxies_config = {"http://": current_proxy,
                                  "https": current_proxy} if current_proxy else None
                if attempt == effective_max_retries - 1: logging.error(
                    f"Thử lại lần cuối thất bại cho {url} với lỗi request: {str(e_req)}."); return None
                await asyncio.sleep(random.uniform(3, 7))

            except Exception as e_generic:
                logging.error(
                    f"Lỗi chung (Cố gắng {attempt + 1}/{effective_max_retries}) for {url}: {str(e_generic)}",
                    exc_info=True)
                if attempt == effective_max_retries - 1: logging.error(

                    f"Thử lại lần cuối thất bại cho {url} với lỗi chung: {str(e_generic)}."); return None
                await asyncio.sleep(random.uniform(2, 5))
        logging.error(f"All {effective_max_retries} thử lại không thành công cho URL: {url}")
        return None

    async def scrape_by_date_range(self, start_date: date_type, end_date: date_type, session: Session,initial_start_page: int, state_save_callback: Callable[[int], None]) -> Dict[str, Any]:
        current_page = initial_start_page
        brands_collected_in_this_run: List[Brand] = []
        request_limit_per_interval = settings.REQUEST_LIMIT_PER_INTERVAL
        request_interval_seconds = settings.REQUEST_INTERVAL_SECONDS
        min_request_delay = settings.MIN_REQUEST_DELAY
        max_request_delay = settings.MAX_REQUEST_DELAY

        scrape_status_result = {
            "status": "unknown_error",
            "brands_processed_count": 0,
            "message": "Scraping did not complete as expected."
        }

        while True:
            if self.request_count >= request_limit_per_interval:
                time_diff = datetime.now() - self.last_request_time
                if time_diff.total_seconds() < request_interval_seconds:
                    sleep_duration = request_interval_seconds - time_diff.total_seconds()
                    logging.info(
                        f"Đạt giới hạn request nội bộ. Nghỉ {sleep_duration:.2f} giây.")
                    await asyncio.sleep(sleep_duration)
                self.request_count = 0
                self.last_request_time = datetime.now()

            start_str = start_date.strftime("%d.%m.%Y")
            end_str = end_date.strftime("%d.%m.%Y")
            url = f"https://vietnamtrademark.net/search?fd={start_str}%20-%20{end_str}&p={current_page}"

            logging.info(
                f"Đang cào trang: {current_page} cho ngày {start_str} (URL: {url})")
            response = await self.make_request(url)
            self.request_count += 1

            if not response:
                logging.error(
                    f"Không nhận được phản hồi cho trang {current_page} (URL: {url}). Dừng xử lý ngày này.")
                scrape_status_result = {
                    "status": "request_error",
                    "brands_processed_count": len(brands_collected_in_this_run),
                    "message": f"Failed to get response for page {current_page} of day {start_str}."
                }
                break

            try:
                soup = BeautifulSoup(response.text, 'html.parser')
            except Exception as e_soup:
                logging.error(f"Lỗi khi parse HTML cho trang {current_page} ngày {start_str}: {e_soup}",
                              exc_info=True)
                scrape_status_result = {
                    "status": "soup_error",
                    "brands_processed_count": len(brands_collected_in_this_run),
                    "message": f"HTML parsing error for page {current_page} of day {start_str}."
                }
                break

            rows = soup.select("table.table tbody tr")
            if not rows:
                if current_page == 1:
                    logging.info(
                        f"Không tìm thấy dữ liệu nào trên trang {current_page} cho ngày {start_str}. Có thể ngày này không có nhãn hiệu.")
                    scrape_status_result = {
                        "status": "no_data_on_first_page",
                        "brands_processed_count": len(brands_collected_in_this_run),
                        "message": f"No data found on the first page for day {start_str}."
                    }
                else:
                    logging.info(
                        f"Không tìm thấy hàng (dữ liệu) nào trên trang {current_page} cho ngày {start_str}. Kết thúc cho ngày này.")
                    scrape_status_result = {
                        "status": "completed_all_pages",
                        "brands_processed_count": len(brands_collected_in_this_run),
                        "message": f"Successfully scraped all pages for day {start_str}."
                    }
                break

            brands_extracted_from_this_page: List[Brand] = []
            page_had_new_valid_data = False
            for row_idx, row in enumerate(rows):
                try:
                    date_text_tag = row.select_one("td:nth-child(7)")
                    if not date_text_tag or not date_text_tag.text.strip():
                        logging.warning(
                            f"Hàng {row_idx + 1} trang {current_page} ngày {start_str}: Thiếu ngày nộp đơn. Bỏ qua hàng.")
                        continue
                    try:
                        parsed_application_date = datetime.strptime(date_text_tag.text.strip(), "%d.%m.%Y").date()
                    except ValueError as ve:
                        logging.warning(
                            f"Hàng {row_idx + 1} trang {current_page} ngày {start_str}: Lỗi parse ngày '{date_text_tag.text.strip()}': {ve}. Bỏ qua hàng.")
                        continue

                    if not (
                            start_date <= parsed_application_date <= end_date):
                        logging.warning(
                            f"Hàng {row_idx + 1} trang {current_page}: Ngày nộp đơn {parsed_application_date.strftime('%Y-%m-%d')} "
                            f"nằm ngoài khoảng đang scrape ({start_date.strftime('%Y-%m-%d')} - {end_date.strftime('%Y-%m-%d')}). Bỏ qua.")
                        continue

                    # ensure_partition_exists(parsed_application_date)    (Đã được chuyển ra ngoài worker function trong main.py)

                    brand_name_tag = row.select_one("td:nth-child(4) label")
                    brand_name = brand_name_tag.text.strip() if brand_name_tag else ""

                    image_tag = row.select_one("td.mau-nhan img")
                    image_url_original_src = image_tag["src"] if image_tag and image_tag.has_attr("src") else None
                    final_image_url_for_db = None
                    if image_url_original_src:
                        current_image_url_to_download = image_url_original_src
                        if current_image_url_to_download.startswith("/"):
                            current_image_url_to_download = f"{settings.SOURCE_WEBSITE_DOMAIN.rstrip('/')}{current_image_url_to_download}"    
                        saved_relative_image_path = await self.download_image(current_image_url_to_download)
                        if saved_relative_image_path:
                            final_image_url_for_db = f"{settings.LOCAL_MEDIA_BASE_URL.rstrip('/')}/{saved_relative_image_path.lstrip('/')}"    

                    product_group_tags = row.select("td:nth-child(5) span")
                    if product_group_tags:
                        product_group_values = [tag.text.strip() for tag in product_group_tags if tag.text.strip()]
                        product_group = ", ".join(product_group_values)
                    else:
                        product_group = ""

                    status_tag = row.select_one("td.trang-thai span.badge")
                    status = status_tag.text.strip() if status_tag else ""

                    application_number_tag = row.select_one("td:nth-child(8) a")
                    application_number = application_number_tag.text.strip() if application_number_tag else ""
                    if not application_number:
                        logging.warning(
                            f"Hàng {row_idx + 1} trang {current_page} ngày {start_str}: Thiếu số đơn. Bỏ qua hàng.")
                        continue

                    applicant_tag = row.select_one("td:nth-child(9)")
                    applicant = applicant_tag.text.strip() if applicant_tag else ""

                    representative_tag = row.select_one("td:nth-child(10)")
                    representative = representative_tag.text.strip() if representative_tag else ""

                    product_detail_tag = row.select_one("td:nth-child(8) a")
                    product_detail_href = product_detail_tag.get(
                        "href") if product_detail_tag else ""

                    stmt = select(Brand).where(Brand.application_number == application_number)
                    existing_brand = session.exec(stmt).first()

                    if existing_brand:
                        logging.info(
                            f"Brand với số đơn {application_number} (trang {current_page}, ngày {start_str}) đã tồn tại. Bỏ qua.")
                        continue

                    brand_obj = Brand(
                        brand_name=brand_name,
                        image_url=final_image_url_for_db if final_image_url_for_db else "",
                        product_group=product_group,
                        status=status,
                        application_date=parsed_application_date,
                        application_number=application_number,
                        applicant=applicant,
                        representative=representative,
                        product_detail=f"{settings.SOURCE_WEBSITE_DOMAIN.rstrip('/')}{product_detail_href}" if product_detail_href else ""


                    )
                    brands_extracted_from_this_page.append(brand_obj)
                    page_had_new_valid_data = True

                except Exception as e_row_processing:
                    row_html_snippet = str(row)[:250]
                    logging.error(
                        f"Lỗi xử lý hàng {row_idx + 1} trên trang {current_page} ngày {start_str}: {e_row_processing}\nHTML Snippet: {row_html_snippet}",
                        exc_info=True)

                    continue

            if brands_extracted_from_this_page:
                logging.info(
                    f"Trang {current_page} ngày {start_str}: Trích xuất được {len(brands_extracted_from_this_page)} nhãn hiệu mới.")
                try:
                    bulk_create(session, brands_extracted_from_this_page)
                    brands_collected_in_this_run.extend(brands_extracted_from_this_page)
                    state_save_callback(current_page)

                except Exception as e_db_commit:
                    logging.error(
                        f"Lỗi khi thêm dữ liệu cho trang {current_page} ngày {start_str} vào DB (có thể do bulk_create): {e_db_commit}",
                        exc_info=True)
                    scrape_status_result = {
                        "status": "db_commit_error",
                        "brands_processed_count": len(brands_collected_in_this_run),
                        "message": f"DB commit error on page {current_page} for day {start_str}."
                    }
                    break

            elif page_had_new_valid_data is False and rows:
                logging.info(
                    f"Trang {current_page} ngày {start_str} đã xử lý nhưng không có dữ liệu mới nào được thêm vào DB.")
                state_save_callback(current_page)

            if scrape_status_result["status"] not in ["request_error", "soup_error",
                                                      "db_commit_error"]:
                if brands_extracted_from_this_page or (page_had_new_valid_data is False and rows):
                    scrape_status_result = {
                        "status": "processing_pages",

                        "brands_processed_count": len(brands_collected_in_this_run),
                        "message": f"Successfully processed page {current_page} for day {start_str}."

                    }

            current_page += 1
            await asyncio.sleep(random.uniform(min_request_delay, max_request_delay))

        scrape_status_result["brands_processed_count"] = len(brands_collected_in_this_run)
        if scrape_status_result["status"] == "completed_all_pages" and len(
                brands_collected_in_this_run) == 0 and initial_start_page == 1:
            pass

        logging.info(
            f"Kết thúc scrape cho ngày {start_date.strftime('%Y-%m-%d')}. "
            f"Trạng thái: {scrape_status_result['status']}. "
            f"Tổng số nhãn hiệu được xử lý trong lần gọi này: {scrape_status_result['brands_processed_count']}.")
        return scrape_status_result

    async def check_pending_brands(self, session: Session):
        logger = logging.getLogger(f"{self.__class__.__name__}.check_pending_brands")
        logger.info("Bắt đầu kiểm tra các đơn có trạng thái 'Đang giải quyết'...")
        statement = select(Brand).where(Brand.status == "Đang giải quyết")
        pending_brands: List[Brand] = session.exec(statement).all()

        if not pending_brands:
            logger.info("✅ Không tìm thấy đơn nào có trạng thái 'Đang giải quyết' để kiểm tra.")
            return

        logger.info(f"🔍 Tìm thấy {len(pending_brands)} đơn có trạng thái 'Đang giải quyết' để kiểm tra.")
        updated_count = 0
        processed_count = 0

        min_delay_check = settings.MIN_DELAY_CHECK_PENDING
        max_delay_check = settings.MAX_DELAY_CHECK_PENDING

        for brand_idx, brand in enumerate(pending_brands):
            processed_count += 1
            logger.info(
                f"Đang xử lý đơn {brand_idx + 1}/{len(pending_brands)}: ID {brand.id}, Số đơn {brand.application_number}")

            if not brand.application_number:
                logger.warning(f"⚠️ Đơn có ID {brand.id} không có số đơn (application_number). Bỏ qua.")
                await asyncio.sleep(
                    random.uniform(min_delay_check / 2, max_delay_check / 2))
                continue

            url = f"https://vietnamtrademark.net/search?q={brand.application_number.strip()}"
            logger.info(f"🌍 Gọi đến VietnamTrademark: {url}")

            response = await self.make_request(url)
            if not response:
                logger.warning(
                    f"❌ Không nhận được phản hồi từ VietnamTrademark cho số đơn {brand.application_number} (ID: {brand.id}). Bỏ qua đơn này.")
                await asyncio.sleep(
                    random.uniform(min_delay_check, max_delay_check))
                continue

            try:
                soup = BeautifulSoup(response.text, 'html.parser')
                target_row = None
                rows_on_page = soup.select("table.table tbody tr")

                if not rows_on_page:
                    logger.warning(
                        f"📄 Không tìm thấy bảng/hàng dữ liệu nào trên trang kết quả cho số đơn {brand.application_number}.")
                    await asyncio.sleep(
                        random.uniform(min_delay_check / 2, max_delay_check / 2))
                    continue

                for r_check in rows_on_page:
                    app_num_tag_check = r_check.select_one("td:nth-child(8) a")
                    if app_num_tag_check and app_num_tag_check.text.strip() == brand.application_number:
                        target_row = r_check
                        break

                if not target_row:
                    logger.warning(
                        f"📄 Không tìm thấy hàng khớp với số đơn {brand.application_number} trên trang kết quả tìm kiếm.")

                    await asyncio.sleep(
                        random.uniform(min_delay_check / 2, max_delay_check / 2))
                    continue

                status_tag = target_row.select_one("td.trang-thai span.badge")
                if status_tag:
                    new_status = status_tag.text.strip()
                    logger.info(
                        f"📊 Trạng thái mới từ web cho {brand.application_number}: '{new_status}' (Trạng thái hiện tại trong DB: '{brand.status}')")

                    if new_status != brand.status:
                        old_status = brand.status
                        brand.status = new_status
                        brand.updated_at = datetime.now(timezone.utc)
                        session.add(brand)
                        updated_count += 1
                        logger.info(
                            f"🔄 CẬP NHẬT: Đơn {brand.application_number} (ID: {brand.id}) thay đổi trạng thái từ '{old_status}' -> '{new_status}'")
                    else:
                        logger.info(
                            f"✅ Trạng thái cho đơn {brand.application_number} (ID: {brand.id}) không thay đổi ('{brand.status}').")
                else:
                    logger.warning(
                        f"📄 Không tìm thấy thẻ trạng thái (status_tag) cho số đơn {brand.application_number} (ID: {brand.id}) trong hàng tương ứng.")

            except Exception as e_check:
                logger.error(
                    f"❌ Lỗi khi xử lý/bóc tách trạng thái cho đơn {brand.application_number} (ID: {brand.id}): {str(e_check)}",
                    exc_info=True)
                await asyncio.sleep(
                    random.uniform(min_delay_check, max_delay_check))
                continue

            if brand_idx < len(pending_brands) - 1:
                await asyncio.sleep(
                    random.uniform(min_delay_check, max_delay_check))

        if updated_count > 0:
            try:
                session.commit()
                logger.info(f"💾 ĐÃ COMMIT THÀNH CÔNG: Cập nhật trạng thái cho {updated_count} đơn vào database.")
            except Exception as e_commit:
                logger.error(f"❌ Lỗi khi commit các thay đổi trạng thái vào database: {e_commit}", exc_info=True)
                session.rollback()
                logger.info("Đã rollback transaction do lỗi commit.")
        elif processed_count > 0:
            logger.info("✅ Không có trạng thái đơn nào cần cập nhật sau khi kiểm tra toàn bộ danh sách.")

        logger.info(f"Hoàn tất kiểm tra. Đã xử lý {processed_count} đơn, cập nhật {updated_count} đơn.")


    async def increment_brand_search_count(self, session: Session, brand_name: str) -> bool:
        logger_service.info(f"📈 Yêu cầu tăng va_count cho nhãn hiệu: '{brand_name}'")
        if not brand_name:
            logger_service.warning("⚠️ Tên nhãn hiệu rỗng, không thể tăng va_count.")
            return False

        try:
            statement = select(Brand).where(Brand.brand_name == brand_name)
            brand_to_update = session.exec(statement).first()

            if brand_to_update:
                logger_service.info(
                    f"🔍 Tìm thấy nhãn hiệu: ID {brand_to_update.id}, Tên: {brand_to_update.brand_name}, va_count hiện tại: {brand_to_update.va_count}")
                brand_to_update.va_count += 1
                brand_to_update.updated_at = datetime.now(timezone.utc)
                session.add(brand_to_update)
                session.commit()
                session.refresh(brand_to_update)
                logger_service.info(f"✅ Đã tăng va_count cho '{brand_name}' thành {brand_to_update.va_count}.")
                return True
            else:
                logger_service.warning(
                    f"🤷 Không tìm thấy nhãn hiệu '{brand_name}' trong cơ sở dữ liệu để tăng va_count.")
                return False
        except Exception as e:
            logger_service.error(f"❌ Lỗi khi tăng va_count cho '{brand_name}': {e}", exc_info=True)
            session.rollback()  # Rollback nếu có lỗi xảy ra
            return False