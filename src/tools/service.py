# src/tools/service.py

import os
import uuid
from typing import List, Optional, Callable
from urllib.parse import urlparse, unquote

import httpx
from bs4 import BeautifulSoup
from datetime import datetime, timezone  # Đã thêm timezone, bỏ timedelta nếu không dùng
import asyncio
from src.tools.models import Brand
from src.tools.config import settings  # Import đối tượng settings đã được khởi tạo
from sqlmodel import Session, select
import logging
from src.tools.database import bulk_create, ensure_partition_exists
import random

from src.tools.state_manager import logging

# from src.tools.database import create_monthly_partitions # Xóa nếu không dùng

# Sử dụng trực tiếp thuộc tính từ đối tượng settings
LOCAL_MEDIA_BASE_URL = settings.LOCAL_MEDIA_BASE_URL
SOURCE_WEBSITE_DOMAIN = settings.SOURCE_WEBSITE_DOMAIN


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
            "DNT": "1",
            "Upgrade-Insecure-Requests": "1"
        }
        # Thêm lại self.project_root_dir
        self.project_root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

    def get_next_proxy(self) -> Optional[str]:
        if not settings.PROXY_IPS or not settings.PROXY_PORTS:
            logging.debug("proxy hoặc ip rỗng . chạy không có proxy.")
            return None

        # settings.PROXY_USERNAME và settings.PROXY_PASSWORD là Optional[str]
        # nên chúng có thể là None.
        has_auth = settings.PROXY_USERNAME and settings.PROXY_PASSWORD

        if not (len(settings.PROXY_IPS) == len(settings.PROXY_PORTS)):
            logging.error("lỗi config proxy: độ dài của proxy không trùng .")
            return None

        proxy_ip = settings.PROXY_IPS[self.proxy_index]
        # PROXY_PORTS là List[int], không cần chuyển đổi nữa
        proxy_port = settings.PROXY_PORTS[self.proxy_index]

        if has_auth:
            proxy_str = f"socks5://{settings.PROXY_USERNAME}:{settings.PROXY_PASSWORD}@{proxy_ip}:{proxy_port}"
        else:
            proxy_str = f"socks5://{proxy_ip}:{proxy_port}"

        self.proxy_index = (self.proxy_index + 1) % len(settings.PROXY_IPS)
        logging.debug(f"dùng proxy số : {proxy_ip}:{proxy_port}")
        return proxy_str

    async def download_image(self,image_url_original: str,base_save_path_on_disk: str = "media_root",image_subfolder: str = "brand_images"  ) -> str | None:
        if not image_url_original:
            logging.warning("download_image gọi với một link ảnh gốc .")
            return None

        # Tạo đường dẫn thư mục lưu ảnh trên đĩa
        # self.project_root_dir là thư mục gốc của dự án của bạn
        full_save_folder_on_disk = os.path.join(self.project_root_dir, base_save_path_on_disk, image_subfolder)
        try:
            os.makedirs(full_save_folder_on_disk, exist_ok=True)
        except OSError as e:
            logging.error(f"không thể tạo một direction  {full_save_folder_on_disk}: {e}")
            return None

        try:
            # Thay thế settings.SSL_VERIFY_DOWNLOAD và settings.DOWNLOAD_TIMEOUT bằng giá trị thực tế hoặc config của bạn
            ssl_verify = getattr(settings, 'SSL_VERIFY_DOWNLOAD', True)
            download_timeout = getattr(settings, 'DOWNLOAD_TIMEOUT', 30.0)

            async with httpx.AsyncClient(verify=ssl_verify, timeout=download_timeout,
                                         follow_redirects=True) as client:
                logging.info(f"Đang cố gắng tải xuống hình ảnh từ: {image_url_original}")
                img_response = await client.get(image_url_original, headers=self.headers)
                img_response.raise_for_status()  # Ném lỗi nếu status code là 4xx hoặc 5xx

                # --- Bắt đầu logic xác định phần mở rộng file ---
                parsed_url = urlparse(image_url_original)
                path_component = unquote(parsed_url.path)
                original_filename_from_url = os.path.basename(path_component)
                _, ext_from_url = os.path.splitext(original_filename_from_url)

                logging.debug(f"DEBUG download_image: URL gốc: '{image_url_original}'")
                logging.debug(f"DEBUG download_image: Tên tệp gốc từ URL: '{original_filename_from_url}'")
                logging.debug(f"DEBUG download_image: Mở rộng từ URL: '{ext_from_url}' (repr: {repr(ext_from_url)})")

                content_type = img_response.headers.get("content-type", "").lower()
                logging.debug(f"DEBUG download_image: Tiêu đề Content-Type: '{content_type}'")

                # Ưu tiên xác định phần mở rộng từ Content-Type
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
                    determined_ext = ".jpg"  # Mặc định an toàn là .jpg nếu không rõ

                logging.debug(
                    f"DEBUG download_image: Phần mở rộng cuối cùng được chọn: '{determined_ext}' (repr: {repr(determined_ext)})")
                # --- Kết thúc logic xác định phần mở rộng file ---

                unique_filename_base = str(uuid.uuid4())
                # Sử dụng determined_ext đã được chuẩn hóa
                unique_filename = f"{unique_filename_base}{determined_ext}"
                logging.debug(
                    f"DEBUG download_image: Tên tệp duy nhất được tạo:'{unique_filename}' (repr: {repr(unique_filename)})")

                save_path_on_disk = os.path.join(full_save_folder_on_disk, unique_filename)

                with open(save_path_on_disk, "wb") as f:
                    f.write(img_response.content)

                # Đường dẫn tương đối này sẽ được dùng để tạo URL truy cập ảnh
                relative_url_path = os.path.join(image_subfolder, unique_filename).replace("\\", "/")
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
                    await asyncio.sleep(random.uniform(min_delay_req, max_delay_req))

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
                    if attempt == effective_max_retries - 1: return None
                    await asyncio.sleep(random.uniform(5, 10))
                    continue
                if attempt == effective_max_retries - 1: return None
                await asyncio.sleep(random.uniform(2, 5))

            except httpx.RequestError as e_req:
                logging.warning(
                    f"Yêu cầu Lỗi (Cố gắng {attempt + 1}/{effective_max_retries}) for {url}: {str(e_req)}")
                current_proxy = self.get_next_proxy()
                proxies_config = {"http://": current_proxy, "https://": current_proxy} if current_proxy else None
                if attempt == effective_max_retries - 1: return None
                await asyncio.sleep(random.uniform(3, 7))

            except Exception as e_generic:
                logging.error(
                    f"Lỗi chung (Cố gắng {attempt + 1}/{effective_max_retries}) for {url}: {str(e_generic)}",
                    exc_info=True)
                if attempt == effective_max_retries - 1: return None
                await asyncio.sleep(random.uniform(2, 5))
        logging.error(f"All {effective_max_retries} thử lại không thành công cho URL: {url}")
        return None

    async def scrape_by_date_range(self,start_date: datetime,end_date: datetime,session: Session,initial_start_page: int,state_save_callback: Callable[[int], None]) -> List[Brand]:  # Trả về danh sách các Brand đã xử lý trong lần chạy này

        current_page = initial_start_page
        brands_collected_in_this_run: List[Brand] = []
        stop_scraping_due_to_duplicate_policy = False

        # Các biến cấu hình từ settings (giữ nguyên)
        request_limit_per_interval = settings.REQUEST_LIMIT_PER_INTERVAL
        request_interval_seconds = settings.REQUEST_INTERVAL_SECONDS
        min_request_delay = settings.MIN_REQUEST_DELAY
        max_request_delay = settings.MAX_REQUEST_DELAY


        while True:  # Lặp qua các trang
            # Xử lý giới hạn request (giữ nguyên)
            if self.request_count >= request_limit_per_interval:
                time_diff = datetime.now() - self.last_request_time
                if time_diff.total_seconds() < request_interval_seconds:
                    sleep_duration = request_interval_seconds - time_diff.total_seconds()
                    logger.info(f"Đạt giới hạn request. Nghỉ {sleep_duration:.2f} giây.")
                    await asyncio.sleep(sleep_duration)
                self.request_count = 0
                self.last_request_time = datetime.now()

            start_str = start_date.strftime("%d.%m.%Y")
            end_str = end_date.strftime("%d.%m.%Y")
            url = f"https://vietnamtrademark.net/search?fd={start_str}%20-%20{end_str}&p={current_page}"

            logging.info(f"Đang cào trang: {current_page} cho URL: {url}")
            response = await self.make_request(url)
            self.request_count += 1

            if not response:
                logging.error(
                    f"Không nhận được phản hồi cho trang {current_page} (URL: {url}). Dừng xử lý khoảng ngày này.")
                break  # Thoát vòng lặp trang

            try:
                soup = BeautifulSoup(response.text, 'html.parser')
            except Exception as e_soup:
                logging.error(f"Lỗi khi parse HTML cho trang {current_page}: {e_soup}", exc_info=True)
                break  # Thoát vòng lặp trang

            rows = soup.select("table.table tbody tr")
            if not rows:
                logging.info(
                    f"Không tìm thấy hàng (dữ liệu) nào trên trang {current_page}. Kết thúc cho khoảng ngày này.")
                break

            brands_extracted_from_this_page: List[Brand] = []  # Lưu các brand từ trang hiện tại
            page_had_new_valid_data = False  # Cờ để biết trang này có dữ liệu mới hợp lệ không

            for row_idx, row in enumerate(rows):
                try:
                    # --- BẮT ĐẦU LOGIC TRÍCH XUẤT DỮ LIỆU TỪ 1 HÀNG (ROW) ---
                    # (Đây là phần code gốc của bạn, tôi chỉ tóm tắt lại các trường cần lấy)
                    date_text_tag = row.select_one("td:nth-child(7)")
                    if not date_text_tag or not date_text_tag.text.strip():
                        logging.warning(f"Hàng {row_idx + 1} trang {current_page}: Thiếu ngày nộp đơn. Bỏ qua hàng.")
                        continue
                    try:
                        parsed_application_date = datetime.strptime(date_text_tag.text.strip(), "%d.%m.%Y").date()
                    except ValueError as ve:
                        logging.warning(
                            f"Hàng {row_idx + 1} trang {current_page}: Lỗi parse ngày '{date_text_tag.text.strip()}': {ve}. Bỏ qua hàng.")
                        continue

                    # Đảm bảo partition tồn tại cho ngày này (quan trọng cho insert sau này)
                    ensure_partition_exists(parsed_application_date)  # Gọi hàm này từ database.py

                    brand_name_tag = row.select_one("td:nth-child(4) label")
                    brand_name = brand_name_tag.text.strip() if brand_name_tag else ""

                    image_tag = row.select_one("td.mau-nhan img")
                    image_url_original_src = image_tag["src"] if image_tag and image_tag.has_attr("src") else None
                    final_image_url_for_db = None
                    if image_url_original_src:
                        current_image_url_to_download = image_url_original_src
                        if current_image_url_to_download.startswith("/"):
                            current_image_url_to_download = f"{SOURCE_WEBSITE_DOMAIN.rstrip('/')}{current_image_url_to_download}"
                        # ... (các xử lý URL khác nếu cần) ...
                        saved_relative_image_path = await self.download_image(current_image_url_to_download)
                        if saved_relative_image_path:
                            final_image_url_for_db = f"{LOCAL_MEDIA_BASE_URL.rstrip('/')}/{saved_relative_image_path.lstrip('/')}"

                    product_group_tag = row.select_one("td:nth-child(5) span")
                    product_group = product_group_tag.text.strip() if product_group_tag else ""

                    status_tag = row.select_one("td.trang-thai span.badge")
                    status = status_tag.text.strip() if status_tag else ""

                    application_number_tag = row.select_one("td:nth-child(8) a")
                    application_number = application_number_tag.text.strip() if application_number_tag else ""
                    if not application_number:
                        logging.warning(f"Hàng {row_idx + 1} trang {current_page}: Thiếu số đơn. Bỏ qua hàng.")
                        continue

                    applicant_tag = row.select_one("td:nth-child(9)")
                    applicant = applicant_tag.text.strip() if applicant_tag else ""

                    representative_tag = row.select_one("td:nth-child(10)")
                    representative = representative_tag.text.strip() if representative_tag else ""

                    stmt = select(Brand).where(Brand.application_number == application_number)
                    existing_brand = session.exec(stmt).first()

                    if existing_brand:
                        logging.info(
                            f"Brand với số đơn {application_number} (trang {current_page}) đã tồn tại trong DB. Bỏ qua.")
                        continue

                    # Tạo đối tượng Brand
                    brand_obj = Brand(
                        brand_name=brand_name,
                        image_url=final_image_url_for_db,
                        product_group=product_group,
                        status=status,
                        application_date=parsed_application_date,
                        application_number=application_number,
                        applicant=applicant,
                        representative=representative
                        # updated_at sẽ được tự động bởi DB hoặc SQLModel default
                    )
                    brands_extracted_from_this_page.append(brand_obj)
                    page_had_new_valid_data = True  # Đánh dấu trang này có dữ liệu mới cần lưu

                except Exception as e_row_processing:
                    row_html_snippet = str(row)[:250]  # Lấy một đoạn HTML để debug
                    logging.error(f"Lỗi xử lý hàng {row_idx + 1} trên trang {current_page}: {e_row_processing}\n"
                                 f"HTML Snippet: {row_html_snippet}", exc_info=True)
                    continue


            # Xử lý lưu dữ liệu của trang hiện tại vào DB
            if brands_extracted_from_this_page:
                logging.info(
                    f"Trang {current_page}: Trích xuất được {len(brands_extracted_from_this_page)} nhãn hiệu mới.")
                try:
                    bulk_create(session, brands_extracted_from_this_page)
                    session.commit()
                    logging.info(
                        f"ĐÃ COMMIT THÀNH CÔNG {len(brands_extracted_from_this_page)} nhãn hiệu từ trang {current_page} vào DB.")
                    brands_collected_in_this_run.extend(brands_extracted_from_this_page)
                    state_save_callback(current_page)

                except Exception as e_db_commit:
                    logging.error(f"Lỗi khi thêm hoặc commit dữ liệu cho trang {current_page} vào DB: {e_db_commit}",
                                 exc_info=True)
                    try:
                        session.rollback()  # Quan trọng: Rollback lại nếu commit lỗi
                        logging.info(f"Đã rollback transaction cho trang {current_page} do lỗi.")
                    except Exception as e_rollback:
                        logging.error(
                            f"Lỗi nghiêm trọng khi rollback transaction cho trang {current_page}: {e_rollback}",
                            exc_info=True)
                    break

            elif page_had_new_valid_data is False and rows:  # Trang có rows nhưng không có data mới (ví dụ: toàn bộ đã tồn tại hoặc bị skip)
                logging.info(f"Trang {current_page} đã được xử lý nhưng không có dữ liệu mới nào được thêm vào DB.")
                state_save_callback(current_page)
            current_page += 1
            await asyncio.sleep(random.uniform(min_request_delay, max_request_delay))


        logging.info(
            f"Kết thúc scrape_by_date_range. Tổng số nhãn hiệu được thu thập và LƯU THÀNH CÔNG trong lần chạy này: {len(brands_collected_in_this_run)}.")
        return brands_collected_in_this_run

    async def check_pending_brands(self, session: Session):
        logging.info("Bắt đầu kiểm tra các thương hiệu đang chờ xử lý...")
        statement = select(Brand).where(Brand.status == "Đang giải quyết")
        pending_brands: List[Brand] = session.exec(statement).all()

        if not pending_brands:
            logging.info("Không tìm thấy thương hiệu nào có trạng thái 'Đang giải quyết'.")
            return

        logging.info(f"Found {len(pending_brands)} nhãn hiệu có trạng thái 'Đang giải quyết' để kiểm tra.")
        updated_count = 0

        min_delay_check = settings.MIN_DELAY_CHECK_PENDING
        max_delay_check = settings.MAX_DELAY_CHECK_PENDING

        for brand_idx, brand in enumerate(pending_brands):
            if brand_idx > 0:
                await asyncio.sleep(random.uniform(min_delay_check, max_delay_check))

            if not brand.application_number:
                logging.warning(f"Thương hiệu đang chờ xử lý với ID{brand.id} không có application_number. Đang bỏ qua.")
                continue

            url = f"https://vietnamtrademark.net/search?q={brand.application_number.strip()}"
            logging.info(f"Checking brand ID {brand.id} (App No: {brand.application_number}) at {url}")
            response = await self.make_request(url)

            if not response:
                logging.warning(
                    f"Không thể tìm kiếm thông tin chi tiết cho thương hiệu đang chờ xử lý {brand.application_number} (ID: {brand.id}).")
                continue

            try:
                soup = BeautifulSoup(response.text, 'html.parser')
                target_row = None
                rows_on_page = soup.select("table.table tbody tr")
                for r_check in rows_on_page:
                    app_num_tag_check = r_check.select_one("td:nth-child(8) a")
                    if app_num_tag_check and app_num_tag_check.text.strip() == brand.application_number:
                        target_row = r_check
                        break

                if not target_row:
                    logging.warning(
                        f"Không tìm thấy hàng cho số ứng dụng {brand.application_number} trên trang kết quả tìm kiếm để kiểm tra đang chờ xử lý.")
                    continue

                status_tag = target_row.select_one("td.trang-thai span.badge")
                if status_tag:
                    new_status = status_tag.text.strip()
                    if new_status != brand.status:
                        old_status = brand.status
                        brand.status = new_status
                        brand.updated_at = datetime.now(timezone.utc)  # Sửa: datetime.now(timezone.utc)
                        session.add(brand)
                        updated_count += 1
                        logging.info(
                            f"Trạng thái cho thương hiệu {brand.application_number} (ID: {brand.id}) UPDATED: '{old_status}' -> '{new_status}'")
                    else:
                        logging.debug(
                            f"Trạng thái cho thương hiệu {brand.application_number} (ID: {brand.id}) is still '{brand.status}'. Không cần cập nhật.")
                else:
                    logging.warning(
                        f"Không tìm thấy huy hiệu trạng thái cho {brand.application_number} (ID: {brand.id}) trong hàng của nó trên trang kết quả tìm kiếm để kiểm tra đang chờ xử lý.")

            except Exception as e_check:
                logging.error(
                    f"Lỗi xử lý trạng thái cho thương hiệu {brand.application_number} (ID: {brand.id}): {str(e_check)}",
                    exc_info=True)
                continue

        if updated_count > 0:
            try:
                session.commit()
                logging.info(f"Đã cam kết cập nhật thành công cho {updated_count} pending brands.")
            except Exception as e_commit:
                logging.error(f"Lỗi khi cam kết cập nhật cho các thương hiệu đang chờ xử lý: {e_commit}", exc_info=True)
                session.rollback()
        else:
            logging.info("Không có cập nhật trạng thái nào được thực hiện cho các thương hiệu đang chờ xử lý sau khi kiểm tra.")