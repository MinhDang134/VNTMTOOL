import asyncio
from datetime import datetime, timedelta, date as date_type
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import Manager
from functools import partial
from sqlmodel import create_engine
from src.tools.config import settings
from src.tools.service import ScraperService
from src.tools.database import get_session, ensure_partition_exists, setup_database_schema
from src.tools.state_manager import (
    load_scrape_state, save_scrape_state,
    load_control_state, save_control_state, get_control_state_path,
    clear_page_state_for_day
)
from src.tele_bot.telegram_notifier import TelegramNotifier
import logging

LOG_OUTPUT_DIR_PATH = "/home/minhdangpy134/Logvntmtool"


try:
    os.makedirs(LOG_OUTPUT_DIR_PATH, exist_ok=True)
except OSError as e:
    print(f"Lỗi khi tạo thư mục log {LOG_OUTPUT_DIR_PATH}: {e}")



LOG_FILE_NAME = "scraper_activity_main.txt"
LOG_FILE_PATH = os.path.join(LOG_OUTPUT_DIR_PATH, LOG_FILE_NAME)


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(processName)s (%(process)d) - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE_PATH, mode='a', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
MEDIA_PHYSICAL_DIR = os.path.join(PROJECT_ROOT, "media_root", "brand_images")
os.makedirs(MEDIA_PHYSICAL_DIR, exist_ok=True)
logging.info(f"Thư mục lưu trữ media: {MEDIA_PHYSICAL_DIR}")

PAGE_STATE_FILE_PATH = os.path.join(PROJECT_ROOT, "scraper_page_state.json")
CONTROL_STATE_FILE_PATH = get_control_state_path(PROJECT_ROOT)

RUN_DURATION_SECONDS = settings.RUN_DURATION_MINUTES * 60
PAUSE_DURATION_SECONDS = settings.PAUSE_DURATION_MINUTES * 60
NUM_PROCESSES = settings.CONCURRENT_SCRAPING_TASKS


def scrape_day_worker(current_day_to_process: date_type, db_url: str, page_state_file_path: str,media_physical_dir_worker: str):
    worker_log_name = f"Worker-{current_day_to_process.strftime('%Y-%m-%d')}"
    log = logging.getLogger(worker_log_name)

    if not log.handlers:
        log.setLevel(logging.INFO)
        worker_log_file = os.path.join(LOG_OUTPUT_DIR_PATH, f"worker-{current_day_to_process.strftime('%Y-%m-%d')}.log")
        file_handler = logging.FileHandler(worker_log_file, mode='a', encoding='utf-8')
        formatter = logging.Formatter('%(asctime)s - %(process)d - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        log.addHandler(file_handler)
        log.propagate = False

    log.info(f"Bắt đầu xử lý ngày: {current_day_to_process}")
    last_processed_page = 0

    try:
        worker_engine = create_engine(db_url, pool_pre_ping=True)
        scraper = ScraperService(media_dir=media_physical_dir_worker)
        day_key = f"brands_{current_day_to_process.strftime('%Y-%m-%d')}_{current_day_to_process.strftime('%Y-%m-%d')}"
        def state_updater_in_memory(page_just_completed: int):
            nonlocal last_processed_page
            last_processed_page = page_just_completed
            log.info(f"Đã xử lý xong trang {page_just_completed}")

        initial_page_for_this_day = load_scrape_state(page_state_file_path, day_key)

        scrape_result = {}
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            dt_for_partition = datetime.combine(current_day_to_process, datetime.min.time())
            ensure_partition_exists(dt_for_partition, worker_engine)
            with get_session(worker_engine) as session:
                scrape_result = loop.run_until_complete(scraper.scrape_by_date_range(
                    start_date=current_day_to_process,
                    end_date=current_day_to_process,
                    session=session,
                    initial_start_page=initial_page_for_this_day,
                    state_save_callback=state_updater_in_memory
                ))
        finally:
            loop.close()

        log.info(f"Hoàn thành xử lý ngày {current_day_to_process} với kết quả: {scrape_result.get('status')}")
        scrape_result['last_processed_page'] = last_processed_page
        return {"date": current_day_to_process, "result": scrape_result}

    except Exception as e:
        import traceback
        log.error(f"Lỗi nghiêm trọng trong worker cho ngày {current_day_to_process}: {e}", exc_info=True)
        error_details = {
            "status": "worker_crash",
            "message": str(e),
            "traceback": traceback.format_exc(),
            "last_processed_page": last_processed_page,
            "brands_processed_count": 0
        }
        return {"date": current_day_to_process, "result": error_details}
    finally:
        if 'worker_engine' in locals() and worker_engine:
            worker_engine.dispose()

def get_next_day_to_process() -> date_type:
    control_state = load_control_state(CONTROL_STATE_FILE_PATH)
    last_completed_str = control_state.get("last_fully_completed_day")

    if last_completed_str:
        try:
            last_completed_date = datetime.strptime(last_completed_str, "%Y-%m-%d").date()
            next_day = last_completed_date + timedelta(days=1)
            logging.info(
                f"Ngày cuối cùng hoàn thành được ghi nhận: {last_completed_str}. Ngày tiếp theo để xử lý: {next_day.strftime('%Y-%m-%d')}")
            return next_day
        except ValueError:
            logging.warning(
                f"Định dạng ngày trong file trạng thái điều khiển không hợp lệ: '{last_completed_str}'. Sẽ bắt đầu từ ngày cấu hình.")

    start_date = date_type(
        settings.INITIAL_SCRAPE_START_YEAR,
        settings.INITIAL_SCRAPE_START_MOTH,
        settings.INITIAL_SCRAPE_START_DAY
    )
    logging.info(
        f"Không có trạng thái ngày hoàn thành hợp lệ. Bắt đầu từ ngày cấu hình mặc định: {start_date.strftime('%Y-%m-%d')}")
    return start_date

def get_overall_end_date() -> date_type:
    if settings.OVERALL_SCRAPE_END_YEAR and settings.OVERALL_SCRAPE_END_MOTH and settings.OVERALL_SCRAPE_END_DAY:
        try:
            return date_type(
                settings.OVERALL_SCRAPE_END_YEAR,
                settings.OVERALL_SCRAPE_END_MOTH,
                settings.OVERALL_SCRAPE_END_DAY
            )
        except (TypeError, ValueError) as e:
            logging.warning(
                f"Một số giá trị OVERALL_SCRAPE_END trong settings không hợp lệ hoặc thiếu: {e}. Sẽ mặc định là ngày hôm qua.")
    return datetime.now().date() - timedelta(days=1)

async def daily_scraping_manager():
    logging.info("Khởi tạo Scraping Manager với Multiprocessing.")

    with ProcessPoolExecutor(max_workers=NUM_PROCESSES) as executor:
        current_day_to_process = get_next_day_to_process()
        overall_end_date = get_overall_end_date()
        active_futures = {}

        while True:
            logging.info(
                f"====== BẮT ĐẦU PHIÊN LÀM VIỆC MỚI ({settings.RUN_DURATION_MINUTES} PHÚT) lúc {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ======")
            session_start_time_ns = asyncio.get_event_loop().time()
            days_processed_in_this_session = 0

            while (asyncio.get_event_loop().time() - session_start_time_ns) < RUN_DURATION_SECONDS:
                for future in as_completed(list(active_futures.keys())):
                    processed_date = active_futures.pop(future)
                    try:
                        worker_output = future.result()
                        result_data = worker_output.get("result", {})
                        scrape_status = result_data.get("status", "unknown_error")
                        last_page = result_data.get("last_processed_page", 0)
                        message = result_data.get("message", scrape_status)

                        day_key = f"brands_{processed_date.strftime('%Y-%m-%d')}_{processed_date.strftime('%Y-%m-%d')}"


                        if last_page > 0:
                            save_scrape_state(PAGE_STATE_FILE_PATH, day_key, last_page, None)

                        logging.info(
                            f"Worker cho ngày {processed_date.strftime('%Y-%m-%d')} đã HOÀN THÀNH. Status: {scrape_status}, LastPage: {last_page}")

                        if scrape_status in ["completed_all_pages", "no_data_on_first_page"]:
                            logging.info(f"✅ HOÀN TẤT XỬ LÝ DỮ LIỆU CHO NGÀY: {processed_date.strftime('%Y-%m-%d')}.")
                            save_control_state(CONTROL_STATE_FILE_PATH, processed_date)
                            clear_page_state_for_day(PAGE_STATE_FILE_PATH, processed_date, None)
                            days_processed_in_this_session += 1

                        elif scrape_status == "worker_crash":
                            traceback_str = result_data.get("traceback", "Không có traceback.")
                            logging.error(
                                f"💀 Worker cho ngày {processed_date.strftime('%Y-%m-%d')} bị CRASH. Lý do: {message}")
                            error_title = f"Worker CRASHed on day {processed_date.strftime('%Y-%m-%d')}"
                            error_message = TelegramNotifier.format_error_message(error_title, traceback_str)
                            TelegramNotifier.send_message(error_message, use_proxy=True, is_error=True)
                        else:
                            logging.warning(
                                f"Ngày {processed_date.strftime('%Y-%m-%d')} chưa hoàn thành bởi worker. Lý do: {message}.")

                    except Exception as e:
                        logging.error(
                            f"Lỗi khi lấy kết quả từ future cho ngày {processed_date.strftime('%Y-%m-%d')}: {e}",
                            exc_info=True)
                        error_title = f"Lỗi MANAGER khi xử lý kết quả ngày {processed_date.strftime('%Y-%m-%d')}"
                        error_message = TelegramNotifier.format_error_message(error_title, e)
                        TelegramNotifier.send_message(error_message, use_proxy=True, is_error=True)

                while len(active_futures) < NUM_PROCESSES:
                    if current_day_to_process > overall_end_date:
                        break

                    logging.info(f"--- Chuẩn bị gửi task cho ngày: {current_day_to_process.strftime('%Y-%m-%d')} ---")
                    worker_func_partial = partial(
                        scrape_day_worker,
                        db_url=settings.DATABASE_URL,
                        page_state_file_path=PAGE_STATE_FILE_PATH,
                        media_physical_dir_worker=MEDIA_PHYSICAL_DIR
                    )
                    future = executor.submit(worker_func_partial, current_day_to_process)
                    active_futures[future] = current_day_to_process
                    logging.info(f"Đã gửi task xử lý ngày {current_day_to_process.strftime('%Y-%m-%d')} cho worker.")
                    current_day_to_process += timedelta(days=1)

                if not active_futures and current_day_to_process > overall_end_date:
                    break
                await asyncio.sleep(1)

            if not active_futures and current_day_to_process > overall_end_date:
                logging.info("Đã xử lý hết tất cả các ngày theo kế hoạch.")
                break

            if active_futures:
                logging.info(f"Hết giờ làm việc, chờ {len(active_futures)} tasks đang chạy hoàn thành...")
                for future in as_completed(list(active_futures.keys())):
                    processed_date = active_futures.pop(future)
                    try:
                        worker_output = future.result()
                        result_data = worker_output.get("result", {})
                        scrape_status = result_data.get("status", "unknown_error")
                        last_page = result_data.get("last_processed_page", 0)
                        message = result_data.get("message", scrape_status)

                        day_key = f"brands_{processed_date.strftime('%Y-%m-%d')}_{processed_date.strftime('%Y-%m-%d')}"
                        if last_page > 0:
                            save_scrape_state(PAGE_STATE_FILE_PATH, day_key, last_page, None)

                        logging.info(
                            f"Worker (sau giờ) cho ngày {processed_date.strftime('%Y-%m-%d')} đã HOÀN THÀNH. Status: {scrape_status}, LastPage: {last_page}")

                        if scrape_status in ["completed_all_pages", "no_data_on_first_page"]:
                            save_control_state(CONTROL_STATE_FILE_PATH, processed_date)
                            clear_page_state_for_day(PAGE_STATE_FILE_PATH, processed_date, None)
                            days_processed_in_this_session += 1

                        elif scrape_status == "worker_crash":
                            traceback_str = result_data.get("traceback", "Không có traceback.")
                            logging.error(
                                f"💀 Worker (sau giờ) cho ngày {processed_date.strftime('%Y-%m-%d')} bị CRASH. Lý do: {message}")
                            error_title = f"Worker (sau giờ) CRASHed on day {processed_date.strftime('%Y-%m-%d')}"
                            error_message = TelegramNotifier.format_error_message(error_title, traceback_str)
                            # ĐÃ SỬA: Thêm use_proxy=True
                            TelegramNotifier.send_message(error_message, use_proxy=True, is_error=True)

                    except Exception as e:
                        logging.error(
                            f"Lỗi khi lấy kết quả từ future (sau giờ) cho ngày {processed_date.strftime('%Y-%m-%d')}: {e}",
                            exc_info=True)
                        error_title = f"Lỗi MANAGER khi xử lý kết quả (sau giờ) ngày {processed_date.strftime('%Y-%m-%d')}"
                        error_message = TelegramNotifier.format_error_message(error_title, e)
                        # ĐÃ SỬA: Thêm use_proxy=True
                        TelegramNotifier.send_message(error_message, use_proxy=True, is_error=True)
                active_futures.clear()

            logging.info(f"====== KẾT THÚC PHIÊN LÀM VIỆC lúc {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ======")

            if current_day_to_process > overall_end_date and not active_futures:
                break

            logging.info(
                f"====== BẮT ĐẦU NGHỈ NGƠI ({settings.PAUSE_DURATION_MINUTES} PHÚT) lúc {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ======")
            await asyncio.sleep(PAUSE_DURATION_SECONDS)
            logging.info(f"====== KẾT THÚC NGHỈ NGƠI lúc {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ======")
            current_day_to_process = get_next_day_to_process()

try:
    setup_database_schema()
except Exception as e_db_setup:
    logging.critical(f"Không thể thiết lập schema database, dừng ứng dụng: {e_db_setup}")
    exit(1)

async def main_async_runner():
    await daily_scraping_manager()

if __name__ == "__main__":
    try:
        TelegramNotifier.send_message("✅ <b>Tool Scraper đã bắt đầu chạy.</b>", use_proxy=True)
        asyncio.run(main_async_runner())
    except KeyboardInterrupt:
        TelegramNotifier.send_message("🟡 <b>Tool bị dừng bởi người dùng (Ctrl+C).</b>", use_proxy=True)
    except Exception as e:
        error_title = "LỖI NGHIÊM TRỌNG - TOÀN BỘ CHƯƠNG TRÌNH ĐÃ DỪNG"
        error_message = TelegramNotifier.format_error_message(error_title, e)
        TelegramNotifier.send_message(error_message, use_proxy=True, is_error=True)
    finally:
        TelegramNotifier.send_message("ℹ️ <b>Chương trình đã kết thúc.</b>", use_proxy=True)
