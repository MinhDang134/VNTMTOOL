import os
import asyncio
import logging
from collections import deque
from functools import partial
from sqlmodel import create_engine
from src.tools.config import settings
from src.tools.service import ScraperService
from src.Exception.logger_config import setup_logging
from src.Exception.exceptions import CustomScrapingError
from src.tele_bot.telegram_notifier import TelegramNotifier
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta, date as date_type
from src.tools.database import get_session, ensure_partition_exists, setup_database_schema
from src.tools.state_manager import (init_db, load_scrape_state, save_page_state, load_control_state,
                                     save_control_state, get_db_path, clear_page_state_for_day,
                                     get_all_in_progress_days)

LOG_OUTPUT_DIR_PATH = "/home/minhdangpy134/Logvntmtool"
try:
    os.makedirs(LOG_OUTPUT_DIR_PATH, exist_ok=True)
except OSError as e:
    print(f"Lỗi khi tạo thư mục log {LOG_OUTPUT_DIR_PATH}: {e}")
setup_logging(LOG_OUTPUT_DIR_PATH)
LOG_FILE_NAME = "scraper_activity_main.txt"
LOG_FILE_PATH = os.path.join(LOG_OUTPUT_DIR_PATH, LOG_FILE_NAME)
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
MEDIA_PHYSICAL_DIR = os.path.join(PROJECT_ROOT, "media_root", "brand_images")
os.makedirs(MEDIA_PHYSICAL_DIR, exist_ok=True)
logging.info(f"Thư mục lưu trữ media: {MEDIA_PHYSICAL_DIR}")
STATE_DB_PATH = get_db_path(PROJECT_ROOT)
init_db(STATE_DB_PATH)
RUN_DURATION_SECONDS = settings.RUN_DURATION_MINUTES * 60
PAUSE_DURATION_SECONDS = settings.PAUSE_DURATION_MINUTES * 60
NUM_PROCESSES = settings.CONCURRENT_SCRAPING_TASKS


def scrape_day_worker(current_day_to_process: date_type, db_url: str, media_physical_dir_worker: str):
    log = logging.getLogger(f"Worker-{current_day_to_process.strftime('%Y-%m-%d')}")
    log.info(f"Bắt đầu xử lý ngày: {current_day_to_process}")
    last_processed_page = 0

    def state_updater_in_memory(page_just_completed: int):
        nonlocal last_processed_page
        last_processed_page = page_just_completed
        log.info(f"Đã xử lý xong trang {page_just_completed}")
        save_page_state(STATE_DB_PATH, day_key, page_just_completed)

    try:
        worker_engine = create_engine(db_url, pool_pre_ping=True)
        scraper = ScraperService(media_dir=media_physical_dir_worker)
        day_key = f"brands_{current_day_to_process.strftime('%Y-%m-%d')}_{current_day_to_process.strftime('%Y-%m-%d')}"

        initial_page_for_this_day = load_scrape_state(STATE_DB_PATH, day_key)

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

    except CustomScrapingError as cse:
        import traceback
        log.error(f"Lỗi scraping đã được định danh cho ngày {cse.day} tại trang {cse.page}: {cse.original_error}",
                  exc_info=True)

        error_logger = logging.getLogger('CrawlErrorLogger')
        log_extra_data = {'day': cse.day, 'page': cse.page}
        error_content = str(cse.original_error).replace('\n', ' ').replace('\r', '')
        error_logger.error(error_content, extra=log_extra_data)

        error_details = {
            "status": "worker_crash",
            "message": str(cse),
            "traceback": traceback.format_exc(),
            "last_processed_page": cse.page - 1 if cse.page > 0 else 0,
            "brands_processed_count": 0
        }
        return {"date": current_day_to_process, "result": error_details}

    except Exception as e:
        import traceback
        log.error(f"Lỗi không xác định trong worker cho ngày {current_day_to_process}: {e}", exc_info=True)

        error_logger = logging.getLogger('CrawlErrorLogger')
        page_at_error = last_processed_page + 1
        day_str = current_day_to_process.strftime('%Y-%m-%d')
        log_extra_data = {'day': day_str, 'page': page_at_error}
        error_content = str(e).replace('\n', ' ').replace('\r', '')
        error_logger.error(error_content, extra=log_extra_data)

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


def get_next_sequential_day_to_process() -> date_type:
    """Hàm này chỉ lấy ngày tuần tự tiếp theo từ control_state."""
    control_state = load_control_state(STATE_DB_PATH)
    last_completed_str = control_state.get("last_fully_completed_day")

    if last_completed_str:
        try:
            last_completed_date = datetime.strptime(last_completed_str, "%Y-%m-%d").date()
            next_day = last_completed_date + timedelta(days=1)
            logging.info(
                f"Ngày hoàn thành cuối cùng là {last_completed_str}. Ngày tuần tự tiếp theo sẽ là: {next_day.strftime('%Y-%m-%d')}")
            return next_day
        except ValueError:
            logging.warning(
                f"Định dạng ngày không hợp lệ '{last_completed_str}'. Bắt đầu từ ngày cấu hình.")

    start_date = date_type(
        settings.INITIAL_SCRAPE_START_YEAR,
        settings.INITIAL_SCRAPE_START_MOTH,
        settings.INITIAL_SCRAPE_START_DAY
    )
    logging.info(
        f"Không có trạng thái. Bắt đầu từ ngày cấu hình mặc định: {start_date.strftime('%Y-%m-%d')}")
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
        overall_end_date = get_overall_end_date()
        active_futures = {}


        priority_task_queue = deque(get_all_in_progress_days(STATE_DB_PATH))
        next_day_to_process = get_next_sequential_day_to_process()
        processed_or_queued = set(priority_task_queue)
        while next_day_to_process in processed_or_queued:
            next_day_to_process += timedelta(days=1)

        while True:
            if not active_futures and not priority_task_queue and next_day_to_process > overall_end_date:
                logging.info("Đã xử lý hết tất cả các ngày theo kế hoạch. Kết thúc chương trình.")
                break

            logging.info(
                f"====== BẮT ĐẦU PHIÊN LÀM VIỆC MỚI ({settings.RUN_DURATION_MINUTES} PHÚT) lúc {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ======")
            session_start_time_ns = asyncio.get_event_loop().time()


            while (asyncio.get_event_loop().time() - session_start_time_ns) < RUN_DURATION_SECONDS:


                if active_futures:
                    for future in as_completed(list(active_futures.keys())):
                        processed_date = active_futures.pop(future)
                        try:
                            worker_output = future.result()
                            result_data = worker_output.get("result", {})
                            scrape_status = result_data.get("status", "unknown_error")

                            logging.info(
                                f"Worker cho ngày {processed_date.strftime('%Y-%m-%d')} đã HOÀN THÀNH. Status: {scrape_status}")

                            if scrape_status in ["completed_all_pages", "no_data_on_first_page"]:
                                logging.info(
                                    f"✅ HOÀN TẤT XỬ LÝ DỮ LIỆU CHO NGÀY: {processed_date.strftime('%Y-%m-%d')}.")
                                save_control_state(STATE_DB_PATH, processed_date)
                                clear_page_state_for_day(STATE_DB_PATH, processed_date)
                            elif scrape_status == "worker_crash":
                                message = result_data.get("message", "Không rõ.")
                                traceback_str = result_data.get("traceback", "Không có traceback.")
                                logging.error(
                                    f"💀 Worker cho ngày {processed_date.strftime('%Y-%m-%d')} bị CRASH. Lý do: {message}")
                                error_title = f"Worker CRASHed on day {processed_date.strftime('%Y-%m-%d')}"
                                error_message = TelegramNotifier.format_error_message(error_title, traceback_str)
                                TelegramNotifier.send_message(error_message, use_proxy=True, is_error=True)
                            else:
                                logging.warning(
                                    f"Ngày {processed_date.strftime('%Y-%m-%d')} chưa hoàn thành. Trạng thái đã được lưu.")
                        except Exception as e:
                            logging.error(f"Lỗi khi lấy kết quả từ future cho ngày {processed_date}: {e}",
                                          exc_info=True)
                            error_title = f"Lỗi MANAGER khi xử lý kết quả ngày {processed_date}"
                            TelegramNotifier.send_message(TelegramNotifier.format_error_message(error_title, e),
                                                          use_proxy=True, is_error=True)


                while len(active_futures) < NUM_PROCESSES:
                    day_for_worker = None
                    if priority_task_queue:
                        day_for_worker = priority_task_queue.popleft()
                    elif next_day_to_process <= overall_end_date:
                        day_for_worker = next_day_to_process
                        next_day_to_process += timedelta(days=1)
                    else:
                        break

                    if day_for_worker:
                        logging.info(f"--- Chuẩn bị gửi task cho ngày: {day_for_worker.strftime('%Y-%m-%d')} ---")
                        future = executor.submit(scrape_day_worker, day_for_worker, settings.DATABASE_URL,
                                                 MEDIA_PHYSICAL_DIR)
                        active_futures[future] = day_for_worker
                    else:
                        break


                if not active_futures:
                    break

                await asyncio.sleep(1)

            logging.info(f"====== KẾT THÚC PHIÊN LÀM VIỆC lúc {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ======")


            if active_futures:
                logging.info(f"Hết giờ làm việc, chờ {len(active_futures)} tasks đang chạy hoàn thành...")
                for future in as_completed(list(active_futures.keys())):
                    active_futures.pop(future)
                logging.info("Tất cả các task trong phiên đã hoàn thành.")


            if not priority_task_queue and next_day_to_process > overall_end_date:
                continue


            logging.info(f"====== BẮT ĐẦU NGHỈ NGƠI ({settings.PAUSE_DURATION_MINUTES} PHÚT) ======")
            await asyncio.sleep(PAUSE_DURATION_SECONDS)
            logging.info("====== KẾT THÚC NGHỈ NGƠI ======")


            current_queued_days = set(priority_task_queue)
            priority_task_queue.extend(
                [d for d in get_all_in_progress_days(STATE_DB_PATH) if d not in current_queued_days])
            next_day_to_process = get_next_sequential_day_to_process()
            while next_day_to_process in set(priority_task_queue):
                next_day_to_process += timedelta(days=1)


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
