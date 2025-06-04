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
import logging   
# Đường dẫn tuyệt đối bạn muốn lưu log
LOG_OUTPUT_DIR_PATH = "/home/minhdangpy134/Logvntmtool"


try:
    os.makedirs(LOG_OUTPUT_DIR_PATH, exist_ok=True)
except OSError as e:
    print(f"Lỗi khi tạo thư mục log {LOG_OUTPUT_DIR_PATH}: {e}")



LOG_FILE_NAME = "scraper_activity2.txt"
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


def scrape_day_worker(current_day_to_process: date_type,db_url: str,page_state_file_path: str,page_state_lock,media_physical_dir_worker: str):
    log = logging.getLogger(f"Worker-{current_day_to_process.strftime('%Y-%m-%d')}")   
    log.info(f"Worker bắt đầu xử lý ngày: {current_day_to_process}")   

    try:   
        worker_engine = create_engine(db_url, pool_pre_ping=True)   

        scraper = ScraperService(   
            media_dir=media_physical_dir_worker,   
        )   

        day_key = f"brands_{current_day_to_process.strftime('%Y-%m-%d')}_{current_day_to_process.strftime('%Y-%m-%d')}"   

        def state_saver_callback_for_day_in_worker(page_just_completed: int):   
            save_scrape_state(page_state_file_path, day_key, page_just_completed, page_state_lock)   

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
                    state_save_callback=state_saver_callback_for_day_in_worker   
                ))   
        finally:   
            loop.close()   

        log.info(
            f"Worker hoàn thành xử lý ngày {current_day_to_process} với kết quả: {scrape_result.get('status')}")   
        return {"date": current_day_to_process, "result": scrape_result}   

    except Exception as e:   
        log.error(f"Lỗi nghiêm trọng trong worker cho ngày {current_day_to_process}: {e}", exc_info=True)   
        return {"date": current_day_to_process,
                "result": {"status": "worker_crash", "message": str(e), "brands_processed_count": 0}}   
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

    with Manager() as manager:   
        page_state_lock = manager.Lock()   

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
                    # completed_futures_dates = []   (Dòng này không cần thiết)
                    for future in as_completed(
                            list(active_futures.keys())):
                        processed_date = active_futures.pop(future)   
                        try:   
                            worker_output = future.result()   
                            scrape_status = worker_output.get("result", {}).get("status",
                                                                                "unknown_error_from_worker")   
                            brands_count = worker_output.get("result", {}).get("brands_processed_count", 0)   
                            message = worker_output.get("result", {}).get("message", scrape_status)   

                            logging.info(
                                f"Worker cho ngày {processed_date.strftime('%Y-%m-%d')} đã HOÀN THÀNH. Status: {scrape_status}, Brands: {brands_count}, Msg: {message}")   

                            if scrape_status in ["completed_all_pages", "no_data_on_first_page"]:   
                                logging.info(
                                    f"✅ HOÀN TẤT XỬ LÝ DỮ LIỆU CHO NGÀY: {processed_date.strftime('%Y-%m-%d')}.")   
                                save_control_state(CONTROL_STATE_FILE_PATH, processed_date)   
                                clear_page_state_for_day(PAGE_STATE_FILE_PATH, processed_date,
                                                         page_state_lock)   
                                days_processed_in_this_session += 1   
                            elif scrape_status == "worker_crash":   
                                logging.error(
                                    f"💀 Worker cho ngày {processed_date.strftime('%Y-%m-%d')} bị CRASH. Lý do: {message}")   
                            else:   
                                logging.warning(
                                    f"Ngày {processed_date.strftime('%Y-%m-%d')} chưa hoàn thành bởi worker. "   
                                    f"Lý do: {message}.")   
                        except Exception as e:   
                            logging.error(
                                f"Lỗi khi lấy kết quả từ future cho ngày {processed_date.strftime('%Y-%m-%d')}: {e}",
                                exc_info=True)   

                    while len(active_futures) < NUM_PROCESSES:   
                        if current_day_to_process > overall_end_date:   
                            logging.info(
                                f"Đã đạt đến ngày kết thúc tổng thể ({overall_end_date.strftime('%Y-%m-%d')}). Không thêm task mới.")   
                            break   

                        logging.info(
                            f"--- Chuẩn bị gửi task cho ngày: {current_day_to_process.strftime('%Y-%m-%d')} ---")   

                        worker_func_partial = partial(   
                            scrape_day_worker,   
                            db_url=settings.DATABASE_URL,   
                            page_state_file_path=PAGE_STATE_FILE_PATH,   
                            page_state_lock=page_state_lock,   
                            media_physical_dir_worker=MEDIA_PHYSICAL_DIR   
                        )   

                        future = executor.submit(worker_func_partial, current_day_to_process)   
                        active_futures[future] = current_day_to_process   
                        logging.info(
                            f"Đã gửi task xử lý ngày {current_day_to_process.strftime('%Y-%m-%d')} cho worker.")   

                        current_day_to_process += timedelta(days=1)   

                        if len(active_futures) == NUM_PROCESSES:   
                            logging.info(
                                f"Đã sử dụng hết {NUM_PROCESSES} worker slots. Chờ worker hoàn thành...")   

                    if not active_futures and current_day_to_process > overall_end_date:   
                        logging.info(
                            "Không còn task nào đang chạy và đã xử lý hết các ngày theo kế hoạch.")   
                        break   

                    await asyncio.sleep(0.1)

                    if (asyncio.get_event_loop().time() - session_start_time_ns) >= RUN_DURATION_SECONDS:   
                        logging.info(
                            "Hết thời gian phiên làm việc quy định. Chờ các task đang chạy hoàn thành...")   
                        break   

                if active_futures:   
                    logging.info(
                        f"Hết giờ làm việc, chờ {len(active_futures)} tasks đang chạy hoàn thành...")   
                    # Chuyển active_futures.keys() thành list để tránh lỗi thay đổi kích thước trong khi lặp
                    for future in as_completed(list(active_futures.keys())):   
                        processed_date = active_futures.pop(future)   
                        try:   
                            worker_output = future.result()   
                            scrape_status = worker_output.get("result", {}).get("status",
                                                                                "unknown_error_from_worker")   
                            brands_count = worker_output.get("result", {}).get("brands_processed_count", 0)   
                            message = worker_output.get("result", {}).get("message", scrape_status)   
                            logging.info(
                                f"Worker (sau giờ) cho ngày {processed_date.strftime('%Y-%m-%d')} đã HOÀN THÀNH. Status: {scrape_status}, Brands: {brands_count}, Msg: {message}")   

                            if scrape_status in ["completed_all_pages", "no_data_on_first_page"]:   
                                save_control_state(CONTROL_STATE_FILE_PATH, processed_date)   
                                clear_page_state_for_day(PAGE_STATE_FILE_PATH, processed_date,
                                                         page_state_lock)   
                                days_processed_in_this_session += 1   
                            elif scrape_status == "worker_crash":   
                                logging.error(
                                    f"💀 Worker (sau giờ) cho ngày {processed_date.strftime('%Y-%m-%d')} bị CRASH. Lý do: {message}")   
                            else:   
                                logging.warning(
                                    f"Ngày (sau giờ) {processed_date.strftime('%Y-%m-%d')} chưa hoàn thành bởi worker. Lý do: {message}.")   
                        except Exception as e:   
                            logging.error(
                                f"Lỗi khi lấy kết quả từ future (sau giờ) cho ngày {processed_date.strftime('%Y-%m-%d')}: {e}",
                                exc_info=True)   
                    active_futures.clear()   

                if days_processed_in_this_session == 0 and current_day_to_process <= overall_end_date:   
                    logging.info(f"Phiên làm việc kết thúc mà không xử lý được ngày nào mới.")   
                elif current_day_to_process > overall_end_date and not active_futures:   
                    logging.info(
                        f"Đã xử lý hết tất cả các ngày cho đến {overall_end_date.strftime('%Y-%m-%d')}. Dừng chương trình.")   
                    break   

                logging.info(
                    f"====== KẾT THÚC PHIÊN LÀM VIỆC lúc {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ======")   

                if current_day_to_process > overall_end_date and not active_futures:   
                    break   

                logging.info(
                    f"====== BẮT ĐẦU NGHỈ NGƠI ({settings.PAUSE_DURATION_MINUTES} PHÚT) lúc {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ======")   
                await asyncio.sleep(PAUSE_DURATION_SECONDS)   
                logging.info(
                    f"====== KẾT THÚC NGHỈ NGƠI lúc {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ======")   

                current_day_to_process = get_next_day_to_process()

try:
    setup_database_schema()
except Exception as e_db_setup:
    logging.critical(f"Không thể thiết lập schema database, dừng ứng dụng: {e_db_setup}")
    exit(1) # Dừng hẳn nếu không thể setup DB


async def main_async_runner():   
    await daily_scraping_manager()   


if __name__ == "__main__":
    try:   
        asyncio.run(main_async_runner())   
    except KeyboardInterrupt:   
        logging.info("Tool bị dừng bởi người dùng (Ctrl+C).")   
    except Exception as e:   
        logging.critical(f"Lỗi nghiêm trọng không bắt được ở phạm vi cao nhất (main_async_runner): {e}",
                         exc_info=True)   
    finally:   
        logging.info("Chương trình kết thúc.")   