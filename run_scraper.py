import asyncio
from datetime import datetime, timedelta, date as date_type
import os
from concurrent.futures import ProcessPoolExecutor, as_completed  # CRISTIANO
from multiprocessing import Manager  # CRISTIANO
from functools import partial  # CRISTIANO
from sqlmodel import create_engine  # CRISTIANO
from src.tools.config import settings
from src.tools.service import ScraperService
from src.tools.database import get_session, ensure_partition_exists  # CRISTIANO
from src.tools.state_manager import (
    load_scrape_state, save_scrape_state,
    load_control_state, save_control_state, get_control_state_path,
    clear_page_state_for_day
)
import logging  # CRISTIANO

logging.basicConfig(  # CRISTIANO
    level=logging.INFO,  # CRISTIANO
    format='%(asctime)s - %(processName)s (%(process)d) - %(name)s - %(levelname)s - %(message)s',  # CRISTIANO
    handlers=[  # CRISTIANO
        logging.FileHandler("scraper_activity.log", mode='a', encoding='utf-8'),  # CRISTIANO
        logging.StreamHandler()  # CRISTIANO
    ]  # CRISTIANO
)  # CRISTIANO

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
MEDIA_PHYSICAL_DIR = os.path.join(PROJECT_ROOT, "media_root", "brand_images")
os.makedirs(MEDIA_PHYSICAL_DIR, exist_ok=True)
logging.info(f"Thư mục lưu trữ media: {MEDIA_PHYSICAL_DIR}")

PAGE_STATE_FILE_PATH = os.path.join(PROJECT_ROOT, "scraper_page_state.json")
CONTROL_STATE_FILE_PATH = get_control_state_path(PROJECT_ROOT)

RUN_DURATION_SECONDS = settings.RUN_DURATION_MINUTES * 60
PAUSE_DURATION_SECONDS = settings.PAUSE_DURATION_MINUTES * 60
NUM_PROCESSES = settings.CONCURRENT_SCRAPING_TASKS


def scrape_day_worker(  # CRISTIANO
        current_day_to_process: date_type,  # CRISTIANO
        db_url: str,  # CRISTIANO
        page_state_file_path: str,  # CRISTIANO
        page_state_lock,  # CRISTIANO
        media_physical_dir_worker: str,  # CRISTIANO
):  # CRISTIANO
    log = logging.getLogger(f"Worker-{current_day_to_process.strftime('%Y-%m-%d')}")  # CRISTIANO
    log.info(f"Worker bắt đầu xử lý ngày: {current_day_to_process}")  # CRISTIANO

    try:  # CRISTIANO
        worker_engine = create_engine(db_url, pool_pre_ping=True)  # CRISTIANO

        scraper = ScraperService(  # CRISTIANO
            media_dir=media_physical_dir_worker,  # CRISTIANO
        )  # CRISTIANO

        day_key = f"brands_{current_day_to_process.strftime('%Y-%m-%d')}_{current_day_to_process.strftime('%Y-%m-%d')}"  # CRISTIANO

        def state_saver_callback_for_day_in_worker(page_just_completed: int):  # CRISTIANO
            save_scrape_state(page_state_file_path, day_key, page_just_completed, page_state_lock)  # CRISTIANO

        initial_page_for_this_day = load_scrape_state(page_state_file_path, day_key)  # CRISTIANO

        scrape_result = {}  # CRISTIANO
        loop = asyncio.new_event_loop()  # CRISTIANO
        asyncio.set_event_loop(loop)  # CRISTIANO
        try:  # CRISTIANO
            dt_for_partition = datetime.combine(current_day_to_process, datetime.min.time())  # CRISTIANO
            ensure_partition_exists(dt_for_partition, worker_engine)  # CRISTIANO

            with get_session(worker_engine) as session:  # CRISTIANO
                scrape_result = loop.run_until_complete(scraper.scrape_by_date_range(  # CRISTIANO
                    start_date=current_day_to_process,  # CRISTIANO
                    end_date=current_day_to_process,  # CRISTIANO
                    session=session,  # CRISTIANO
                    initial_start_page=initial_page_for_this_day,  # CRISTIANO
                    state_save_callback=state_saver_callback_for_day_in_worker  # CRISTIANO
                ))  # CRISTIANO
        finally:  # CRISTIANO
            loop.close()  # CRISTIANO

        log.info(
            f"Worker hoàn thành xử lý ngày {current_day_to_process} với kết quả: {scrape_result.get('status')}")  # CRISTIANO
        return {"date": current_day_to_process, "result": scrape_result}  # CRISTIANO

    except Exception as e:  # CRISTIANO
        log.error(f"Lỗi nghiêm trọng trong worker cho ngày {current_day_to_process}: {e}", exc_info=True)  # CRISTIANO
        return {"date": current_day_to_process,
                "result": {"status": "worker_crash", "message": str(e), "brands_processed_count": 0}}  # CRISTIANO
    finally:  # CRISTIANO
        if 'worker_engine' in locals() and worker_engine:  # CRISTIANO
            worker_engine.dispose()  # CRISTIANO


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


async def daily_scraping_manager():  # CRISTIANO
    logging.info("Khởi tạo Scraping Manager với Multiprocessing.")  # CRISTIANO

    with Manager() as manager:  # CRISTIANO
        page_state_lock = manager.Lock()  # CRISTIANO

        with ProcessPoolExecutor(max_workers=NUM_PROCESSES) as executor:  # CRISTIANO
            current_day_to_process = get_next_day_to_process()  # CRISTIANO
            overall_end_date = get_overall_end_date()  # CRISTIANO

            active_futures = {}  # CRISTIANO

            while True:  # CRISTIANO
                logging.info(  # CRISTIANO
                    f"====== BẮT ĐẦU PHIÊN LÀM VIỆC MỚI ({settings.RUN_DURATION_MINUTES} PHÚT) lúc {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ======")  # CRISTIANO
                session_start_time_ns = asyncio.get_event_loop().time()  # CRISTIANO

                days_processed_in_this_session = 0  # CRISTIANO

                while (asyncio.get_event_loop().time() - session_start_time_ns) < RUN_DURATION_SECONDS:  # CRISTIANO
                    # completed_futures_dates = [] # CRISTIANO (Dòng này không cần thiết)
                    for future in as_completed(
                            list(active_futures.keys())):  # CRISTIANO (Thêm list() để tránh lỗi RuntimeError: dictionary changed size during iteration)
                        processed_date = active_futures.pop(future)  # CRISTIANO
                        try:  # CRISTIANO
                            worker_output = future.result()  # CRISTIANO
                            scrape_status = worker_output.get("result", {}).get("status",
                                                                                "unknown_error_from_worker")  # CRISTIANO
                            brands_count = worker_output.get("result", {}).get("brands_processed_count", 0)  # CRISTIANO
                            message = worker_output.get("result", {}).get("message", scrape_status)  # CRISTIANO

                            logging.info(
                                f"Worker cho ngày {processed_date.strftime('%Y-%m-%d')} đã HOÀN THÀNH. Status: {scrape_status}, Brands: {brands_count}, Msg: {message}")  # CRISTIANO

                            if scrape_status in ["completed_all_pages", "no_data_on_first_page"]:  # CRISTIANO
                                logging.info(
                                    f"✅ HOÀN TẤT XỬ LÝ DỮ LIỆU CHO NGÀY: {processed_date.strftime('%Y-%m-%d')}.")  # CRISTIANO
                                save_control_state(CONTROL_STATE_FILE_PATH, processed_date)  # CRISTIANO
                                clear_page_state_for_day(PAGE_STATE_FILE_PATH, processed_date,
                                                         page_state_lock)  # CRISTIANO
                                days_processed_in_this_session += 1  # CRISTIANO
                            elif scrape_status == "worker_crash":  # CRISTIANO
                                logging.error(
                                    f"💀 Worker cho ngày {processed_date.strftime('%Y-%m-%d')} bị CRASH. Lý do: {message}")  # CRISTIANO
                            else:  # CRISTIANO
                                logging.warning(
                                    f"Ngày {processed_date.strftime('%Y-%m-%d')} chưa hoàn thành bởi worker. "  # CRISTIANO
                                    f"Lý do: {message}.")  # CRISTIANO
                        except Exception as e:  # CRISTIANO
                            logging.error(
                                f"Lỗi khi lấy kết quả từ future cho ngày {processed_date.strftime('%Y-%m-%d')}: {e}",
                                exc_info=True)  # CRISTIANO

                    while len(active_futures) < NUM_PROCESSES:  # CRISTIANO
                        if current_day_to_process > overall_end_date:  # CRISTIANO
                            logging.info(
                                f"Đã đạt đến ngày kết thúc tổng thể ({overall_end_date.strftime('%Y-%m-%d')}). Không thêm task mới.")  # CRISTIANO
                            break  # CRISTIANO

                        logging.info(
                            f"--- Chuẩn bị gửi task cho ngày: {current_day_to_process.strftime('%Y-%m-%d')} ---")  # CRISTIANO

                        worker_func_partial = partial(  # CRISTIANO
                            scrape_day_worker,  # CRISTIANO
                            db_url=settings.DATABASE_URL,  # CRISTIANO
                            page_state_file_path=PAGE_STATE_FILE_PATH,  # CRISTIANO
                            page_state_lock=page_state_lock,  # CRISTIANO
                            media_physical_dir_worker=MEDIA_PHYSICAL_DIR  # CRISTIANO
                        )  # CRISTIANO

                        future = executor.submit(worker_func_partial, current_day_to_process)  # CRISTIANO
                        active_futures[future] = current_day_to_process  # CRISTIANO
                        logging.info(
                            f"Đã gửi task xử lý ngày {current_day_to_process.strftime('%Y-%m-%d')} cho worker.")  # CRISTIANO

                        current_day_to_process += timedelta(days=1)  # CRISTIANO

                        if len(active_futures) == NUM_PROCESSES:  # CRISTIANO
                            logging.info(
                                f"Đã sử dụng hết {NUM_PROCESSES} worker slots. Chờ worker hoàn thành...")  # CRISTIANO

                    if not active_futures and current_day_to_process > overall_end_date:  # CRISTIANO
                        logging.info(
                            "Không còn task nào đang chạy và đã xử lý hết các ngày theo kế hoạch.")  # CRISTIANO
                        break  # CRISTIANO

                    await asyncio.sleep(0.1)  # CRISTIANO (Giảm thời gian sleep để kiểm tra future nhanh hơn)

                    if (asyncio.get_event_loop().time() - session_start_time_ns) >= RUN_DURATION_SECONDS:  # CRISTIANO
                        logging.info(
                            "Hết thời gian phiên làm việc quy định. Chờ các task đang chạy hoàn thành...")  # CRISTIANO
                        break  # CRISTIANO

                if active_futures:  # CRISTIANO
                    logging.info(
                        f"Hết giờ làm việc, chờ {len(active_futures)} tasks đang chạy hoàn thành...")  # CRISTIANO
                    # Chuyển active_futures.keys() thành list để tránh lỗi thay đổi kích thước trong khi lặp
                    for future in as_completed(list(active_futures.keys())):  # CRISTIANO
                        processed_date = active_futures.pop(future)  # CRISTIANO
                        try:  # CRISTIANO
                            worker_output = future.result()  # CRISTIANO
                            scrape_status = worker_output.get("result", {}).get("status",
                                                                                "unknown_error_from_worker")  # CRISTIANO
                            brands_count = worker_output.get("result", {}).get("brands_processed_count", 0)  # CRISTIANO
                            message = worker_output.get("result", {}).get("message", scrape_status)  # CRISTIANO
                            logging.info(
                                f"Worker (sau giờ) cho ngày {processed_date.strftime('%Y-%m-%d')} đã HOÀN THÀNH. Status: {scrape_status}, Brands: {brands_count}, Msg: {message}")  # CRISTIANO

                            if scrape_status in ["completed_all_pages", "no_data_on_first_page"]:  # CRISTIANO
                                save_control_state(CONTROL_STATE_FILE_PATH, processed_date)  # CRISTIANO
                                clear_page_state_for_day(PAGE_STATE_FILE_PATH, processed_date,
                                                         page_state_lock)  # CRISTIANO
                                days_processed_in_this_session += 1  # CRISTIANO
                            elif scrape_status == "worker_crash":  # CRISTIANO
                                logging.error(
                                    f"💀 Worker (sau giờ) cho ngày {processed_date.strftime('%Y-%m-%d')} bị CRASH. Lý do: {message}")  # CRISTIANO
                            else:  # CRISTIANO
                                logging.warning(
                                    f"Ngày (sau giờ) {processed_date.strftime('%Y-%m-%d')} chưa hoàn thành bởi worker. Lý do: {message}.")  # CRISTIANO
                        except Exception as e:  # CRISTIANO
                            logging.error(
                                f"Lỗi khi lấy kết quả từ future (sau giờ) cho ngày {processed_date.strftime('%Y-%m-%d')}: {e}",
                                exc_info=True)  # CRISTIANO
                    active_futures.clear()  # CRISTIANO

                if days_processed_in_this_session == 0 and current_day_to_process <= overall_end_date:  # CRISTIANO
                    logging.info(f"Phiên làm việc kết thúc mà không xử lý được ngày nào mới.")  # CRISTIANO
                elif current_day_to_process > overall_end_date and not active_futures:  # CRISTIANO
                    logging.info(
                        f"Đã xử lý hết tất cả các ngày cho đến {overall_end_date.strftime('%Y-%m-%d')}. Dừng chương trình.")  # CRISTIANO
                    break  # CRISTIANO

                logging.info(
                    f"====== KẾT THÚC PHIÊN LÀM VIỆC lúc {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ======")  # CRISTIANO

                if current_day_to_process > overall_end_date and not active_futures:  # CRISTIANO
                    break  # CRISTIANO

                logging.info(
                    f"====== BẮT ĐẦU NGHỈ NGƠI ({settings.PAUSE_DURATION_MINUTES} PHÚT) lúc {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ======")  # CRISTIANO
                await asyncio.sleep(PAUSE_DURATION_SECONDS)  # CRISTIANO
                logging.info(
                    f"====== KẾT THÚC NGHỈ NGƠI lúc {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ======")  # CRISTIANO

                current_day_to_process = get_next_day_to_process()  # CRISTIANO


async def main_async_runner():  # CRISTIANO
    await daily_scraping_manager()  # CRISTIANO


if __name__ == "__main__":
    try:  # CRISTIANO
        asyncio.run(main_async_runner())  # CRISTIANO
    except KeyboardInterrupt:  # CRISTIANO
        logging.info("Tool bị dừng bởi người dùng (Ctrl+C).")  # CRISTIANO
    except Exception as e:  # CRISTIANO
        logging.critical(f"Lỗi nghiêm trọng không bắt được ở phạm vi cao nhất (main_async_runner): {e}",
                         exc_info=True)  # CRISTIANO
    finally:  # CRISTIANO
        logging.info("Chương trình kết thúc.")  # CRISTIANO