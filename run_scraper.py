# run_scraper.py
import asyncio
from datetime import datetime, timedelta, date as date_type  # nội dung từ gemini (thêm timedelta, date_type)
import os
from src.tools.config import settings  # Từ code gốc
from src.tools.service import ScraperService  # Từ code gốc
from src.tools.database import get_session  # Từ code gốc
from src.tools.state_manager import (  # nội dung từ gemini (cập nhật imports)
    load_scrape_state, logging, save_scrape_state,
    load_control_state, save_control_state, get_control_state_path,
    clear_page_state_for_day
)

# Cấu hình logging chung, ghi ra file và console
logging.basicConfig(  # nội dung từ gemini (cấu hình logging mới)
    level=logging.INFO,  # nội dung từ gemini
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # nội dung từ gemini
    handlers=[  # nội dung từ gemini
        logging.FileHandler("scraper_activity.log", mode='a', encoding='utf-8'),  # nội dung từ gemini (thêm encoding)
        logging.StreamHandler()  # nội dung từ gemini
    ]  # nội dung từ gemini
)

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))  # Từ code gốc
MEDIA_PHYSICAL_DIR = os.path.join(PROJECT_ROOT, "media_root", "brand_images")  # Từ code gốc
os.makedirs(MEDIA_PHYSICAL_DIR, exist_ok=True)  # Từ code gốc
logging.info(f"Thư mục lưu trữ media: {MEDIA_PHYSICAL_DIR}")  # Từ code gốc

# Đường dẫn tới file lưu trạng thái trang cho từng ngày (thay thế STATE_FILE_PATH cũ)
PAGE_STATE_FILE_PATH = os.path.join(PROJECT_ROOT, "scraper_page_state.json")  # nội dung từ gemini
# Đường dẫn tới file lưu trạng thái điều khiển (ngày cuối cùng xử lý xong)
CONTROL_STATE_FILE_PATH = get_control_state_path(PROJECT_ROOT)  # nội dung từ gemini (sử dụng hàm từ state_manager)

RUN_DURATION_SECONDS = settings.RUN_DURATION_MINUTES * 60  # nội dung từ gemini
PAUSE_DURATION_SECONDS = settings.PAUSE_DURATION_MINUTES * 60  # nội dung từ gemini


def get_next_day_to_process() -> date_type:  # nội dung từ gemini (toàn bộ hàm mới)
    """Xác định ngày tiếp theo cần xử lý dựa trên trạng thái đã lưu hoặc cấu hình."""  # nội dung từ gemini
    control_state = load_control_state(CONTROL_STATE_FILE_PATH)  # nội dung từ gemini
    last_completed_str = control_state.get("last_fully_completed_day")  # nội dung từ gemini

    if last_completed_str:  # nội dung từ gemini
        try:  # nội dung từ gemini
            last_completed_date = datetime.strptime(last_completed_str, "%Y-%m-%d").date()  # nội dung từ gemini
            next_day = last_completed_date + timedelta(days=1)  # nội dung từ gemini
            logging.info(
                f"Ngày cuối cùng hoàn thành được ghi nhận: {last_completed_str}. Ngày tiếp theo để xử lý: {next_day.strftime('%Y-%m-%d')}")  # nội dung từ gemini
            return next_day  # nội dung từ gemini
        except ValueError:  # nội dung từ gemini
            logging.warning(
                f"Định dạng ngày trong file trạng thái điều khiển không hợp lệ: '{last_completed_str}'. Sẽ bắt đầu từ ngày cấu hình.")  # nội dung từ gemini

    # Nếu không có trạng thái hoặc lỗi, bắt đầu từ ngày cấu hình trong settings
    start_date = date_type(  # nội dung từ gemini
        settings.INITIAL_SCRAPE_START_YEAR,  # nội dung từ gemini
        settings.INITIAL_SCRAPE_START_MOTH,  # nội dung từ gemini (Lưu ý: 'MOTH' trong settings, nên là 'MONTH')
        settings.INITIAL_SCRAPE_START_DAY  # nội dung từ gemini
    )  # nội dung từ gemini
    logging.info(
        f"Không có trạng thái ngày hoàn thành hợp lệ. Bắt đầu từ ngày cấu hình mặc định: {start_date.strftime('%Y-%m-%d')}")  # nội dung từ gemini
    return start_date  # nội dung từ gemini


def get_overall_end_date() -> date_type:  # nội dung từ gemini (toàn bộ hàm mới)
    """Lấy ngày kết thúc tổng thể của quá trình scrape từ settings, hoặc mặc định là ngày hôm qua."""  # nội dung từ gemini
    if settings.OVERALL_SCRAPE_END_YEAR and settings.OVERALL_SCRAPE_END_MOTH and settings.OVERALL_SCRAPE_END_DAY:  # nội dung từ gemini
        try:  # nội dung từ gemini
            # Pydantic đã chuyển đổi các biến này thành int nếu chúng được khai báo là int trong Settings
            return date_type(  # nội dung từ gemini
                settings.OVERALL_SCRAPE_END_YEAR,  # nội dung từ gemini
                settings.OVERALL_SCRAPE_END_MOTH,  # nội dung từ gemini (Lưu ý: 'MOTH' trong settings)
                settings.OVERALL_SCRAPE_END_DAY  # nội dung từ gemini
            )  # nội dung từ gemini
        except (TypeError,
                ValueError) as e:  # nội dung từ gemini (Phòng trường hợp giá trị Optional là None hoặc không hợp lệ)
            logging.warning(
                f"Một số giá trị OVERALL_SCRAPE_END trong settings không hợp lệ hoặc thiếu: {e}. Sẽ mặc định là ngày hôm qua.")  # nội dung từ gemini
    # Mặc định là ngày hôm qua nếu không có ngày kết thúc tổng thể được cấu hình
    return datetime.now().date() - timedelta(days=1)  # nội dung từ gemini


async def daily_scraping_task():  # nội dung từ gemini (toàn bộ hàm mới)
    """Tác vụ chính, quản lý việc cào dữ liệu theo từng ngày và chu kỳ chạy/nghỉ."""  # nội dung từ gemini
    scraper = ScraperService()  # nội dung từ gemini
    current_day_to_process = get_next_day_to_process()  # nội dung từ gemini
    overall_end_date = get_overall_end_date()  # nội dung từ gemini

    while True:  # Vòng lặp chính cho chu kỳ chạy/nghỉ # nội dung từ gemini
        logging.info(
            f"====== BẮT ĐẦU PHIÊN LÀM VIỆC ({settings.RUN_DURATION_MINUTES} PHÚT) lúc {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ======")  # nội dung từ gemini
        session_start_time = asyncio.get_event_loop().time()  # nội dung từ gemini
        session_processed_any_day = False  # nội dung từ gemini (Theo dõi xem phiên có xử lý ngày nào không)

        # Vòng lặp xử lý các ngày trong một phiên làm việc (giới hạn bởi RUN_DURATION_SECONDS)
        while (asyncio.get_event_loop().time() - session_start_time) < RUN_DURATION_SECONDS:  # nội dung từ gemini
            if current_day_to_process > overall_end_date:  # nội dung từ gemini
                logging.info(f"Đã đạt đến ngày kết thúc tổng thể ({overall_end_date.strftime('%Y-%m-%d')}). "
                             f"Tạm dừng phiên làm việc, sẽ kiểm tra lại sau giai đoạn nghỉ.")  # nội dung từ gemini
                break  # Thoát vòng lặp phiên làm việc # nội dung từ gemini

            logging.info(
                f"--- Chuẩn bị xử lý cho ngày: {current_day_to_process.strftime('%Y-%m-%d')} ---")  # nội dung từ gemini

            # Key cho trạng thái trang của ngày hiện tại (ví dụ: "brands_2023-01-15_2023-01-15")
            day_key = f"brands_{current_day_to_process.strftime('%Y-%m-%d')}_{current_day_to_process.strftime('%Y-%m-%d')}"  # nội dung từ gemini
            initial_page_for_this_day = load_scrape_state(PAGE_STATE_FILE_PATH, day_key)  # nội dung từ gemini

            scrape_result = {"status": "not_started", "brands_processed_count": 0,
                             "message": "Chưa bắt đầu"}  # nội dung từ gemini
            try:  # nội dung từ gemini
                # get_session() nên được gọi trong vòng lặp để mỗi ngày/phiên có session mới nếu cần
                with get_session() as session:  # nội dung từ gemini
                    def state_saver_callback_for_day(page_just_completed: int):  # nội dung từ gemini
                        save_scrape_state(PAGE_STATE_FILE_PATH, day_key, page_just_completed)  # nội dung từ gemini

                    logging.info(
                        f"🚀 Bắt đầu scrape cho ngày [{current_day_to_process.strftime('%Y-%m-%d')}], từ trang {initial_page_for_this_day}.")  # nội dung từ gemini
                    scrape_result = await scraper.scrape_by_date_range(  # nội dung từ gemini
                        start_date=current_day_to_process,  # nội dung từ gemini
                        end_date=current_day_to_process,  # Cào theo từng ngày # nội dung từ gemini
                        session=session,  # nội dung từ gemini
                        initial_start_page=initial_page_for_this_day,  # nội dung từ gemini
                        state_save_callback=state_saver_callback_for_day  # nội dung từ gemini
                    )  # nội dung từ gemini

            except Exception as e_main_scope:  # nội dung từ gemini
                logging.error(
                    f"Lỗi không mong muốn trong quá trình xử lý ngày {current_day_to_process.strftime('%Y-%m-%d')}: {e_main_scope}",
                    exc_info=True)  # nội dung từ gemini
                scrape_result = {"status": "critical_error", "brands_processed_count": 0,
                                 "message": str(e_main_scope)}  # nội dung từ gemini
                await asyncio.sleep(5)  # Chờ một chút trước khi dừng phiên làm việc # nội dung từ gemini
                break  # Dừng phiên làm việc nếu có lỗi nghiêm trọng # nội dung từ gemini

            session_processed_any_day = True  # Đánh dấu phiên này có hoạt động # nội dung từ gemini
            status = scrape_result.get("status", "unknown_error")  # nội dung từ gemini
            brands_count = scrape_result.get("brands_processed_count", 0)  # nội dung từ gemini
            message = scrape_result.get("message", status)  # nội dung từ gemini

            if status in ["completed_all_pages", "no_data_on_first_page"]:  # nội dung từ gemini
                logging.info(f"✅ HOÀN TẤT XỬ LÝ DỮ LIỆU CHO NGÀY: {current_day_to_process.strftime('%Y-%m-%d')}. "
                             f"Số nhãn hiệu đã xử lý: {brands_count}. Lý do: {message}")  # nội dung từ gemini
                save_control_state(CONTROL_STATE_FILE_PATH,
                                   current_day_to_process)  # Lưu ngày này đã hoàn thành # nội dung từ gemini
                clear_page_state_for_day(PAGE_STATE_FILE_PATH,
                                         current_day_to_process)  # Xóa trạng thái trang của ngày đã xong # nội dung từ gemini
                current_day_to_process += timedelta(days=1)  # Chuyển sang ngày tiếp theo để xử lý # nội dung từ gemini
            else:  # Các trường hợp lỗi (request_error, soup_error, db_commit_error, critical_error) hoặc ngày chưa xong # nội dung từ gemini
                logging.warning(f"Ngày {current_day_to_process.strftime('%Y-%m-%d')} chưa hoàn thành hoặc gặp lỗi. "
                                f"Lý do: {message}. Sẽ thử lại trong phiên làm việc tiếp theo.")  # nội dung từ gemini
                # Trạng thái trang của ngày này đã được state_save_callback lưu.
                # Dừng xử lý các ngày tiếp theo trong phiên này, chuyển sang giai đoạn nghỉ.
                break  # Thoát khỏi vòng lặp xử lý các ngày trong phiên hiện tại # nội dung từ gemini

            # Kiểm tra lại thời gian phiên làm việc sau mỗi ngày xử lý xong
            if (asyncio.get_event_loop().time() - session_start_time) >= RUN_DURATION_SECONDS:  # nội dung từ gemini
                logging.info("Hết thời gian phiên làm việc quy định.")  # nội dung từ gemini
                break  # Thoát khỏi vòng lặp xử lý các ngày trong phiên # nội dung từ gemini

            await asyncio.sleep(1)  # Nghỉ ngắn giữa các ngày nếu còn thời gian trong phiên # nội dung từ gemini

        # Logging kết thúc phiên làm việc
        if not session_processed_any_day and current_day_to_process <= overall_end_date:  # nội dung từ gemini
            logging.info(
                f"Phiên làm việc kết thúc mà không xử lý được ngày nào (có thể do lỗi ban đầu hoặc đã đạt giới hạn ngày xử lý).")  # nội dung từ gemini
        elif not session_processed_any_day and current_day_to_process > overall_end_date:  # nội dung từ gemini
            logging.info(
                f"Đã đạt ngày kết thúc tổng thể. Sẽ kiểm tra lại sau giai đoạn nghỉ ngơi.")  # nội dung từ gemini

        logging.info(
            f"====== KẾT THÚC PHIÊN LÀM VIỆC lúc {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ======")  # nội dung từ gemini
        logging.info(
            f"====== BẮT ĐẦU NGHỈ NGƠI ({settings.PAUSE_DURATION_MINUTES} PHÚT) lúc {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ======")  # nội dung từ gemini
        await asyncio.sleep(PAUSE_DURATION_SECONDS)  # nội dung từ gemini
        logging.info(
            f"====== KẾT THÚC NGHỈ NGƠI lúc {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ======")  # nội dung từ gemini

        # Cập nhật lại ngày cần xử lý phòng trường hợp control_state thay đổi bởi tiến trình khác (hiếm)
        # hoặc để đảm bảo logic bắt đầu ngày mới là chính xác sau khi nghỉ.
        new_next_day = get_next_day_to_process()  # nội dung từ gemini
        if new_next_day < current_day_to_process and status in ["completed_all_pages",
                                                                "no_data_on_first_page"]:  # nội dung từ gemini
            # Điều này có nghĩa là ngày vừa xử lý xong đã được lưu đúng, và get_next_day_to_process đã tính ngày tiếp theo
            # Không cần làm gì current_day_to_process đã được tăng lên
            pass  # nội dung từ gemini
        else:  # nội dung từ gemini
            current_day_to_process = new_next_day  # nội dung từ gemini


async def main():  # nội dung từ gemini (hàm main mới)
    # Bạn có thể chạy check_pending_brands ở đây nếu muốn, ví dụ tạo một task chạy định kỳ
    # async def periodic_pending_check_task():
    #     scraper_for_pending = ScraperService()
    #     while True:
    #         logging.info("Bắt đầu tác vụ kiểm tra định kỳ các đơn đang chờ xử lý...")
    #         try:
    #             with get_session() as session:
    #                 await scraper_for_pending.check_pending_brands(session)
    #         except Exception as e_pending:
    #             logging.error(f"Lỗi trong tác vụ kiểm tra định kỳ các đơn đang chờ: {e_pending}", exc_info=True)
    #         await asyncio.sleep(3600) # Ví dụ: chạy mỗi giờ
    #
    # asyncio.create_task(periodic_pending_check_task())
    await daily_scraping_task()  # nội dung từ gemini


if __name__ == "__main__":
    try:  # nội dung từ gemini (thêm try-except)
        asyncio.run(main())  # nội dung từ gemini
    except KeyboardInterrupt:  # nội dung từ gemini
        logging.info("Tool bị dừng bởi người dùng (Ctrl+C).")  # nội dung từ gemini
    except Exception as e:  # nội dung từ gemini
        logging.critical(f"Lỗi nghiêm trọng không bắt được ở phạm vi cao nhất (main): {e}",
                         exc_info=True)  # nội dung từ gemini