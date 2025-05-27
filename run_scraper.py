import asyncio
from datetime import datetime, timedelta, date as date_type
import os
from src.tools.config import settings
from src.tools.service import ScraperService
from src.tools.database import get_session
from src.tools.state_manager import (
    load_scrape_state, logging, save_scrape_state,
    load_control_state, save_control_state, get_control_state_path,
    clear_page_state_for_day
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scraper_activity.log", mode='a', encoding='utf-8'),
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


def get_next_day_to_process() -> date_type:  # nội dung từ gemini (toàn bộ hàm mới)
    """Xác định ngày tiếp theo cần xử lý dựa trên trạng thái đã lưu hoặc cấu hình."""
    control_state = load_control_state(CONTROL_STATE_FILE_PATH)
    last_completed_str = control_state.get("last_fully_completed_day")

    if last_completed_str:
        try:
            last_completed_date = datetime.strptime(last_completed_str, "%Y-%m-%d").date()
            next_day = last_completed_date + timedelta(days=1)  # nội dung từ gemini
            logging.info(
                f"Ngày cuối cùng hoàn thành được ghi nhận: {last_completed_str}. Ngày tiếp theo để xử lý: {next_day.strftime('%Y-%m-%d')}")
            return next_day
        except ValueError:
            logging.warning(
                f"Định dạng ngày trong file trạng thái điều khiển không hợp lệ: '{last_completed_str}'. Sẽ bắt đầu từ ngày cấu hình.")

    # Nếu không có trạng thái hoặc lỗi, bắt đầu từ ngày cấu hình trong settings
    start_date = date_type(
        settings.INITIAL_SCRAPE_START_YEAR,
        settings.INITIAL_SCRAPE_START_MOTH,
        settings.INITIAL_SCRAPE_START_DAY
    )
    logging.info(
        f"Không có trạng thái ngày hoàn thành hợp lệ. Bắt đầu từ ngày cấu hình mặc định: {start_date.strftime('%Y-%m-%d')}")
    return start_date


def get_overall_end_date() -> date_type:
    """Lấy ngày kết thúc tổng thể của quá trình scrape từ settings, hoặc mặc định là ngày hôm qua."""
    if settings.OVERALL_SCRAPE_END_YEAR and settings.OVERALL_SCRAPE_END_MOTH and settings.OVERALL_SCRAPE_END_DAY:
        try:
            return date_type(
                settings.OVERALL_SCRAPE_END_YEAR,
                settings.OVERALL_SCRAPE_END_MOTH,
                settings.OVERALL_SCRAPE_END_DAY
            )
        except (TypeError,
                ValueError) as e:
            logging.warning(
                f"Một số giá trị OVERALL_SCRAPE_END trong settings không hợp lệ hoặc thiếu: {e}. Sẽ mặc định là ngày hôm qua.")
    return datetime.now().date() - timedelta(days=1)


async def daily_scraping_task():
    """Tác vụ chính, quản lý việc cào dữ liệu theo từng ngày và chu kỳ chạy/nghỉ."""
    scraper = ScraperService()
    current_day_to_process = get_next_day_to_process()
    overall_end_date = get_overall_end_date()

    while True:
        logging.info(
            f"====== BẮT ĐẦU PHIÊN LÀM VIỆC ({settings.RUN_DURATION_MINUTES} PHÚT) lúc {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ======")
        session_start_time = asyncio.get_event_loop().time()
        session_processed_any_day = False

        # Vòng lặp xử lý các ngày trong một phiên làm việc (giới hạn bởi RUN_DURATION_SECONDS)
        while (asyncio.get_event_loop().time() - session_start_time) < RUN_DURATION_SECONDS:
            if current_day_to_process > overall_end_date:
                logging.info(f"Đã đạt đến ngày kết thúc tổng thể ({overall_end_date.strftime('%Y-%m-%d')}). "
                             f"Tạm dừng phiên làm việc, sẽ kiểm tra lại sau giai đoạn nghỉ.")
                break
            logging.info(
                f"--- Chuẩn bị xử lý cho ngày: {current_day_to_process.strftime('%Y-%m-%d')} ---")

            # Key cho trạng thái trang của ngày hiện tại (ví dụ: "brands_2023-01-15_2023-01-15")
            day_key = f"brands_{current_day_to_process.strftime('%Y-%m-%d')}_{current_day_to_process.strftime('%Y-%m-%d')}"
            initial_page_for_this_day = load_scrape_state(PAGE_STATE_FILE_PATH, day_key)  # nội dung từ gemini

            scrape_result = {"status": "not_started", "brands_processed_count": 0,
                             "message": "Chưa bắt đầu"}
            try:
                with get_session() as session:
                    def state_saver_callback_for_day(page_just_completed: int):
                        save_scrape_state(PAGE_STATE_FILE_PATH, day_key, page_just_completed)

                    logging.info(
                        f"🚀 Bắt đầu scrape cho ngày [{current_day_to_process.strftime('%Y-%m-%d')}], từ trang {initial_page_for_this_day}.")
                    scrape_result = await scraper.scrape_by_date_range(
                        start_date=current_day_to_process,
                        end_date=current_day_to_process,
                        session=session,
                        initial_start_page=initial_page_for_this_day,
                        state_save_callback=state_saver_callback_for_day
                    )

            except Exception as e_main_scope:
                logging.error(
                    f"Lỗi không mong muốn trong quá trình xử lý ngày {current_day_to_process.strftime('%Y-%m-%d')}: {e_main_scope}",
                    exc_info=True)
                scrape_result = {"status": "critical_error", "brands_processed_count": 0,
                                 "message": str(e_main_scope)}
                await asyncio.sleep(5)
                break  # Dừng phiên làm việc nếu có lỗi nghiêm trọng # nội dung từ gemini

            session_processed_any_day = True
            status = scrape_result.get("status", "unknown_error")
            brands_count = scrape_result.get("brands_processed_count", 0)
            message = scrape_result.get("message", status)

            if status in ["completed_all_pages", "no_data_on_first_page"]:
                logging.info(f"✅ HOÀN TẤT XỬ LÝ DỮ LIỆU CHO NGÀY: {current_day_to_process.strftime('%Y-%m-%d')}. "
                             f"Số nhãn hiệu đã xử lý: {brands_count}. Lý do: {message}")
                save_control_state(CONTROL_STATE_FILE_PATH,
                                   current_day_to_process)  # Lưu ngày này đã hoàn thành # nội dung từ gemini
                clear_page_state_for_day(PAGE_STATE_FILE_PATH,
                                         current_day_to_process)  # Xóa trạng thái trang của ngày đã xong # nội dung từ gemini
                current_day_to_process += timedelta(days=1)  # Chuyển sang ngày tiếp theo để xử lý # nội dung từ gemini
            else:  # Các trường hợp lỗi (request_error, soup_error, db_commit_error, critical_error) hoặc ngày chưa xong # nội dung từ gemini
                logging.warning(f"Ngày {current_day_to_process.strftime('%Y-%m-%d')} chưa hoàn thành hoặc gặp lỗi. "
                                f"Lý do: {message}. Sẽ thử lại trong phiên làm việc tiếp theo.")
                # Trạng thái trang của ngày này đã được state_save_callback lưu.
                # Dừng xử lý các ngày tiếp theo trong phiên này, chuyển sang giai đoạn nghỉ.
                break  # Thoát khỏi vòng lặp xử lý các ngày trong phiên hiện tại # nội dung từ gemini

            # Kiểm tra lại thời gian phiên làm việc sau mỗi ngày xử lý xong
            if (asyncio.get_event_loop().time() - session_start_time) >= RUN_DURATION_SECONDS:
                logging.info("Hết thời gian phiên làm việc quy định.")
                break  # Thoát khỏi vòng lặp xử lý các ngày trong phiên

            await asyncio.sleep(1)  # Nghỉ ngắn giữa các ngày nếu còn thời gian trong phiên

        # Logging kết thúc phiên làm việc
        if not session_processed_any_day and current_day_to_process <= overall_end_date:
            logging.info(
                f"Phiên làm việc kết thúc mà không xử lý được ngày nào (có thể do lỗi ban đầu hoặc đã đạt giới hạn ngày xử lý).")
        elif not session_processed_any_day and current_day_to_process > overall_end_date:
            logging.info(
                f"Đã đạt ngày kết thúc tổng thể. Sẽ kiểm tra lại sau giai đoạn nghỉ ngơi.")

        logging.info(
            f"====== KẾT THÚC PHIÊN LÀM VIỆC lúc {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ======")
        logging.info(
            f"====== BẮT ĐẦU NGHỈ NGƠI ({settings.PAUSE_DURATION_MINUTES} PHÚT) lúc {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ======")
        await asyncio.sleep(PAUSE_DURATION_SECONDS)
        logging.info(
            f"====== KẾT THÚC NGHỈ NGƠI lúc {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ======")

        # Cập nhật lại ngày cần xử lý phòng trường hợp control_state thay đổi bởi tiến trình khác (hiếm)
        # hoặc để đảm bảo logic bắt đầu ngày mới là chính xác sau khi nghỉ.
        new_next_day = get_next_day_to_process()  # nội dung từ gemini
        if new_next_day < current_day_to_process and status in ["completed_all_pages",
                                                                "no_data_on_first_page"]:
            # Điều này có nghĩa là ngày vừa xử lý xong đã được lưu đúng, và get_next_day_to_process đã tính ngày tiếp theo
            # Không cần làm gì current_day_to_process đã được tăng lên
            pass
        else:
            current_day_to_process = new_next_day


async def main():  # nội dung từ gemini (hàm main mới)
    # chạy check_pending_brands ở đây nếu muốn, ví dụ tạo một task chạy định kỳ
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
    await daily_scraping_task()


if __name__ == "__main__":
    try:  #  (thêm try-except)
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Tool bị dừng bởi người dùng (Ctrl+C).")
    except Exception as e:
        logging.critical(f"Lỗi nghiêm trọng không bắt được ở phạm vi cao nhất (main): {e}",
                         exc_info=True)