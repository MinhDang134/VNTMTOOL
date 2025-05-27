import asyncio
from datetime import datetime
import os
from src.tools.service import ScraperService
from src.tools.database import get_session
from src.tools.state_manager import load_scrape_state, logging, save_scrape_state

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
MEDIA_PHYSICAL_DIR = os.path.join(PROJECT_ROOT, "media_root", "brand_images")
os.makedirs(MEDIA_PHYSICAL_DIR, exist_ok=True)
logging.info(f"Thư mục lưu trữ media: {MEDIA_PHYSICAL_DIR}")
# Đường dẫn tới file lưu trạng thái cào dữ liệu
STATE_FILE_PATH = os.path.join(PROJECT_ROOT, "scraper_state.json")


async def run_scraper():
    scraper = ScraperService()
    start_date = datetime(2025, 1, 5)
    end_date = datetime(2025, 1, 7)

    date_range_key = f"brands_{start_date.strftime('%Y-%m-%d')}_{end_date.strftime('%Y-%m-%d')}"

    initial_page_to_scrape = load_scrape_state(STATE_FILE_PATH, date_range_key)
    try:

        with get_session() as session:
            try:
                logging.info(
                    f"🚀 Bắt đầu scrape cho khoảng ngày [{start_date.date()} - {end_date.date()}], từ trang {initial_page_to_scrape}.")
                def state_saver_callback(page_just_completed: int):
                    save_scrape_state(STATE_FILE_PATH, date_range_key, page_just_completed)


                brands_processed = await scraper.scrape_by_date_range(
                    start_date=start_date,
                    end_date=end_date,
                    session=session,
                    initial_start_page=initial_page_to_scrape,
                    state_save_callback=state_saver_callback
                )

                if brands_processed is not None:
                    logging.info(
                        f"✅ Hoàn tất scrape cho khoảng ngày. Đã xử lý {len(brands_processed)} nhãn hiệu trong lần chạy này.")
                else:
                    logging.warning("ℹ️ Quá trình scrape có thể đã gặp lỗi và không trả về danh sách nhãn hiệu.")

            except Exception as e_service:
                logging.error(f"❌ Lỗi nghiêm trọng trong ScraperService: {str(e_service)}", exc_info=True)

    except Exception as e_main_scope:
        logging.error(f"Lỗi không mong muốn ở phạm vi chính của run_scraper: {e_main_scope}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(run_scraper())
