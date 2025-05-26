import asyncio
from datetime import datetime
import logging
import os
from src.tools.service import ScraperService
from src.tools.database import get_session, ensure_partition_exists # ensure_partition_exists có vẻ không cần ở đây nữa

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
MEDIA_PHYSICAL_DIR = os.path.join(PROJECT_ROOT, "media_root", "brand_images")
os.makedirs(MEDIA_PHYSICAL_DIR, exist_ok=True)
logging.info(f"Thư mục lưu trữ media: {MEDIA_PHYSICAL_DIR}")

async def run_scraper():
    scraper = ScraperService()
    start_date = datetime(2022, 2, 1)
    end_date = datetime(2022, 2, 3)

    with get_session() as session: # Context manager này sẽ tự rollback nếu có lỗi không được xử lý
        try:
            logging.info(f"🚀 Bắt đầu scrape từ {start_date.date()} đến {end_date.date()}")
            brands = await scraper.scrape_by_date_range(start_date, end_date, session)
            if brands is not None: # brands là danh sách trả về từ service
                logging.info(f"✅ Quá trình scrape hoàn tất. Service đã xử lý {len(brands)} nhãn hiệu.")
            else:
                logging.warning("ℹ️ Quá trình scrape có thể đã gặp lỗi và không trả về danh sách nhãn hiệu.")
        except Exception as e:
            logging.error(f"❌ Lỗi nghiêm trọng trong quá trình chạy scraper: {str(e)}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(run_scraper())