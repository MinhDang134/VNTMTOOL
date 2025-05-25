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
os.makedirs(MEDIA_PHYSICAL_DIR, exist_ok=True) # Rất tốt! Đảm bảo thư mục tồn tại
logging.info(f"Thư mục lưu trữ media: {MEDIA_PHYSICAL_DIR}")

async def run_scraper():
    scraper = ScraperService()
    start_date = datetime(2022, 2, 1)
    end_date = datetime(2022, 2, 28)

    with get_session() as session: # Context manager này sẽ tự rollback nếu có lỗi không được xử lý
        try:
            logging.info(f"🚀 Bắt đầu scrape từ {start_date.date()} đến {end_date.date()}")
            brands = await scraper.scrape_by_date_range(start_date, end_date, session)

            # ---- PHẦN NÀY NÊN BỎ ĐI ----
            # Lý do: scrape_by_date_range trong service.py đã gọi ensure_partition_exists
            # và bulk_create (bao gồm cả session.add và session.commit).
            # Việc gọi lại ở đây sẽ thừa và có thể gây lỗi.
            #
            # for brand in brands:
            #     ensure_partition_exists(brand.application_date) # <- Đã làm trong service
            #     session.add(brand) # <- Đã làm trong service (qua bulk_create)
            # session.commit() # <- Đã làm trong service (qua bulk_create)
            # ---- KẾT THÚC PHẦN NÊN BỎ ----

            if brands is not None: # brands là danh sách trả về từ service
                logging.info(f"✅ Quá trình scrape hoàn tất. Service đã xử lý {len(brands)} nhãn hiệu.")
            else:
                # Trường hợp này xảy ra nếu scrape_by_date_range bị lỗi và trả về None trước khi xử lý brands
                logging.warning("ℹ️ Quá trình scrape có thể đã gặp lỗi và không trả về danh sách nhãn hiệu.")

        except Exception as e:
            # Lỗi ở đây thường là lỗi kết nối DB ban đầu, hoặc lỗi không mong muốn bên ngoài service.
            # Service đã có try-except riêng cho logic scrape.
            logging.error(f"❌ Lỗi nghiêm trọng trong quá trình chạy scraper: {str(e)}", exc_info=True)
            # session.rollback() sẽ được tự động gọi bởi context manager 'with get_session()' nếu có lỗi thoát ra khỏi khối try

if __name__ == "__main__":
    asyncio.run(run_scraper())