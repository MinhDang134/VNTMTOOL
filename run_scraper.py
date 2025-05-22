import asyncio
from datetime import datetime
import logging

from src.tools.service import ScraperService
from src.tools.database import get_session
from src.tools.database import ensure_partition_exists  # bạn nên tách logic partition ra file riêng như `partition.py`

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

async def run_scraper():
    # ❌ Không cần gọi create_tables() nếu bạn đã tạo bảng partition thủ công
    # create_tables()  # Gọi chỗ khác nếu cần cho các bảng khác

    scraper = ScraperService()
    start_date = datetime(2022, 1, 1)
    end_date = datetime(2022, 3, 1)

    with get_session() as session:
        try:
            logging.info(f"🚀 Bắt đầu scrape từ {start_date.date()} đến {end_date.date()}")

            # 👉 Gọi scraper
            brands = await scraper.scrape_by_date_range(start_date, end_date, session)

            # 👉 Tạo partition (chỉ khi cần)
            for brand in brands:
                ensure_partition_exists(brand.application_date)
                session.add(brand)

            session.commit()
            logging.info(f"✅ Đã scrape và lưu {len(brands)} nhãn hiệu thành công.")

        except Exception as e:
            logging.error(f"❌ Lỗi khi chạy scraper: {str(e)}")

if __name__ == "__main__":
    asyncio.run(run_scraper())
