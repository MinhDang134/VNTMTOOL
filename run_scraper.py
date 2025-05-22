import asyncio
from datetime import datetime
from src.tools.service import ScraperService
from src.tools.database import get_session, create_tables
import logging
from src.tools.database import create_monthly_partitions

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

async def run_scraper():
    create_tables()

    scraper = ScraperService()
    start_date = datetime(2022, 1, 1)
    end_date = datetime(2022, 3, 1)

    with get_session() as session:
        try:
            # 💡 Tạo partition trước khi insert
            create_monthly_partitions(session, start_date, end_date)

            # Rồi mới scrape
            brands = await scraper.scrape_by_date_range(start_date, end_date, session)
            print(f"Đã scrape được {len(brands)} nhãn hiệu")
        except Exception as e:
            logging.error(f"Lỗi khi chạy scraper: {str(e)}")

if __name__ == "__main__":
    asyncio.run(run_scraper()) 