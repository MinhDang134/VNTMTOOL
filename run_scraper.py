import asyncio
from datetime import datetime
from src.tools.service import ScraperService
from src.tools.database import get_session, create_tables

async def run_scraper():
    # Khởi tạo database và tạo bảng
    create_tables()
    
    # Tạo instance của ScraperService
    scraper = ScraperService()
    

    start_date = datetime(2022, 1, 1)
    end_date = datetime(2022, 3, 1)
    
    # Sử dụng context manager để quản lý session
    with get_session() as session:
        try:

            brands = await scraper.scrape_by_date_range(start_date, end_date, session)
            print(f"Đã scrape được {len(brands)} nhãn hiệu")
        except Exception as e:
            print(f"Lỗi khi chạy scraper: {str(e)}")

if __name__ == "__main__":
    asyncio.run(run_scraper()) 