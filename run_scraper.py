import asyncio # thư viện bất đồng bộ
from datetime import datetime # thư viện datatime
import logging # thư viện log ghi ra

from src.tools.service import ScraperService # import hàm từ service
from src.tools.database import get_session # import hàm từ database
from src.tools.database import ensure_partition_exists  # import hàm từ database

# Cấu hình logging
logging.basicConfig( # hàm ghi log thông tin ra với cấu hình đơn giản

    level=logging.INFO, # level log thông tin ra info
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s' # format mà nó loc ra thông tin
)
async def run_scraper():# khai báo một hàm bất đồng bộ
    # ❌ Không cần gọi create_tables() nếu bạn đã tạo bảng partition thủ công
    # create_tables()  # Gọi chỗ khác nếu cần cho các bảng khác

    scraper = ScraperService() # tạo một biến để gán vào scraper bây mình sẽ đi sâu vào nha
    start_date = datetime(2022, 2, 1) # tạo biến nhập năm ngày thời gian các thứ
    end_date = datetime(2022, 2, 28) # tạo biến nhập ngày kết thúc của tool

    with get_session() as session: #  khi mà thức hiện cái with này thì nó sẽ tự động để lấy cái session dùng xong nó tự đóng
        try: # câu lệnh try
            logging.info(f"🚀 Bắt đầu scrape từ {start_date.date()} đến {end_date.date()}")
            # đây là câu log khi mà bắt đầu thôi start với cả end

            #  truyền giá trị session vào một cái hàm trong scraper gồm start , end , session vào
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
