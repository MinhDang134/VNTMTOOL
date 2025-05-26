# update_application_status.py
import asyncio
import logging
from datetime import datetime, timezone

# Đảm bảo các import này trỏ đúng đến các module trong dự án của bạn
from src.tools.database import get_session
# Giả sử model Brand được định nghĩa trong src.tools.models
# from src.tools.models import Brand # Không cần trực tiếp ở đây nếu check_pending_brands xử lý
from src.tools.service import ScraperService
# from src.tools.config import settings # Có thể cần nếu có config đặc thù cho script này

# --- Thiết lập Logging ---
# Sử dụng cấu hình logging nhất quán
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(), # Log ra console
        # logging.FileHandler("update_status.log") # Tùy chọn: Log ra file
    ]
)
# Tạo một logger instance cụ thể cho module này
logger = logging.getLogger(__name__)

async def main_update_statuses():
    """
    Hàm chính để kích hoạt quy trình cập nhật trạng thái các đơn "đang giải quyết".
    Hàm này sẽ được gọi bởi Cron Job.
    """
    logger.info("🚀 Bắt đầu quy trình cập nhật trạng thái đơn theo lịch.")
    scraper = ScraperService() # Khởi tạo ScraperService

    try:
        # Sử dụng context manager để quản lý session
        with get_session() as session:
            # Gọi phương thức check_pending_brands đã có (và được tinh chỉnh) trong ScraperService
            # Phương thức này sẽ thực hiện toàn bộ logic:
            # 1. Truy vấn DB lấy đơn "đang giải quyết"
            # 2. Gọi API VietnamTrademark cho mỗi đơn
            # 3. Bóc tách trạng thái mới
            # 4. So sánh và cập nhật vào DB nếu khác
            await scraper.check_pending_brands(session)
    except Exception as e_main:
        logger.error(f"❌ Lỗi nghiêm trọng trong quy trình chính (main_update_statuses): {e_main}", exc_info=True)

    logger.info("🏁 Kết thúc quy trình cập nhật trạng thái đơn theo lịch.")

if __name__ == "__main__":
    # Kịch bản này được thiết kế để chạy bởi Cron Job lúc 00:00 hàng ngày.
    # Ví dụ Cron Job (Linux):
    # 0 0 * * * /usr/bin/python3 /path/to/your_project/update_application_status.py
    #
    # Quan trọng:
    # - Thay thế `/path/to/your_project/` bằng đường dẫn thực tế đến thư mục dự án của bạn.
    # - Đảm bảo môi trường Python mà cron sử dụng có đủ các thư viện cần thiết (ví dụ: httpx, beautifulsoup4, sqlmodel).
    # - Nếu dùng virtual environment, cron job nên kích hoạt venv trước khi chạy script, ví dụ:
    #   0 0 * * * /path/to/your_project/venv/bin/python /path/to/your_project/update_application_status.py
    # - Cân nhắc đặt biến môi trường PYTHONIOENCODING=utf-8 cho cron nếu có vấn đề về encoding.

    asyncio.run(main_update_statuses())