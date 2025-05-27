# ---- BẮT ĐẦU CODE THÊM VÀO ĐỂ SỬA LỖI MODULE NOT FOUND ----
import sys # Phải import sys và os ở đây, trước khi sử dụng
import os

# Đoạn code này sẽ xác định đường dẫn đến thư mục gốc của dự án (vntmtool)
# và thêm nó vào sys.path để Python có thể tìm thấy module 'src'.
# __file__ sẽ là /home/minhdangpy134/vntmtool/src/tools/update_application_status.py
# os.path.abspath(__file__) đảm bảo đường dẫn là tuyệt đối
# os.path.dirname(...) sẽ đi lên một cấp thư mục
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SRC_DIR = os.path.dirname(SCRIPT_DIR)
PROJECT_ROOT = os.path.dirname(SRC_DIR)

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# (Tùy chọn) Bạn có thể giữ lại các dòng print này để kiểm tra trong log cron
# print(f"Đã thêm vào sys.path từ update_application_status.py: {PROJECT_ROOT}")
# print(f"sys.path hiện tại từ update_application_status.py: {sys.path}")
# ---- KẾT THÚC CODE THÊM VÀO ----

# Bây giờ mới đến các import gốc của bạn
import asyncio
import logging
# from datetime import datetime, timezone # Dòng này chưa thấy bạn dùng, nếu cần thì giữ lại

# Các import từ 'src' bây giờ nên hoạt động
from src.tools.database import get_session
from src.tools.service import ScraperService

# Phần còn lại của script của bạn giữ nguyên...
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
    ]
)

logger = logging.getLogger(__name__)

async def main_update_statuses():
    logger.info("🚀 Bắt đầu quy trình cập nhật trạng thái đơn theo lịch.")
    scraper = ScraperService()

    try:
        with get_session() as session:
            await scraper.check_pending_brands(session)
    except Exception as e_main:
        logger.error(f"❌ Lỗi nghiêm trọng trong quy trình chính (main_update_statuses): {e_main}", exc_info=True)

    logger.info("🏁 Kết thúc quy trình cập nhật trạng thái đơn theo lịch.")

if __name__ == "__main__":
    print("Chạy main_update_statuses từ __main__")
    asyncio.run(main_update_statuses())