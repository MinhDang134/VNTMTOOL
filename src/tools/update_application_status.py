import sys
import os
import asyncio
import logging
from src.tools.database import get_session
from src.tools.service import ScraperService
from src.tools.config import settings

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SRC_DIR_PATH = os.path.dirname(SCRIPT_DIR)
PROJECT_ROOT_PATH = os.path.dirname(SRC_DIR_PATH)

if PROJECT_ROOT_PATH not in sys.path:
    sys.path.insert(0, PROJECT_ROOT_PATH)

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

    if hasattr(settings, 'MEDIA_BRAND_IMAGES_SUBPATH'):
        brand_media_physical_dir = os.path.join(PROJECT_ROOT_PATH, settings.MEDIA_BRAND_IMAGES_SUBPATH)
    else:
        default_media_path = os.path.join(PROJECT_ROOT_PATH, "media_files", "brand_images")
        logger.warning(
            f"Không tìm thấy cấu hình 'MEDIA_BRAND_IMAGES_SUBPATH' trong settings. "
            f"Sử dụng đường dẫn mặc định: {default_media_path}. "
            f"Vui lòng cấu hình đường dẫn này trong src/tools/config.py."
        )
        brand_media_physical_dir = default_media_path

    try:
        os.makedirs(brand_media_physical_dir, exist_ok=True)
        logger.info(f"Thư mục media cho brand images được đặt tại: {brand_media_physical_dir}")
    except OSError as e:
        logger.error(f"Không thể tạo thư mục media tại {brand_media_physical_dir}: {e}")
        return

    scraper = ScraperService(media_dir=brand_media_physical_dir)

    try:
        with get_session() as session:
            await scraper.check_pending_brands(session)
    except Exception as e_main:
        logger.error(f"❌ Lỗi nghiêm trọng trong quy trình chính (main_update_statuses): {e_main}", exc_info=True)

    logger.info("🏁 Kết thúc quy trình cập nhật trạng thái đơn theo lịch.")

if __name__ == "__main__":
    print("Chạy main_update_statuses từ __main__")
    asyncio.run(main_update_statuses())