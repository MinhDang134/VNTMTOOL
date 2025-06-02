import asyncio
from sqlalchemy import create_engine

import logging
from run_scraper import MEDIA_PHYSICAL_DIR, main_async_runner
from src.tools.config import settings
from src.tools.database import get_session
from src.tools.service import ScraperService

ai_logger = logging.getLogger("AIInteractionHandler")
import logging
import os
import asyncio


LOG_OUTPUT_DIR_PATH_AI = "/home/minhdangpy134/Logvntmtool"
LOG_FILE_NAME_AI = "ai_handler_activity.txt"

try:
    os.makedirs(LOG_OUTPUT_DIR_PATH_AI, exist_ok=True)
except OSError as e:
    print(f"Lỗi khi tạo thư mục log {LOG_OUTPUT_DIR_PATH_AI}: {e}")

LOG_FILE_PATH_AI = os.path.join(LOG_OUTPUT_DIR_PATH_AI, LOG_FILE_NAME_AI)


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(processName)s (%(process)d) - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE_PATH_AI, mode='a', encoding='utf-8'),
        logging.StreamHandler()
    ]
)


async def handle_ai_brand_search_and_update_count(brand_name_from_ai: str):
    ai_logger.info(
        f"🤖 AI Handler: Nhận yêu cầu tìm, cập nhật count và lấy thông tin cho nhãn hiệu: '{brand_name_from_ai}'")

    if not brand_name_from_ai:
        ai_logger.warning("🤖 AI Handler: Tên nhãn hiệu rỗng, không xử lý.")
        return {"status": "error", "message": "Brand name is empty", "data": []}

    scraper = ScraperService(media_dir=MEDIA_PHYSICAL_DIR)

    ai_handler_engine = None
    try:
        ai_handler_engine = create_engine(settings.DATABASE_URL, pool_pre_ping=True, echo=False)
        with get_session(ai_handler_engine) as session:
            ai_logger.info(f"🤖 AI Handler: Đang gọi increment_brand_search_count cho '{brand_name_from_ai}'...")

            list_of_brand_details = await scraper.increment_brand_search_count(session=session,
                                                                               brand_name=brand_name_from_ai)

            if list_of_brand_details:
                ai_logger.info(
                    f"✅ AI Handler: Cập nhật va_count và lấy thông tin thành công cho '{brand_name_from_ai}'. Số lượng bản ghi: {len(list_of_brand_details)}")
                return {
                    "status": "success",
                    "message": f"va_count updated and data retrieved for '{brand_name_from_ai}'.",
                    "updated_count_for": brand_name_from_ai,
                    "data": list_of_brand_details
                }
            else:
                ai_logger.warning(
                    f"⚠️ AI Handler: Không tìm thấy nhãn hiệu '{brand_name_from_ai}' để cập nhật va_count hoặc có lỗi xảy ra trong service.")
                return {
                    "status": "not_found",
                    "message": f"No brand found with name '{brand_name_from_ai}' or internal error during update/fetch.",
                    "data": []
                }

    except Exception as e:
        ai_logger.error(f"❌ AI Handler: Lỗi nghiêm trọng khi xử lý cho '{brand_name_from_ai}': {e}", exc_info=True)
        return {"status": "error", "message": f"Internal server error: {str(e)}", "data": []}
    finally:
        if ai_handler_engine:
            ai_handler_engine.dispose()
            ai_logger.debug("🤖 AI Handler: Database engine disposed.")



async def example_ai_trigger():
    brand_to_search = "FreeClip"
    result = await handle_ai_brand_search_and_update_count(brand_to_search)
    ai_logger.info(f"Kết quả từ AI Handler cho '{brand_to_search}':")
    if result.get('data'):
        ai_logger.info(f"  Data (số lượng: {len(result.get('data'))}):")
        for idx, brand_item in enumerate(result.get('data')):
            ai_logger.info(f"    Item {idx + 1}: {brand_item}")
    else:
        ai_logger.info(f"  Data: []")


if __name__ == "__main__":
    try:
        asyncio.run(example_ai_trigger())
    except KeyboardInterrupt:
        logging.info("Tool bị dừng bởi người dùng (Ctrl+C).")
    except Exception as e:
        logging.critical(f"Lỗi nghiêm trọng không bắt được ở phạm vi cao nhất (main_async_runner): {e}", exc_info=True)
    finally:
        logging.info("Chương trình kết thúc.")