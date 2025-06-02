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

    ai_logger.info(f"🤖 AI Handler: Nhận yêu cầu tìm và cập nhật count cho nhãn hiệu: '{brand_name_from_ai}'")

    if not brand_name_from_ai:
        ai_logger.warning("🤖 AI Handler: Tên nhãn hiệu rỗng, không xử lý.")
        return {"status": "error", "message": "Brand name is empty"}
    scraper = ScraperService(media_dir=MEDIA_PHYSICAL_DIR)# lấy vị trí scraperService


    ai_handler_engine = None
    try:
        ai_handler_engine = create_engine(settings.DATABASE_URL, pool_pre_ping=True,echo=False)
        with get_session(ai_handler_engine) as session:  # Lấy session từ engine này
            ai_logger.info(f"🤖 AI Handler: Đang gọi increment_brand_search_count cho '{brand_name_from_ai}'...")
            success = await scraper.increment_brand_search_count(session=session, brand_name=brand_name_from_ai)

            if success:
                ai_logger.info(f"✅ AI Handler: Cập nhật va_count thành công cho '{brand_name_from_ai}'.")
                return {"status": "success", "message": f"va_count updated for {brand_name_from_ai}.",
                        "updated_count_for": brand_name_from_ai}
            else:
                ai_logger.warning(
                    f"⚠️ AI Handler: Không thể cập nhật va_count cho '{brand_name_from_ai}' (có thể không tìm thấy hoặc lỗi).")
                return {"status": "not_found_or_error",
                        "message": f"Could not update va_count for {brand_name_from_ai}."}

    except Exception as e:
        ai_logger.error(f"❌ AI Handler: Lỗi nghiêm trọng khi xử lý cho '{brand_name_from_ai}': {e}", exc_info=True)
        return {"status": "error", "message": f"Internal server error: {str(e)}"}
    finally:
        if ai_handler_engine:
            ai_handler_engine.dispose()
            ai_logger.debug("🤖 AI Handler: Database engine disposed.")



async def example_ai_trigger():

    brand_to_search = "soft time"
    result = await handle_ai_brand_search_and_update_count(brand_to_search) # truyền ronaldo vào
    print(f"Kết quả từ AI Handler cho '{brand_to_search}': {result}")

    brand_to_search_2 = "soft me"
    result_2 = await handle_ai_brand_search_and_update_count(brand_to_search_2)
    print(f"Kết quả từ AI Handler cho '{brand_to_search_2}': {result_2}")

    brand_to_search_3 = "soft it"
    result_3 = await handle_ai_brand_search_and_update_count(brand_to_search_3)
    print(f"Kết quả từ AI Handler cho '{brand_to_search_3}': {result_3}")


if __name__ == "__main__":
    try:
        asyncio.run(example_ai_trigger())
    except KeyboardInterrupt:
        logging.info("Tool bị dừng bởi người dùng (Ctrl+C).")
    except Exception as e:
        logging.critical(f"Lỗi nghiêm trọng không bắt được ở phạm vi cao nhất (main_async_runner): {e}", exc_info=True)
    finally:
        logging.info("Chương trình kết thúc.")