import requests
import traceback
from src.tools.config import settings  # Import settings từ trong cùng package


class TelegramNotifier:
    BASE_URL = f"https://api.telegram.org/bot{settings.BOT_TOKEN_BOT}/sendMessage"
    CHAT_ID = settings.CHAT_ID_BOT

    @staticmethod
    def send_message(text: str, is_error: bool = False):
        if not settings.BOT_TOKEN_BOT or not settings.CHAT_ID_BOT:
            return

        # Rút gọn tin nhắn nếu quá dài
        if len(text) > 4000:
            text = text[:4000] + "\n... (nội dung đã được rút gọn)"

        payload = {
            'chat_id': TelegramNotifier.CHAT_ID,
            'text': text,
            'parse_mode': 'HTML'
        }

        try:
            requests.post(TelegramNotifier.BASE_URL, data=payload, timeout=10)
        except Exception as e:
            print(f"Lỗi nghiêm trọng: Không thể gửi thông báo tới Telegram: {e}")

    @staticmethod
    def format_error_message(title: str, error_info=None) -> str:
        message = f"🔥🔥🔥 <b>{title}</b> 🔥🔥🔥"
        if error_info:
            # Nếu error_info là một exception, lấy traceback
            if isinstance(error_info, Exception):
                tb_lines = traceback.format_exception(type(error_info), error_info, error_info.__traceback__)
                tb_text = "".join(tb_lines)
                message += f"\n\n<pre>{tb_text}</pre>"
            # Nếu là chuỗi, chỉ cần nối vào
            else:
                message += f"\n\n<pre>{str(error_info)}</pre>"
        return message