import asyncio
import httpx  # Dùng httpx thay cho requests
import traceback
from src.tools.config import settings


class TelegramNotifier:
    BOT_TOKEN = settings.BOT_TOKEN_BOT
    CHAT_ID = settings.CHAT_ID_BOT
    BASE_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"


    @staticmethod
    async def _send_async(text: str):
        if not TelegramNotifier.BOT_TOKEN or not TelegramNotifier.CHAT_ID:
            return

        payload = {
            'chat_id': TelegramNotifier.CHAT_ID,
            'text': text,
            'parse_mode': 'HTML'
        }

        proxies = settings.PROXY_URL_BOT if hasattr(settings, 'PROXY_URL') and settings.PROXY_URL else None

        try:
            async with httpx.AsyncClient(proxies=proxies) as client:
                await client.post(TelegramNotifier.BASE_URL, data=payload, timeout=20.0)
        except Exception as e:
            print(f"Lỗi nghiêm trọng khi gửi thông báo Telegram bất đồng bộ: {e}")

    @staticmethod
    def send_message(text: str, is_error: bool = False):
        if len(text) > 4000:
            text = text[:4000] + "\n... (nội dung đã được rút gọn)"
        try:
            asyncio.run(TelegramNotifier._send_async(text))
        except Exception as e:
            print(f"Lỗi khi khởi chạy tác vụ gửi Telegram: {e}")

    @staticmethod
    def format_error_message(title: str, error_info=None) -> str:
        message = f" <b>{title}</b> "
        if error_info:
            if isinstance(error_info, Exception):
                tb_lines = traceback.format_exception(type(error_info), error_info, error_info.__traceback__)
                tb_text = "".join(tb_lines)
                message += f"\n\n<pre>{tb_text}</pre>"
            else:
                message += f"\n\n<pre>{str(error_info)}</pre>"
        return message