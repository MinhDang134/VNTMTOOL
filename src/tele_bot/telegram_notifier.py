import asyncio
import httpx
import traceback
from src.tools.config import settings


class TelegramNotifier:
    BOT_TOKEN = settings.BOT_TOKEN_BOT
    CHAT_ID = settings.CHAT_ID_BOT
    BASE_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage" # địa chỉ url gốc của telegram bot

    @staticmethod
    async def _send_async(text: str, use_proxy: bool): # một là phải gửi text , hai là đúng sai use_proxy = true
        if not TelegramNotifier.BOT_TOKEN or not TelegramNotifier.CHAT_ID: # trường hợp mà cái token và chat id của anh chưa có thì
            print("Thiếu BOT_TOKEN hoặc CHAT_ID để gửi thông báo Telegram.") # in ra cái log bên dưới
            return

        payload = { # pay load mọt cái from in ra thông tin
            'chat_id': TelegramNotifier.CHAT_ID,
            'text': text,
            'parse_mode': 'HTML'
        }


        proxies = None #proxies này bằng none nếu mà
        if use_proxy: #
            # Đảm bảo biến PROXY_URL_BOT tồn tại và có giá trị
            if hasattr(settings, 'PROXY_URL_BOT') and settings.PROXY_URL_BOT:
                proxies = settings.PROXY_URL_BOT
                print(f"Đang gửi thông báo Telegram qua proxy: {proxies}")
            else:
                print("Cảnh báo: Yêu cầu sử dụng proxy nhưng PROXY_URL_BOT không được cấu hình.")
        else:
            print("Đang gửi thông báo Telegram không qua proxy.") # lấy đước cái link proxies socks 5

        try: # nó sẽ lấy cái giá trị của proxies = proxies và cho thành client
            async with httpx.AsyncClient(proxies=proxies) as client: # dùng cái ht
                await client.post(TelegramNotifier.BASE_URL, data=payload, timeout=20.0)
        except Exception as e:
            print(f"Lỗi nghiêm trọng khi gửi thông báo Telegram (proxy: {use_proxy}): {e}")
            traceback.print_exc()

    @staticmethod
    def send_message(text: str, use_proxy: bool, is_error: bool = False):
        # sau khi nhận được tin nhắn từ bên kia
        if len(text) > 4000: # nếu mà cái độ dài của cái text mà lơn hơn 4000  thì nội dungtext được dút goijn
            text = text[:4000] + "\n... (nội dung đã được rút gọn)"
        try: # trường hợp mà nó không lơn hơn 4000 thì sẽ gửi tin nhắn qua hàm sen_async, use proxy và text
            asyncio.run(TelegramNotifier._send_async(text, use_proxy=use_proxy))
        except RuntimeError as e:
            if "cannot run loop while another loop is running" in str(e):
                loop = asyncio.get_event_loop()
                loop.create_task(TelegramNotifier._send_async(text, use_proxy=use_proxy))
                # đâu nó đã gửi xong
            else:
                print(f"Lỗi Runtime khi gửi Telegram: {e}")
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