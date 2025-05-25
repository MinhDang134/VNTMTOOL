# src/tools/config/settings.py

from pydantic_settings import BaseSettings

from typing import List, Optional

import os

from dotenv import load_dotenv



# Load biến môi trường từ file .env

load_dotenv()



class Settings(BaseSettings):

    # Database settings

    DATABASE_URL: str = os.getenv("DATABASE_URL", "postgresql://user:password@host:port/dbname") # Thêm default

    DB_USER: Optional[str] = os.getenv("DB_USER")

    DB_PASSWORD: Optional[str] = os.getenv("DB_PASSWORD")

    DB_NAME: Optional[str] = os.getenv("DB_NAME")



    # Proxy settings

    # Đảm bảo các biến môi trường IP_ONE, PORT_ONE,... tồn tại hoặc cung cấp giá trị mặc định nếu chúng có thể rỗng

    PROXY_IPS: List[str] = [

        ip for ip in [os.getenv("IP_ONE"), os.getenv("IP_TWO"), os.getenv("IP_THREE")] if ip is not None

    ]

    PROXY_PORTS: List[int] = [

        int(port) for port in [os.getenv("PORT_ONE"), os.getenv("PORT_TWO"), os.getenv("PORT_THREE")] if port is not None and port.isdigit()

    ]

    PROXY_USERNAME: Optional[str] = os.getenv("USER_NAME") # Giữ USER_NAME nếu bạn đã đặt trong .env

    PROXY_PASSWORD: Optional[str] = os.getenv("USER_PASSWORD") # Giữ USER_PASSWORD



    # Media URLs (Thêm mới)

    LOCAL_MEDIA_BASE_URL: str = os.getenv("LOCAL_MEDIA_BASE_URL", "http://localhost:8000/media")

    SOURCE_WEBSITE_DOMAIN: str = os.getenv("SOURCE_WEBSITE_DOMAIN", "https://vietnamtrademark.net")



    # Request params (Thêm mới và điều chỉnh)

    MAX_REQUEST_RETRIES: int = int(os.getenv("MAX_REQUEST_RETRIES", "3"))

    REQUEST_TIMEOUT: float = float(os.getenv("REQUEST_TIMEOUT", "60.0"))

    SSL_VERIFY_REQUEST: bool = os.getenv("SSL_VERIFY_REQUEST", "False").lower() == 'true'

    SSL_VERIFY_DOWNLOAD: bool = os.getenv("SSL_VERIFY_DOWNLOAD", "False").lower() == 'true'

    DOWNLOAD_TIMEOUT: float = float(os.getenv("DOWNLOAD_TIMEOUT", "30.0"))



    # Scraper delays and limits (Điều chỉnh từ REQUEST_LIMIT và REQUEST_DELAY của bạn)

    # REQUEST_LIMIT của bạn là 10 requests per minute, ta sẽ chuyển thành interval

    REQUEST_LIMIT_PER_INTERVAL: int = int(os.getenv("REQUEST_LIMIT", os.getenv("REQUEST_LIMIT_PER_INTERVAL", "20"))) # Sử dụng REQUEST_LIMIT của bạn nếu có

    REQUEST_INTERVAL_SECONDS: int = int(os.getenv("REQUEST_INTERVAL_SECONDS", "60")) # 60 giây cho REQUEST_LIMIT ở trên



    # REQUEST_DELAY của bạn là 0.1s, ta dùng nó làm MIN_DELAY, MAX_DELAY có thể lớn hơn một chút

    MIN_REQUEST_DELAY: float = float(os.getenv("REQUEST_DELAY", os.getenv("MIN_REQUEST_DELAY", "0.5"))) # Sử dụng REQUEST_DELAY của bạn làm min

    MAX_REQUEST_DELAY: float = float(os.getenv("MAX_REQUEST_DELAY", "1.5"))



    MIN_DELAY_CHECK_PENDING: float = float(os.getenv("MIN_DELAY_CHECK_PENDING", "1.5"))

    MAX_DELAY_CHECK_PENDING: float = float(os.getenv("MAX_DELAY_CHECK_PENDING", "3.5"))



    class Config:

        env_file = ".env"

        env_file_encoding = 'utf-8'

        # extra = "allow" # Bạn có thể giữ 'allow' hoặc đổi thành 'ignore'

        # 'ignore' sẽ an toàn hơn nếu bạn muốn Pydantic chỉ load các trường đã định nghĩa.

        extra = 'ignore'



settings = Settings()



# In ra để kiểm tra (chỉ khi debug, nên xóa sau đó)

# print("Loaded settings:")

# print(f"  DATABASE_URL: {settings.DATABASE_URL}")

# print(f"  PROXY_IPS: {settings.PROXY_IPS}")

# print(f"  PROXY_PORTS: {settings.PROXY_PORTS}")

# print(f"  LOCAL_MEDIA_BASE_URL: {settings.LOCAL_MEDIA_BASE_URL}")

# print(f"  MIN_REQUEST_DELAY: {settings.MIN_REQUEST_DELAY}")




# from pydantic_settings import BaseSettings
# from typing import List
# import os
# from dotenv import load_dotenv
#
# load_dotenv()
#
# class Settings(BaseSettings):
#     # Database settings
#     DATABASE_URL: str = os.getenv("DATABASE_URL")
#     DB_USER: str = os.getenv("DB_USER")
#     DB_PASSWORD: str = os.getenv("DB_PASSWORD")
#     DB_NAME: str = os.getenv("DB_NAME")
#
#     # Proxy settings
#     PROXY_IPS: List[str] = [
#         os.getenv("IP_ONE"),
#         os.getenv("IP_TWO"),
#         os.getenv("IP_THREE")
#     ]
#     PROXY_PORTS: List[int] = [
#         int(os.getenv("PORT_ONE")),
#         int(os.getenv("PORT_TWO")),
#         int(os.getenv("PORT_THREE"))
#     ]
#     PROXY_USERNAME: str = os.getenv("USER_NAME")
#     PROXY_PASSWORD: str = os.getenv("USER_PASSWORD")
#
#     # Scraping settings
#     REQUEST_LIMIT: int = 10  # requests per minute
#     REQUEST_DELAY: float = 0.1  # seconds between requests
#
#     class Config:
#         env_file = ".env"
#         extra = "allow"
#
# settings = Settings()
#
