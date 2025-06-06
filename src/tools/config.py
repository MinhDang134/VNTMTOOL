from pydantic_settings import BaseSettings
from typing import List, Optional
import os
from dotenv import load_dotenv
load_dotenv()
class Settings(BaseSettings):


    DATABASE_URL: str = os.getenv("DATABASE_URL", "postgresql://user:password@host:port/dbname")
    DB_USER: Optional[str] = os.getenv("DB_USER")
    DB_PASSWORD: Optional[str] = os.getenv("DB_PASSWORD")
    DB_NAME: Optional[str] = os.getenv("DB_NAME")

    INITIAL_SCRAPE_START_YEAR: int = int(os.getenv("INITIAL_SCRAPE_START_YEAR"))
    INITIAL_SCRAPE_START_MOTH: int = int(os.getenv("INITIAL_SCRAPE_START_MOTH"))
    INITIAL_SCRAPE_START_DAY: int = int(os.getenv("INITIAL_SCRAPE_START_DAY"))

    OVERALL_SCRAPE_END_YEAR: Optional[int] = os.getenv("OVERALL_SCRAPE_END_YEAR")
    OVERALL_SCRAPE_END_MOTH: Optional[int] = os.getenv("OVERALL_SCRAPE_END_MOTH")
    OVERALL_SCRAPE_END_DAY: Optional[int] = os.getenv("OVERALL_SCRAPE_END_DAY")

    PROXY_IPS: List[str] = [
        ip for ip in [os.getenv("IP_ONE"), os.getenv("IP_TWO"), os.getenv("IP_THREE")] if ip is not None
    ]
    PROXY_PORTS: List[int] = [
        int(port) for port in [os.getenv("PORT_ONE"), os.getenv("PORT_TWO"), os.getenv("PORT_THREE")] if port is not None and port.isdigit()
    ]
    PROXY_USERNAME: Optional[str] = os.getenv("USER_NAME")
    PROXY_PASSWORD: Optional[str] = os.getenv("USER_PASSWORD")
    LOCAL_MEDIA_BASE_URL: str = os.getenv("LOCAL_MEDIA_BASE_URL")
    SOURCE_WEBSITE_DOMAIN: str = os.getenv("SOURCE_WEBSITE_DOMAIN")
    MAX_REQUEST_RETRIES: int = int(os.getenv("MAX_REQUEST_RETRIES"))
    REQUEST_TIMEOUT: float = float(os.getenv("REQUEST_TIMEOUT"))
    SSL_VERIFY_REQUEST: bool = os.getenv("SSL_VERIFY_REQUEST").lower() == 'true'
    SSL_VERIFY_DOWNLOAD: bool = os.getenv("SSL_VERIFY_DOWNLOAD").lower() == 'true'
    DOWNLOAD_TIMEOUT: float = float(os.getenv("DOWNLOAD_TIMEOUT"))
    REQUEST_LIMIT_PER_INTERVAL: int = int(os.getenv("REQUEST_LIMIT", os.getenv("REQUEST_LIMIT_PER_INTERVAL")))
    REQUEST_INTERVAL_SECONDS: int = int(os.getenv("REQUEST_INTERVAL_SECONDS"))
    MIN_REQUEST_DELAY: float = float(os.getenv("REQUEST_DELAY", os.getenv("MIN_REQUEST_DELAY")))
    MAX_REQUEST_DELAY: float = float(os.getenv("MAX_REQUEST_DELAY"))
    MIN_DELAY_CHECK_PENDING: float = float(os.getenv("MIN_DELAY_CHECK_PENDING"))
    MAX_DELAY_CHECK_PENDING: float = float(os.getenv("MAX_DELAY_CHECK_PENDING"))
    MEDIA_BRAND_IMAGES_SUBPATH : Optional[str] = os.getenv("MEDIA_BRAND_IMAGES_SUBPATH")
##
    RUN_DURATION_MINUTES: int = int(os.getenv("RUN_DURATION_MINUTES"))
    PAUSE_DURATION_MINUTES: int = int(os.getenv("PAUSE_DURATION_MINUTES"))

    CONCURRENT_SCRAPING_TASKS: int = int(os.getenv("CONCURRENT_SCRAPING_TASKS"))


    PROXY_LOGIN_BOT : str = os.getenv("PROXY_LOGIN")
    PROXY_PASSWORD_BOT: str = os.getenv("PROXY_PASSWORD")
    BOT_TOKEN_BOT: str = os.getenv("BOT_TOKEN")
    CHAT_ID_BOT: str = os.getenv("CHAT_ID")
    PROXY_IP_HTTP_BOT: str = os.getenv("PROXY_IP_HTTP")
    PROXY_PORT_HTTP_BOT: int  = int (os.getenv("PROXY_PORT_HTTP"))
    PROXY_URL_BOT: str = os.getenv("PROXY_URL")






    class Config:
        env_file = ".env"
        env_file_encoding = 'utf-8'
        extra = 'ignore'


settings = Settings()

