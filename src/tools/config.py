from pydantic_settings import BaseSettings
from typing import List
import os
from dotenv import load_dotenv

load_dotenv()

class Settings(BaseSettings):
    # Database settings
    DATABASE_URL: str = os.getenv("DATABASE_URL")
    DB_USER: str = os.getenv("DB_USER")
    DB_PASSWORD: str = os.getenv("DB_PASSWORD")
    DB_NAME: str = os.getenv("DB_NAME")
    
    # Proxy settings
    PROXY_IPS: List[str] = [
        os.getenv("IP_ONE"),
        os.getenv("IP_TWO"),
        os.getenv("IP_THREE")
    ]
    PROXY_PORTS: List[int] = [
        int(os.getenv("PORT_ONE")),
        int(os.getenv("PORT_TWO")),
        int(os.getenv("PORT_THREE"))
    ]
    PROXY_USERNAME: str = os.getenv("USER_NAME")
    PROXY_PASSWORD: str = os.getenv("USER_PASSWORD")
    
    # Scraping settings
    REQUEST_LIMIT: int = 10  # requests per minute
    REQUEST_DELAY: float = 0.1  # seconds between requests
    
    class Config:
        env_file = ".env"
        extra = "allow"

settings = Settings()

