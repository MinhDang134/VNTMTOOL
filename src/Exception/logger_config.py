import os
import logging
from logging.handlers import RotatingFileHandler

def setup_logging(log_dir: str):
    main_log_path = os.path.join(log_dir, "scraper_activity.log")
    main_file_handler = RotatingFileHandler(
        main_log_path, maxBytes=5*1024*1024, backupCount=5, encoding='utf-8'
    )
    main_formatter = logging.Formatter(
        '%(asctime)s - %(processName)s (%(process)d) - %(levelname)s - %(message)s'
    )
    main_file_handler.setFormatter(main_formatter)

    error_log_path = os.path.join(log_dir, "crawl_errors.log")
    error_file_handler = RotatingFileHandler(
        error_log_path, maxBytes=5*1024*1024, backupCount=5, encoding='utf-8'
    )
    error_file_handler.setLevel(logging.ERROR)
    error_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    error_file_handler.setFormatter(error_formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(main_formatter)

    root_logger = logging.getLogger()

    # Xóa các handler cũ nếu có để tránh log bị trùng lặp khi gọi lại hàm
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(main_file_handler)
    root_logger.addHandler(error_file_handler)
    root_logger.addHandler(stream_handler)

    error_logger = logging.getLogger('CrawlErrorLogger')
    error_logger.setLevel(logging.ERROR)

    logging.info("Hệ thống logging đã được thiết lập với file xoay vòng.")