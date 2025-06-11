import sqlite3
import os
import logging
from datetime import date, datetime
from typing import Dict, Optional,List

_connection = None

def get_db_path(project_root: str) -> str:
    return os.path.join(project_root, "scraper_state.sqlite3")

def get_connection(db_path: str):
    global _connection
    if _connection is None:
        try:
            _connection = sqlite3.connect(db_path, timeout=30.0)
            logging.info(f"Đã kết nối đến state database: {db_path}")
        except sqlite3.Error as e:
            logging.critical(f"Không thể kết nối đến state database {db_path}: {e}", exc_info=True)
            raise
    return _connection

def init_db(db_path: str):
    try:
        conn = get_connection(db_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS page_state (
                date_range_key TEXT PRIMARY KEY,
                last_completed_page INTEGER NOT NULL,
                updated_at TEXT NOT NULL
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS control_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        ''')
        conn.commit()
        logging.info("State database đã được kiểm tra và khởi tạo (nếu cần).")
    except sqlite3.Error as e:
        logging.error(f"Lỗi khi khởi tạo state database: {e}", exc_info=True)
        conn.rollback()

def save_page_state(db_path: str, date_range_key: str, last_completed_page: int):
    try:
        conn = get_connection(db_path)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO page_state (date_range_key, last_completed_page, updated_at)
            VALUES (?, ?, ?)
        ''', (date_range_key, last_completed_page, datetime.now().isoformat()))
        conn.commit()
        logging.info(f"[SQLite] Đã lưu trạng thái: Key '{date_range_key}', Page: {last_completed_page}")
    except sqlite3.Error as e:
        logging.error(f"[SQLite] Lỗi khi lưu page_state cho key '{date_range_key}': {e}", exc_info=True)

def load_scrape_state(db_path: str, date_range_key: str, default_page: int = 1) -> int:
    try:
        conn = get_connection(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT last_completed_page FROM page_state WHERE date_range_key = ?", (date_range_key,))
        result = cursor.fetchone()
        if result:
            last_completed_page = result[0]
            start_page = last_completed_page + 1
            logging.info(f"[SQLite] Tìm thấy trạng thái cho key '{date_range_key}'. Bắt đầu từ trang: {start_page}")
            return start_page
        else:
            logging.info(f"[SQLite] Không có trạng thái cho key '{date_range_key}'. Bắt đầu từ trang mặc định {default_page}.")
            return default_page
    except sqlite3.Error as e:
        logging.error(f"[SQLite] Lỗi khi tải scrape_state cho key '{date_range_key}': {e}. Trả về trang mặc định.", exc_info=True)
        return default_page

def clear_page_state_for_day(db_path: str, day_to_clear: date):
    day_key_to_clear = f"brands_{day_to_clear.strftime('%Y-%m-%d')}_{day_to_clear.strftime('%Y-%m-%d')}"
    try:
        conn = get_connection(db_path)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM page_state WHERE date_range_key = ?", (day_key_to_clear,))
        conn.commit()
        if cursor.rowcount > 0:
            logging.info(f"[SQLite] Đã xóa trạng thái trang cho ngày: {day_to_clear.strftime('%Y-%m-%d')}")
    except sqlite3.Error as e:
        logging.error(f"[SQLite] Lỗi khi xóa page_state cho ngày {day_to_clear.strftime('%Y-%m-%d')}: {e}", exc_info=True)

def save_control_state(db_path: str, last_fully_completed_day: date):
    key = "last_fully_completed_day"
    value = last_fully_completed_day.strftime("%Y-%m-%d")
    try:
        conn = get_connection(db_path)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO control_state (key, value, updated_at) VALUES (?, ?, ?)
        ''', (key, value, datetime.now().isoformat()))
        conn.commit()
        logging.info(f"[SQLite] Đã lưu control_state: {key} = {value}")
    except sqlite3.Error as e:
        logging.error(f"[SQLite] Lỗi khi lưu control_state '{key}': {e}", exc_info=True)

def load_control_state(db_path: str) -> Dict[str, str]:
    key = "last_fully_completed_day"
    try:
        conn = get_connection(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT value FROM control_state WHERE key = ?", (key,))
        result = cursor.fetchone()
        if result:
            try:
                datetime.strptime(result[0], "%Y-%m-%d")
                return {key: result[0]}
            except (ValueError, TypeError):
                logging.warning(f"[SQLite] Định dạng ngày trong control_state không hợp lệ: '{result[0]}'. Bỏ qua.")
                return {key: None}
        else:
            return {key: None}
    except sqlite3.Error as e:
        logging.error(f"[SQLite] Lỗi khi tải control_state '{key}': {e}. Trả về trạng thái mặc định.", exc_info=True)
        return {key: None}


def get_all_in_progress_days(db_path: str) -> List[date]:
    dates = []
    try:
        conn = get_connection(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT date_range_key FROM page_state")
        results = cursor.fetchall()
        for row in results:
            try:
                day_str = row[0].split('_')[1]
                d = datetime.strptime(day_str, "%Y-%m-%d").date()
                dates.append(d)
            except (IndexError, ValueError):
                logging.warning(f"[SQLite] Bỏ qua key có định dạng không hợp lệ: {row[0]}")
                continue

        dates.sort()  # Sắp xếp các ngày theo thứ tự thời gian
        if dates:
            logging.info(f"[SQLite] Tìm thấy các ngày đang xử lý dở dang: {[d.isoformat() for d in dates]}")
        return dates
    except sqlite3.Error as e:
        logging.error(f"[SQLite] Lỗi khi lấy danh sách các ngày đang xử lý dở: {e}", exc_info=True)
        return []
