import sqlite3
import os
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from datetime import datetime
from typing import List, Dict, Any

# --- Cấu hình ---
current_file_dir = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(current_file_dir))
STATE_DB_PATH = os.path.join(PROJECT_ROOT, "scraper_state.sqlite3")

# Khởi tạo FastAPI
app = FastAPI(
    title="Scraper Dashboard API",
    description="API để theo dõi trạng thái của tool scraper.",
    version="1.0.0"
)

# Cấu hình để FastAPI có thể tìm thấy file HTML trong thư mục 'templates'
templates = Jinja2Templates(directory="src/templates")


def get_db_connection():
    """Tạo kết nối mới đến DB cho mỗi request để đảm bảo an toàn."""
    try:
        conn = sqlite3.connect(f"file:{STATE_DB_PATH}?mode=ro", uri=True, timeout=5.0)
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.OperationalError:
        print(f"Lỗi: không thể mở database {STATE_DB_PATH}. File có thể chưa được tạo bởi tool chính.")
        return None


@app.get("/", response_class=HTMLResponse, summary="Trang Dashboard chính")
async def read_root(request: Request):
    """
    Phục vụ file dashboard.html làm giao diện người dùng.
    """
    return templates.TemplateResponse("dashboard.html", {"request": request})


@app.get("/api/status", summary="Lấy trạng thái scraper hiện tại")
async def api_status() -> Dict[str, Any]:
    """
    Đây là API cung cấp dữ liệu trạng thái dưới dạng JSON.
    """
    conn = get_db_connection()
    if not conn:
        return {
            "error": "Không thể kết nối đến database trạng thái. Hãy chắc chắn rằng tool scraper đang chạy hoặc đã chạy ít nhất một lần."
        }

    # --- Phần logic đọc database này giữ nguyên y hệt phiên bản Flask ---

    # 1. Lấy ngày cuối cùng đã hoàn thành
    control_cursor = conn.cursor()
    control_cursor.execute("SELECT value FROM control_state WHERE key = 'last_fully_completed_day'")
    control_row = control_cursor.fetchone()
    last_completed_day = control_row['value'] if control_row else "Chưa có ngày nào hoàn thành"

    # 2. Lấy các ngày đang chạy dở
    page_cursor = conn.cursor()
    page_cursor.execute(
        "SELECT date_range_key, last_completed_page, updated_at FROM page_state ORDER BY updated_at DESC")
    in_progress_tasks = []
    for row in page_cursor.fetchall():
        day_str = row['date_range_key'].split('_')[1]
        in_progress_tasks.append({
            "day": day_str,
            "last_page": row['last_completed_page'],
            "last_update": row['updated_at']
        })

    conn.close()

    # 3. Đóng gói dữ liệu và trả về (FastAPI tự động chuyển dict thành JSON)
    return {
        "last_completed_day": last_completed_day,
        "in_progress_tasks": in_progress_tasks,
        "dashboard_updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }