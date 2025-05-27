# src/tools/state_manager.py
import json
import os
import logging
from datetime import datetime, date


# --- Các hàm load_scrape_state và save_scrape_state dựa trên snippet bạn cung cấp ---
# Các hàm này quản lý việc lưu trang cuối cùng hoàn thành cho một "date_range_key" cụ thể
# trong một file JSON có cấu trúc lồng nhau.

def load_scrape_state(state_file_path: str, date_range_key: str,
                      default_page: int = 1) -> int:  #  (thêm default_page vào signature cho nhất quán)
    """
    Tải trang bắt đầu để cào cho một date_range_key cụ thể.
    Trả về trang tiếp theo cần cào (last_completed_page + 1).
    """
    try:
        if not os.path.exists(state_file_path):
            logging.info(
                f"File trạng thái '{state_file_path}' không tồn tại. Bắt đầu từ trang {default_page} cho khóa '{date_range_key}'.")
            return default_page

        with open(state_file_path, 'r', encoding='utf-8') as f:
            states = json.load(f)

        # Lấy last_completed_page từ cấu trúc lồng nhau, ví dụ: states["some_key"]["last_completed_page"]
        last_completed_page = states.get(date_range_key, {}).get("last_completed_page", 0)

        start_page = last_completed_page + 1
        if start_page > default_page:  #  (So sánh với default_page)
            logging.info(
                f"Tìm thấy trạng thái cho khóa '{date_range_key}'. Trang cuối hoàn thành: {last_completed_page}. Bắt đầu từ trang: {start_page}.")
        else:
            logging.info(f"Không có trạng thái trước đó cho khóa '{date_range_key}'. Bắt đầu từ trang {default_page}.")
        return start_page
    except (IOError, json.JSONDecodeError,
            TypeError) as e:  #  (Thêm TypeError phòng trường hợp states không phải dict)
        logging.warning(
            f"Lỗi khi tải trạng thái từ '{state_file_path}' cho khóa '{date_range_key}': {e}. Bắt đầu từ trang {default_page}.")
        return default_page


def save_scrape_state(state_file_path: str, date_range_key: str, last_completed_page: int):
    """
    Lưu trang cuối cùng đã hoàn thành cho một date_range_key.
    Sử dụng cấu trúc JSON lồng nhau.
    """
    try:
        states = {}
        if os.path.exists(state_file_path):
            # Đọc file hiện tại một cách an toàn
            try:
                with open(state_file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    if content.strip():  # Chỉ load nếu file không rỗng
                        states = json.loads(content)
                    if not isinstance(states, dict):  # Đảm bảo states là một dict
                        logging.warning(
                            f"File trạng thái '{state_file_path}' không chứa một đối tượng JSON hợp lệ. Sẽ tạo mới.")
                        states = {}
            except json.JSONDecodeError:
                logging.warning(f"File trạng thái '{state_file_path}' bị lỗi JSON hoặc trống. Sẽ tạo/ghi đè.")
                states = {}  # Reset states nếu file lỗi
            except IOError as e_read:
                logging.error(f"Không thể đọc file trạng thái '{state_file_path}': {e_read}. Sẽ cố gắng ghi đè.")
                states = {}

        # Tạo hoặc cập nhật mục cho date_range_key
        if date_range_key not in states or not isinstance(states.get(date_range_key),dict):  # (đảm bảo states[date_range_key] là dict)
            states[date_range_key] = {}
        states[date_range_key]["last_completed_page"] = last_completed_page

        with open(state_file_path, 'w', encoding='utf-8') as f:
            json.dump(states, f, indent=4, ensure_ascii=False)
        logging.info(
            f"Đã lưu trạng thái vào '{state_file_path}' cho khóa '{date_range_key}': trang cuối hoàn thành = {last_completed_page}.")
    except IOError as e_write:
        logging.error(f"Không thể lưu trạng thái vào '{state_file_path}' cho khóa '{date_range_key}': {e_write}",
                      exc_info=True)


# --- Các hàm mới/đã điều chỉnh để quản lý trạng thái điều khiển và xóa trạng thái ngày ---
# Các hàm này rất cần thiết cho logic mới trong run_scraper.py

CONTROL_STATE_FILENAME = "scraper_control_state.json"


def get_control_state_path(project_root: str) -> str:   
    """Trả về đường dẫn đầy đủ đến file trạng thái điều khiển."""
    return os.path.join(project_root, CONTROL_STATE_FILENAME)


def load_control_state(control_state_file_path: str) -> dict:   
    """Tải trạng thái điều khiển (ví dụ: ngày cuối cùng đã xử lý xong)."""
    default_state = {"last_fully_completed_day": None}
    if not os.path.exists(control_state_file_path):
        logging.info(
            f"File trạng thái điều khiển '{control_state_file_path}' không tồn tại. Trả về trạng thái mặc định.")
        return default_state
    try:   
        with open(control_state_file_path, 'r', encoding='utf-8') as f:
            state = json.load(f)
            # Xác thực định dạng ngày nếu có
            if state.get("last_fully_completed_day"):
                try:   
                    datetime.strptime(state["last_fully_completed_day"], "%Y-%m-%d")
                except ValueError:
                    logging.warning(
                        f"Giá trị 'last_fully_completed_day' ('{state['last_fully_completed_day']}') " 
                        f"trong {control_state_file_path} không phải là định dạng ngày YYYY-MM-DD hợp lệ. Sẽ bỏ qua giá trị này.")
                    state["last_fully_completed_day"] = None
            if "last_fully_completed_day" not in state:  # Đảm bảo key luôn tồn tại
                state["last_fully_completed_day"] = None
            return state
    except json.JSONDecodeError:
        logging.error(
            f"Lỗi giải mã JSON từ file trạng thái điều khiển: {control_state_file_path}. Trả về trạng thái mặc định.")
        return default_state
    except Exception as e:
        logging.error(
            f"Lỗi không xác định khi tải trạng thái điều khiển từ {control_state_file_path}: {e}. Trả về trạng thái mặc định.",
            exc_info=True)
        return default_state


def save_control_state(control_state_file_path: str, last_fully_completed_day: date):
    """Lưu ngày cuối cùng đã được xử lý hoàn chỉnh vào file trạng thái điều khiển."""
    state_data = {"last_fully_completed_day": last_fully_completed_day.strftime("%Y-%m-%d")}
    try:   
        with open(control_state_file_path, 'w', encoding='utf-8') as f:
            json.dump(state_data, f, indent=4, ensure_ascii=False)
        logging.info(
            f"Đã lưu trạng thái điều khiển: last_fully_completed_day = {state_data['last_fully_completed_day']} vào {control_state_file_path}")
    except Exception as e:
        logging.error(f"Lỗi khi lưu trạng thái điều khiển vào {control_state_file_path}: {e}",
                      exc_info=True)


def clear_page_state_for_day(page_state_file_path: str, day_to_clear: date):
    """
    Xóa trạng thái trang (page state) cho một ngày cụ thể khỏi file lưu trạng thái trang.
    Hàm này sẽ làm việc với cấu trúc JSON lồng nhau mà `save_scrape_state` của bạn tạo ra.
    """   
    day_key_to_clear = f"brands_{day_to_clear.strftime('%Y-%m-%d')}_{day_to_clear.strftime('%Y-%m-%d')}"

    states = {}
    if os.path.exists(page_state_file_path):
        try:   
            with open(page_state_file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                if content.strip():
                    states = json.loads(content)
                if not isinstance(states, dict):
                    logging.warning(
                        f"File trạng thái trang '{page_state_file_path}' không chứa một đối tượng JSON hợp lệ. Không thể xóa trạng thái.")   
                    return   
        except json.JSONDecodeError:   
            logging.warning(
                f"File trạng thái trang '{page_state_file_path}' bị lỗi JSON. Không thể xóa trạng thái.")   
            return   
        except IOError as e_read:   
            logging.error(
                f"Không thể đọc file trạng thái trang '{page_state_file_path}' để xóa: {e_read}")   
            return   

    if day_key_to_clear in states:   
        del states[day_key_to_clear]   
        try:   
            with open(page_state_file_path, 'w', encoding='utf-8') as f:   
                json.dump(states, f, indent=4, ensure_ascii=False)   
            logging.info(
                f"Đã xóa trạng thái trang cho ngày {day_to_clear.strftime('%Y-%m-%d')} (key: {day_key_to_clear}) từ {page_state_file_path}")   
        except IOError as e_write:   
            logging.error(
                f"Không thể ghi lại file trạng thái trang '{page_state_file_path}' sau khi xóa key '{day_key_to_clear}': {e_write}")   
    else:   
        logging.info(
            f"Không tìm thấy trạng thái trang để xóa cho ngày {day_to_clear.strftime('%Y-%m-%d')} (key: {day_key_to_clear}) trong {page_state_file_path}")   