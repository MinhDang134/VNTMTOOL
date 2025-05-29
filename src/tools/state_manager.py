import json
import os
import logging
from datetime import datetime, date
from multiprocessing import Lock    


def load_scrape_state(state_file_path: str, date_range_key: str,
                      default_page: int = 1) -> int:
    try:
        if not os.path.exists(state_file_path):
            logging.info(
                f"File trạng thái '{state_file_path}' không tồn tại. Bắt đầu từ trang {default_page} cho khóa '{date_range_key}'.")
            return default_page

        with open(state_file_path, 'r', encoding='utf-8') as f:
            states = json.load(f)

        last_completed_page = states.get(date_range_key, {}).get("last_completed_page", 0)

        start_page = last_completed_page + 1
        if start_page > default_page:
            logging.info(
                f"Tìm thấy trạng thái cho khóa '{date_range_key}'. Trang cuối hoàn thành: {last_completed_page}. Bắt đầu từ trang: {start_page}.")
        else:
            logging.info(f"Không có trạng thái trước đó cho khóa '{date_range_key}'. Bắt đầu từ trang {default_page}.")
        return start_page
    except (IOError, json.JSONDecodeError,
            TypeError) as e:
        logging.warning(
            f"Lỗi khi tải trạng thái từ '{state_file_path}' cho khóa '{date_range_key}': {e}. Bắt đầu từ trang {default_page}.")
        return default_page

def save_scrape_state(state_file_path: str, date_range_key: str, last_completed_page: int, lock: Lock):    
    with lock:    
        try:
            states = {}
            if os.path.exists(state_file_path):
                try:
                    with open(state_file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                        if content.strip():
                            states = json.loads(content)
                        if not isinstance(states, dict):
                            logging.warning(
                                f"File trạng thái '{state_file_path}' không chứa một đối tượng JSON hợp lệ. Sẽ tạo mới.")
                            states = {}
                except json.JSONDecodeError:
                    logging.warning(f"File trạng thái '{state_file_path}' bị lỗi JSON hoặc trống. Sẽ tạo/ghi đè.")
                    states = {}
                except IOError as e_read:
                    logging.error(f"Không thể đọc file trạng thái '{state_file_path}': {e_read}. Sẽ cố gắng ghi đè.")
                    states = {}

            if date_range_key not in states or not isinstance(states.get(date_range_key),dict):    
                states[date_range_key] = {}
            states[date_range_key]["last_completed_page"] = last_completed_page

            with open(state_file_path, 'w', encoding='utf-8') as f:
                json.dump(states, f, indent=4, ensure_ascii=False)
            logging.info(
                f"Đã lưu trạng thái vào '{state_file_path}' cho khóa '{date_range_key}': trang cuối hoàn thành = {last_completed_page}.")
        except IOError as e_write:
            logging.error(f"Không thể lưu trạng thái vào '{state_file_path}' cho khóa '{date_range_key}': {e_write}",
                          exc_info=True)

CONTROL_STATE_FILENAME = "scraper_control_state.json"

def get_control_state_path(project_root: str) -> str:
    return os.path.join(project_root, CONTROL_STATE_FILENAME)

def load_control_state(control_state_file_path: str) -> dict:
    default_state = {"last_fully_completed_day": None}
    if not os.path.exists(control_state_file_path):
        logging.info(
            f"File trạng thái điều khiển '{control_state_file_path}' không tồn tại. Trả về trạng thái mặc định.")
        return default_state
    try:
        with open(control_state_file_path, 'r', encoding='utf-8') as f:
            state = json.load(f)
            if state.get("last_fully_completed_day"):
                try:
                    datetime.strptime(state["last_fully_completed_day"], "%Y-%m-%d")
                except ValueError:
                    logging.warning(
                        f"Giá trị 'last_fully_completed_day' ('{state['last_fully_completed_day']}') "
                        f"trong {control_state_file_path} không phải là định dạng ngày YAML-MM-DD hợp lệ. Sẽ bỏ qua giá trị này.")
                    state["last_fully_completed_day"] = None
            if "last_fully_completed_day" not in state:
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
    state_data = {"last_fully_completed_day": last_fully_completed_day.strftime("%Y-%m-%d")}
    try:
        with open(control_state_file_path, 'w', encoding='utf-8') as f:
            json.dump(state_data, f, indent=4, ensure_ascii=False)
        logging.info(
            f"Đã lưu trạng thái điều khiển: last_fully_completed_day = {state_data['last_fully_completed_day']} vào {control_state_file_path}")
    except Exception as e:
        logging.error(f"Lỗi khi lưu trạng thái điều khiển vào {control_state_file_path}: {e}",
                      exc_info=True)

def clear_page_state_for_day(page_state_file_path: str, day_to_clear: date, lock: Lock):    
    day_key_to_clear = f"brands_{day_to_clear.strftime('%Y-%m-%d')}_{day_to_clear.strftime('%Y-%m-%d')}"
    with lock:    
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