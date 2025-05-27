# src/tools/state_manager.py
import json
import os
import logging



def load_scrape_state(state_file_path: str, date_range_key: str) -> int:
    try:
        if not os.path.exists(state_file_path):
            logging.info(
                f"File trạng thái '{state_file_path}' không tồn tại. Bắt đầu từ trang 1 cho khóa '{date_range_key}'.")
            return 1

        with open(state_file_path, 'r', encoding='utf-8') as f:
            states = json.load(f)

        last_completed_page = states.get(date_range_key, {}).get("last_completed_page", 0)

        start_page = last_completed_page + 1
        if start_page > 1:
            logging.info(
                f"Tìm thấy trạng thái cho khóa '{date_range_key}'. Trang cuối hoàn thành: {last_completed_page}. Bắt đầu từ trang: {start_page}.")
        else:
            logging.info(f"Không có trạng thái trước đó cho khóa '{date_range_key}'. Bắt đầu từ trang 1.")
        return start_page
    except (IOError, json.JSONDecodeError) as e:
        logging.warning(
            f"Lỗi khi tải trạng thái từ '{state_file_path}' cho khóa '{date_range_key}': {e}. Bắt đầu từ trang 1.")
        return 1


def save_scrape_state(state_file_path: str, date_range_key: str, last_completed_page: int):

    try:
        states = {}
        if os.path.exists(state_file_path):
            with open(state_file_path, 'r', encoding='utf-8') as f:
                try:
                    states = json.load(f)
                except json.JSONDecodeError:
                    logging.warning(f"File trạng thái '{state_file_path}' bị lỗi hoặc trống. Sẽ tạo/ghi đè.")

        if date_range_key not in states:
            states[date_range_key] = {}
        states[date_range_key]["last_completed_page"] = last_completed_page

        with open(state_file_path, 'w', encoding='utf-8') as f:
            json.dump(states, f, indent=4, ensure_ascii=False)
        logging.info(
            f"Đã lưu trạng thái vào '{state_file_path}' cho khóa '{date_range_key}': trang cuối hoàn thành = {last_completed_page}.")
    except IOError as e:
        logging.error(f"Không thể lưu trạng thái vào '{state_file_path}' cho khóa '{date_range_key}': {e}",
                     exc_info=True)