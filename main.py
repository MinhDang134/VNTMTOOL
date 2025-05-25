# Ví dụ: main.py (nếu bạn dùng FastAPI)
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
import os

app = FastAPI()

# Thư mục gốc của dự án (nơi chứa media_root)
# Cách xác định này phụ thuộc vào cấu trúc dự án của bạn
# Nếu main.py cùng cấp với media_root:
PROJECT_ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
MEDIA_FILES_DIR = os.path.join(PROJECT_ROOT_DIR, "media_root")

# Mount thư mục "media_root" (chứa "brand_images") để phục vụ tại URL "/media"
# Ví dụ: file tại "media_root/brand_images/abc.jpg"
# sẽ có thể truy cập qua "http://localhost:8000/media/brand_images/abc.jpg"
if os.path.exists(MEDIA_FILES_DIR):
    app.mount("/media", StaticFiles(directory=MEDIA_FILES_DIR), name="media")
    print(f"Serving static files from '{MEDIA_FILES_DIR}' at '/media'")
else:
    print(f"Warning: Directory for media files not found: '{MEDIA_FILES_DIR}'")


@app.get("/")
async def read_root():
    return {"message": "API is running. Images might be at /media/brand_images/your_image.jpg"}

# Các API endpoints khác của bạn...