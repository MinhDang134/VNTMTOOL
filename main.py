# # Ví dụ: main.py (nếu bạn dùng FastAPI)
# from fastapi import FastAPI
# from fastapi.staticfiles import StaticFiles
# import os
#
# app = FastAPI()
#
#
# PROJECT_ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
# MEDIA_FILES_DIR = os.path.join(PROJECT_ROOT_DIR, "media_root")
#
# if os.path.exists(MEDIA_FILES_DIR):
#     app.mount("/media", StaticFiles(directory=MEDIA_FILES_DIR), name="media")
#     print(f"Serving static files from '{MEDIA_FILES_DIR}' at '/media'")
# else:
#     print(f"Warning: Directory for media files not found: '{MEDIA_FILES_DIR}'")
#
#
# @app.get("/")
# async def read_root():
#     return {"message": "API is running. Images might be at /media/brand_images/your_image.jpg"}
#
# # Các API endpoints khác của bạn...