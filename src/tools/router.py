import uvicorn
from fastapi import FastAPI, HTTPException
import subprocess
import sys
import os
from pathlib import Path
from pydantic import BaseModel
from typing import Optional, Dict, Any, List
from src.tools.state_manager import (load_control_state,get_all_in_progress_days,get_db_path)
PROJECT_ROOT = Path(os.path.abspath(__file__)).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

app = FastAPI(
    title="Scraper Control API",
    description="API để bắt đầu, dừng và theo dõi tiến trình cào dữ liệu."
)


scraper_process: Optional[subprocess.Popen] = None


class StatusResponse(BaseModel):
    status: str
    pid: Optional[int] = None
    control_state: Optional[Dict[str, Any]] = None
    in_progress_days: Optional[List[str]] = None
    message: Optional[str] = None
    error_details: Optional[str] = None



@app.post("/run", status_code=202, response_model=StatusResponse)
async def start_scraping():
    global scraper_process
    if scraper_process and scraper_process.poll() is None:
        raise HTTPException(status_code=400, detail="Tiến trình cào dữ liệu đã đang chạy.")

    scraper_script_path = PROJECT_ROOT / "run_scraper.py"
    if not scraper_script_path.exists():
        raise HTTPException(status_code=500, detail=f"Không tìm thấy file scraper tại: {scraper_script_path}")

    try:
        python_executable = sys.executable
        print(f"Bắt đầu chạy script: {scraper_script_path} bằng python: {python_executable}")


        scraper_process = subprocess.Popen(
            [python_executable, str(scraper_script_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            cwd=str(PROJECT_ROOT)
        )
        return StatusResponse(message="Tiến trình cào dữ liệu đã được bắt đầu.", status="starting", pid=scraper_process.pid)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi khi bắt đầu tiến trình: {str(e)}")


@app.get("/status", response_model=StatusResponse)
async def get_status():
    global scraper_process
    state_db_path = get_db_path(str(PROJECT_ROOT))
    control_state = {}
    in_progress_days = []

    try:
        if os.path.exists(state_db_path):
             control_state = load_control_state(state_db_path)
             in_progress_days = [d.strftime("%Y-%m-%d") for d in get_all_in_progress_days(state_db_path)]
    except Exception as e:
        print(f"Không thể đọc state DB: {e}")

    if scraper_process and scraper_process.poll() is None:
        return StatusResponse(
            status="running",
            pid=scraper_process.pid,
            control_state=control_state,
            in_progress_days=in_progress_days
        )

    if scraper_process:
        try:
            stdout, stderr = scraper_process.communicate(timeout=1)
        except subprocess.TimeoutExpired:
            stdout, stderr = "", "Timeout khi lấy logs."

        return StatusResponse(
            status="stopped",
            pid=scraper_process.pid,
            message=f"Tiến trình đã dừng với mã thoát: {scraper_process.returncode}",
            control_state=control_state,
            in_progress_days=in_progress_days,
            error_details=stderr
        )

    return StatusResponse(
        status="not_started",
        control_state=control_state,
        in_progress_days=in_progress_days
    )


@app.post("/stop", response_model=StatusResponse)
async def stop_scraping():
    global scraper_process
    if not scraper_process or scraper_process.poll() is not None:
        raise HTTPException(status_code=404, detail="Không có tiến trình nào đang chạy để dừng.")

    pid = scraper_process.pid
    try:
        scraper_process.terminate()
        scraper_process.wait(timeout=10)
        message = f"Tiến trình (PID: {pid}) đã được yêu cầu dừng."
    except subprocess.TimeoutExpired:
        scraper_process.kill()
        message = f"Tiến trình (PID: {pid}) không phản hồi và đã bị buộc dừng."

    return StatusResponse(status="stopped", message=message, pid=pid)

@app.get('/')
async def root():
    return "Chào mừng đến với Scraper Control API."


if __name__ == '__main__':
    uvicorn.run("src.tools.router:app", host="0.0.0.0", port=8022, reload=True)