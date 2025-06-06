ARG PYTHON_VERSION=3.12
FROM python:${PYTHON_VERSION}-slim AS base


ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV APP_HOME=/app
ENV PYTHONPATH=${APP_HOME}


RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    unzip \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*


WORKDIR ${APP_HOME}

COPY requirements/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


COPY . ${APP_HOME}/

# Tạo các thư mục cần thiết cho media, logs, và data (nếu state files không nằm ở root)
# Bạn cần đảm bảo ứng dụng có quyền ghi vào các thư mục này.
RUN mkdir -p ${APP_HOME}/media_root/brand_images
RUN mkdir -p ${APP_HOME}/logs

CMD ["python", "run_scraper.py"]

