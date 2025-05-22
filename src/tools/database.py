from datetime import datetime, timedelta

from sqlmodel import SQLModel, Session, create_engine
from contextlib import contextmanager
from typing import Generator
from src.tools.config import settings
import logging
from sqlalchemy import text

# Tạo engine kết nối đến PostgreSQL
engine = create_engine(
    settings.DATABASE_URL,
    echo=True,  # Log các câu query SQL
    pool_pre_ping=True,  # Kiểm tra kết nối trước khi sử dụng
    pool_size=5,         # Số lượng kết nối trong pool
    max_overflow=10      # Số lượng kết nối tối đa có thể tạo thêm
)

# Tạo tất cả bảng trong database
def create_tables():
    pass
    # SQLModel.metadata.create_all(engine)

# Tạo các partition hàng tháng trong khoảng thời gian cho trước
def create_monthly_partitions(session: Session, start: datetime, end: datetime):
    current = start.replace(day=1)
    while current < end:
        if current.month == 12:
            next_month = current.replace(year=current.year + 1, month=1, day=1)
        else:
            next_month = current.replace(month=current.month + 1, day=1)
        table_name = f"brand_{current.strftime('%Y_%m')}"
        sql = f"""
        CREATE TABLE IF NOT EXISTS "{table_name}"
        PARTITION OF brand
        FOR VALUES FROM ('{current.date()}') TO ('{next_month.date()}');
        """
        session.exec(text(sql))
        current = next_month

# Context manager để quản lý session
@contextmanager
def get_session() -> Generator[Session, None, None]:
    session = Session(engine)
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        logging.error(f"Database error: {str(e)}")
        raise
    finally:
        session.close()

# Hàm helper để thêm nhiều bản ghi
def bulk_create(session: Session, objects: list[SQLModel]) -> None:
    try:
        session.add_all(objects)
        session.commit()
    except Exception as e:
        session.rollback()
        logging.error(f"Bulk create error: {str(e)}")
        raise

# Hàm helper để cập nhật nhiều bản ghi
def bulk_update(session: Session, objects: list[SQLModel]) -> None:
    try:
        for obj in objects:
            session.add(obj)
        session.commit()
    except Exception as e:
        session.rollback()
        logging.error(f"Bulk update error: {str(e)}")
        raise

# Hàm helper để xóa nhiều bản ghi
def bulk_delete(session: Session, objects: list[SQLModel]) -> None:
    try:
        for obj in objects:
            session.delete(obj)
        session.commit()
    except Exception as e:
        session.rollback()
        logging.error(f"Bulk delete error: {str(e)}")
        raise

# Tạo partition cho tháng kế tiếp nếu chưa tồn tại
def create_next_month_partition() -> None:
    next_month = datetime.now() + timedelta(days=30)
    partition_name = f"brand_{next_month.strftime('%Y_%m')}"
    start_date = next_month.replace(day=1)
    end_date = (start_date + timedelta(days=32)).replace(day=1)

    check_query = text("""
        SELECT EXISTS (
            SELECT 1 
            FROM information_schema.tables 
            WHERE table_name = :table_name AND table_schema = 'public'
        );
    """)
    create_query = text(f"""
        CREATE TABLE IF NOT EXISTS "{partition_name}" 
        PARTITION OF brand 
        FOR VALUES FROM ('{start_date.strftime('%Y-%m-%d')}') 
        TO ('{end_date.strftime('%Y-%m-%d')}');
    """)

    try:
        with engine.begin() as conn:
            exists = conn.execute(check_query, {"table_name": partition_name}).scalar()
            if not exists:
                conn.execute(create_query)
                logging.info(f"Created new partition: {partition_name}")
    except Exception as e:
        logging.error(f"Error creating partition: {str(e)}")
        raise e

# Trả về tên bảng phân vùng theo ngày cho trước
def get_partition_name(date: datetime) -> str:
    return f"brand_{date.strftime('%Y_%m')}"

# Đảm bảo rằng partition tương ứng với ngày đã tồn tại, nếu chưa thì tạo
def get_partition_name(date: datetime) -> str:
    return f"brand_{date.strftime('%Y_%m')}"

def ensure_partition_exists(date: datetime) -> None:
    partition_name = get_partition_name(date)
    start_date = date.replace(day=1)
    end_date = (start_date + timedelta(days=32)).replace(day=1)

    check_query = text("""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_name = :table_name
              AND table_schema = 'public'
        );
    """)

    create_query = text(f"""
        CREATE TABLE IF NOT EXISTS "{partition_name}"
        PARTITION OF brand
        FOR VALUES FROM ('{start_date.strftime('%Y-%m-%d')}')
        TO ('{end_date.strftime('%Y-%m-%d')}');
    """)

    try:
        with engine.begin() as conn:
            exists = conn.execute(check_query, {"table_name": partition_name}).scalar()
            if exists:
                logging.debug(f"✅ Partition '{partition_name}' đã tồn tại, không cần tạo lại.")
            else:
                conn.execute(create_query)
                logging.info(f"📦 Đã tạo partition mới: '{partition_name}'")
    except Exception as e:
        logging.error(f"❌ Lỗi khi kiểm tra/tạo partition '{partition_name}': {str(e)}")
        raise