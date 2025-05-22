from datetime import datetime, timedelta

from sqlmodel import SQLModel, Session, create_engine
from contextlib import contextmanager
from typing import Generator
from src.tools.config import settings
import logging

# Tạo engine kết nối đến PostgreSQL
engine = create_engine(
    settings.DATABASE_URL,
    echo=True,  # Log các câu query SQL
    pool_pre_ping=True,  # Kiểm tra kết nối trước khi sử dụng
    pool_size=5,  # Số lượng kết nối trong pool
    max_overflow=10  # Số lượng kết nối tối đa có thể tạo thêm
)

# Tạo tất cả bảng trong database
def create_tables():
    SQLModel.metadata.create_all(engine)

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

# Hàm helper để tạo partition mới cho tháng tiếp theo
def create_next_month_partition() -> None:
    from datetime import datetime, timedelta
    from sqlalchemy import text
    
    next_month = datetime.now() + timedelta(days=30)
    partition_name = f"brand_{next_month.strftime('%Y_%m')}"
    start_date = next_month.replace(day=1)
    end_date = (start_date + timedelta(days=32)).replace(day=1)
    
    with engine.connect() as conn:
        try:
            # Kiểm tra xem partition đã tồn tại chưa
            check_query = text(f"""
                SELECT EXISTS (
                    SELECT 1 
                    FROM information_schema.tables 
                    WHERE table_name = '{partition_name}'
                );
            """)
            exists = conn.execute(check_query).scalar()
            
            if not exists:
                # Tạo partition mới
                create_query = text(f"""
                    CREATE TABLE {partition_name} 
                    PARTITION OF brand 
                    FOR VALUES FROM ('{start_date.strftime('%Y-%m-%d')}') 
                    TO ('{end_date.strftime('%Y-%m-%d')}');
                """)
                conn.execute(create_query)
                conn.commit()
                logging.info(f"Created new partition: {partition_name}")
        except Exception as e:
            conn.rollback()
            logging.error(f"Error creating partition: {str(e)}")
            raise

# Hàm helper để lấy partition name dựa trên ngày
def get_partition_name(date: datetime) -> str:
    return f"brand_{date.strftime('%Y_%m')}"

# Hàm helper để kiểm tra và tạo partition nếu cần
def ensure_partition_exists(date: datetime) -> None:
    partition_name = get_partition_name(date)
    
    with engine.connect() as conn:
        try:
            # Kiểm tra xem partition đã tồn tại chưa
            from cgitb import text
            check_query = text(f"""
                SELECT EXISTS (
                    SELECT 1 
                    FROM information_schema.tables 
                    WHERE table_name = '{partition_name}'
                );
            """)
            exists = conn.execute(check_query).scalar()
            
            if not exists:
                # Tạo partition mới
                start_date = date.replace(day=1)
                end_date = (start_date + timedelta(days=32)).replace(day=1)
                
                create_query = text(f"""
                    CREATE TABLE {partition_name} 
                    PARTITION OF brand 
                    FOR VALUES FROM ('{start_date.strftime('%Y-%m-%d')}') 
                    TO ('{end_date.strftime('%Y-%m-%d')}');
                """)
                conn.execute(create_query)
                conn.commit()
                logging.info(f"Created new partition: {partition_name}")
        except Exception as e:
            conn.rollback()
            logging.error(f"Error creating partition: {str(e)}")
            raise 