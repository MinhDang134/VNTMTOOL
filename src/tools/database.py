from datetime import datetime, timedelta
from sqlmodel import SQLModel, Session, create_engine
from contextlib import contextmanager
from typing import Generator
from src.tools.config import settings
import logging
from sqlalchemy import text

# engine = create_engine(settings.DATABASE_URL,echo=True,pool_pre_ping=True,pool_size=5,max_overflow=10)   (Dòng này sẽ được comment hoặc xóa)

@contextmanager
def get_session(engine_instance: create_engine) -> Generator[Session, None, None]:
    session = Session(engine_instance)  
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        logging.error(f"Database error: {str(e)}")
        raise
    finally:
        session.close()

def bulk_create(session: Session, objects: list[SQLModel]) -> None:
    try:
        session.add_all(objects)  
        # session.commit()   (Commit sẽ được xử lý bởi get_session context manager)
    except Exception as e:
        # session.rollback()   (Rollback sẽ được xử lý bởi get_session context manager)
        logging.error(f"Bulk create error: {str(e)}")
        raise

def get_partition_name(date: datetime) -> str:
    return f"brand_{date.strftime('%Y_%m')}"

def ensure_partition_exists(date: datetime, engine_instance: create_engine) -> None:
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
        with engine_instance.connect() as conn:  
            trans = conn.begin()  
            try:  
                exists_result = conn.execute(check_query, {"table_name": partition_name})  
                exists = exists_result.scalar_one_or_none()  
                if exists:  
                    logging.debug(f"✅ Partition '{partition_name}' đã tồn tại, không cần tạo lại.")  
                else:  
                    conn.execute(create_query)  
                    logging.info(f"📦 Đã tạo partition mới: '{partition_name}'")  
                trans.commit()  
            except Exception as e_inner:  
                trans.rollback()  
                logging.error(f"❌ Lỗi bên trong transaction khi kiểm tra/tạo partition '{partition_name}': {str(e_inner)}")  
                raise  
    except Exception as e:
        logging.error(f"❌ Lỗi khi kiểm tra/tạo partition '{partition_name}': {str(e)}")  
        raise  