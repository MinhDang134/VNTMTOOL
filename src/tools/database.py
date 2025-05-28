from datetime import datetime, timedelta
from sqlmodel import SQLModel, Session, create_engine # CRISTIANO (Đảm bảo Session từ sqlmodel được import)
from contextlib import contextmanager
from typing import Generator
from src.tools.config import settings
import logging
from sqlalchemy import text

# engine = create_engine(settings.DATABASE_URL,echo=True,pool_pre_ping=True,pool_size=5,max_overflow=10) # CRISTIANO (Dòng này sẽ được comment hoặc xóa)

@contextmanager
def get_session(engine_instance: create_engine) -> Generator[Session, None, None]: # CRISTIANO (Thêm type hint cho engine_instance và Session từ sqlmodel)
    session = Session(engine_instance) # CRISTIANO
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        logging.error(f"Database error: {str(e)}")
        raise
    finally:
        session.close()

def bulk_create(session: Session, objects: list[SQLModel]) -> None: # CRISTIANO (Session từ sqlmodel)
    try:
        session.add_all(objects) # CRISTIANO
        # session.commit() # CRISTIANO (Commit sẽ được xử lý bởi get_session context manager)
    except Exception as e:
        # session.rollback() # CRISTIANO (Rollback sẽ được xử lý bởi get_session context manager)
        logging.error(f"Bulk create error: {str(e)}")
        raise

def get_partition_name(date: datetime) -> str:
    return f"brand_{date.strftime('%Y_%m')}"

def ensure_partition_exists(date: datetime, engine_instance: create_engine) -> None: # CRISTIANO (Thêm engine_instance và type hint)
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
        with engine_instance.connect() as conn: # CRISTIANO
            trans = conn.begin() # CRISTIANO
            try: # CRISTIANO
                exists_result = conn.execute(check_query, {"table_name": partition_name}) # CRISTIANO
                exists = exists_result.scalar_one_or_none() # CRISTIANO
                if exists: # CRISTIANO
                    logging.debug(f"✅ Partition '{partition_name}' đã tồn tại, không cần tạo lại.") # CRISTIANO
                else: # CRISTIANO
                    conn.execute(create_query) # CRISTIANO
                    logging.info(f"📦 Đã tạo partition mới: '{partition_name}'") # CRISTIANO
                trans.commit() # CRISTIANO
            except Exception as e_inner: # CRISTIANO
                trans.rollback() # CRISTIANO
                logging.error(f"❌ Lỗi bên trong transaction khi kiểm tra/tạo partition '{partition_name}': {str(e_inner)}") # CRISTIANO
                raise # CRISTIANO
    except Exception as e:
        logging.error(f"❌ Lỗi khi kiểm tra/tạo partition '{partition_name}': {str(e)}") # CRISTIANO
        raise # CRISTIANO