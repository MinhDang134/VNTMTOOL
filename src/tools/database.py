import os
import sys
from datetime import datetime, timedelta
from sqlmodel import SQLModel, Session, create_engine as create_engine_sqlmodel
from contextlib import contextmanager
from typing import Generator
from src.tools.config import settings
import logging
from sqlalchemy import text, Engine, create_engine, inspect
import logging
logger = logging.getLogger(__name__)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SRC_DIR_PATH = os.path.dirname(SCRIPT_DIR)
PROJECT_ROOT_PATH = os.path.dirname(SRC_DIR_PATH)

if PROJECT_ROOT_PATH not in sys.path:
    sys.path.insert(0, PROJECT_ROOT_PATH)


db_engine: Engine = create_engine_sqlmodel(
    settings.DATABASE_URL,
    echo=True,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10
)

@contextmanager
def get_session(engine_to_use: Engine = db_engine) -> Generator[Session, None, None]:
    session = Session(engine_to_use)
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
    except Exception as e:
        logging.error(f"Bulk create error: {str(e)}")
        raise

def get_partition_name(date: datetime) -> str:
    return f"brand_{date.strftime('%Y_%m')}"

def ensure_partition_exists(date: datetime,engine_to_use: Engine = db_engine) -> None:
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
        with engine_to_use.connect() as conn:
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


def setup_database_schema():
    engine = create_engine(settings.DATABASE_URL)
    db_user_for_owner = settings.DB_USER if settings.DB_USER else settings.DATABASE_URL.split('://')[1].split(':')[0]

    create_sequence_sql = """
    CREATE SEQUENCE IF NOT EXISTS public.brand_id_seq;
    """


    create_parent_table_sql = f"""
    CREATE TABLE public.brand (
        id integer NOT NULL DEFAULT nextval('public.brand_id_seq'::regclass),
        brand_name text COLLATE pg_catalog."default",
        image_url text COLLATE pg_catalog."default",
        product_group text COLLATE pg_catalog."default",
        status text COLLATE pg_catalog."default",
        application_date date NOT NULL,
        application_number text COLLATE pg_catalog."default",
        applicant text COLLATE pg_catalog."default",
        representative text COLLATE pg_catalog."default",
        product_detail text COLLATE pg_catalog."default",
        va_count integer NOT NULL DEFAULT 0,
        created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
        updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT brand_pkey PRIMARY KEY (id, application_date)
    ) PARTITION BY RANGE (application_date);
    """


    create_indexes_sql = """
                         CREATE INDEX IF NOT EXISTS ix_brand_application_number ON public.brand (application_number);
                         CREATE INDEX IF NOT EXISTS ix_brand_brand_name ON public.brand (brand_name); \
                         """


    alter_owner_sql = f"""
    ALTER TABLE IF EXISTS public.brand OWNER TO "{db_user_for_owner}";
    """

    with engine.connect() as connection:
        try:
            inspector = inspect(connection)
            table_exists = 'brand' in inspector.get_table_names(schema='public')
            is_partitioned = False
            if table_exists:
                partition_check_sql = text("""
                                           SELECT c.relkind
                                           FROM pg_catalog.pg_class c
                                                    LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                                           WHERE c.relname = 'brand'
                                             AND n.nspname = 'public';
                                           """)
                result = connection.execute(partition_check_sql).scalar_one_or_none()
                if result == 'p':
                    is_partitioned = True

            if table_exists and is_partitioned:
                logger.info("✅ Bảng 'brand' (partitioned parent) đã tồn tại.")
            else:
                if table_exists and not is_partitioned:
                    logger.warning(
                        "⚠️ Bảng 'brand' tồn tại nhưng không phải là partitioned table. Sẽ xóa và tạo lại. CẨN THẬN MẤT DỮ LIỆU NẾU CÓ!")
                    connection.execute(text("DROP TABLE IF EXISTS public.brand CASCADE;"))
                    logger.info("⚡️ Đã xóa bảng 'brand' cũ.")

                logger.info("⏳ Đang tạo sequence 'brand_id_seq' (nếu chưa có)...")
                connection.execute(text(create_sequence_sql))
                logger.info("✅ Sequence 'brand_id_seq' đã được kiểm tra/tạo.")

                logger.info("⏳ Đang tạo bảng 'brand' (partitioned parent)...")
                connection.execute(text(create_parent_table_sql))
                logger.info("✅ Đã tạo bảng 'brand' (partitioned parent).")

                logger.info("⏳ Đang tạo indexes cho bảng 'brand'...")
                connection.execute(text(create_indexes_sql))
                logger.info("✅ Đã tạo indexes.")

                logger.info(f"⏳ Đang đặt OWNER của bảng 'brand' thành '{db_user_for_owner}'...")
                connection.execute(text(alter_owner_sql))
                logger.info(f"✅ Đã đặt OWNER cho bảng 'brand'.")

            connection.commit()
            logger.info("🚀 Thiết lập schema database hoàn tất.")

        except Exception as e:
            logger.error(f"❌ Lỗi nghiêm trọng khi thiết lập schema database: {e}", exc_info=True)
            connection.rollback()
            raise