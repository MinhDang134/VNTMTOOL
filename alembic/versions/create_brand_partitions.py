from alembic import op
from datetime import datetime, timedelta


def upgrade():
    op.execute("""
               CREATE TABLE brand
               (
                   id                 SERIAL,
                   brand_name         TEXT,
                   image_url          TEXT,
                   product_group      TEXT,
                   status             TEXT,
                   application_date   DATE,
                   application_number TEXT,
                   applicant          TEXT,
                   representative     TEXT,
                   created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                   updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
               ) PARTITION BY RANGE (application_date);
               """)

    current_date = datetime.now().replace(day=1)
    for i in range(12):
        start_date = (current_date + timedelta(days=30 * i)).replace(day=1)
        end_date = (start_date + timedelta(days=32)).replace(day=1)
        partition_name = f"brand_{start_date.strftime('%Y_%m')}"

        op.execute(f"""
            CREATE TABLE {partition_name} 
            PARTITION OF brand 
            FOR VALUES FROM ('{start_date.strftime('%Y-%m-%d')}') 
            TO ('{end_date.strftime('%Y-%m-%d')}');
        """)


def downgrade():
    op.execute("DROP TABLE brand CASCADE;")
