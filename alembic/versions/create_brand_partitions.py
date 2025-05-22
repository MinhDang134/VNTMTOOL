from alembic import op
from datetime import datetime, timedelta

def upgrade():
    # Create partitioned table
    op.execute("""
        CREATE TABLE brand (
            id SERIAL,
            brand_name VARCHAR,
            image_url VARCHAR,
            product_group VARCHAR,
            status VARCHAR,
            application_date TIMESTAMP,
            application_number VARCHAR,
            applicant VARCHAR,
            representative VARCHAR,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            PRIMARY KEY (id, application_date)
        ) PARTITION BY RANGE (application_date);
    """)
    
    # Create partitions for next 12 months
    current_date = datetime.now()
    for i in range(12):
        start_date = current_date + timedelta(days=30*i)
        end_date = start_date + timedelta(days=30)
        partition_name = f"brand_{start_date.strftime('%Y_%m')}"
        
        op.execute(f"""
            CREATE TABLE {partition_name} 
            PARTITION OF brand 
            FOR VALUES FROM ('{start_date.strftime('%Y-%m-%d')}') 
            TO ('{end_date.strftime('%Y-%m-%d')}');
        """)

def downgrade():
    op.drop_table('brand') 