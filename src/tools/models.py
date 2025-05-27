#src/tools/models.py
from datetime import date, datetime
from typing import Optional
from sqlmodel import SQLModel, Field
from sqlalchemy import Column, Date

class Brand(SQLModel, table=True):
    __tablename__ = "brand"
    id: Optional[int] = Field(default=None, primary_key=True)
    application_number: str = Field(index=True)
    # SỬA CHỖ NÀY: Bỏ index=True trong Field
    application_date: date = Field(sa_column=Column(Date))
    brand_name: str = Field(index=True)
    image_url: str
    product_group: str
    status: str
    applicant: str
    representative: str
    product_detail: str
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
