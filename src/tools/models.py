#src/tools/models.py
from datetime import date, datetime
from typing import Optional
from sqlmodel import SQLModel, Field
from sqlalchemy import Column, Date, Integer


class Brand(SQLModel, table=True):
    __tablename__ = "brand"
    id: Optional[int] = Field(default=None, primary_key=True)
    application_number: str = Field(primary_key=True, index=True)
    application_date: date = Field(sa_column=Column(Date, primary_key=True))
    brand_name: str = Field(index=True)
    image_url: str
    product_group: str
    status: str
    applicant: str
    representative: str
    product_detail: str
    va_count: int = Field(default=0, sa_column=Column(Integer, nullable=False, default=0, server_default="0"))
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
