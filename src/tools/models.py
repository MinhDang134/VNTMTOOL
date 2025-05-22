from datetime import datetime
from typing import Optional
from sqlmodel import SQLModel, Field

class Brand(SQLModel, table=True):
    __tablename__ = "brand"
    id: Optional[int] = Field(default=None)
    brand_name: str = Field(index=True) # ttên brand
    image_url: str # hinh ảnh
    product_group: str # nhóm sản phẩm
    status: str # trang thái
    application_date: datetime # ngày nộp đơn
    application_number: str = Field(index=True,primary_key=True) # số đơn khóa chính
    applicant: str # chủ đơn
    representative: str # đại diện shcn
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        arbitrary_types_allowed = True

