from datetime import datetime
from pydantic import BaseModel, HttpUrl, Field

class BrandCreate(BaseModel):
    brand_name: str
    image_url: HttpUrl
    product_group: str
    status: str
    application_date: datetime
    application_number: str
    applicant: str
    representative: str

class BrandUpdate(BaseModel):
    status: str
    updated_at: datetime = Field(default_factory=datetime.utcnow)

