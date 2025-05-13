from pydantic import BaseModel, validator
from datetime import datetime
from config.logging_config.modern_log import LoggingConfig

class MetadataValidation(BaseModel):
    
    file_name: str
    author_name: str
    author_email: str
    author_profile: str
    uploaded_date: datetime
    created_date: datetime
    size: int
    filetype: str
    location: str
    modified_by_name: str
    modified_by_email: str
    modified_profile: str
    modified_time: datetime

    @validator('file_name')
    def file_name_max_length(cls, v):
        if len(v) > 62:
            raise ValueError(f"File name must be less than 62(+filetype) characters, file name '{len(v)}'")
        return v