# models/aws_account.py

from datetime import datetime
from typing import List, Dict
from enum import Enum
from pydantic import BaseModel, Field, constr

class EnvironmentType(str, Enum):
    PRD = "prd"
    DEV = "dev"
    BOTH = "both"

class AWSAccountBase(BaseModel):
    """AWS 계정 등록을 위한 기본 모델"""
    aws_account_id: constr(min_length=12, max_length=12, pattern=r"^\d+$")
    aws_account_name: str = Field(..., min_length=1)
    regions: List[str] = Field(..., min_items=1)
    environment_type: EnvironmentType = Field(..., description="계정의 환경 타입 (prd/dev/both)")
    description: str | None = None

class AWSAccountCreate(AWSAccountBase):
    pass

class AWSAccountInDB(AWSAccountBase):
    """MongoDB에 저장되는 AWS 계정 모델"""
    create_at: datetime = Field(default_factory=datetime.utcnow)
    update_at: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }

class AWSAccountUpdate(BaseModel):
    """AWS 계정 업데이트를 위한 모델"""
    aws_account_name: str | None = None
    regions: List[str] | None = None
    environment_type: EnvironmentType | None = None
    description: str | None = None

class AWSAccountResponse(AWSAccountBase):
    """API 응답을 위한 모델"""
    create_at: datetime
    update_at: datetime