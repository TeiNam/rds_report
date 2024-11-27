# apis/v1/generate_report.py
from typing import Optional
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from report_tools.generators.generate_monthly_report import (
    generate_monthly_report,
    get_previous_month,
    get_month_date_range
)
import logging

router = APIRouter(prefix="/reports", tags=["reports"])
logger = logging.getLogger(__name__)


class GenerateReportRequest(BaseModel):
    """리포트 생성 요청"""
    year: Optional[int] = None
    month: Optional[int] = None

    class Config:
        json_schema_extra = {
            "example": {
                "year": 2024,
                "month": 3
            }
        }


class GenerateReportResponse(BaseModel):
    """리포트 생성 응답"""
    year: int
    month: int
    period: dict
    output_dir: str
    report_file: str  # 리포트 파일 경로 추가
    message: str


@router.post("/generate",
             response_model=GenerateReportResponse,
             summary="RDS 월간 리포트 생성",
             description="지정된 년월의 RDS 인스턴스 월간 리포트를 생성합니다. "
                         "년월을 지정하지 않으면 전월 기준으로 생성됩니다.")
async def generate_report(request: GenerateReportRequest):
    try:
        # 년월 미지정시 전월 기준
        year = request.year
        month = request.month

        if not year or not month:
            year, month = get_previous_month()

        # 시작일과 종료일 계산
        start_date, end_date = get_month_date_range(year, month)

        # 리포트 생성
        report_result = await generate_monthly_report(year, month)

        return GenerateReportResponse(
            year=year,
            month=month,
            period={
                "start_date": start_date,
                "end_date": end_date
            },
            output_dir=report_result["output_dir"],
            report_file=report_result["report_file"],
            message=f"{year}년 {month}월 RDS 인스턴스 리포트가 생성되었습니다."
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"리포트 생성 중 오류가 발생했습니다: {str(e)}"
        )