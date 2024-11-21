# report_tools/base.py
from datetime import datetime, timedelta
from typing import Optional, Tuple
import logging

logger = logging.getLogger(__name__)


class ReportBaseTool:
    """리포트 도구의 기본 클래스"""

    def __init__(self, start_date: Optional[str] = None, end_date: Optional[str] = None):
        """
        Args:
            start_date: 시작일 (YYYY-MM-DD)
            end_date: 종료일 (YYYY-MM-DD)
        """
        self._start_date = None
        self._end_date = None
        self.set_date_range(start_date, end_date)

    def set_date_range(self, start_date: Optional[str], end_date: Optional[str]) -> None:
        """날짜 범위 설정

        Args:
            start_date: 시작일 (YYYY-MM-DD)
            end_date: 종료일 (YYYY-MM-DD)
        """
        if start_date and end_date:
            try:
                self._start_date = datetime.strptime(start_date, "%Y-%m-%d")
                self._end_date = datetime.strptime(end_date, "%Y-%m-%d")
                if self._start_date >= self._end_date:
                    raise ValueError("시작일이 종료일보다 늦을 수 없습니다.")
            except ValueError as e:
                logger.error(f"날짜 형식 오류: {e}")
                raise
        else:
            # 기본값: 이번 달 1일부터 오늘까지
            today = datetime.utcnow()
            self._end_date = today
            self._start_date = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    @property
    def start_date(self) -> datetime:
        """시작일 반환"""
        return self._start_date

    @property
    def end_date(self) -> datetime:
        """종료일 반환"""
        return self._end_date

    def get_date_range_str(self) -> Tuple[str, str]:
        """시작일과 종료일을 문자열로 반환"""
        return (
            self._start_date.strftime("%Y-%m-%d"),
            self._end_date.strftime("%Y-%m-%d")
        )

    def get_query_range(self) -> Tuple[datetime, datetime]:
        """쿼리용 날짜 범위 반환 (종료일 다음 날까지)"""
        return (
            self._start_date,
            self._end_date + timedelta(days=1)
        )