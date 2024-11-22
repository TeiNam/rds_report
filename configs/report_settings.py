# configs/report_settings.py
import os
import json
import logging
from typing import List
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# .env 파일 로드
load_dotenv()


class ReportSettings:
    """리포트 생성 관련 설정"""

    @staticmethod
    def get_report_target_instances() -> List[str]:
        """월간 리포트에 포함할 분석 대상 인스턴스 목록 반환

        Returns:
            List[str]: 분석 대상 인스턴스 목록. 설정이 없거나 유효하지 않은 경우 빈 리스트 반환.

        Note:
            - 리포트에는 전체 인스턴스 통계가 포함되지만,
            - 여기서 지정된 인스턴스들은 상세 메트릭 분석 및 그래프가 추가됨
        """
        try:
            # .env에서 JSON 배열 형식의 문자열 가져오기
            instances_json = os.getenv('REPORT_TARGET_INSTANCES')

            # 환경변수가 없는 경우 빈 리스트 반환
            if not instances_json:
                logger.info("REPORT_TARGET_INSTANCES not set. No instances will be analyzed in detail.")
                return []

            # JSON 파싱
            instances = json.loads(instances_json)

            # 타입 체크
            if not isinstance(instances, list):
                logger.warning("REPORT_TARGET_INSTANCES must be a JSON array")
                return []

            # 유효한 인스턴스 이름만 필터링
            valid_instances = [
                inst.strip()
                for inst in instances
                if isinstance(inst, str) and inst.strip()
            ]

            if not valid_instances:
                logger.info("No valid instances found in REPORT_TARGET_INSTANCES")
                return []

            return valid_instances

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse REPORT_TARGET_INSTANCES: {e}")
            return []
        except Exception as e:
            logger.error(f"Error in get_report_target_instances: {e}")
            return []