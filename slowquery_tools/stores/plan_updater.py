from typing import Dict, Any, List, Optional
import logging
from datetime import datetime
from modules.mongodb_connector import MongoDBConnector
from configs.mongo_conf import mongo_settings
from slowquery_tools.base import BaseSlowQueryTool
from models.slowquery import SlowQueryInfo

logger = logging.getLogger(__name__)


class PlanUpdater(BaseSlowQueryTool):
    """쿼리 실행 계획 MongoDB 저장"""

    def __init__(self):
        super().__init__()
        self.collection_name = mongo_settings.MONGO_SLOW_DIGEST_INFO_COLLECTION
        self._collection = None

    async def _get_collection(self):
        """MongoDB 컬렉션 가져오기"""
        if self._collection is None:
            self._collection = await MongoDBConnector.get_collection(
                self.collection_name
            )
        return self._collection

    async def update_explain_results(
            self,
            query_info: SlowQueryInfo,
            explain_results: Dict[str, Any]
    ) -> bool:
        """
        쿼리 실행 계획 결과 MongoDB에 저장

        Args:
            query_info: 슬로우 쿼리 정보
            explain_results: 실행 계획 분석 결과

        Returns:
            bool: 저장 성공 여부
        """
        try:
            collection = await self._get_collection()
            result = await collection.update_one(
                {'_id': query_info._id},
                {
                    '$set': {
                        'plan': explain_results['plan'],
                        'analyzed_at': explain_results['analyzed_at']
                    }
                }
            )

            if result.modified_count > 0:
                logger.info(f"쿼리 실행 계획 저장 완료: {query_info._id}")
                return True
            else:
                logger.warning(f"쿼리 실행 계획 저장 실패 (변경 없음): {query_info._id}")
                return False

        except Exception as e:
            logger.error(f"쿼리 실행 계획 저장 실패: {str(e)}")
            return False


# 싱글톤 인스턴스
_updater_instance = None


def get_plan_updater() -> PlanUpdater:
    """PlanUpdater 싱글톤 인스턴스 반환"""
    global _updater_instance
    if _updater_instance is None:
        _updater_instance = PlanUpdater()
    return _updater_instance