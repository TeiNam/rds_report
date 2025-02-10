from datetime import datetime
from typing import Dict, List, Optional, Union
from functools import lru_cache
from modules.mongodb_connector import MongoDBConnector
from configs.mongo_conf import mongo_settings
import logging

logger = logging.getLogger(__name__)


class SlowQueryStatsLoader:
    """슬로우 쿼리 통계 데이터 로더"""

    def __init__(self):
        self.collection_name = mongo_settings.MONGO_SLOW_QUERY_COLLECTION
        self._collection = None

    async def _get_collection(self):
        """MongoDB 컬렉션 객체 반환"""
        if self._collection is None:
            self._collection = await MongoDBConnector.get_collection(self.collection_name)
        return self._collection

    async def get_instance_queries(
            self,
            instance_id: str,
            start_date: Optional[datetime] = None,
            end_date: Optional[datetime] = None,
    ) -> List[Dict]:
        try:
            collection = await self._get_collection()

            # 기존 매칭 조건은 유지
            match_stage = {"instance_id": instance_id}
            if start_date or end_date:
                match_stage["date"] = {}
                if start_date:
                    match_stage["date"]["$gte"] = start_date.strftime("%Y-%m-%d")
                if end_date:
                    match_stage["date"]["$lte"] = end_date.strftime("%Y-%m-%d")

            # 정렬 조건 추가: 연도와 월을 기준으로 정렬
            pipeline = [
                {"$match": match_stage},
                {"$unwind": "$slow_queries"},
                {
                    "$project": {
                        "instance_id": 1,
                        "date": 1,
                        "year": 1,
                        "month": 1,
                        # 누락된 필드들 추가
                        "digest_query": "$slow_queries.digest_query",
                        "execution_count": "$slow_queries.execution_count",
                        "total_time": "$slow_queries.total_time",
                        "avg_time": "$slow_queries.avg_time",
                        "avg_rows_examined": "$slow_queries.avg_rows_examined",
                        "users": "$slow_queries.users",
                        "first_seen": "$slow_queries.first_seen",
                        "last_seen": "$slow_queries.last_seen"
                    }
                },
                {"$sort": {
                    "year": 1,
                    "month": 1,
                    "total_time": -1
                }}
            ]

            # 결과 조회
            cursor = collection.aggregate(pipeline, allowDiskUse=True)
            results = []
            async for doc in cursor:
                results.append(doc)

            logger.info(
                f"인스턴스 {instance_id}의 슬로우 쿼리 통계 "
                f"{len(results)}건 조회 완료"
            )
            return results

        except Exception as e:
            logger.error(f"슬로우 쿼리 통계 조회 실패: {str(e)}")
            raise

    @lru_cache(maxsize=100)
    async def get_instance_monthly_summary(
            self,
            instance_id: str,
            year: int,
            month: int
    ) -> Dict[str, Union[int, float, List[Dict]]]:
        """인스턴스의 월별 슬로우 쿼리 요약 통계"""
        try:
            collection = await self._get_collection()

            # 집계 파이프라인
            pipeline = [
                {
                    "$match": {
                        "instance_id": instance_id,
                        "year": year,
                        "month": month
                    }
                },
                {"$unwind": "$slow_queries"},
                {
                    "$group": {
                        "_id": None,
                        "total_queries": {"$sum": 1},
                        "total_execution_count": {"$sum": "$slow_queries.execution_count"},
                        "total_time": {"$sum": "$slow_queries.total_time"},
                        "unique_digests": {"$addToSet": "$slow_queries.digest_query"},
                        "queries": {
                            "$push": {
                                "digest_query": "$slow_queries.digest_query",
                                "execution_count": "$slow_queries.execution_count",
                                "avg_time": "$slow_queries.avg_time",
                                "total_time": "$slow_queries.total_time",
                                "avg_rows_examined": "$slow_queries.avg_rows_examined"
                            }
                        }
                    }
                }
            ]

            # 결과 조회
            cursor = collection.aggregate(pipeline, allowDiskUse=True)
            async for result in cursor:
                # 실행 시간 기준 상위 쿼리
                top_by_time = sorted(
                    result['queries'],
                    key=lambda x: x['total_time'],
                    reverse=True
                )[:10]

                # 실행 횟수 기준 상위 쿼리
                top_by_count = sorted(
                    result['queries'],
                    key=lambda x: x['execution_count'],
                    reverse=True
                )[:10]

                stats = {
                    'total_queries': result['total_queries'],
                    'total_execution_count': result['total_execution_count'],
                    'total_execution_time': result['total_time'],
                    'avg_execution_time': result['total_time'] / result['total_execution_count'],
                    'unique_digests': len(result['unique_digests']),
                    'top_queries_by_time': top_by_time,
                    'top_queries_by_count': top_by_count
                }

                logger.info(
                    f"인스턴스 {instance_id}의 "
                    f"{year}년 {month}월 월별 요약 통계 조회 완료"
                )
                return stats

            logger.warning(f"해당 기간의 데이터가 없습니다.")
            return {}

        except Exception as e:
            logger.error(f"월별 요약 통계 조회 실패: {str(e)}")
            raise

    def clear_cache(self):
        """캐시 초기화"""
        self.get_instance_monthly_summary.cache_clear()


# 싱글톤 인스턴스
_loader_instance = None


def get_stats_loader() -> SlowQueryStatsLoader:
    """SlowQueryStatsLoader 싱글톤 인스턴스 반환"""
    global _loader_instance
    if _loader_instance is None:
        _loader_instance = SlowQueryStatsLoader()
    return _loader_instance