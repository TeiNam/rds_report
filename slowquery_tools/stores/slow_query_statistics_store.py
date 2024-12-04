from typing import Dict, List
import logging
from datetime import datetime
from modules.mongodb_connector import MongoDBConnector
from configs.mongo_conf import mongo_settings
from pymongo import IndexModel, ASCENDING, DESCENDING
from pymongo.errors import CollectionInvalid

logger = logging.getLogger(__name__)


class SlowQueryStatisticsStore:
    """슬로우 쿼리 통계 저장소"""

    @classmethod
    async def initialize_collection(cls) -> None:
        """컬렉션 및 인덱스 초기화"""
        try:
            # 컬렉션 생성 시도
            try:
                await MongoDBConnector.create_collection(
                    mongo_settings.MONGO_MONTHLY_SLOW_STATISTICS_COLLECTION
                )
                logger.info(f"컬렉션 생성 완료: {mongo_settings.MONGO_MONTHLY_SLOW_STATISTICS_COLLECTION}")
            except CollectionInvalid:
                logger.info(f"컬렉션이 이미 존재함: {mongo_settings.MONGO_MONTHLY_SLOW_STATISTICS_COLLECTION}")

            # 인덱스 생성
            indexes = [
                {
                    "keys": [
                        ("instance_id", ASCENDING),
                        ("year", ASCENDING),
                        ("month", ASCENDING)
                    ],
                    "unique": True,
                    "name": "idx_instance_year_month"
                },
                {
                    "keys": [("created_at", DESCENDING)],
                    "name": "idx_created_at"
                },
                {
                    "keys": [("total_stats.total_execution_time", DESCENDING)],
                    "name": "idx_total_execution_time"
                },
                {
                    "keys": [("total_stats.total_slow_queries", DESCENDING)],
                    "name": "idx_total_slow_queries"
                }
            ]

            for index in indexes:
                try:
                    await MongoDBConnector.create_index(
                        mongo_settings.MONGO_MONTHLY_SLOW_STATISTICS_COLLECTION,
                        index["keys"],
                        name=index["name"],
                        unique=index.get("unique", False)
                    )
                    logger.info(f"인덱스 생성 완료: {index['name']}")
                except Exception as e:
                    if "already exists" in str(e):
                        logger.info(f"인덱스가 이미 존재함: {index['name']}")
                    else:
                        raise

        except Exception as e:
            logger.error(f"컬렉션 초기화 실패: {str(e)}")
            raise

    @classmethod
    async def store_statistics(cls, instance_id: str, year: int, month: int, stats: Dict) -> None:
        """월간 통계 저장"""
        try:
            # 문서 구조화
            document = {
                # 메타 정보
                "instance_id": instance_id,
                "year": year,
                "month": month,
                "created_at": datetime.utcnow(),

                # 전체 통계
                "total_stats": {
                    "unique_digest_count": stats['total_stats']['unique_digest_count'],
                    "total_slow_queries": stats['total_stats']['total_slow_queries'],
                    "total_execution_count": stats['total_stats']['total_execution_count'],
                    "total_execution_time": stats['total_stats']['total_execution_time'],
                    "avg_execution_time": stats['total_stats']['avg_execution_time'],
                    "total_examined_rows": stats['total_stats']['total_examined_rows'],
                    "query_types": {
                        "read": stats['total_stats']['read_queries'],
                        "write": stats['total_stats']['write_queries'],
                        "ddl": stats['total_stats']['ddl_queries']
                    }
                },

                # 사용자별 통계
                "user_stats": [
                    {
                        "username": user_stat['user'],
                        "unique_digest_count": user_stat['unique_digest_count'],
                        "slow_query_count": user_stat['slow_query_count'],
                        "execution_count": user_stat['total_execution_count'],
                        "execution_time": user_stat['total_execution_time'],
                        "avg_execution_time": user_stat['avg_execution_time'],
                        "examined_rows": user_stat['total_examined_rows']
                    }
                    for user_stat in stats['user_stats']
                ],

                # 다이제스트 통계 (상위 10개만 저장)
                "digest_stats": [
                    {
                        "query_digest": digest['digest_query'],
                        "execution_count": digest['execution_count'],
                        "total_time": digest['total_time'],
                        "avg_time": digest['avg_time'],
                        "total_examined_rows": digest['total_examined_rows'],
                        "avg_examined_rows": digest['avg_examined_rows'],
                        "unique_users": digest['unique_users'],
                        "users": digest['users'],
                        "first_seen": datetime.fromisoformat(digest['first_seen']),
                        "last_seen": datetime.fromisoformat(digest['last_seen'])
                    }
                    for digest in stats['digest_stats'][:10]
                ]
            }

            # 컬렉션 가져오기
            collection = await MongoDBConnector.get_collection(
                mongo_settings.MONGO_MONTHLY_SLOW_STATISTICS_COLLECTION
            )

            # 업데이트 수행
            update_result = await collection.update_one(
                {
                    "instance_id": instance_id,
                    "year": year,
                    "month": month
                },
                {"$set": document},
                upsert=True
            )

            if update_result.modified_count > 0:
                logger.info(f"기존 통계 업데이트 완료 (modified_count: {update_result.modified_count})")
            elif update_result.upserted_id:
                logger.info(f"새 통계 저장 완료 (upserted_id: {update_result.upserted_id})")
            else:
                logger.warning("통계 저장/업데이트가 수행되지 않았습니다.")

            logger.info(
                f"통계 저장 완료 - 인스턴스: {instance_id}, "
                f"기간: {year}/{month:02d}"
            )

        except Exception as e:
            logger.error(f"통계 저장 실패: {str(e)}")
            raise

    @classmethod
    async def get_statistics(
            cls,
            instance_id: str,
            year: int,
            month: int
    ) -> Dict:
        """월간 통계 조회"""
        try:
            collection = await MongoDBConnector.get_collection(
                mongo_settings.MONGO_MONTHLY_SLOW_STATISTICS_COLLECTION
            )
            result = await collection.find_one({
                "instance_id": instance_id,
                "year": year,
                "month": month
            })
            return result

        except Exception as e:
            logger.error(f"통계 조회 실패: {str(e)}")
            raise