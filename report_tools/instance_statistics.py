# report_tools/instance_statistics.py
from typing import List, Dict, Any, Optional, Set
from datetime import datetime, timedelta
from motor.motor_asyncio import AsyncIOMotorDatabase
import logging

from modules.mongodb_connector import MongoDBConnector
from configs.mongo_conf import mongo_settings
from report_tools.base import ReportBaseTool

logger = logging.getLogger(__name__)


class InstanceStatisticsTool(ReportBaseTool):
    """RDS 인스턴스 통계 도구"""

    def __init__(self, start_date: Optional[str] = None, end_date: Optional[str] = None):
        super().__init__(start_date, end_date)
        self.collection_name = mongo_settings.MONGO_RDS_INSTANCE_ALL_STAT_COLLECTION

    async def _get_database(self) -> AsyncIOMotorDatabase:
        """데이터베이스 연결 반환"""
        return await MongoDBConnector.get_database()

    async def _aggregate_data(self, pipeline: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """MongoDB aggregation 실행"""
        db = await self._get_database()
        collection = db[self.collection_name]
        return await collection.aggregate(pipeline).to_list(length=None)

    async def get_daily_statistics(self, target_date: Optional[datetime] = None) -> Dict[str, Any]:
        """일일 인스턴스 통계 조회

        Args:
            target_date: 통계를 조회할 날짜 (기본값: end_date)

        Returns:
            Dict[str, Any]: 일일 통계 정보
        """
        try:
            today = target_date if target_date else self._end_date
            tomorrow = today + timedelta(days=1)
            today = today.replace(hour=0, minute=0, second=0, microsecond=0)

            # 기본 통계 파이프라인
            pipeline = [
                {
                    "$match": {
                        "timestamp": {
                            "$gte": today.strftime("%Y-%m-%d %H:%M:%S"),
                            "$lt": tomorrow.strftime("%Y-%m-%d %H:%M:%S")
                        }
                    }
                },
                {
                    "$unwind": "$instances"
                },
                {
                    "$group": {
                        "_id": None,
                        "total_instances": {"$sum": 1},
                        "accounts": {"$addToSet": "$account_id"},
                        "dev_instances": {
                            "$sum": {
                                "$cond": [
                                    {"$eq": ["$instances.Tags.env", "dev"]},
                                    1,
                                    0
                                ]
                            }
                        },
                        "prd_instances": {
                            "$sum": {
                                "$cond": [
                                    {"$eq": ["$instances.Tags.env", "prd"]},
                                    1,
                                    0
                                ]
                            }
                        },
                        "regions": {"$addToSet": "$instances.Region"},
                        "instance_classes": {"$push": "$instances.DBInstanceClass"}
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "date": {"$dateToString": {"format": "%Y-%m-%d", "date": today}},
                        "total_instances": 1,
                        "account_count": {"$size": "$accounts"},
                        "dev_instances": 1,
                        "prd_instances": 1,
                        "region_count": {"$size": "$regions"},
                        "instance_classes": 1
                    }
                }
            ]

            result = await self._aggregate_data(pipeline)

            if not result:
                return {
                    "date": today.date().isoformat(),
                    "total_instances": 0,
                    "account_count": 0,
                    "dev_instances": 0,
                    "prd_instances": 0,
                    "region_count": 0,
                    "instance_classes": {},
                    "accounts": [],
                    "regions": []
                }

            result = result[0]

            # 인스턴스 클래스별 개수 계산
            result["instance_classes"] = {
                cls: result["instance_classes"].count(cls)
                for cls in set(result["instance_classes"])
            }

            # 계정별 통계
            account_pipeline = [
                {
                    "$match": {
                        "timestamp": {
                            "$gte": today.strftime("%Y-%m-%d %H:%M:%S"),
                            "$lt": tomorrow.strftime("%Y-%m-%d %H:%M:%S")
                        }
                    }
                },
                {
                    "$group": {
                        "_id": "$account_id",
                        "instance_count": {"$sum": "$total_instances"}
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "account_id": "$_id",
                        "instance_count": 1
                    }
                }
            ]

            # 리전별 통계
            region_pipeline = [
                {
                    "$match": {
                        "timestamp": {
                            "$gte": today.strftime("%Y-%m-%d %H:%M:%S"),
                            "$lt": tomorrow.strftime("%Y-%m-%d %H:%M:%S")
                        }
                    }
                },
                {"$unwind": "$instances"},
                {
                    "$group": {
                        "_id": "$instances.Region",
                        "instance_count": {"$sum": 1}
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "region": "$_id",
                        "instance_count": 1
                    }
                }
            ]

            result["accounts"] = await self._aggregate_data(account_pipeline)
            result["regions"] = await self._aggregate_data(region_pipeline)

            return result

        except Exception as e:
            logger.error(f"일일 통계 조회 중 오류 발생: {str(e)}")
            raise

    async def get_period_statistics(self) -> Dict[str, Any]:
        """설정된 기간의 인스턴스 통계 조회

        Returns:
            Dict[str, Any]: 기간별 통계 정보
        """
        try:
            query_start, query_end = self.get_query_range()
            db = await self._get_database()
            collection = db[self.collection_name]

            # 데이터 존재 기간 확인
            date_range_pipeline = [
                {
                    "$match": {
                        "timestamp": {
                            "$gte": query_start.strftime("%Y-%m-%d %H:%M:%S"),
                            "$lt": query_end.strftime("%Y-%m-%d %H:%M:%S")
                        }
                    }
                },
                {
                    "$group": {
                        "_id": None,
                        "first_date": {"$min": "$timestamp"},
                        "last_date": {"$max": "$timestamp"}
                    }
                }
            ]

            date_range = await collection.aggregate(date_range_pipeline).to_list(None)
            if not date_range:
                return self._get_empty_period_statistics()

            first_date = date_range[0]["first_date"].split()[0]
            last_date = date_range[0]["last_date"].split()[0]

            logger.info(f"데이터 기간: {first_date} ~ {last_date}")

            # 첫날과 마지막날 인스턴스 목록
            first_day_instances = await self._get_instance_ids(collection, first_date)
            last_day_instances = await self._get_instance_ids(collection, last_date)

            # 변경사항 분석
            added_instances = list(last_day_instances - first_day_instances)
            removed_instances = list(first_day_instances - last_day_instances)

            logger.info(f"첫날 ({first_date}) 인스턴스 수: {len(first_day_instances)}")
            logger.info(f"마지막날 ({last_date}) 인스턴스 수: {len(last_day_instances)}")
            logger.info(f"추가된 인스턴스: {len(added_instances)}")
            logger.info(f"제거된 인스턴스: {len(removed_instances)}")

            return {
                "year": query_start.year,
                "month": query_start.month,
                "total_instances_start": len(first_day_instances),
                "total_instances_end": len(last_day_instances),
                "instances_added": len(added_instances),
                "instances_removed": len(removed_instances),
                "added_instances": sorted(added_instances),
                "removed_instances": sorted(removed_instances),
                "data_range": {
                    "start": first_date,
                    "end": last_date
                }
            }

        except Exception as e:
            logger.error(f"기간별 통계 조회 중 오류 발생: {str(e)}")
            raise

    async def _get_instance_ids(self, collection, date: str) -> Set[str]:
        """특정 날짜의 인스턴스 ID 목록 조회"""
        pipeline = [
            {
                "$match": {
                    "timestamp": {"$regex": f"^{date}"}
                }
            },
            {"$unwind": "$instances"},
            {
                "$group": {
                    "_id": None,
                    "instance_ids": {"$addToSet": "$instances.DBInstanceIdentifier"}
                }
            }
        ]

        result = await collection.aggregate(pipeline).to_list(None)
        return set(result[0]["instance_ids"]) if result else set()

    def _get_empty_period_statistics(self) -> Dict[str, Any]:
        """빈 통계 정보 반환"""
        return {
            "year": self._start_date.year,
            "month": self._start_date.month,
            "total_instances_start": 0,
            "total_instances_end": 0,
            "instances_added": 0,
            "instances_removed": 0,
            "added_instances": [],
            "removed_instances": [],
            "data_range": {
                "start": None,
                "end": None
            }
        }

