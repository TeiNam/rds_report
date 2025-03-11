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
            # 기본 통계 파이프라인 수정
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
                        "instance_classes": {
                            "$push": {
                                "$cond": [
                                    {"$eq": ["$instances.Engine", "aurora-mysql-serverless"]},
                                    "serverless",
                                    "$instances.DBInstanceClass"
                                ]
                            }
                        }
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

            # account_count와 region_count 계산 추가
            result["account_count"] = len(result["accounts"])
            result["region_count"] = len(result["regions"])

            # date 필드 추가
            result["date"] = today.date().isoformat()

            # _id 필드 제거 (필요 없는 필드)
            if "_id" in result:
                del result["_id"]

            logger.info(f"{today.date().isoformat()} 일자 인스턴스 통계 조회 완료")
            logger.info(f"- 총 인스턴스 수: {result['total_instances']}")
            logger.info(f"- 개발 인스턴스: {result['dev_instances']}")
            logger.info(f"- 운영 인스턴스: {result['prd_instances']}")
            logger.info(f"- 계정 수: {result['account_count']}")
            logger.info(f"- 리전 수: {result['region_count']}")

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

            # 추가된 인스턴스 생성일자 포함
            added_instances = list(last_day_instances - first_day_instances)
            added_details = await self._get_instance_creation_dates(collection, added_instances)

            # 제거된 인스턴스 삭제일자 포함
            removed_instances = list(first_day_instances - last_day_instances)
            removed_details = await self._get_instance_deletion_dates(collection, removed_instances, last_date)

            logger.info(f"첫날 ({first_date}) 인스턴스 수: {len(first_day_instances)}")
            logger.info(f"마지막날 ({last_date}) 인스턴스 수: {len(last_day_instances)}")
            logger.info(f"추가된 인스턴스: {len(added_instances)}")
            logger.info(f"제거된 인스턴스: {len(removed_instances)}")

            return {
                "year": query_start.year,
                "month": query_start.month,
                "total_instances_start": len(first_day_instances),
                "total_instances_end": len(last_day_instances),
                "instances_added": added_details,
                "instances_removed": removed_details,
                "data_range": {
                    "start": first_date,
                    "end": last_date
                }
            }

        except Exception as e:
            logger.error(f"기간별 통계 조회 중 오류 발생: {str(e)}")
            raise

    async def _get_instance_creation_dates(self, collection, instance_ids: List[str]) -> List[Dict[str, Any]]:
        """추가된 인스턴스의 생성일자 조회"""
        pipeline = [
            {"$unwind": "$instances"},
            {
                "$match": {
                    "instances.DBInstanceIdentifier": {"$in": instance_ids}
                }
            },
            {
                "$group": {
                    "_id": "$instances.DBInstanceIdentifier",
                    "InstanceCreateTime": {"$first": "$instances.InstanceCreateTime"}
                }
            }
        ]
        result = await collection.aggregate(pipeline).to_list(None)
        return [
            {
                "id": r["_id"],
                "created_at": r["InstanceCreateTime"].split(" ")[0]  # 날짜만 추출
            }
            for r in result
        ]

    async def _get_instance_deletion_dates(self, collection, instance_ids: List[str], last_date: str) -> List[
        Dict[str, Any]]:
        """제거된 인스턴스의 삭제일자 조회"""
        result = []
        for instance_id in instance_ids:
            pipeline = [
                {"$unwind": "$instances"},
                {
                    "$match": {
                        "instances.DBInstanceIdentifier": instance_id,
                        "timestamp": {"$lte": f"{last_date} 23:59:59"}
                    }
                },
                {"$sort": {"timestamp": -1}},  # 최신 데이터 우선 정렬
                {"$limit": 1},  # 가장 최근의 데이터만 추출
                {
                    "$project": {
                        "_id": 0,
                        "id": "$instances.DBInstanceIdentifier",
                        "deleted_at": {"$substr": ["$timestamp", 0, 10]}  # 날짜만 추출
                    }
                }
            ]
            query_result = await collection.aggregate(pipeline).to_list(None)
            if query_result:
                result.extend(query_result)
        return result

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

