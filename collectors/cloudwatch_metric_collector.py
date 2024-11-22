# collectors/cloudwatch_collector.py

import logging
import asyncio
import pytz
from datetime import datetime, time, timedelta
from typing import Dict, List, Any, Optional
from botocore.exceptions import ClientError

from modules.aws_session_manager import AWSSessionManager
from modules.mongodb_connector import MongoDBConnector
from configs.cloudwatch_conf import CloudWatchSettings
from configs.mongo_conf import mongo_settings

logger = logging.getLogger(__name__)
kst = pytz.timezone('Asia/Seoul')

MAX_CONCURRENT_TASKS = 20

class RDSCloudWatchCollector:
    """RDS CloudWatch 메트릭 수집기"""

    def __init__(self, session_manager: AWSSessionManager):
        """
        Args:
            session_manager: AWS 세션 관리자 인스턴스
        """
        self.session_manager = session_manager
        self.settings = CloudWatchSettings()
        self._instance_info = self.session_manager.get_instance_info()
        self._metric_cache = {}
        self._cache_ttl = 3600  # 캐시 유효시간 (1시간)

    @property
    def collection_name(self) -> str:
        """MongoDB 컬렉션 이름 반환"""
        return mongo_settings.MONGO_MONTHLY_CW_RDS_METRIC_COLLECTION

    async def collect_metrics_monthly(
            self,
            year: int,
            month: int
    ) -> Dict[str, Any]:
        """
        월 단위 CloudWatch 메트릭 수집

        Args:
            year: 수집 연도
            month: 수집 월

        Returns:
            Dict[str, Any]: 계정별, 날짜별 수집된 메트릭 데이터
        """
        try:
            if not self._instance_info:
                raise ValueError("인스턴스 정보가 초기화되지 않았습니다")

            # 해당 월의 시작일과 마지막 일 계산
            start_date = datetime(year, month, 1)
            if month == 12:
                end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
            else:
                end_date = datetime(year, month + 1, 1) - timedelta(days=1)

            logger.info(
                f"{year}년 {month}월 메트릭 수집 시작: "
                f"{len(self._instance_info.accounts)}개 계정, "
                f"{self._instance_info.total_instances}개 인스턴스"
            )

            # 계정별로 메트릭 수집
            account_metrics = {}
            for account in self._instance_info.accounts:
                metrics = await self._collect_monthly_metrics(
                    account=account,
                    start_date=start_date,
                    end_date=end_date
                )
                if metrics:  # 수집된 메트릭이 있는 경우만 저장
                    account_metrics[account.account_id] = metrics

            if not account_metrics:
                logger.warning("수집된 메트릭이 없습니다")
                return {}

            # MongoDB에 저장
            await self._save_monthly_metrics(
                account_metrics=account_metrics,
                year=year,
                month=month
            )

            return account_metrics

        except Exception as e:
            logger.error(f"월간 메트릭 수집 중 오류 발생: {e}")
            raise

    MAX_CONCURRENT_TASKS = 20  # 클래스 변수로 추가

    # collectors/cloudwatch_metric_collector.py의 _collect_monthly_metrics 메서드 수정

    async def _collect_monthly_metrics(
            self,
            account: Any,
            start_date: datetime,
            end_date: datetime
    ) -> Dict[str, Dict]:
        """
        계정의 월간 메트릭 수집

        Args:
            account: 계정 정보
            start_date: 시작 날짜
            end_date: 종료 날짜

        Returns:
            Dict[str, Dict]: 인스턴스별 일일 메트릭
        """
        try:
            logger.info(
                f"\n[{account.account_id}] "
                f"{account.instance_count}개 인스턴스 처리 시작: "
                f"[{', '.join(inst.instance_identifier for inst in account.instances)}]"  # 인스턴스 목록 추가
            )

            monthly_metrics = {}
            current_date = start_date

            while current_date <= end_date:
                date_str = current_date.strftime('%Y-%m-%d')
                logger.info(f"\n[{account.account_id}] {date_str} 메트릭 수집 시작")

                # 인스턴스별 병렬 처리
                tasks = [
                    self._collect_instance_metrics(
                        account_id=account.account_id,
                        instance=instance,
                        date=current_date
                    )
                    for instance in account.instances
                ]

                instance_results = await asyncio.gather(*tasks, return_exceptions=True)

                # 결과 처리
                successful_instances = []  # 성공한 인스턴스 목록
                for instance, metrics in zip(account.instances, instance_results):
                    if isinstance(metrics, Exception):
                        logger.error(
                            f"인스턴스 {instance.instance_identifier} "
                            f"처리 실패: {metrics}"
                        )
                        continue

                    if metrics:
                        if instance.instance_identifier not in monthly_metrics:
                            monthly_metrics[instance.instance_identifier] = {}
                        monthly_metrics[instance.instance_identifier][date_str] = metrics
                        successful_instances.append(instance.instance_identifier)

                current_date += timedelta(days=1)
                logger.info(
                    f"[{account.account_id}] {date_str} 메트릭 수집 완료 "
                    f"({len(successful_instances)}/{len(account.instances)} 인스턴스): "
                    f"[{', '.join(sorted(successful_instances))}]"  # 성공한 인스턴스 목록
                )

            return monthly_metrics

        except Exception as e:
            logger.error(
                f"계정 {account.account_id}의 "
                f"월간 메트릭 수집 중 오류 발생: {e}"
            )
            raise

    def _chunk_instances(self, instances: List[Any], chunk_size: int) -> List[List[Any]]:
        """인스턴스 리스트를 청크로 분할"""
        return [instances[i:i + chunk_size] for i in range(0, len(instances), chunk_size)]

    async def _collect_instance_metrics(
            self,
            account_id: str,
            instance: Any,
            date: datetime
    ) -> Optional[Dict[str, Any]]:
        """
        인스턴스별 메트릭 수집

        Args:
            account_id: AWS 계정 ID
            instance: 인스턴스 정보
            date: 수집 대상 날짜

        Returns:
            Optional[Dict[str, Any]]: 수집된 메트릭 데이터
        """
        try:
            logger.debug(
                f"인스턴스 {instance.instance_identifier} "
                f"({instance.region}) 메트릭 수집 시작"
            )

            cloudwatch = self.session_manager.get_client(
                'cloudwatch',
                account_id,
                instance.region
            )

            # Aurora 여부 확인
            is_aurora = await self._check_aurora_instance(
                cloudwatch,
                instance.instance_identifier,
                date
            )

            # 수집할 메트릭 결정
            metrics_to_collect = (
                self.settings.METRICS if is_aurora
                else self.settings.COMMON_METRICS
            )

            # 메트릭 병렬 수집
            tasks = [
                self._get_metric_data(
                    cloudwatch,
                    metric,
                    instance.instance_identifier,
                    date
                )
                for metric in metrics_to_collect
            ]

            results = await asyncio.gather(*tasks)

            # 결과 처리
            instance_metrics = {}
            for metric, data in zip(metrics_to_collect, results):
                if data:
                    instance_metrics[metric] = data

            if instance_metrics:
                logger.debug(
                    f"✓ {instance.instance_identifier}: "
                    f"{len(instance_metrics)}/{len(metrics_to_collect)} "
                    f"메트릭 수집 완료"
                )
            else:
                logger.warning(
                    f"✗ {instance.instance_identifier}: "
                    f"수집된 메트릭 없음"
                )

            return instance_metrics if instance_metrics else None

        except Exception as e:
            logger.error(
                f"인스턴스 {instance.instance_identifier} "
                f"메트릭 수집 실패: {e}"
            )
            raise

    async def _check_aurora_instance(
            self,
            cloudwatch: Any,
            instance_id: str,
            date: datetime
    ) -> bool:
        """
        Aurora 인스턴스 여부 확인

        Args:
            cloudwatch: CloudWatch 클라이언트
            instance_id: 인스턴스 식별자
            date: 확인 대상 날짜

        Returns:
            bool: Aurora 여부
        """
        try:
            test_metrics = ['ServerlessDatabaseCapacity', 'AuroraReplicaLag']

            tasks = [
                self._get_metric_data(
                    cloudwatch,
                    metric,
                    instance_id,
                    date,
                    is_test=True
                )
                for metric in test_metrics
            ]

            results = await asyncio.gather(*tasks)

            return any(result is not None for result in results)

        except Exception as e:
            logger.error(
                f"Aurora 여부 확인 중 오류 발생 "
                f"(인스턴스: {instance_id}): {e}"
            )
            return False

    async def _get_metric_data(
            self,
            cloudwatch: Any,
            metric_name: str,
            instance_id: str,
            date: datetime,
            is_test: bool = False
    ) -> Optional[Dict[str, Any]]:
        """CloudWatch 메트릭 데이터 조회 (캐시 적용)"""
        try:
            # 캐시 키 생성
            cache_key = f"{instance_id}:{metric_name}:{date.strftime('%Y-%m-%d')}"

            # 캐시 확인
            cached_data = self._metric_cache.get(cache_key)
            if cached_data:
                cache_time, data = cached_data
                if (datetime.now() - cache_time).total_seconds() < self._cache_ttl:
                    return data

            # KST -> UTC 변환
            start_time_kst = datetime.combine(date.date(), time.min).replace(tzinfo=kst)
            end_time_kst = datetime.combine(date.date(), time.max).replace(tzinfo=kst)

            start_time_utc = start_time_kst.astimezone(pytz.UTC)
            end_time_utc = end_time_kst.astimezone(pytz.UTC)

            dimensions = [{
                'Name': 'DBInstanceIdentifier',
                'Value': instance_id
            }]

            response = await asyncio.to_thread(
                cloudwatch.get_metric_statistics,
                Namespace='AWS/RDS',
                MetricName=metric_name,
                Dimensions=dimensions,
                StartTime=start_time_utc,
                EndTime=end_time_utc,
                Period=86400,
                Statistics=['Average', 'Maximum', 'Minimum']
            )

            if not response['Datapoints']:
                if not is_test and metric_name in ['CPUUtilization', 'DatabaseConnections']:
                    logger.warning(
                        f"핵심 메트릭 {metric_name}에 대한 "
                        f"데이터가 없습니다 (인스턴스: {instance_id})"
                    )
                return None

            # 통계 계산
            result = self._calculate_statistics(response['Datapoints'])

            # 결과 캐싱
            if result:
                self._metric_cache[cache_key] = (datetime.now(), result)

            return result

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if not is_test and error_code != 'InvalidParameterCombination':
                logger.error(
                    f"CloudWatch API 오류 "
                    f"(메트릭: {metric_name}, 인스턴스: {instance_id}): {e}"
                )
            return None
        except Exception as e:
            if not is_test:
                logger.error(
                    f"예상치 못한 오류 "
                    f"(메트릭: {metric_name}, 인스턴스: {instance_id}): {e}"
                )
            return None

    def clear_cache(self):
        """캐시 초기화"""
        self._metric_cache.clear()

    def remove_expired_cache(self):
        """만료된 캐시 제거"""
        now = datetime.now()
        expired_keys = [
            key for key, (cache_time, _) in self._metric_cache.items()
            if (now - cache_time).total_seconds() >= self._cache_ttl
        ]
        for key in expired_keys:
            del self._metric_cache[key]

    def _calculate_statistics(
            self,
            datapoints: List[Dict]
    ) -> Dict[str, Any]:
        """
        메트릭 통계 계산

        Args:
            datapoints: CloudWatch 데이터 포인트 목록

        Returns:
            Dict[str, Any]: 계산된 통계값
        """
        if not datapoints:
            return None

        max_value = max(point['Maximum'] for point in datapoints)
        min_value = min(point['Minimum'] for point in datapoints)
        avg_value = sum(point['Average'] for point in datapoints) / len(datapoints)

        max_point = max(datapoints, key=lambda x: x['Maximum'])
        min_point = min(datapoints, key=lambda x: x['Minimum'])

        return {
            'max': {
                'value': float(max_value),
                'timestamp': max_point['Timestamp'].astimezone(kst).isoformat()
            },
            'min': {
                'value': float(min_value),
                'timestamp': min_point['Timestamp'].astimezone(kst).isoformat()
            },
            'avg': float(avg_value)
        }

    async def _save_monthly_metrics(
            self,
            account_metrics: Dict[str, Dict],
            year: int,
            month: int
    ) -> None:
        """
        월간 메트릭 데이터를 MongoDB에 저장 (계정별 도큐먼트)

        Args:
            account_metrics: 계정별 메트릭 데이터
            year: 수집 연도
            month: 수집 월
        """
        try:
            db = await MongoDBConnector.get_database()
            collection = db[self.collection_name]

            # 각 계정별로 하나의 도큐먼트로 저장
            for account_id, metrics in account_metrics.items():
                document = {
                    "env": self._instance_info.env,
                    "year": year,
                    "month": month,
                    "account_id": account_id,
                    "metrics": {
                        instance_id: {
                            "daily_metrics": daily_metrics,
                            "monthly_summary": self._calculate_monthly_summary(daily_metrics)
                        }
                        for instance_id, daily_metrics in metrics.items()
                    },
                    "created_at": datetime.now(kst).isoformat()
                }

                # 계정별 도큐먼트 upsert
                filter_doc = {
                    "env": document["env"],
                    "year": year,
                    "month": month,
                    "account_id": account_id
                }

                await collection.update_one(
                    filter_doc,
                    {"$set": document},
                    upsert=True
                )

                logger.info(
                    f"계정 {account_id}의 "
                    f"{year}년 {month}월 메트릭 저장 완료 "
                    f"({len(metrics)}개 인스턴스)"
                )

        except Exception as e:
            logger.error(f"MongoDB 저장 중 오류 발생: {e}")
            raise

    def _calculate_monthly_summary(self, daily_metrics: Dict) -> Dict:
        """
        일별 메트릭을 바탕으로 월간 요약 통계 계산

        Args:
            daily_metrics: 일별 메트릭 데이터

        Returns:
            Dict: 월간 요약 통계
        """
        monthly_summary = {}

        # 각 메트릭 유형별로 처리
        for metric_name in set().union(*(day_metrics.keys() for day_metrics in daily_metrics.values())):
            metric_values = []
            max_value = float('-inf')
            min_value = float('inf')
            max_timestamp = None
            min_timestamp = None

            # 각 일자별 메트릭 처리
            for date_metrics in daily_metrics.values():
                if metric_name in date_metrics:
                    daily_metric = date_metrics[metric_name]

                    # 최대값 업데이트
                    if daily_metric['max']['value'] > max_value:
                        max_value = daily_metric['max']['value']
                        max_timestamp = daily_metric['max']['timestamp']

                    # 최소값 업데이트
                    if daily_metric['min']['value'] < min_value:
                        min_value = daily_metric['min']['value']
                        min_timestamp = daily_metric['min']['timestamp']

                    # 평균값 계산을 위해 저장
                    metric_values.append(daily_metric['avg'])

            # 데이터가 있는 경우만 처리
            if metric_values:
                monthly_summary[metric_name] = {
                    'max': {
                        'value': float(max_value),
                        'timestamp': max_timestamp
                    },
                    'min': {
                        'value': float(min_value),
                        'timestamp': min_timestamp
                    },
                    'avg': float(sum(metric_values) / len(metric_values)),
                    'days_collected': len(metric_values)  # 데이터가 수집된 일수
                }

        return monthly_summary
