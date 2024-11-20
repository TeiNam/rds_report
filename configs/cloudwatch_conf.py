from pydantic_settings import BaseSettings
from typing import List


class CloudWatchSettings(BaseSettings):
    AWS_REGION: str = "ap-northeast-2"

    # 모든 RDS 엔진 공통 메트릭
    COMMON_METRICS: List[str] = [
        "CPUUtilization",  # CPU 사용률 (%)
        "FreeableMemory",  # 사용 가능한 메모리 (bytes)
        "FreeStorageSpace",  # 사용 가능한 스토리지 공간 (bytes)
        "DatabaseConnections",  # 활성 데이터베이스 연결 수
        "ReadIOPS",  # 초당 디스크 읽기 작업 수
        "WriteIOPS",  # 초당 디스크 쓰기 작업 수
        "ReadLatency",  # 평균 디스크 읽기 지연 시간 (초)
        "WriteLatency",  # 평균 디스크 쓰기 지연 시간 (초)
        "NetworkReceiveThroughput",  # 네트워크 수신 처리량 (bytes/초)
        "NetworkTransmitThroughput",  # 네트워크 전송 처리량 (bytes/초)
        "DiskQueueDepth",  # 디스크 큐의 대기 중인 I/O 요청 수
        "SwapUsage"  # 스왑 사용량 (bytes)
    ]

    # Aurora MySQL 전용 메트릭
    AURORA_METRICS: List[str] = [
        # 쿼리 성능 관련
        "SelectLatency",  # SELECT 쿼리 지연 시간
        "SelectThroughput",  # SELECT 쿼리 처리량
        "CommitLatency",  # COMMIT 작업 지연 시간
        "CommitThroughput",  # COMMIT 작업 처리량
        "DDLLatency",  # DDL 작업 지연 시간
        "DDLThroughput",  # DDL 작업 처리량
        "DMLLatency",  # 전체 DML 작업 지연 시간
        "DMLThroughput",  # 전체 DML 작업 처리량
        "Deadlocks",  # 발생한 교착 상태(deadlock) 수
        "Queries",  # 초당 실행된 쿼리 수

        # Binlog 관련
        "AuroraBinlogReplicaLag",  # Binlog 복제 지연 시간
        "BinlogDiskUsage",  # Binlog가 사용하는 디스크 공간
        "ReplicalLag",  # 복제 지연 시간
    ]

    @property
    def METRICS(self) -> List[str]:
        """모든 수집 대상 메트릭 반환"""
        return self.COMMON_METRICS + self.AURORA_METRICS

    class Config:
        env_file = ".env"
        extra = "ignore"