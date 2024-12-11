import logging
import os
from typing import Optional, Dict, Any
from abc import ABC, abstractmethod
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class BaseSlowQueryTool(ABC):
    """슬로우 쿼리 도구 기본 클래스"""

    def __init__(self):
        self._aws_secrets = {}  # 캐시된 시크릿 정보
        self._aws_session = None

    async def _get_db_password(self, instance_id: str) -> str:
        """
        데이터베이스 비밀번호 조회

        Args:
            instance_id: RDS 인스턴스 ID

        Returns:
            str: 데이터베이스 비밀번호

        Note:
            AWS Secrets Manager에서 비밀번호를 가져오거나
            환경 변수에서 가져오는 등의 방식으로 구현
        """
        try:
            # 캐시된 비밀번호가 있으면 반환
            if instance_id in self._aws_secrets:
                return self._aws_secrets[instance_id]

            # 환경 변수에서 비밀번호 조회 (개발/테스트용)
            if os.getenv('ENVIRONMENT') == 'development':
                password = os.getenv('DB_PASSWORD')
                if password:
                    self._aws_secrets[instance_id] = password
                    return password

            # AWS Secrets Manager에서 비밀번호 조회
            secret_id = f"rds/{instance_id}/admin"
            session = await self._get_aws_session()
            client = session.client('secretsmanager')

            response = client.get_secret_value(SecretId=secret_id)
            if 'SecretString' in response:
                self._aws_secrets[instance_id] = response['SecretString']
                return response['SecretString']

            raise ValueError(f"비밀번호를 찾을 수 없음: {instance_id}")

        except ClientError as e:
            logger.error(f"Secrets Manager 접근 실패: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"비밀번호 조회 실패: {str(e)}")
            raise

    async def _get_aws_session(self):
        """AWS 세션 반환"""
        if self._aws_session is None:
            self._aws_session = boto3.Session()
        return self._aws_session

    @abstractmethod
    async def _get_collection(self):
        """MongoDB 컬렉션 반환"""
        pass

    def _extract_database_name(self, query: str) -> Optional[str]:
        """
        쿼리에서 데이터베이스명 추출

        Args:
            query: SQL 쿼리

        Returns:
            Optional[str]: 데이터베이스명
        """
        import re

        # USE 문에서 추출
        use_match = re.search(r"USE\s+`?(\w+)`?", query, re.IGNORECASE)
        if use_match:
            return use_match.group(1)

        # FROM/JOIN 절에서 추출
        db_match = re.search(r"FROM\s+`?(\w+)`?\.", query, re.IGNORECASE)
        if db_match:
            return db_match.group(1)

        return None

    def _is_valid_query(self, query: str) -> bool:
        """
        쿼리 유효성 검사

        Args:
            query: SQL 쿼리

        Returns:
            bool: 유효성 여부
        """
        if not query or not isinstance(query, str):
            return False

        # 기본적인 SQL 키워드 체크
        valid_keywords = {'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'ALTER', 'DROP'}
        first_word = query.strip().split()[0].upper()

        return first_word in valid_keywords