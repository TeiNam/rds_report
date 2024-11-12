# modules/aws_session_manager.py

import boto3
import os
import json
import subprocess
import pytz
from datetime import datetime, timedelta
from botocore.exceptions import ClientError
from typing import Optional, Dict, Any, List, Literal
from enum import Enum
import logging
from pathlib import Path
import configparser
from modules.instance_fetcher import InstanceFetcher, InstanceQueryResult, AccountInfo

logger = logging.getLogger(__name__)


class EnvironmentType(Enum):
    LOCAL = "local"
    EC2 = "ec2"
    EKS = "eks"


class AWSSSOConfig:
    """기본 SSO 설정"""
    SSO_START_URL = "https://torder.awsapps.com/start"
    SSO_REGION = "ap-northeast-2"
    DEFAULT_REGION = "ap-northeast-2"
    ROLE_NAME = "AdministratorAccess"


class AWSSSOLogin:
    """AWS SSO 로그인 처리 클래스"""

    def __init__(self, profile_name: str = "default", region: str = "ap-northeast-2"):
        self.profile_name = profile_name
        self.region = region
        self.cache_dir = Path.home() / '.aws' / 'sso' / 'cache'
        self._config = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        """AWS config 파일에서 SSO 설정을 읽어옵니다."""
        config = configparser.ConfigParser()
        config_path = Path.home() / '.aws' / 'config'

        if not config_path.exists():
            raise FileNotFoundError("AWS config file not found")

        config.read(config_path)

        # 프로파일 섹션 이름 결정
        if self.profile_name == "default":
            section_name = "default"
        else:
            section_name = f"profile {self.profile_name}"

        if section_name not in config:
            raise ValueError(f"Profile '{self.profile_name}' not found in AWS config")

        return dict(config[section_name])

    def _get_cached_credentials(self) -> Optional[Dict[str, Any]]:
        """SSO 캐시에서 자격 증명을 찾습니다."""
        if not self.cache_dir.exists():
            return None

        try:
            # 가장 최근의 캐시 파일 찾기
            latest_cache = None
            latest_time = datetime.min.replace(tzinfo=pytz.UTC)

            for cache_file in self.cache_dir.glob('*.json'):
                try:
                    with open(cache_file, 'r') as f:
                        cache_data = json.load(f)
                        if 'expiresAt' in cache_data:
                            # ISO 형식의 시간을 UTC 시간으로 파싱
                            expires_at = datetime.fromisoformat(
                                cache_data['expiresAt'].replace('Z', '+00:00')
                            ).astimezone(pytz.UTC)

                            if expires_at > latest_time:
                                latest_time = expires_at
                                latest_cache = cache_data
                except (json.JSONDecodeError, KeyError, ValueError):
                    continue

            return latest_cache

        except Exception as e:
            logger.error(f"캐시 처리 중 오류 발생: {str(e)}")
            return None

    def _ensure_sso_login(self) -> None:
        """SSO 로그인 상태를 확인하고 필요한 경우 로그인을 수행합니다."""
        try:
            # 캐시된 자격 증명 확인
            cached_creds = self._get_cached_credentials()

            if cached_creds and 'expiresAt' in cached_creds:
                # 현재 시간을 UTC로 변환
                now = datetime.now(pytz.UTC)
                # 만료 시간을 UTC로 파싱
                expires_at = datetime.fromisoformat(
                    cached_creds['expiresAt'].replace('Z', '+00:00')
                ).astimezone(pytz.UTC)

                if now < expires_at:
                    logger.debug("Using cached SSO credentials")
                    return

            logger.info(f"AWS SSO 로그인 필요: {self.profile_name}")

            # AWS CLI를 통한 SSO 로그인 실행
            try:
                subprocess.run(
                    ["aws", "sso", "login", "--profile", self.profile_name],
                    check=True
                )
                logger.info(f"AWS SSO 로그인 성공: {self.profile_name}")

            except subprocess.CalledProcessError as e:
                logger.error(f"AWS SSO 로그인 실패: {str(e)}")
                raise

        except Exception as e:
            logger.error(f"SSO 로그인 처리 중 오류 발생: {str(e)}")
            raise

    def get_session(self) -> boto3.Session:
        """
        AWS 세션을 생성하고 반환합니다.
        SSO 로그인이 필요한 경우 자동으로 처리합니다.
        """
        self._ensure_sso_login()

        try:
            session = boto3.Session(
                profile_name=self.profile_name,
                region_name=self.region
            )
            # 세션 유효성 테스트
            session.client('sts').get_caller_identity()
            return session

        except Exception as e:
            logger.error(f"세션 생성 실패: {str(e)}")
            raise


class AWSSessionManager:
    def __init__(self):
        self.environment = self._detect_environment()
        self._sessions: Dict[str, boto3.Session] = {}
        self._instance_info: Optional[InstanceQueryResult] = None
        self.sso_config = AWSSSOConfig()

    def _detect_environment(self) -> EnvironmentType:
        """실행 환경 감지"""
        if os.path.exists("/var/run/secrets/kubernetes.io"):
            return EnvironmentType.EKS
        try:
            import requests
            requests.get("http://169.254.169.254/latest/meta-data/", timeout=1)
            return EnvironmentType.EC2
        except:
            return EnvironmentType.LOCAL

    async def initialize(self, env: Literal['prd', 'dev'], end_date: Optional[str] = None) -> None:
        """
        특정 날짜의 인스턴스 정보를 기준으로 세션 초기화

        Args:
            env: 환경 구분 ('prd' 또는 'dev')
            end_date: 기준 날짜 (YYYY-MM-DD), None일 경우 오늘 날짜
        """
        try:
            # 날짜 설정
            if end_date is None:
                end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")

            # 인스턴스 정보 조회
            self._instance_info = await InstanceFetcher.get_instances(
                env=env,
                start_date=start_date,
                end_date=end_date
            )

            if not self._instance_info.accounts:
                logger.warning(f"No instances found for environment '{env}' at {end_date}")
                return

            logger.info(f"Found {self._instance_info.total_instances} instances "
                        f"across {len(self._instance_info.accounts)} accounts "
                        f"for date {self._instance_info.latest_date}")

            # 각 계정별 세션 초기화
            for account in self._instance_info.accounts:
                await self._initialize_session(account)

        except Exception as e:
            logger.error(f"Failed to initialize AWS sessions: {e}")
            raise

    async def _initialize_session(self, account: AccountInfo) -> None:
        """계정별 세션 초기화"""
        try:
            if self.environment == EnvironmentType.LOCAL:
                session = self._get_sso_session(account.account_id)
            else:
                session = self._get_role_session(account.account_id)

            self._sessions[account.account_id] = session
            logger.info(f"Successfully initialized session for account: {account.account_id} "
                        f"(with {account.instance_count} instances)")

        except Exception as e:
            logger.error(f"Failed to initialize session for account {account.account_id}: {e}")
            raise

    def _get_sso_session(self, account_id: str) -> boto3.Session:
        """SSO 기반 세션 생성"""
        profile_name = f"AdministratorAccess-{account_id}"
        sso_login = AWSSSOLogin(
            profile_name=profile_name,
            region=self.sso_config.DEFAULT_REGION
        )
        return sso_login.get_session()

    def _get_role_session(self, account_id: str) -> boto3.Session:
        """IAM 역할 기반 세션 생성"""
        role_arn = f"arn:aws:iam::{account_id}:role/{self.sso_config.ROLE_NAME}"

        session = boto3.Session(region_name=self.sso_config.DEFAULT_REGION)
        sts = session.client('sts')

        response = sts.assume_role(
            RoleArn=role_arn,
            RoleSessionName=f"monitor-{account_id}"
        )

        credentials = response['Credentials']
        return boto3.Session(
            aws_access_key_id=credentials['AccessKeyId'],
            aws_secret_access_key=credentials['SecretAccessKey'],
            aws_session_token=credentials['SessionToken'],
            region_name=self.sso_config.DEFAULT_REGION
        )

    def get_session(self, account_id: str) -> Optional[boto3.Session]:
        """특정 계정의 세션 반환"""
        session = self._sessions.get(account_id)
        if not session:
            raise ValueError(f"No session found for account: {account_id}")
        return session

    def get_client(self, service_name: str, account_id: str, region: Optional[str] = None) -> Any:
        """특정 서비스의 클라이언트 반환"""
        session = self.get_session(account_id)
        return session.client(
            service_name,
            region_name=region or self.sso_config.DEFAULT_REGION
        )

    def get_resource(self, service_name: str, account_id: str, region: Optional[str] = None) -> Any:
        """특정 서비스의 리소스 반환"""
        session = self.get_session(account_id)
        return session.resource(
            service_name,
            region_name=region or self.sso_config.DEFAULT_REGION
        )

    def get_instance_info(self) -> Optional[InstanceQueryResult]:
        """현재 초기화된 인스턴스 정보 반환"""
        return self._instance_info


# 테스트를 위한 메인 코드
if __name__ == "__main__":
    import asyncio
    import logging
    from datetime import datetime

    # 로깅 설정
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


    async def test_sessions():
        try:
            # 세션 매니저 초기화
            logger.info("AWS Session Manager 초기화 중...")
            session_manager = AWSSessionManager()

            # 오늘 날짜 기준으로 프로덕션 환경 인스턴스 조회
            today = datetime.now().strftime("%Y-%m-%d")
            await session_manager.initialize(env='prd', end_date=today)

            instance_info = session_manager.get_instance_info()
            if not instance_info:
                logger.error("인스턴스 정보를 찾을 수 없습니다")
                return

            print("\n" + "=" * 80)
            print(f"AWS Session Manager 테스트 결과")
            print("=" * 80)
            print(f"조회 기준일: {instance_info.latest_date}")
            print(f"전체 계정 수: {len(instance_info.accounts)}")
            print(f"전체 인스턴스 수: {instance_info.total_instances}")
            print("-" * 80)

            # 계정별 정보 출력 및 테스트
            for account in instance_info.accounts:
                print(f"\n[계정: {account.account_id}]")
                print(f"인스턴스 수: {account.instance_count}")

                # STS 테스트로 세션 확인
                try:
                    sts = session_manager.get_client('sts', account.account_id)
                    identity = sts.get_caller_identity()
                    print(f"세션 확인 완료:")
                    print(f"  - Account: {identity['Account']}")
                    print(f"  - UserID: {identity['UserId']}")
                    print(f"  - ARN: {identity['Arn']}")
                except Exception as e:
                    logger.error(f"세션 테스트 실패: {str(e)}")
                    continue

                # 인스턴스 정보 출력
                if account.instances:
                    print("\n인스턴스 목록:")
                    for idx, instance in enumerate(account.instances, 1):
                        print(f"\n  {idx}. {instance.instance_identifier}")
                        print(f"     Region: {instance.region}")
                        print(f"     Tags: {instance.tags}")

                        try:
                            # RDS 상세 정보 조회
                            rds = session_manager.get_client(
                                'rds',
                                account.account_id,
                                instance.region
                            )
                            response = rds.describe_db_instances(
                                DBInstanceIdentifier=instance.instance_identifier
                            )
                            db = response['DBInstances'][0]

                            print(f"     상태: {db['DBInstanceStatus']}")
                            print(f"     엔진: {db['Engine']} {db.get('EngineVersion', 'N/A')}")
                            print(f"     크기: {db['DBInstanceClass']}")
                            print(f"     스토리지: {db.get('AllocatedStorage', 'N/A')} GB")
                            if 'Endpoint' in db:
                                print(f"     엔드포인트: {db['Endpoint'].get('Address', 'N/A')}")
                            print(f"     다중 AZ: {db.get('MultiAZ', False)}")

                        except ClientError as e:
                            error_code = e.response['Error']['Code']
                            error_msg = e.response['Error']['Message']
                            print(f"     [오류] {error_code}: {error_msg}")
                        except Exception as e:
                            print(f"     [오류] 인스턴스 정보 조회 실패: {str(e)}")

                print("-" * 80)

        except Exception as e:
            logger.error(f"테스트 중 오류 발생: {str(e)}")
            raise


    # 메인 함수 실행
    try:
        asyncio.run(test_sessions())
        print("\n테스트가 완료되었습니다.")
    except KeyboardInterrupt:
        print("\n사용자에 의해 테스트가 중단되었습니다.")
    except Exception as e:
        print(f"\n테스트 실행 중 오류가 발생했습니다: {str(e)}")
