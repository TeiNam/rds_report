# main.py

import logging
import sys
import importlib
from contextlib import asynccontextmanager
from pathlib import Path
from fastapi import FastAPI, APIRouter

# 프로젝트 루트 디렉토리를 Python 경로에 추가
project_root = str(Path(__file__).parent)
if project_root not in sys.path:
    sys.path.append(project_root)

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)


def auto_register_routers(app: FastAPI, apis_dir: str = "apis") -> None:
    """
    apis 디렉토리의 모든 라우터를 자동으로 등록

    Args:
        app: FastAPI 애플리케이션 인스턴스
        apis_dir: APIs 모듈이 있는 디렉토리 경로
    """
    try:
        # apis 디렉토리 경로 설정
        current_dir = Path(__file__).parent
        apis_path = current_dir / apis_dir

        if not apis_path.exists():
            logger.warning(f"APIs 디렉토리를 찾을 수 없습니다: {apis_path}")
            return

        # apis 디렉토리 내의 모든 Python 파일 검색
        for python_file in apis_path.glob("**/*.py"):
            if python_file.name.startswith("_"):
                continue

            # 파일 경로를 모듈 경로로 변환
            module_path = str(python_file.relative_to(current_dir))[:-3].replace("/", ".").replace("\\", ".")

            try:
                # 모듈 동적 로드
                module = importlib.import_module(module_path)

                # 모듈에서 라우터 찾기
                for attr_name in dir(module):
                    attr = getattr(module, attr_name)
                    if isinstance(attr, APIRouter):
                        # 라우터의 태그 설정
                        module_name = python_file.stem
                        if not attr.tags:
                            attr.tags = [module_name]

                        # 기존 prefix 제거 (만약 있다면)
                        if hasattr(attr, "prefix"):
                            original_prefix = attr.prefix
                            attr.prefix = ""  # 기존 prefix 제거
                        else:
                            original_prefix = ""

                        # 파일 경로에서 버전 정보 추출 (v1, v2 등)
                        relative_path = python_file.relative_to(apis_path)
                        version = relative_path.parts[0] if len(relative_path.parts) > 1 else ""

                        # 새로운 prefix 설정
                        if version:
                            new_prefix = f"/api/{version}{original_prefix}"
                        else:
                            new_prefix = f"/api{original_prefix}"

                        # 라우터 등록
                        app.include_router(attr, prefix=new_prefix)
                        logger.info(f"라우터 등록 완료: {module_name} ({new_prefix})")

            except Exception as e:
                logger.error(f"라우터 로드 중 오류 발생 ({module_path}): {str(e)}")
                logger.exception(e)

    except Exception as e:
        logger.error(f"라우터 자동 등록 중 오류 발생: {str(e)}")
        logger.exception(e)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI 애플리케이션 라이프사이클 관리
    """
    # 시작 시 실행
    logger.info("RDS Report Service가 시작되었습니다.")
    yield
    # 종료 시 실행
    logger.info("RDS Report Service가 종료됩니다.")


# FastAPI 애플리케이션 생성
app = FastAPI(
    title="RDS Report Service",
    description="AWS RDS 인스턴스 정보 수집 및 리포트 생성 서비스",
    version="1.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json",
    lifespan=lifespan  # 라이프사이클 이벤트 핸들러 등록
)

# 라우터 자동 등록
auto_register_routers(app)


# 헬스 체크 엔드포인트
@app.get("/health", tags=["health"])
async def health_check():
    return {
        "status": "healthy",
        "service": "rds-report-service"
    }


# 개발 서버 실행을 위한 코드
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )