# configs/ai_conf.py
from functools import lru_cache
from typing import Dict, Any, Optional, Literal
from pydantic_settings import BaseSettings

ModelName = Literal[
    "gpt-4",  # 고성능, 복잡한 분석
    "gpt-3.5-turbo",  # 기본적인 분석, 빠른 응답
    "claude-3-haiku-20240307",  # 빠른 응답, 기본 분석
    "claude-3-sonnet-20240229",  # 중간 수준 분석, 균형잡힌 성능
    "anthropic.claude-v2",  # AWS Bedrock용 모델
    "llama2"  # 로컬 실행용
]


class AIConfig(BaseSettings):
    """AI 모델 관련 설정"""

    # 모델 선택 기준
    MODEL_QUALITY_PREFERENCE: Literal["fast", "balanced", "quality"] = "balanced"

    # OpenAI 설정
    OPENAI_API_KEY: Optional[str] = None
    # gpt-3.5-turbo: 빠른 처리, 기본적인 분석
    # gpt-4: 복잡한 분석, 고품질 결과
    OPENAI_MODEL_NAME: ModelName = "gpt-3.5-turbo"
    OPENAI_MAX_TOKENS: int = 4000
    OPENAI_TEMPERATURE: float = 0.3  # 리포트 생성을 위해 낮은 temperature

    # Anthropic 설정
    ANTHROPIC_API_KEY: Optional[str] = None
    # claude-3-haiku-20240307: 가장 빠른 처리, 기본적인 분석
    # claude-3-sonnet-20240229: 중간 수준, 균형잡힌 성능
    CLAUDE_MODEL_NAME: ModelName = "claude-3-sonnet-20240229"
    CLAUDE_MAX_TOKENS: int = 4000

    # AWS Bedrock 설정
    AWS_ACCESS_KEY_ID: Optional[str] = None
    AWS_SECRET_ACCESS_KEY: Optional[str] = None
    AWS_REGION: str = "us-east-1"
    BEDROCK_MODEL_ID: ModelName = "anthropic.claude-v2"

    # Ollama 설정
    OLLAMA_BASE_URL: str = "http://localhost:11434/api"
    OLLAMA_MODEL_NAME: ModelName = "EEVE-Korean-10.8B:latest"

    class Config:
        env_file = ".env"
        case_sensitive = False
        extra = "ignore"

    def get_recommended_model(self) -> Dict[str, Any]:
        """선호도에 따른 추천 모델 반환"""
        models = {
            "fast": {
                "provider": "anthropic",
                "model": "claude-3-haiku-20240307",
                "description": "빠른 응답 속도, 기본적인 데이터 분석"
            },
            "balanced": {
                "provider": "anthropic",
                "model": "claude-3-sonnet-20240229",
                "description": "균형 잡힌 성능, 상세한 분석"
            },
            "quality": {
                "provider": "openai",
                "model": "gpt-4",
                "description": "최고 품질의 분석, 복잡한 데이터 처리"
            }
        }
        return models[self.MODEL_QUALITY_PREFERENCE]

    def update_model_preference(self, preference: Literal["fast", "balanced", "quality"]):
        """모델 선호도 업데이트"""
        self.MODEL_QUALITY_PREFERENCE = preference
        model_info = self.get_recommended_model()

        if model_info["provider"] == "anthropic":
            self.CLAUDE_MODEL_NAME = model_info["model"]
        elif model_info["provider"] == "openai":
            self.OPENAI_MODEL_NAME = model_info["model"]


@lru_cache()
def get_ai_config() -> AIConfig:
    """AI 설정 싱글톤 인스턴스 반환"""
    return AIConfig()