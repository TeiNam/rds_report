# modules/ai/factory.py
from typing import Dict, Type
from modules.ai.models.interface import AIModel
from modules.ai.models.ollama import OllamaModel
from modules.ai.models.bedrock import BedrockModel
from modules.ai.models.openai import OpenAIModel
from modules.ai.models.claude import ClaudeModel
from modules.ai.exceptions import ModelNotFoundError


class AIModelFactory:
    """AI 모델 생성을 위한 팩토리 클래스"""

    _models: Dict[str, Type[AIModel]] = {
        "ollama": OllamaModel,
        "bedrock": BedrockModel,
        "openai": OpenAIModel,
        "claude": ClaudeModel
    }

    @classmethod
    def get_model(cls, model_type: str) -> AIModel:
        """지정된 타입의 AI 모델 인스턴스 반환"""
        model_class = cls._models.get(model_type.lower())
        if not model_class:
            raise ModelNotFoundError(f"모델 타입 '{model_type}'를 찾을 수 없습니다")

        return model_class()

    @classmethod
    def available_models(cls) -> list[str]:
        """사용 가능한 모델 타입 목록 반환"""
        return list(cls._models.keys())