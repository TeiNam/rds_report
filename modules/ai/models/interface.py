from abc import ABC, abstractmethod
from typing import Optional, Dict, Any


class AIModel(ABC):
    """AI 모델 인터페이스"""

    @abstractmethod
    async def initialize(self) -> None:
        """모델 초기화 및 인증 설정"""
        pass

    @abstractmethod
    async def generate_text(self, prompt: str, **kwargs) -> str:
        """텍스트 생성"""
        pass

    @abstractmethod
    async def is_available(self) -> bool:
        """모델 사용 가능 여부 확인"""
        pass