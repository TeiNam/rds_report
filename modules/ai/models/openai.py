# ai/models/openai.py
from openai import AsyncOpenAI
from typing import Optional
from modules.ai.models.interface import AIModel
from modules.ai.exceptions import AIModuleException
from configs.ai_conf import get_ai_config


class OpenAIModel(AIModel):
    def __init__(self) -> None:
        self.config = get_ai_config()
        self.client: Optional[AsyncOpenAI] = None

    async def initialize(self) -> None:
        if not self.config.OPENAI_API_KEY:
            raise APIKeyNotFoundError("OpenAI API 키가 필요합니다.")

        self.client = AsyncOpenAI(api_key=self.config.OPENAI_API_KEY)

    async def generate_text(self, prompt: str, **kwargs) -> str:
        if not self.client:
            await self.initialize()

        try:
            response = await self.client.chat.completions.create(
                model=self.config.OPENAI_MODEL_NAME,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=kwargs.get("max_tokens", self.config.OPENAI_MAX_TOKENS),
                temperature=kwargs.get("temperature", self.config.OPENAI_TEMPERATURE)
            )
            return response.choices[0].message.content
        except Exception as e:
            raise AIModuleException(f"OpenAI 오류: {str(e)}")

    async def is_available(self) -> bool:
        try:
            await self.initialize()
            return True
        except:
            return False