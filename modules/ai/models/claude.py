# ai/models/claude.py
from anthropic import AsyncAnthropic
from typing import Optional
from modules.ai.models.interface import AIModel
from modules.ai.exceptions import AIModuleException
from configs.ai_conf import get_ai_config


class ClaudeModel(AIModel):
    def __init__(self) -> None:
        self.config = get_ai_config()
        self.client: Optional[AsyncAnthropic] = None

    async def initialize(self) -> None:
        if not self.config.ANTHROPIC_API_KEY:
            raise APIKeyNotFoundError("Anthropic API 키가 필요합니다.")

        self.client = AsyncAnthropic(api_key=self.config.ANTHROPIC_API_KEY)

    async def generate_text(self, prompt: str, **kwargs) -> str:
        if not self.client:
            await self.initialize()

        try:
            response = await self.client.messages.create(
                model=self.config.CLAUDE_MODEL_NAME,
                max_tokens=kwargs.get("max_tokens", self.config.CLAUDE_MAX_TOKENS),
                messages=[{"role": "user", "content": prompt}]
            )
            return response.content[0].text
        except Exception as e:
            raise AIModuleException(f"Claude 오류: {str(e)}")

    async def is_available(self) -> bool:
        try:
            await self.initialize()
            return True
        except:
            return False