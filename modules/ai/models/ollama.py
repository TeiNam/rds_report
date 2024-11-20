import aiohttp
from typing import Optional
from modules.ai.models.interface import AIModel
from modules.ai.exceptions import AIModuleException
from configs.ai_conf import get_ai_config


class OllamaModel(AIModel):
    def __init__(self) -> None:
        self.config = get_ai_config()
        self.base_url = self.config.OLLAMA_BASE_URL
        self.model_name = self.config.OLLAMA_MODEL_NAME

    async def initialize(self) -> None:
        if not await self.is_available():
            raise AIModuleException("Ollama 서비스를 사용할 수 없습니다.")

    async def generate_text(self, prompt: str, **kwargs) -> str:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                    f"{self.base_url}/generate",
                    json={
                        "model": self.model_name,
                        "prompt": prompt,
                        "options": kwargs
                    }
            ) as response:
                if response.status == 200:
                    result = await response.json()
                    return result.get("response", "")
                raise AIModuleException(f"Ollama 오류: {await response.text()}")

    async def is_available(self) -> bool:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{self.base_url}/version") as response:
                    return response.status == 200
        except:
            return False