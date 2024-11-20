# ai/models/bedrock.py
import boto3
import json
from typing import Optional
from modules.ai.models.interface import AIModel
from modules.ai.exceptions import AIModuleException
from configs.ai_conf import get_ai_config


class BedrockModel(AIModel):
    def __init__(self) -> None:
        self.config = get_ai_config()
        self.client = None

    async def initialize(self) -> None:
        if not self.config.AWS_ACCESS_KEY_ID or not self.config.AWS_SECRET_ACCESS_KEY:
            raise APIKeyNotFoundError("AWS 자격 증명이 필요합니다.")

        self.client = boto3.client(
            'bedrock-runtime',
            aws_access_key_id=self.config.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=self.config.AWS_SECRET_ACCESS_KEY,
            region_name=self.config.AWS_REGION
        )

    async def generate_text(self, prompt: str, **kwargs) -> str:
        if not self.client:
            await self.initialize()

        try:
            body = json.dumps({
                "prompt": prompt,
                "max_tokens_to_sample": kwargs.get("max_tokens", self.config.CLAUDE_MAX_TOKENS),
                "temperature": kwargs.get("temperature", 0.3),
            })

            response = self.client.invoke_model(
                modelId=self.config.BEDROCK_MODEL_ID,
                body=body
            )

            response_body = json.loads(response['body'].read())
            return response_body['completion']
        except Exception as e:
            raise AIModuleException(f"Bedrock 오류: {str(e)}")

    async def is_available(self) -> bool:
        try:
            await self.initialize()
            return True
        except:
            return False