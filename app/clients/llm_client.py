from openai import OpenAI


class LLMClient:
    def __init__(self, settings) -> None:
        self.enabled = bool(settings.openai_api_key) and settings.enable_llm_fallback
        self.model = settings.openai_model
        self.client = OpenAI(api_key=settings.openai_api_key) if self.enabled else None

    def ask(self, prompt: str) -> str:
        if not self.enabled or self.client is None:
            return "LLM fallback is disabled."

        response = self.client.responses.create(
            model=self.model,
            input=prompt,
        )
        return response.output_text
