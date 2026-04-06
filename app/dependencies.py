from app.clients.databricks_client import DatabricksClient
from app.clients.llm_client import LLMClient
from app.config import settings


def get_databricks_client() -> DatabricksClient:
    return DatabricksClient(settings)


def get_llm_client() -> LLMClient:
    return LLMClient(settings)
