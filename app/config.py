import os
from dataclasses import dataclass


def _get_bool_env(name: str, default: str = "true") -> bool:
    return os.getenv(name, default).strip().lower() in {"true", "1", "yes", "y"}


@dataclass(frozen=True)
class Settings:
    openai_api_key: str | None = os.getenv("OPENAI_API_KEY")
    openai_model: str = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

    uc_catalog: str = os.getenv("UC_CATALOG", "vehicle_transactions")
    uc_schema: str = os.getenv("UC_SCHEMA", "gold")

    driver_table: str = os.getenv("DRIVER_TABLE", "driver_performance")
    vehicle_table: str = os.getenv("VEHICLE_TABLE", "vehicle_efficiency")
    fleet_summary_table: str = os.getenv("FLEET_SUMMARY_TABLE", "daily_fleet_summary")

    enable_llm_fallback: bool = _get_bool_env("ENABLE_LLM_FALLBACK", "true")


settings = Settings()
