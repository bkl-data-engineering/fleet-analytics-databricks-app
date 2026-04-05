from fastapi import APIRouter, Depends
from pydantic import BaseModel

from app.config import settings
from app.dependencies import get_databricks_client, get_llm_client
from app.services.fleet_daily_summary_service import FleetDailySummaryService


router = APIRouter()


class QuestionRequest(BaseModel):
    question: str


class QuestionResponse(BaseModel):
    answer: str


@router.post("/fleet-daily-summary", response_model=QuestionResponse)
def ask_fleet_summary(
    request: QuestionRequest,
    db_client=Depends(get_databricks_client),
    llm_client=Depends(get_llm_client),
) -> QuestionResponse:
    service = FleetDailySummaryService(
        db_client=db_client,
        llm_client=llm_client,
        table_name=settings.fleet_summary_table,
    )
    answer = service.answer_question(request.question)
    return QuestionResponse(answer=answer)
