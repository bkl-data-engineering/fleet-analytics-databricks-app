from fastapi import APIRouter, Depends
from pydantic import BaseModel

from app.dependencies import get_databricks_client, get_llm_client
from app.services.fleet_daily_summary_service import FleetDailySummaryService

router = APIRouter()


class QuestionRequest(BaseModel):
    question: str


@router.post("/fleet-daily-summary")
def ask_fleet_summary(
    request: QuestionRequest,
    db_client=Depends(get_databricks_client),
    llm_client=Depends(get_llm_client),
) -> dict[str, str]:
    service = FleetDailySummaryService(db_client=db_client, llm_client=llm_client)
    answer = service.answer_question(request.question)
    return {"answer": answer}
