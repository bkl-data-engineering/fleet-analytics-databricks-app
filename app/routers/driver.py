from fastapi import APIRouter, Depends
from pydantic import BaseModel

from app.dependencies import get_databricks_client, get_llm_client
from app.services.driver_analytics_service import DriverAnalyticsService

router = APIRouter()


class QuestionRequest(BaseModel):
    question: str


@router.post("/driver")
def ask_driver(
    request: QuestionRequest,
    db_client=Depends(get_databricks_client),
    llm_client=Depends(get_llm_client),
) -> dict[str, str]:
    service = DriverAnalyticsService(db_client=db_client, llm_client=llm_client)
    answer = service.answer_question(request.question)
    return {"answer": answer}
