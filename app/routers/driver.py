from fastapi import APIRouter, Depends
from pydantic import BaseModel

from app.dependencies import get_llm_client
from app.services.driver_analytics_service import DriverAnalyticsService


router = APIRouter()


class QuestionRequest(BaseModel):
    question: str


class QuestionResponse(BaseModel):
    answer: str


@router.post("/driver", response_model=QuestionResponse)
def ask_driver(
    request: QuestionRequest,
    llm_client=Depends(get_llm_client),
) -> QuestionResponse:
    service = DriverAnalyticsService(
        llm_client=llm_client,
    )
    answer = service.answer_question(request.question)
    return QuestionResponse(answer=answer)
