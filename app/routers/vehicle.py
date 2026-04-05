from fastapi import APIRouter, Depends
from pydantic import BaseModel

from app.config import settings
from app.dependencies import get_databricks_client, get_llm_client
from app.services.vehicle_efficiency_service import VehicleEfficiencyService


router = APIRouter()


class QuestionRequest(BaseModel):
    question: str


class QuestionResponse(BaseModel):
    answer: str


@router.post("/vehicle", response_model=QuestionResponse)
def ask_vehicle(
    request: QuestionRequest,
    db_client=Depends(get_databricks_client),
    llm_client=Depends(get_llm_client),
) -> QuestionResponse:
    service = VehicleEfficiencyService(
        db_client=db_client,
        llm_client=llm_client,
        table_name=settings.vehicle_table,
    )
    answer = service.answer_question(request.question)
    return QuestionResponse(answer=answer)
