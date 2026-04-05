from fastapi import APIRouter, Depends
from pydantic import BaseModel

from app.dependencies import get_databricks_client, get_llm_client
from app.services.vehicle_efficiency_service import VehicleEfficiencyService

router = APIRouter()


class QuestionRequest(BaseModel):
    question: str


@router.post("/vehicle")
def ask_vehicle(
    request: QuestionRequest,
    db_client=Depends(get_databricks_client),
    llm_client=Depends(get_llm_client),
) -> dict[str, str]:
    service = VehicleEfficiencyService(db_client=db_client, llm_client=llm_client)
    answer = service.answer_question(request.question)
    return {"answer": answer}
