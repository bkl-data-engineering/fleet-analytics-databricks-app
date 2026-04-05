from fastapi import FastAPI
from app.routers import driver, vehicle, fleet_summary

app = FastAPI(
    title="Fleet Analytics Databricks App",
    description="FastAPI app for fleet analytics on Databricks",
    version="0.1.0",
)

@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}

app.include_router(driver.router, prefix="/ask", tags=["driver"])
app.include_router(vehicle.router, prefix="/ask", tags=["vehicle"])
app.include_router(fleet_summary.router, prefix="/ask", tags=["fleet-summary"])
