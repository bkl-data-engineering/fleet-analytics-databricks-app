class VehicleEfficiencyService:
    def __init__(self, db_client, llm_client) -> None:
        self.db_client = db_client
        self.llm_client = llm_client

    def answer_question(self, question: str) -> str:
        normalized = question.lower()

        df = self.db_client.read_table("vehicle_efficiency")

        if "highest fuel cost" in normalized:
            top_row = (
                df.orderBy(df.total_fuel_used.desc())
                .select("vehicle_id", "total_fuel_used")
                .first()
            )
            if top_row:
                return (
                    f"Vehicle {top_row['vehicle_id']} has the highest fuel cost: "
                    f"{top_row['total_fuel_used']}"
                )

        prompt = (
            "You are answering a fleet analytics question using the vehicle_efficiency table. "
            f"Question: {question}"
        )
        return self.llm_client.ask(prompt)
