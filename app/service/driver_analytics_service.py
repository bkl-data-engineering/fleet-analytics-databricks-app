class DriverAnalyticsService:
    def __init__(self, db_client, llm_client) -> None:
        self.db_client = db_client
        self.llm_client = llm_client

    def answer_question(self, question: str) -> str:
        normalized = question.lower()

        df = self.db_client.read_table("driver_performance")

        if "highest fuel cost" in normalized:
            top_row = (
                df.orderBy(df.total_fuel_cost.desc())
                .select("driver_id", "total_fuel_cost")
                .first()
            )
            if top_row:
                return (
                    f"Driver {top_row['driver_id']} has the highest fuel cost: "
                    f"{top_row['total_fuel_cost']}"
                )

        prompt = (
            "You are answering a fleet analytics question using the driver_performance table. "
            f"Question: {question}"
        )
        return self.llm_client.ask(prompt)
