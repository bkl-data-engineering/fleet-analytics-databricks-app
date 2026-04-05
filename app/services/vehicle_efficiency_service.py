from __future__ import annotations

from typing import Optional

from pyspark.sql import functions as F


class VehicleEfficiencyService:
    """
    Service for answering vehicle efficiency questions from the vehicle_efficiency table.

    Design:
    - Use deterministic Spark-based logic for common question patterns
    - Fall back to LLM only when direct handling is not available
    """

    def __init__(
        self,
        db_client,
        llm_client,
        table_name: str,
        context_limit: int = 20,
    ) -> None:
        self.db_client = db_client
        self.llm_client = llm_client
        self.table_name = table_name
        self.context_limit = context_limit

    def _base_df(self):
        return self.db_client.read_table(self.table_name)

    def get_context_data(self, limit: Optional[int] = None) -> str:
        """
        Return a small JSON sample from the vehicle table for LLM fallback context.
        Keep this tightly bounded to avoid excessive memory use.
        """
        df = self._base_df()

        if limit is None:
            limit = self.context_limit

        df_small = df.select(
            "vehicle_id",
            "total_fuel_used",
            "total_distance_driven",
            "avg_mpg",
        ).limit(limit)

        return df_small.toPandas().to_json(orient="records")

    def _direct_vehicle_answer(self, user_query: str) -> Optional[str]:
        """
        Handle common vehicle efficiency questions directly with Spark.
        """
        q = user_query.lower().strip()
        df = self._base_df()

        if any(
            phrase in q
            for phrase in ["highest avg mpg", "best avg mpg", "max avg mpg", "top avg mpg"]
        ):
            row = df.orderBy(F.col("avg_mpg").desc()).first()
            if row:
                return (
                    f"Vehicle {row['vehicle_id']} has the highest average MPG "
                    f"at {row['avg_mpg']:.2f}."
                )

        if any(
            phrase in q
            for phrase in ["lowest avg mpg", "worst avg mpg", "min avg mpg", "bottom avg mpg"]
        ):
            row = df.orderBy(F.col("avg_mpg").asc()).first()
            if row:
                return (
                    f"Vehicle {row['vehicle_id']} has the lowest average MPG "
                    f"at {row['avg_mpg']:.2f}."
                )

        if any(
            phrase in q
            for phrase in [
                "highest fuel used",
                "most fuel used",
                "max fuel used",
                "top fuel used",
            ]
        ):
            row = df.orderBy(F.col("total_fuel_used").desc()).first()
            if row:
                return (
                    f"Vehicle {row['vehicle_id']} has the highest fuel used "
                    f"at {row['total_fuel_used']:.2f}."
                )

        if any(
            phrase in q
            for phrase in [
                "lowest fuel used",
                "least fuel used",
                "min fuel used",
                "bottom fuel used",
            ]
        ):
            row = df.orderBy(F.col("total_fuel_used").asc()).first()
            if row:
                return (
                    f"Vehicle {row['vehicle_id']} has the lowest fuel used "
                    f"at {row['total_fuel_used']:.2f}."
                )

        if any(
            phrase in q for phrase in ["highest distance driven", "most distance driven"]
        ):
            row = df.orderBy(F.col("total_distance_driven").desc()).first()
            if row:
                return (
                    f"Vehicle {row['vehicle_id']} has the highest distance driven "
                    f"at {row['total_distance_driven']:.2f}."
                )

        if any(
            phrase in q for phrase in ["lowest distance driven", "least distance driven"]
        ):
            row = df.orderBy(F.col("total_distance_driven").asc()).first()
            if row:
                return (
                    f"Vehicle {row['vehicle_id']} has the lowest distance driven "
                    f"at {row['total_distance_driven']:.2f}."
                )

        if "average mpg" in q:
            avg_val = df.select(F.avg("avg_mpg")).first()[0]
            if avg_val is not None:
                return f"The average MPG across all vehicles is {avg_val:.2f}."

        if "average fuel used" in q:
            avg_val = df.select(F.avg("total_fuel_used")).first()[0]
            if avg_val is not None:
                return f"The average fuel used across all vehicles is {avg_val:.2f}."

        if "average distance" in q:
            avg_val = df.select(F.avg("total_distance_driven")).first()[0]
            if avg_val is not None:
                return (
                    f"The average distance driven across all vehicles is {avg_val:.2f}."
                )

        if "vehicle count" in q or "how many vehicles" in q:
            vehicle_count = df.count()
            return f"There are {vehicle_count} vehicles in the dataset."

        return None

    def _llm_fallback_answer(self, user_query: str) -> str:
        """
        Fall back to LLM using a small structured sample from the table.
        """
        context = self.get_context_data()

        prompt = f"""
You are a Fleet Analytics assistant.

Answer using only the provided JSON data.
If the answer cannot be determined from the data, say you do not know.

Data (JSON):
{context}

Question:
{user_query}
""".strip()

        return self.llm_client.ask(prompt)

    def answer_question(self, user_query: str) -> str:
        """
        Public entrypoint for answering vehicle efficiency questions.
        """
        direct_answer = self._direct_vehicle_answer(user_query)
        if direct_answer:
            return direct_answer

        return self._llm_fallback_answer(user_query)
