from __future__ import annotations

import re
from typing import Optional

from pyspark.sql import functions as F


class DriverAnalyticsService:
    """
    Service for answering driver analytics questions from the driver_performance table.

    Design:
    - Try deterministic Spark-based answers first for common question patterns
    - Fall back to LLM only when needed
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
        Return a small JSON sample from the driver table for LLM fallback context.
        Keep this tightly bounded to avoid excessive memory use.
        """
        df = self._base_df()

        if limit is None:
            limit = self.context_limit

        df_small = df.select(
            "driver_id",
            "total_fuel_cost",
            "total_distance_driven",
            "avg_mpg",
        ).limit(limit)

        return df_small.toPandas().to_json(orient="records")

    def _direct_analytics_answer(self, user_query: str) -> Optional[str]:
        """
        Handle common analytics questions directly with Spark aggregations/orderings.
        """
        df = self._base_df()
        q = user_query.lower().strip()

        if any(phrase in q for phrase in ["highest avg mpg", "best avg mpg"]):
            row = df.orderBy(F.col("avg_mpg").desc()).first()
            if row:
                return (
                    f"Driver {row['driver_id']} has the highest average MPG "
                    f"at {row['avg_mpg']:.2f}."
                )

        if any(phrase in q for phrase in ["lowest avg mpg", "worst avg mpg"]):
            row = df.orderBy(F.col("avg_mpg").asc()).first()
            if row:
                return (
                    f"Driver {row['driver_id']} has the lowest average MPG "
                    f"at {row['avg_mpg']:.2f}."
                )

        if any(phrase in q for phrase in ["highest fuel cost", "most fuel cost"]):
            row = df.orderBy(F.col("total_fuel_cost").desc()).first()
            if row:
                return (
                    f"Driver {row['driver_id']} has the highest fuel cost "
                    f"at {row['total_fuel_cost']:.2f}."
                )

        if any(phrase in q for phrase in ["lowest fuel cost", "least fuel cost"]):
            row = df.orderBy(F.col("total_fuel_cost").asc()).first()
            if row:
                return (
                    f"Driver {row['driver_id']} has the lowest fuel cost "
                    f"at {row['total_fuel_cost']:.2f}."
                )

        if any(
            phrase in q for phrase in ["highest distance driven", "most distance driven"]
        ):
            row = df.orderBy(F.col("total_distance_driven").desc()).first()
            if row:
                return (
                    f"Driver {row['driver_id']} has the highest distance driven "
                    f"at {row['total_distance_driven']:.2f}."
                )

        if any(
            phrase in q for phrase in ["lowest distance driven", "least distance driven"]
        ):
            row = df.orderBy(F.col("total_distance_driven").asc()).first()
            if row:
                return (
                    f"Driver {row['driver_id']} has the lowest distance driven "
                    f"at {row['total_distance_driven']:.2f}."
                )

        if "average mpg" in q:
            avg_val = df.select(F.avg("avg_mpg")).first()[0]
            if avg_val is not None:
                return f"The average MPG across all drivers is {avg_val:.2f}."

        if "average fuel cost" in q:
            avg_val = df.select(F.avg("total_fuel_cost")).first()[0]
            if avg_val is not None:
                return f"The average fuel cost across all drivers is {avg_val:.2f}."

        if "average distance" in q:
            avg_val = df.select(F.avg("total_distance_driven")).first()[0]
            if avg_val is not None:
                return (
                    f"The average distance driven across all drivers is {avg_val:.2f}."
                )

        match = re.search(r"(?:driver|driver id)\s+(\S+)", q)
        if match:
            driver_id = match.group(1)
            row = df.filter(F.col("driver_id").cast("string") == driver_id).first()
            if row:
                return (
                    f"Driver {row['driver_id']} stats: "
                    f"fuel cost: {row['total_fuel_cost']:.2f}, "
                    f"distance: {row['total_distance_driven']:.2f}, "
                    f"MPG: {row['avg_mpg']:.2f}."
                )

        return None

    def _llm_analytics_answer(self, user_query: str) -> str:
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
        Public entrypoint for answering user questions.
        """
        direct_answer = self._direct_analytics_answer(user_query)
        if direct_answer:
            return direct_answer

        return self._llm_analytics_answer(user_query)
