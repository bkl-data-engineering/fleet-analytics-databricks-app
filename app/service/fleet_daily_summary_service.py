from __future__ import annotations

from typing import Optional
import re

from pyspark.sql import functions as F


class FleetDailySummaryService:
    """
    Service for querying the daily_fleet_summary table.

    Expected table schema:
      - trip_date
      - total_fuel_cost
      - total_distance_driven
      - total_transactions
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
        Return a small JSON sample ordered by most recent dates.
        """
        row_limit = limit or self.context_limit

        df_small = (
            self._base_df()
            .select(
                "trip_date",
                "total_fuel_cost",
                "total_distance_driven",
                "total_transactions",
            )
            .orderBy(F.col("trip_date").desc())
            .limit(row_limit)
        )

        return df_small.toPandas().to_json(orient="records", date_format="iso")

    def _direct_daily_answer(self, user_query: str) -> Optional[str]:
        """
        Handle common daily summary queries directly using Spark.
        """
        q = user_query.lower().strip()
        df = self._base_df()

        # Averages
        if any(
            phrase in q
            for phrase in [
                "average daily fuel cost",
                "avg daily fuel cost",
                "mean daily fuel cost",
            ]
        ):
            val = df.select(F.avg("total_fuel_cost")).first()[0]
            if val is not None:
                return f"The average daily fuel cost is {val:.2f}."

        if any(
            phrase in q
            for phrase in [
                "average daily distance",
                "avg daily distance",
                "mean daily distance",
            ]
        ):
            val = df.select(F.avg("total_distance_driven")).first()[0]
            if val is not None:
                return f"The average daily distance driven is {val:.2f}."

        if any(
            phrase in q
            for phrase in [
                "average daily transactions",
                "avg daily transactions",
                "mean daily transactions",
            ]
        ):
            val = df.select(F.avg("total_transactions")).first()[0]
            if val is not None:
                return f"The average daily transaction count is {val:.2f}."

        # Maximums
        if any(
            phrase in q
            for phrase in [
                "highest daily fuel cost",
                "max daily fuel cost",
                "most daily fuel cost",
            ]
        ):
            row = df.orderBy(F.col("total_fuel_cost").desc()).first()
            if row:
                return (
                    f"The highest daily fuel cost was {row['total_fuel_cost']:.2f} "
                    f"on {row['trip_date']}."
                )

        if any(
            phrase in q
            for phrase in [
                "highest daily distance",
                "max daily distance",
                "most daily distance",
            ]
        ):
            row = df.orderBy(F.col("total_distance_driven").desc()).first()
            if row:
                return (
                    f"The highest daily distance driven was "
                    f"{row['total_distance_driven']:.2f} on {row['trip_date']}."
                )

        if any(
            phrase in q
            for phrase in [
                "highest daily transactions",
                "max daily transactions",
                "most daily transactions",
            ]
        ):
            row = df.orderBy(F.col("total_transactions").desc()).first()
            if row:
                return (
                    f"The highest daily transaction count was "
                    f"{row['total_transactions']} on {row['trip_date']}."
                )

        # Minimums
        if any(
            phrase in q
            for phrase in [
                "lowest daily fuel cost",
                "min daily fuel cost",
                "least daily fuel cost",
            ]
        ):
            row = df.orderBy(F.col("total_fuel_cost").asc()).first()
            if row:
                return (
                    f"The lowest daily fuel cost was {row['total_fuel_cost']:.2f} "
                    f"on {row['trip_date']}."
                )

        if any(
            phrase in q
            for phrase in [
                "lowest daily distance",
                "min daily distance",
                "least daily distance",
            ]
        ):
            row = df.orderBy(F.col("total_distance_driven").asc()).first()
            if row:
                return (
                    f"The lowest daily distance driven was "
                    f"{row['total_distance_driven']:.2f} on {row['trip_date']}."
                )

        if any(
            phrase in q
            for phrase in [
                "lowest daily transactions",
                "min daily transactions",
                "least daily transactions",
            ]
        ):
            row = df.orderBy(F.col("total_transactions").asc()).first()
            if row:
                return (
                    f"The lowest daily transaction count was "
                    f"{row['total_transactions']} on {row['trip_date']}."
                )

        # Totals
        if "total fuel cost" in q and "all days" in q:
            val = df.select(F.sum("total_fuel_cost")).first()[0]
            if val is not None:
                return f"The total fuel cost across all days is {val:.2f}."

        if "total distance" in q and "all days" in q:
            val = df.select(F.sum("total_distance_driven")).first()[0]
            if val is not None:
                return f"The total distance driven across all days is {val:.2f}."

        if "total transactions" in q and "all days" in q:
            val = df.select(F.sum("total_transactions")).first()[0]
            if val is not None:
                return f"The total transaction count across all days is {val}."

        # Date-specific query
        match = re.search(r"\b(\d{4}-\d{2}-\d{2})\b", q)
        if match:
            trip_date = match.group(1)
            row = (
                df.filter(F.col("trip_date").cast("string") == trip_date)
                .select(
                    "trip_date",
                    "total_fuel_cost",
                    "total_distance_driven",
                    "total_transactions",
                )
                .first()
            )
            if row:
                return (
                    f"On {row['trip_date']}, total fuel cost was "
                    f"{row['total_fuel_cost']:.2f}, total distance driven was "
                    f"{row['total_distance_driven']:.2f}, and total transactions "
                    f"was {row['total_transactions']}."
                )

        # Count
        if any(p in q for p in ["how many days", "day count", "number of days"]):
            count_val = df.count()
            return f"There are {count_val} daily records in the fleet summary table."

        return None

    def _llm_fallback_answer(self, user_query: str) -> str:
        """
        Fall back to LLM using sampled JSON context.
        """
        context = self.get_context_data()

        prompt = f"""
You are a Fleet Daily Summary assistant.

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
        Public entrypoint.
        """
        direct_answer = self._direct_daily_answer(user_query)
        if direct_answer:
            return direct_answer

        return self._llm_fallback_answer(user_query)
