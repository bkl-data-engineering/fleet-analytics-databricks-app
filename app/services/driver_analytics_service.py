from __future__ import annotations

import os
import re
from typing import Optional

from databricks.sdk import WorkspaceClient


class DriverAnalyticsService:
    def __init__(
        self,
        llm_client,
        context_limit: int = 20,
    ) -> None:
        self.llm_client = llm_client
        self.context_limit = context_limit

        catalog = os.getenv("UC_CATALOG")
        schema = os.getenv("UC_SCHEMA")
        table = os.getenv("DRIVER_TABLE")

        if not all([catalog, schema, table]):
            raise ValueError("Missing UC_CATALOG / UC_SCHEMA / DRIVER_TABLE env vars")

        self.table_name = f"{catalog}.{schema}.{table}"

        self.warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")
        if not self.warehouse_id:
            raise ValueError("Missing DATABRICKS_WAREHOUSE_ID")

        self.w = WorkspaceClient()

    def _to_float(self, value) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    def _run_sql(self, sql: str):
        print(f"[DEBUG] - sql: {sql}")

        response = self.w.statement_execution.execute_statement(
            warehouse_id=self.warehouse_id,
            statement=sql,
            wait_timeout="30s",
        )

        status = getattr(response.status, "state", None) if getattr(response, "status", None) else None
        print(f"[DEBUG] - statement status: {status}")
        print(f"[DEBUG] - statement id: {getattr(response, 'statement_id', None)}")

        if (
            response
            and getattr(response, "result", None)
            and getattr(response.result, "data_array", None)
        ):
            return response.result.data_array

        raise ValueError(
            f"No data returned from SQL execution. "
            f"status={status}, statement_id={getattr(response, 'statement_id', None)}"
        )

    def _direct_analytics_answer(self, user_query: str) -> Optional[str]:
        q = user_query.lower().strip()
        print(f"[DEBUG] - q: {q}")

        if "highest avg mpg" in q or "best avg mpg" in q:
            sql = f"""
                SELECT driver_id, avg_mpg
                FROM {self.table_name}
                ORDER BY avg_mpg DESC
                LIMIT 1
            """
            row = self._run_sql(sql)[0]
            return (
                f"Driver {row[0]} has the highest average MPG at "
                f"{self._to_float(row[1]):.2f}."
            )

        if "lowest avg mpg" in q or "worst avg mpg" in q:
            sql = f"""
                SELECT driver_id, avg_mpg
                FROM {self.table_name}
                ORDER BY avg_mpg ASC
                LIMIT 1
            """
            row = self._run_sql(sql)[0]
            return (
                f"Driver {row[0]} has the lowest average MPG at "
                f"{self._to_float(row[1]):.2f}."
            )

        if "highest fuel cost" in q or "most fuel cost" in q:
            sql = f"""
                SELECT driver_id, total_fuel_cost
                FROM {self.table_name}
                ORDER BY total_fuel_cost DESC
                LIMIT 1
            """
            row = self._run_sql(sql)[0]
            return (
                f"Driver {row[0]} has the highest fuel cost at "
                f"{self._to_float(row[1]):.2f}."
            )

        if "lowest fuel cost" in q or "least fuel cost" in q:
            sql = f"""
                SELECT driver_id, total_fuel_cost
                FROM {self.table_name}
                ORDER BY total_fuel_cost ASC
                LIMIT 1
            """
            row = self._run_sql(sql)[0]
            return (
                f"Driver {row[0]} has the lowest fuel cost at "
                f"{self._to_float(row[1]):.2f}."
            )

        if "highest distance driven" in q or "most distance driven" in q:
            sql = f"""
                SELECT driver_id, total_distance_driven
                FROM {self.table_name}
                ORDER BY total_distance_driven DESC
                LIMIT 1
            """
            row = self._run_sql(sql)[0]
            return (
                f"Driver {row[0]} has the highest distance driven at "
                f"{self._to_float(row[1]):.2f}."
            )

        if "lowest distance driven" in q or "least distance driven" in q:
            sql = f"""
                SELECT driver_id, total_distance_driven
                FROM {self.table_name}
                ORDER BY total_distance_driven ASC
                LIMIT 1
            """
            row = self._run_sql(sql)[0]
            return (
                f"Driver {row[0]} has the lowest distance driven at "
                f"{self._to_float(row[1]):.2f}."
            )

        if "average mpg" in q:
            sql = f"""
                SELECT AVG(avg_mpg)
                FROM {self.table_name}
            """
            val = self._run_sql(sql)[0][0]
            return f"The average MPG across all drivers is {self._to_float(val):.2f}."

        if "average fuel cost" in q:
            sql = f"""
                SELECT AVG(total_fuel_cost)
                FROM {self.table_name}
            """
            val = self._run_sql(sql)[0][0]
            return f"The average fuel cost across all drivers is {self._to_float(val):.2f}."

        if "average distance" in q:
            sql = f"""
                SELECT AVG(total_distance_driven)
                FROM {self.table_name}
            """
            val = self._run_sql(sql)[0][0]
            return (
                f"The average distance driven across all drivers is "
                f"{self._to_float(val):.2f}."
            )

        match = re.search(r"(?:driver|driver id)\s+(\S+)", q)
        if match:
            driver_id = match.group(1)

            sql = f"""
                SELECT driver_id, total_fuel_cost, total_distance_driven, avg_mpg
                FROM {self.table_name}
                WHERE CAST(driver_id AS STRING) = '{driver_id}'
                LIMIT 1
            """
            row = self._run_sql(sql)[0]
            return (
                f"Driver {row[0]} stats: "
                f"fuel cost: {self._to_float(row[1]):.2f}, "
                f"distance: {self._to_float(row[2]):.2f}, "
                f"MPG: {self._to_float(row[3]):.2f}."
            )

        return None

    def get_context_data(self) -> str:
        sql = f"""
            SELECT driver_id, total_fuel_cost, total_distance_driven, avg_mpg
            FROM {self.table_name}
            LIMIT {self.context_limit}
        """
        data = self._run_sql(sql)
        return str(data)

    def _llm_analytics_answer(self, user_query: str) -> str:
        context = self.get_context_data()

        prompt = f"""
You are a Fleet Analytics assistant.

Answer using only the provided data.
If the answer cannot be determined from the data, say you do not know.

Data:
{context}

Question:
{user_query}
""".strip()

        return self.llm_client.ask(prompt)

    def answer_question(self, user_query: str) -> str:
        direct_answer = self._direct_analytics_answer(user_query)
        if direct_answer:
            return direct_answer

        return self._llm_analytics_answer(user_query)
