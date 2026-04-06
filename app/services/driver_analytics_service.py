from __future__ import annotations

import os
import re
from typing import Optional

from databricks.sdk import WorkspaceClient


class DriverAnalyticsService:
    def __init__(
        self,
        llm_client,
        table_name: str,
        warehouse_id: str,
        context_limit: int = 20,
    ) -> None:
        self.llm_client = llm_client
        self.table_name = table_name
        self.context_limit = context_limit

        self.w = WorkspaceClient()
        self.warehouse_id = warehouse_id

    def _run_sql(self, sql: str):
        response = self.w.statement_execution.execute_statement(
            warehouse_id=self.warehouse_id,
            statement=sql,
        )
        return response

    def _direct_analytics_answer(self, user_query: str) -> Optional[str]:
        q = user_query.lower().strip()

        if "highest avg mpg" in q or "best avg mpg" in q:
            sql = f"""
                SELECT driver_id, avg_mpg
                FROM {self.table_name}
                ORDER BY avg_mpg DESC
                LIMIT 1
            """
            result = self._run_sql(sql)
            row = result.result.data_array[0]
            return f"Driver {row[0]} has the highest average MPG at {row[1]:.2f}."

        if "lowest avg mpg" in q:
            sql = f"""
                SELECT driver_id, avg_mpg
                FROM {self.table_name}
                ORDER BY avg_mpg ASC
                LIMIT 1
            """
            result = self._run_sql(sql)
            row = result.result.data_array[0]
            return f"Driver {row[0]} has the lowest average MPG at {row[1]:.2f}."

        if "average mpg" in q:
            sql = f"""
                SELECT AVG(avg_mpg)
                FROM {self.table_name}
            """
            result = self._run_sql(sql)
            val = result.result.data_array[0][0]
            return f"The average MPG across all drivers is {val:.2f}."

        match = re.search(r"(?:driver|driver id)\s+(\S+)", q)
        if match:
            driver_id = match.group(1)

            sql = f"""
                SELECT driver_id, total_fuel_cost, total_distance_driven, avg_mpg
                FROM {self.table_name}
                WHERE driver_id = '{driver_id}'
                LIMIT 1
            """
            result = self._run_sql(sql)
            row = result.result.data_array[0]

            return (
                f"Driver {row[0]} stats: "
                f"fuel cost: {row[1]:.2f}, "
                f"distance: {row[2]:.2f}, "
                f"MPG: {row[3]:.2f}."
            )

        return None

    def get_context_data(self) -> str:
        sql = f"""
            SELECT driver_id, total_fuel_cost, total_distance_driven, avg_mpg
            FROM {self.table_name}
            LIMIT {self.context_limit}
        """
        result = self._run_sql(sql)
        return str(result.result.data_array)

    def _llm_analytics_answer(self, user_query: str) -> str:
        context = self.get_context_data()

        prompt = f"""
You are a Fleet Analytics assistant.

Answer using only the provided data.
If unknown, say you do not know.

Data:
{context}

Question:
{user_query}
""".strip()

        return self.llm_client.ask(prompt)

    def answer_question(self, user_query: str) -> str:
        direct = self._direct_analytics_answer(user_query)
        if direct:
            return direct

        return self._llm_analytics_answer(user_query)
