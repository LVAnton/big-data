from abc import ABC, abstractmethod
from typing import Any, Optional, Dict, List, Tuple
from airflow.providers.postgres.hooks.postgres import PostgresHook


class PostgresInterface(ABC):
    @abstractmethod
    def run(self, sql: str, parameters: Optional[tuple] = None) -> Any:
        ...

    @abstractmethod
    def get_first(self, sql: str, parameters: Optional[tuple] = None) -> Any:
        ...

    @abstractmethod
    def insert(self, table: str, data: Dict[str, Any]) -> None:
        ...

    @abstractmethod
    def select(self,
               table: str,
               columns: Optional[List[str]] = None,
               where: Optional[str] = None,
               params: Optional[Tuple] = None
               ) -> List[Dict[str, Any]]:
        ...


class PostgresHookWrapper(PostgresInterface):
    def __init__(self, conn_id: str = "ods_postgres"):
        self.hook = PostgresHook(postgres_conn_id=conn_id)

    def run(self, sql: str, parameters: Optional[tuple] = None) -> Any:
        return self.hook.run(sql, parameters=parameters)

    def get_first(self, sql: str, parameters: Optional[tuple] = None) -> Any:
        return self.hook.get_first(sql, parameters=parameters)

    def insert(self, table: str, data: Dict[str, Any]) -> None:
        keys = ', '.join(data.keys())
        vals = ', '.join(['%s'] * len(data))
        sql = f"INSERT INTO {table} ({keys}) VALUES ({vals})"
        self.hook.run(sql, parameters=tuple(data.values()))

    def select(self, table: str, columns: Optional[List[str]] = None,
               where: Optional[str] = None, params: Optional[Tuple] = None) -> List[Dict[str, Any]]:
        cols = ', '.join(columns) if columns else '*'
        sql = f"SELECT {cols} FROM {table}"
        if where:
            sql += f" WHERE {where}"
        results = self.hook.get_records(sql, parameters=params)
        if not results:
            return []
        col_names = [desc[0] for desc in self.hook.get_conn().cursor().description]
        return [dict(zip(col_names, row)) for row in results]


def get_postgres_client() -> PostgresInterface:
    return PostgresHookWrapper()
