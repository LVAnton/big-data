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

    @abstractmethod
    def upsert(self, table: str, data: Dict[str, Any], conflict_column: str) -> None:
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

        df = self.hook.get_pandas_df(sql, parameters=params)
        return df.to_dict(orient="records")

    def upsert(self, table: str, data: Dict[str, Any], conflict_column: str) -> None:
        keys = ', '.join(data.keys())
        vals = ', '.join(['%s'] * len(data))
        update = ', '.join([f"{k} = EXCLUDED.{k}" for k in data.keys() if k != conflict_column])
        sql = f"""
            INSERT INTO {table} ({keys}) VALUES ({vals})
            ON CONFLICT ({conflict_column}) DO UPDATE SET {update}
        """
        self.hook.run(sql, parameters=tuple(data.values()))


def get_postgres_client() -> PostgresInterface:
    return PostgresHookWrapper()
