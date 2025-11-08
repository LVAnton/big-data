from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from airflow.providers.mongo.hooks.mongo import MongoHook


class MongoInterface(ABC):
    @abstractmethod
    def insert_one(self, db: str, coll: str, doc: Dict[str, Any]) -> Any:
        ...

    @abstractmethod
    def upsert_one(self, db: str, coll: str, filter: Dict[str, Any], doc: Dict[str, Any]) -> None:
        ...

    @abstractmethod
    def find(self, db: str, coll: str, filter: Dict[str, Any], limit: Optional[int] = None) -> List[Dict[str, Any]]:
        ...

    @abstractmethod
    def aggregate(self, db: str, coll: str, pipeline: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        ...

    @abstractmethod
    def collection_exists(self, db: str, coll: str) -> bool:
        ...

    @abstractmethod
    def create_database(self, db: str) -> None:
        ...

    @abstractmethod
    def create_collection(self, db: str, coll: str) -> None:
        ...



class MongoHookWrapper(MongoInterface):
    def __init__(self, conn_id: str = "ods_mongo", database: Optional[str] = None):
        self.hook = MongoHook(conn_id=conn_id)
        self.client = self.hook.get_conn()
        self.database = database

    def _db(self, db: Optional[str]):
        return self.client[db or self.database or self.hook.get_default_database()]

    def insert_one(self, db: str, coll: str, doc: Dict[str, Any]) -> Any:
        return self._db(db)[coll].insert_one(doc)

    def upsert_one(self, db: str, coll: str, filter: Dict[str, Any], doc: Dict[str, Any]) -> None:
        self._db(db)[coll].update_one(filter, {"$set": doc}, upsert=True)

    def find(self, db: str, coll: str, filter: Dict[str, Any], limit: Optional[int] = None) -> List[Dict[str, Any]]:
        cur = self._db(db)[coll].find(filter)
        if limit:
            cur = cur.limit(limit)
        return list(cur)

    def aggregate(self, db: str, coll: str, pipeline: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        cur = self._db(db)[coll].aggregate(pipeline)
        return list(cur)

    def collection_exists(self, db: str, coll: str) -> bool:
        return coll in self._db(db).list_collection_names()

    def create_database(self, db: str) -> None:
        temp_coll = "_init_collection"
        if temp_coll not in self.client[db].list_collection_names():
            self.client[db][temp_coll].insert_one({"created": True})
            self.client[db][temp_coll].drop()

    def create_collection(self, db: str, coll: str) -> None:
        db_ref = self._db(db)
        if coll not in db_ref.list_collection_names():
            db_ref.create_collection(coll)

def get_mongo_client() -> MongoInterface:
    return MongoHookWrapper()