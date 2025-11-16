# etl/mongo_utils.py
from pymongo import MongoClient, errors
from typing import Dict, Any

def create_collection_with_validator(uri: str, db_name: str, collection_name: str, validator: Dict[str, Any], drop_if_exists: bool = False):
    """
    Creates collection with json schema validator. Returns dict with status.
    uri: mongodb connection string, e.g. "mongodb://localhost:27017"
    """
    try:
        client = MongoClient(uri)
        db = client[db_name]
        if drop_if_exists and collection_name in db.list_collection_names():
            db.drop_collection(collection_name)
        options = {"validator": validator} if validator else {}
        db.create_collection(collection_name, **options)
        return {"ok": True, "collection": f"{db_name}.{collection_name}"}
    except errors.CollectionInvalid:
        # collection exists
        return {"ok": False, "error": "collection exists"}
    except Exception as e:
        return {"ok": False, "error": str(e)}
