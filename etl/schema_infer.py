# etl/schema_infer.py
def infer_schema(docs):
    # docs: list of dicts
    schema = {"type": "object", "properties": {}}
    for d in docs:
        if not isinstance(d, dict):
            continue
        for k, v in d.items():
            if k.startswith("_"):  # skip meta keys
                continue
            t = type(v).__name__
            prop = schema["properties"].setdefault(k, {"types": set(), "example": None})
            prop["types"].add(t)
            if prop["example"] is None:
                prop["example"] = v
    # finalize: convert sets
    for k in list(schema["properties"].keys()):
        schema["properties"][k]["types"] = list(schema["properties"][k]["types"])
    return schema
