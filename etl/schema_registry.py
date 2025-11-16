# etl/schema_registry.py
import os, json, datetime, hashlib, traceback
from etl.schema_infer import infer_schema

def _nowz():
    return datetime.datetime.utcnow().isoformat() + "Z"

# PROMOTION threshold: only include fields seen >= this many times in logical schema
MIN_PROMOTE_COUNT = 2

def _is_plausible_field_name(name: str) -> bool:
    if not isinstance(name, str):
        return False
    name = name.strip()
    if not name:
        return False
    # reject very long or binary-like names
    if len(name) > 120:
        return False
    # reject strings with many non-printable chars
    if any(ord(c) < 32 for c in name):
        return False
    return True

class SchemaRegistry:
    """
    Maintains:
     - per-upload instance schemas (timestamped files)
     - a single logical schema per source that evolves (logical.json)
    The logical schema fields track: name, types (set), count, first_seen, last_seen, example
    """
    def __init__(self, storage_root="store"):
        self.root = storage_root
        self.schemas_dir = os.path.join(self.root, "schemas")
        os.makedirs(self.schemas_dir, exist_ok=True)

    def _source_dir(self, source_id):
        if not source_id:
            source_id = "default"
        d = os.path.join(self.schemas_dir, source_id)
        os.makedirs(d, exist_ok=True)
        return d

    def _logical_path(self, source_id):
        return os.path.join(self._source_dir(source_id), "logical.json")

    def _instance_path(self, source_id, generated_at, schema_id):
        return os.path.join(self._source_dir(source_id), f"{generated_at}__{schema_id}.json")

    def _load_json(self, path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None

    def get_logical_schema(self, source_id):
        p = self._logical_path(source_id)
        return self._load_json(p)

    def get_schema_history(self, source_id):
        d = self._source_dir(source_id)
        files = []
        for f in sorted(os.listdir(d)):
            if f.endswith(".json") and f != "logical.json":
                j = self._load_json(os.path.join(d, f))
                if j:
                    files.append(j)
        return files

    def compute_diff(self, old_fields_map, new_fields_map):
        # old/new are dict maps name->fieldmeta
        added = [k for k in new_fields_map if k not in old_fields_map]
        removed = [k for k in old_fields_map if k not in new_fields_map]
        type_changes = []
        for k in new_fields_map:
            if k in old_fields_map:
                old_types = set(old_fields_map[k].get("types", []))
                new_types = set(new_fields_map[k].get("types", []))
                if old_types != new_types:
                    type_changes.append({"field": k, "old": list(old_types), "new": list(new_types)})
        return {"added": added, "removed": removed, "type_changes": type_changes}

    def _make_field_meta(self, name, types, example, seen_time):
        return {
            "name": name,
            "types": list(sorted(set(types))),
            "count": 1,
            "first_seen": seen_time,
            "last_seen": seen_time,
            "example": example
        }

    def _merge_field_meta(self, old_meta, new_meta):
        if not old_meta:
            return new_meta
        merged = {}
        merged["name"] = new_meta.get("name") or old_meta.get("name")
        merged["types"] = list(sorted(set(old_meta.get("types",[]) + new_meta.get("types",[]))))
        merged["count"] = old_meta.get("count",0) + new_meta.get("count",0)
        merged["first_seen"] = old_meta.get("first_seen") or new_meta.get("first_seen")
        merged["last_seen"] = new_meta.get("last_seen") or old_meta.get("last_seen")
        merged["example"] = old_meta.get("example") or new_meta.get("example")
        return merged

    def _properties_to_fields_map(self, properties_dict, seen_time):
        out = {}
        if not isinstance(properties_dict, dict):
            return out
        for k, v in properties_dict.items():
            try:
                types = v.get("types", []) if isinstance(v, dict) else []
                example = v.get("example") if isinstance(v, dict) else None
                out[k] = self._make_field_meta(k, types, example, seen_time)
            except Exception:
                continue
        return out

    def _fields_map_to_doc(self, fields_map):
        fields = []
        for k, meta in sorted(fields_map.items()):
            fields.append({
                "name": meta.get("name"),
                "types": meta.get("types", []),
                "count": meta.get("count", 0),
                "first_seen": meta.get("first_seen"),
                "last_seen": meta.get("last_seen"),
                "example": meta.get("example"),
            })
        return fields

    def evolve_schema(self, source_id, sample_docs, file_id=None):
        """
        Infer schema from sample_docs, create instance schema (unique id),
        and merge into logical schema for source (union / update counts).
        Returns logical_id, instance_id, logical_doc, instance_doc, diff
        """
        if sample_docs is None:
            sample_docs = []
        if not isinstance(sample_docs, list):
            sample_docs = [sample_docs]

        seen_time = _nowz()
        safe_docs = [d for d in sample_docs if isinstance(d, dict)]
        try:
            inferred = infer_schema(safe_docs)
        except Exception:
            inferred = {"type": "object", "properties": {}}
        properties = inferred.get("properties", {}) or {}

        # instance schema (unique per upload)
        instance_hash_input = json.dumps(properties, sort_keys=True) + (file_id or seen_time)
        instance_id = f"schema_inst_{hashlib.sha1(instance_hash_input.encode()).hexdigest()[:10]}"
        instance_doc = {
            "schema_instance_id": instance_id,
            "generated_at": seen_time,
            "source_id": source_id,
            "file_id": file_id,
            "properties": properties,
            "fields": self._fields_map_to_doc(self._properties_to_fields_map(properties, seen_time)),
        }

        # load existing logical schema map
        logical = self.get_logical_schema(source_id) or {"logical_id": None, "generated_at": None, "fields_map": {}}
        old_map = logical.get("fields_map", {}) or {}

        # convert inferred properties to a fresh fields_map
        new_map = self._properties_to_fields_map(properties, seen_time)

        # merge: union types and update counts/last_seen
        merged_map = dict(old_map)  # shallow copy
        for fname, meta in new_map.items():
            if fname in merged_map:
                merged_map[fname] = self._merge_field_meta(merged_map.get(fname), meta)
            else:
                merged_map[fname] = meta

        # apply promotion filter: only keep fields seen >= MIN_PROMOTE_COUNT
        promoted_map = {}
        for fname, meta in merged_map.items():
            try:
                if meta.get("count", 1) >= MIN_PROMOTE_COUNT and _is_plausible_field_name(fname):
                    promoted_map[fname] = meta
            except Exception:
                continue

        # compute diff against previous logical fields (use old_map)
        diff = self.compute_diff(old_map, promoted_map)

        # build logical doc with a deterministic logical id (hash of field names+types)
        logical_hash_input = json.dumps({k: promoted_map[k].get("types", []) for k in sorted(promoted_map.keys())}, sort_keys=True)
        logical_id = f"schema_log_{hashlib.sha1(logical_hash_input.encode()).hexdigest()[:10]}"

        logical_doc = {
            "logical_id": logical_id,
            "updated_at": seen_time,
            "source_id": source_id,
            "fields_map": promoted_map,
            "fields": self._fields_map_to_doc(promoted_map),
        }

        # persist instance doc (per-upload)
        try:
            p_inst = self._instance_path(source_id, seen_time, instance_id)
            with open(p_inst, "w", encoding="utf-8") as f:
                json.dump(instance_doc, f, indent=2)
        except Exception as e:
            instance_doc["storage_error"] = str(e)

        # persist logical doc (overwrite)
        try:
            p_log = self._logical_path(source_id)
            with open(p_log, "w", encoding="utf-8") as f:
                json.dump(logical_doc, f, indent=2)
        except Exception as e:
            logical_doc["storage_error"] = str(e)

        return logical_id, instance_id, logical_doc, instance_doc, diff

    # backward-compatible wrapper
    def register_schema_for_source(self, source_id, sample_docs, file_id=None):
        logical_id, instance_id, logical_doc, instance_doc, diff = self.evolve_schema(source_id, sample_docs, file_id=file_id)
        return logical_id, logical_doc, diff
