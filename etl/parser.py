# etl/parser.py
import re, json, io, csv
from typing import List, Dict, Any
from bs4 import BeautifulSoup
from pdfminer.high_level import extract_text as pdf_extract_text

# Regex helpers
_RE_CONTROL = re.compile(r'[\x00-\x08\x0e-\x1f\x7f-\x9f]+')
_RE_LONG_TOKEN = re.compile(r'\S{120,}')  # tokens longer than 120 chars are likely garbage
_RE_SAFE_KEY = re.compile(r'^[A-Za-z0-9_\- ]{1,60}$')
_RE_KEY_NORMALIZE = re.compile(r'[^A-Za-z0-9_]+')

def _clean_text(raw: str) -> str:
    if not isinstance(raw, str):
        raw = str(raw)
    text = _RE_CONTROL.sub(' ', raw)                # remove control characters
    text = text.replace('\r', '\n')
    # remove extremely long tokens (replace with a space)
    text = _RE_LONG_TOKEN.sub(' ', text)
    # normalize whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def _try_extract_pdf_text(b: bytes) -> str:
    try:
        # pdfminer expects a filename or file-like object
        fp = io.BytesIO(b)
        txt = pdf_extract_text(fp)
        return txt or ""
    except Exception:
        # fallback to UTF-8 decode
        try:
            return b.decode('utf-8', errors='ignore')
        except:
            return str(b)

def _repair_json_fragment(s: str) -> str:
    # common fixes: trailing commas in objects/arrays, single quotes -> double quotes (naive)
    # Only apply conservative fixes to avoid false positives.
    s2 = s
    # remove trailing commas before closing braces/brackets
    s2 = re.sub(r',\s*([\]}])', r'\1', s2)
    # don't aggressively replace quotes except in simple cases
    return s2

def _extract_brace_jsons(text: str) -> List[str]:
    """
    Use stack to extract balanced {...} blocks (non-recursive regex prone to failure).
    Returns list of string fragments (the raw braces text).
    """
    res = []
    stack = []
    start = None
    for i, ch in enumerate(text):
        if ch == '{':
            if not stack:
                start = i
            stack.append('{')
        elif ch == '}':
            if stack:
                stack.pop()
                if not stack and start is not None:
                    frag = text[start:i+1]
                    res.append(frag)
                    start = None
    return res

def try_parse_json(text: str):
    if not isinstance(text, str):
        return None
    s = text.strip()
    # try direct parse
    try:
        return json.loads(s)
    except Exception:
        pass
    # attempt repair for common issues
    try:
        repaired = _repair_json_fragment(s)
        return json.loads(repaired)
    except Exception:
        return None

def extract_json_fragments(text: str) -> List[Dict]:
    fragments = []
    # try whole text first
    full = try_parse_json(text)
    if isinstance(full, dict):
        fragments.append(full)
    # then scan for balanced braces
    for frag in _extract_brace_jsons(text):
        obj = try_parse_json(frag)
        if isinstance(obj, dict):
            fragments.append(obj)
    return fragments

def extract_html_tables(text: str) -> List[Dict]:
    tables = []
    try:
        soup = BeautifulSoup(text, "html.parser")
        for table in soup.find_all("table"):
            # gather rows
            rows = []
            for tr in table.find_all("tr"):
                cols = [td.get_text(separator=' ', strip=True) for td in tr.find_all(['td','th'])]
                if cols:
                    rows.append(cols)
            if not rows:
                continue
            # choose header: first row if likely header (contains non-numeric)
            hdr = rows[0]
            if len(rows) >= 2 and any(re.search(r'[A-Za-z]', h or '') for h in hdr):
                headers = [h if h else f"col{i}" for i,h in enumerate(hdr)]
                for r in rows[1:]:
                    # pad
                    while len(r) < len(headers):
                        r.append("")
                    obj = {headers[i].strip(): (r[i].strip() if i < len(r) else "") for i in range(len(headers))}
                    tables.append(_sanitize_keys(obj))
            else:
                # if no clear header but uniform row widths, try to convert using index headers
                if len(rows) >= 2 and all(len(r)==len(rows[0]) for r in rows):
                    headers = [f"col{i}" for i in range(len(rows[0]))]
                    for r in rows:
                        obj = {headers[i]: r[i].strip() for i in range(len(r))}
                        tables.append(_sanitize_keys(obj))
    except Exception:
        pass
    return tables

def extract_csv_fragments(text: str) -> List[Dict]:
    fragments = []
    lines = [ln for ln in text.splitlines() if ln.strip()]
    # check blocks of up to 20 lines to find small tables
    for i in range(0, max(1,len(lines))):
        block = "\n".join(lines[i:i+20])
        if block.count(",") < 1:
            continue
        try:
            reader = list(csv.reader(io.StringIO(block)))
            if len(reader) < 2:
                continue
            # require header-like first row (contains letters)
            header = reader[0]
            if not any(re.search(r'[A-Za-z]', h or '') for h in header):
                continue
            # require consistent column counts for data rows
            good_rows = [r for r in reader[1:] if len(r)==len(header)]
            if not good_rows:
                continue
            for r in good_rows:
                obj = {header[j].strip(): (r[j].strip() if j < len(r) else "") for j in range(len(header))}
                fragments.append(_sanitize_keys(obj))
        except Exception:
            continue
    return fragments

def extract_kv_pairs(text: str) -> List[tuple]:
    kv = []
    for line in text.splitlines():
        if ":" not in line:
            continue
        parts = line.split(":", 1)
        key = parts[0].strip()
        val = parts[1].strip()
        # sanity check key
        if not key or len(key) > 80:
            continue
        if not _RE_SAFE_KEY.match(key):
            # try simpler sanitize
            key2 = re.sub(r'[^A-Za-z0-9_ ]+', '', key).strip()
            if not key2 or len(key2) < 1:
                continue
            key = key2
        kv.append((key, val))
    return kv

def _sanitize_keys(d: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(d, dict):
        return d
    out = {}
    for k,v in d.items():
        if not isinstance(k, str):
            continue
        k2 = k.strip().lower()
        k2 = re.sub(r'\s+', '_', k2)
        k2 = _RE_KEY_NORMALIZE.sub('', k2)
        if not k2 or len(k2) > 60:
            # skip implausible keys
            continue
        out[k2] = v
    return out

def extract_fragments_from_bytes(b: bytes, filename: str = None, mimetype: str = None) -> dict:
    # decode / extract text
    if filename and filename.lower().endswith(".pdf"):
        text = _try_extract_pdf_text(b)
    else:
        try:
            text = b.decode("utf-8", errors="ignore")
        except Exception:
            text = str(b)
    text = _clean_text(text)

    result = {}
    try:
        jsons = extract_json_fragments(text)
    except Exception:
        jsons = []
    try:
        html_tables = extract_html_tables(text)
    except Exception:
        html_tables = []
    try:
        csvs = extract_csv_fragments(text)
    except Exception:
        csvs = []
    try:
        kv = extract_kv_pairs(text)
    except Exception:
        kv = []

    result["json_fragments"] = jsons
    result["html_tables"] = html_tables
    result["csv_fragments"] = csvs
    result["kv_pairs"] = kv
    result["raw_text"] = text[:10000]
    return result
