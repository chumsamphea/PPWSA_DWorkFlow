# -*- coding: utf-8 -*-
"""
ប្រព័ន្ធគ្រប់គ្រងលំហូរឯកសារ ការងារសិក្សា និងអនុវត្តគម្រោងបណ្តាញផ្គត់ផ្គង់ទឹកស្អាត — server ក្នុងបណ្តាញការិយាល័យ (LAN)

ដំណើរការ ៖  python server.py            (port 8080 ជាលំនាំដើម)
           python server.py --port 9000
ត្រូវការតែ Python 3.8 ឬខ្ពស់ជាងនេះ — មិនចាំបាច់ដំឡើងកញ្ចប់បន្ថែមទេ។

ទិន្នន័យ ៖ data/workflow.db (SQLite) នៅលើកុំព្យូទ័រនេះ
បម្រុងទុក ៖ data/backups/ (ស្វ័យប្រវត្តិរៀងរាល់ថ្ងៃ រក្សាទុក 60 ច្បាប់)
"""
import argparse
import hashlib
import hmac
import json
import os
import re
import secrets
import socket
import sqlite3
import sys
import threading
import time
from datetime import datetime, timedelta
from http.cookies import SimpleCookie
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs, unquote, quote
import mimetypes
import tempfile
import zipfile

BASE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE, "data")
DB_PATH = os.path.join(DATA_DIR, "workflow.db")
BACKUP_DIR = os.path.join(DATA_DIR, "backups")
PLAN_DIR = os.path.join(DATA_DIR, "plans")
PFILE_DIR = os.path.join(DATA_DIR, "project-files")
MAX_PLAN_BYTES = 200 * 1024 * 1024
STATIC = os.path.join(BASE, "static")
SEED = os.path.join(BASE, "seed", "seed.json")
PSEED = os.path.join(BASE, "seed", "projects.json")
SESSION_DAYS = 30
KEEP_BACKUPS = 60

TAB_KEYS = ("dash", "list", "plan", "proj", "impl", "pipe", "report", "settings")
EDIT_TABS = ("list", "plan", "proj", "impl", "settings")
FULL_PERMS = {"dash": "view", "list": "edit", "plan": "edit", "proj": "edit", "impl": "edit", "pipe": "view", "report": "view", "settings": "edit", "download": True}
DEFAULT_PERMS = {"dash": "view", "list": "edit", "plan": "edit", "proj": "view", "impl": "view", "pipe": "view", "report": "view", "settings": "view", "download": True}

DATE_KEYS = ["l", "n", "o", "p", "q", "ak", "ao", "ap", "aq", "ar", "at", "au", "av", "aw", "ax",
             "ay", "az", "ba", "bb", "bc", "bd", "be", "bf"]
PIPES = ["63", "90", "110", "160", "225", "250", "300", "315", "400"]
VALVES = ["65", "80", "100", "150", "200", "250", "300"]
TEXT_FIELDS = {"code": 60, "desc": 1000, "village": 120, "sangkat": 120, "khan": 120, "type": 200,
               "requester": 200, "phone": 60, "source": 200, "dept": 200, "reviewer": 200, "payer": 80,
               "other": 2000, "policyResult": 500, "special": 80, "note": 4000,
               "geo": 1000, "planner": 200, "srcOrg": 300, "srcRef": 200, "srcContact": 200, "srcInfo": 2000}
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

write_lock = threading.Lock()


def now():
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


# ------------------------------------------------------------------ database
def connect():
    con = sqlite3.connect(DB_PATH, timeout=15)
    con.row_factory = sqlite3.Row
    return con


def init_db():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(BACKUP_DIR, exist_ok=True)
    con = connect()
    con.execute("PRAGMA journal_mode=WAL")
    con.executescript("""
        CREATE TABLE IF NOT EXISTS users(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          username TEXT UNIQUE NOT NULL COLLATE NOCASE,
          name TEXT NOT NULL,
          role TEXT NOT NULL DEFAULT 'user',
          perms TEXT NOT NULL DEFAULT '{}',
          pw_hash TEXT NOT NULL,
          active INTEGER NOT NULL DEFAULT 1,
          created_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS sessions(
          token TEXT PRIMARY KEY, user_id INTEGER NOT NULL, expires_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS requests(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          data TEXT NOT NULL, version INTEGER NOT NULL DEFAULT 1,
          created_by INTEGER, created_at TEXT NOT NULL,
          updated_by INTEGER, updated_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS history(
          id INTEGER PRIMARY KEY AUTOINCREMENT, request_id INTEGER NOT NULL,
          action TEXT NOT NULL, user_id INTEGER, at TEXT NOT NULL, changes TEXT);
        CREATE INDEX IF NOT EXISTS ix_hist_req ON history(request_id);
        CREATE TABLE IF NOT EXISTS projects(
          id TEXT PRIMARY KEY, data TEXT NOT NULL, version INTEGER NOT NULL DEFAULT 1,
          updated_by INTEGER, updated_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS plan_files(
          id INTEGER PRIMARY KEY AUTOINCREMENT, request_id INTEGER NOT NULL,
          name TEXT NOT NULL, size INTEGER NOT NULL, path TEXT NOT NULL,
          uploaded_by INTEGER, at TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS ix_pf_req ON plan_files(request_id);
        CREATE TABLE IF NOT EXISTS project_files(
          id INTEGER PRIMARY KEY AUTOINCREMENT, group_id TEXT NOT NULL, cat TEXT NOT NULL,
          name TEXT NOT NULL, size INTEGER NOT NULL, path TEXT NOT NULL,
          uploaded_by INTEGER, at TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS ix_pjf_g ON project_files(group_id);
        CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
    """)
    con.execute("INSERT OR IGNORE INTO meta(key,value) VALUES('rev','1')")
    con.execute("INSERT OR IGNORE INTO meta(key,value) VALUES('prev','1')")
    pn = con.execute("SELECT COUNT(*) FROM projects").fetchone()[0]
    pseeded = con.execute("SELECT value FROM meta WHERE key='pseeded'").fetchone()
    if pn == 0 and not pseeded and os.path.exists(PSEED):
        with open(PSEED, encoding="utf-8") as f:
            groups = json.load(f)
        ts = now()
        for g in groups:
            con.execute("INSERT INTO projects(id,data,version,updated_at) VALUES(?,?,1,?)",
                        (g["id"], json.dumps(g["data"], ensure_ascii=False), ts))
        con.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('pseeded',?)", (ts,))
        print("  បាននាំចូលគម្រោង %d ឯកសារ (%d ជួរ) ពី Excel" % (len(groups), sum(len(g["data"].get("rows", [])) for g in groups)))
    n = con.execute("SELECT COUNT(*) FROM requests").fetchone()[0]
    seeded = con.execute("SELECT value FROM meta WHERE key='seeded'").fetchone()
    if n == 0 and not seeded and os.path.exists(SEED):
        with open(SEED, encoding="utf-8") as f:
            rows = json.load(f)
        ts = now()
        for r in rows:
            cur = con.execute("INSERT INTO requests(data,version,created_at,updated_at) VALUES(?,?,?,?)",
                              (json.dumps(r, ensure_ascii=False), 1, ts, ts))
            con.execute("INSERT INTO history(request_id,action,user_id,at,changes) VALUES(?,?,?,?,?)",
                        (cur.lastrowid, "import", None, ts, "{}"))
        con.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('seeded',?)", (ts,))
        print("  បាននាំចូលសំណើ %d ពីឯកសារ Excel" % len(rows))
    con.commit()
    con.close()


def bump_rev(con):
    con.execute("UPDATE meta SET value=CAST(value AS INTEGER)+1 WHERE key='rev'")


def get_rev(con):
    return int(con.execute("SELECT value FROM meta WHERE key='rev'").fetchone()[0])


def bump_prev(con):
    con.execute("UPDATE meta SET value=CAST(value AS INTEGER)+1 WHERE key='prev'")


def get_prev(con):
    return int(con.execute("SELECT value FROM meta WHERE key='prev'").fetchone()[0])


def clean_project(d):
    if not isinstance(d, dict) or not isinstance(d.get("rows"), list) or not d["rows"]:
        raise ValueError("គម្រោងត្រូវមានយ៉ាងហោចណាស់ 1 ជួរ")
    if len(d["rows"]) > 3000:
        raise ValueError("ជួរច្រើនពេក")
    rows = []
    for r in d["rows"]:
        if not isinstance(r, dict):
            raise ValueError("ទិន្នន័យជួរមិនត្រឹមត្រូវ")
        dates = r.get("dates") or {}
        for k, v in dates.items():
            if v and not DATE_RE.match(str(v)):
                raise ValueError("កាលបរិច្ឆេទមិនត្រឹមត្រូវ ៖ %s" % v)
        rows.append(r)
    out = {"doc": str(d.get("doc") or "").strip()[:120], "key": str(d.get("key") or "")[:300], "rows": rows}
    if len(json.dumps(out, ensure_ascii=False)) > 1_500_000:
        raise ValueError("គម្រោងធំពេក")
    return out


IMPL_FIELDS = ("bst", "bnote", "bteam", "bpay", "boff", "dst", "dteam", "dnote", "dpoff", "tnote", "rtn", "rtnote", "rtak", "rth", "sn_ak")
IMPL_DATES = ("ao", "ak", "rt", "bo", "do", "dp", "tp1", "tp2", "tp3", "tp4")  # implementation date, and sending back to the price team


def impl_change_only(old, new):
    """True when new differs from old only in branch-implementation fields."""
    def strip(d):
        rows = []
        for r in d.get("rows") or []:
            r = {k: v for k, v in r.items() if k not in IMPL_FIELDS}
            r["dates"] = {k: v for k, v in (r.get("dates") or {}).items() if k not in IMPL_DATES and v}
            rows.append(r)
        return {"doc": d.get("doc") or "", "rows": rows}
    return json.dumps(strip(old), sort_keys=True, ensure_ascii=False) == json.dumps(strip(new), sort_keys=True, ensure_ascii=False)


def user_names(con):
    """id -> name, including users that were deleted (kept so history still shows who did what)"""
    r = con.execute("SELECT value FROM settings WHERE key='deleted_users'").fetchone()
    names = {int(k): v for k, v in (json.loads(r[0]) if r else {}).items()}
    names.update({r["id"]: r["name"] for r in con.execute("SELECT id,name FROM users")})
    return names


def get_lists(con):
    r = con.execute("SELECT value FROM settings WHERE key='lists'").fetchone()
    return json.loads(r[0]) if r else None


# ------------------------------------------------------------------ passwords / perms
def hash_pw(pw, salt=None):
    salt = salt or secrets.token_hex(16)
    dk = hashlib.pbkdf2_hmac("sha256", pw.encode("utf-8"), bytes.fromhex(salt), 200000)
    return "pbkdf2$%s$%s" % (salt, dk.hex())


def check_pw(pw, stored):
    try:
        _, salt, _h = stored.split("$")
    except ValueError:
        return False
    return hmac.compare_digest(hash_pw(pw, salt), stored)


def clean_perms(p):
    p = p if isinstance(p, dict) else {}
    out = {}
    for k in TAB_KEYS:
        fb = {"plan": "list", "impl": "proj"}.get(k)
        v = p.get(k, p.get(fb, DEFAULT_PERMS[k]) if fb else DEFAULT_PERMS[k])
        allowed = ("none", "view", "edit") if k in EDIT_TABS else ("none", "view")
        out[k] = v if v in allowed else DEFAULT_PERMS[k]
    out["download"] = bool(p.get("download", DEFAULT_PERMS["download"]))
    hide = p.get("hide")
    out["hide"] = [str(x) for x in hide if re.match(r"^[a-z]{2,10}\.[a-z]{2,10}$", str(x))][:80] if isinstance(hide, list) else []
    return out


PLAN_FIELDS = ("pipes", "valves", "extras", "meters", "houses", "payer", "other", "planner")


def keep_plan(u, data, old):
    """«៣. សិក្សាប្លង់» in the request form: only users who can edit the plan tab may change it"""
    if perms_of(u).get("plan") == "edit":
        return data
    old = old or {}
    dates = dict(data.get("dates") or {})
    for k in ("q", "ak"):
        v = (old.get("dates") or {}).get(k)
        if v:
            dates[k] = v
        else:
            dates.pop(k, None)
    data["dates"] = dates
    for k in PLAN_FIELDS:
        if k in old:
            data[k] = old[k]
    return data


def perms_of(u):
    if u["role"] == "admin":
        return dict(FULL_PERMS)
    try:
        return clean_perms(json.loads(u["perms"] or "{}"))
    except ValueError:
        return dict(DEFAULT_PERMS)


# ------------------------------------------------------------------ backups
def snapshot(con):
    reqs = [{"id": str(r["id"]), "data": json.loads(r["data"])} for r in con.execute("SELECT * FROM requests ORDER BY id")]
    projs = [{"id": r["id"], "data": json.loads(r["data"])} for r in con.execute("SELECT * FROM projects ORDER BY id")]
    pfiles = [dict(r) for r in con.execute("SELECT id,request_id,name,size,path,at FROM plan_files ORDER BY id")]
    jfiles = [dict(r) for r in con.execute("SELECT id,group_id,cat,name,size,path,at FROM project_files ORDER BY id")]
    users = [{"id": r["id"], "username": r["username"], "name": r["name"], "role": r["role"], "perms": r["perms"],
              "pw_hash": r["pw_hash"], "active": r["active"], "created_at": r["created_at"]}
             for r in con.execute("SELECT * FROM users ORDER BY id")]
    du = con.execute("SELECT value FROM settings WHERE key='deleted_users'").fetchone()
    counts = snap_counts(reqs, projs)
    counts["users"] = len(users)
    lists = get_lists(con) or {}
    counts["opts"] = sum(len(v) for v in lists.values() if isinstance(v, list))
    pr = con.execute("SELECT value FROM settings WHERE key='prefs'").fetchone()
    prefs = json.loads(pr[0]) if pr else {}
    counts["prefs"] = len(prefs)
    return {"format": "wf-backup", "version": 3, "createdAt": datetime.now().isoformat(timespec="seconds"),
            "requests": reqs, "lists": get_lists(con), "projects": projs,
            "planFiles": pfiles, "projectFiles": jfiles, "users": users,
            "deletedUsers": json.loads(du[0]) if du else None, "prefs": prefs, "counts": counts}


def save_prefs(con, prefs, commit=True):
    """report settings of the admin's browser (Recap / sending letter: logo, signatures, page set-up)"""
    if not isinstance(prefs, dict):
        return
    clean = {str(k)[:60]: str(v)[:3_000_000] for k, v in list(prefs.items())[:200]
             if re.match(r"^wf_(rc_|sd_)[\w-]{1,50}$", str(k)) and isinstance(v, str)}
    con.execute("INSERT OR REPLACE INTO settings(key,value) VALUES('prefs',?)", (json.dumps(clean, ensure_ascii=False),))
    if commit:
        con.commit()


def restore_users(con, users, me):
    """users from a backup: missing accounts are re-created (with their password); existing ones get name / role /
    permissions back but keep their current password. The admin doing the restore is never demoted or disabled."""
    if not isinstance(users, list):
        return None
    n = 0
    for x in users:
        if not isinstance(x, dict):
            continue
        un = str(x.get("username") or "").strip()
        if not re.match(r"^[A-Za-z0-9._-]{3,30}$", un):
            continue
        name = str(x.get("name") or un).strip()[:80] or un
        role = x.get("role") if x.get("role") in ("admin", "user") else "user"
        try:
            perms = json.dumps(clean_perms(json.loads(x.get("perms") or "{}") if isinstance(x.get("perms"), str) else x.get("perms")))
        except ValueError:
            perms = json.dumps(dict(DEFAULT_PERMS))
        active = 1 if x.get("active", 1) else 0
        row = con.execute("SELECT * FROM users WHERE username=?", (un,)).fetchone()
        if row:
            if row["id"] == me["id"]:
                con.execute("UPDATE users SET name=? WHERE id=?", (name, row["id"]))
            else:
                con.execute("UPDATE users SET name=?, role=?, perms=?, active=? WHERE id=?", (name, role, perms, active, row["id"]))
        else:
            pw = str(x.get("pw_hash") or "")
            if not re.match(r"^pbkdf2\$[0-9a-f]+\$[0-9a-f]+$", pw):
                continue
            con.execute("INSERT INTO users(username,name,role,perms,pw_hash,active,created_at) VALUES(?,?,?,?,?,?,?)",
                        (un, name, role, perms, pw, active, str(x.get("created_at") or now())[:19]))
        n += 1
    return n


def snap_counts(reqs, projs):
    """what a backup holds, per tab"""
    rows = [s for g in projs for s in (g.get("data") or {}).get("rows") or []]
    return {"req": len(reqs),
            "plan": sum(1 for r in reqs if ((r.get("data") or {}).get("dates") or {}).get("q")),
            "pg": len(projs), "prow": len(rows),
            "impl": sum(1 for x in rows if (x.get("dates") or {}).get("ak"))}


def backup(tag="auto"):
    os.makedirs(BACKUP_DIR, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    con = connect()
    try:
        payload = snapshot(con)
        jpath = os.path.join(BACKUP_DIR, "workflow-%s-%s.json" % (stamp, tag))
        with open(jpath, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
        out = sqlite3.connect(os.path.join(BACKUP_DIR, "workflow-%s-%s.db" % (stamp, tag)))
        with out:
            con.backup(out)
        out.close()
    finally:
        con.close()
    for ext in (".json", ".db"):
        files = sorted(f for f in os.listdir(BACKUP_DIR) if f.endswith(ext))
        for f in files[:-KEEP_BACKUPS]:
            try:
                os.remove(os.path.join(BACKUP_DIR, f))
            except OSError:
                pass
    return os.path.basename(jpath)


def backup_loop():
    last = None
    while True:
        today = datetime.now().date()
        if today != last:
            try:
                backup("daily")
                last = today
            except Exception as e:
                print("  ការបម្រុងទុកមិនបានសម្រេច ៖", e)
        time.sleep(600)


# ------------------------------------------------------------------ validation
def clean_request(b):
    if not isinstance(b, dict):
        raise ValueError("ទិន្នន័យមិនត្រឹមត្រូវ")
    out = {}
    for k, mx in TEXT_FIELDS.items():
        v = b.get(k, "")
        out[k] = ("" if v is None else str(v).strip())[:mx]

    def numv(v):
        if v in ("", None):
            return None
        try:
            f = float(v)
        except (TypeError, ValueError):
            raise ValueError("តម្លៃលេខមិនត្រឹមត្រូវ ៖ %s" % v)
        if f < 0:
            raise ValueError("តម្លៃលេខមិនអាចតិចជាង 0")
        return int(f) if f.is_integer() else f

    size_ok = lambda k: bool(re.match(r"^\d{1,4}$", str(k)))
    out["pipes"] = {str(k): numv(v) for k, v in (b.get("pipes") or {}).items() if size_ok(k) and numv(v) is not None}
    out["valves"] = {str(k): numv(v) for k, v in (b.get("valves") or {}).items() if size_ok(k) and numv(v) is not None}
    ex = b.get("extras") if isinstance(b.get("extras"), dict) else {}
    out["extras"] = {str(k).strip()[:80]: numv(v) for k, v in list(ex.items())[:60] if str(k).strip() and numv(v) is not None}
    out["meters"] = numv(b.get("meters"))
    out["houses"] = numv(b.get("houses"))
    out["planLen"] = numv(b.get("planLen"))
    out["dates"] = {}
    for k, v in (b.get("dates") or {}).items():
        if k in DATE_KEYS and v:
            if not DATE_RE.match(str(v)):
                raise ValueError("កាលបរិច្ឆេទមិនត្រឹមត្រូវ ៖ %s" % v)
            out["dates"][k] = str(v)
    if not out["code"]:
        raise ValueError("សូមបំពេញលេខសម្គាល់ឯកសារ")
    if not out["dates"].get("l"):
        raise ValueError("សូមបំពេញកាលបរិច្ឆេទស្នើសុំ")
    return out


def diff(old, new):
    ch = {}
    for k in set(old) | set(new):
        if isinstance(old.get(k), dict) or isinstance(new.get(k), dict):
            o, n = old.get(k) or {}, new.get(k) or {}
            for sk in set(o) | set(n):
                if o.get(sk) != n.get(sk):
                    ch["%s.%s" % (k, sk)] = [o.get(sk), n.get(sk)]
        elif old.get(k) != new.get(k):
            ch[k] = [old.get(k), new.get(k)]
    return ch


# ------------------------------------------------------------------ HTTP
# ---------- RapidOCR (optional) ----------
_OCR = {"eng": None, "err": None, "tried": False}
_OCR_LOCK = threading.Lock()


def ocr_engine():
    with _OCR_LOCK:
        if not _OCR["tried"]:
            _OCR["tried"] = True
            errs = []
            for mod in ("rapidocr_onnxruntime", "rapidocr"):
                try:
                    _OCR["eng"] = __import__(mod, fromlist=["RapidOCR"]).RapidOCR()
                    _OCR["err"] = None
                    break
                except Exception as e:  # not installed / broken install
                    errs.append("%s: %s" % (mod, e))
            if not _OCR["eng"]:
                _OCR["err"] = "; ".join(errs)
        return _OCR["eng"], _OCR["err"]


def ocr_lines(eng, data):
    """image bytes -> text lines (boxes on the same row joined left→right)"""
    with _OCR_LOCK:
        res = eng(data)
    items = []
    if isinstance(res, tuple):  # rapidocr_onnxruntime: ([[box, text, score], ...], elapse)
        for it in (res[0] or []):
            items.append((it[0], str(it[1])))
    else:  # rapidocr >= 2: object with boxes / txts
        for b, t in zip(getattr(res, "boxes", None) or [], getattr(res, "txts", None) or []):
            items.append((b, str(t)))
    boxes = []
    for b, t in items:
        ys = [float(pt[1]) for pt in b]
        xs = [float(pt[0]) for pt in b]
        if t.strip():
            boxes.append({"y0": min(ys), "y1": max(ys), "x": min(xs), "t": t.strip()})
    boxes.sort(key=lambda b: (b["y0"] + b["y1"]) / 2)
    rows = []
    for b in boxes:
        c = (b["y0"] + b["y1"]) / 2
        h = b["y1"] - b["y0"]
        if rows:
            r = rows[-1]
            if abs(c - r["c"]) <= max(4.0, min(h, r["h"]) * 0.5):
                r["w"].append(b)
                continue
        rows.append({"c": c, "h": h, "w": [b]})
    return [" ".join(w["t"] for w in sorted(r["w"], key=lambda w: w["x"])) for r in rows]


# ---------- Claude AI (optional, needs an Anthropic API key + Internet on the server) ----------
AI_MODELS = ("claude-sonnet-5-5", "claude-opus-5-5", "claude-haiku-4-5-20251001")


def ai_conf(con):
    r = con.execute("SELECT value FROM settings WHERE key='claude_ai'").fetchone()
    try:
        c = json.loads(r[0]) if r else {}
    except ValueError:
        c = {}
    key = (c.get("key") or os.environ.get("ANTHROPIC_API_KEY") or "").strip()
    return {"key": key, "model": c.get("model") or AI_MODELS[0], "env": bool(not c.get("key") and key)}


def ai_call(conf, prompt, images):
    import urllib.request
    import urllib.error
    content = []
    for b64 in images[:5]:
        mt = "image/png" if b64.startswith("iVBOR") else "image/jpeg"
        content.append({"type": "image", "source": {"type": "base64", "media_type": mt, "data": b64}})
    content.append({"type": "text", "text": prompt + "\n\nReply with the JSON only, no explanation."})
    req = urllib.request.Request(os.environ.get("ANTHROPIC_BASE_URL", "https://api.anthropic.com").rstrip("/") + "/v1/messages", method="POST",
                                 data=json.dumps({"model": conf["model"], "max_tokens": 16000,
                                                  "messages": [{"role": "user", "content": content}]}).encode("utf-8"),
                                 headers={"x-api-key": conf["key"], "anthropic-version": "2023-06-01",
                                          "content-type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=240) as r:
            res = json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        try:
            msg = json.loads(e.read().decode("utf-8")).get("error", {}).get("message", "")
        except Exception:
            msg = ""
        code = {401: "API key មិនត្រឹមត្រូវ", 403: "API key គ្មានសិទ្ធិ", 404: "រកមិនឃើញ Model «%s»" % conf["model"],
                429: "ហៅញឹកពេក ឬអស់ឥណទាន (credit)", 529: "Claude រវល់ — សូមព្យាយាមម្តងទៀត"}.get(e.code, "HTTP %s" % e.code)
        raise Api(502, "Claude AI ៖ %s%s" % (code, (" — " + msg[:200]) if msg else ""))
    except Exception as e:
        raise Api(502, "មិនអាចភ្ជាប់ទៅ Claude AI (api.anthropic.com) ៖ %s — សូមពិនិត្យ Internet របស់កុំព្យូទ័រ Server" % e)
    text = "".join(b.get("text", "") for b in res.get("content", []) if b.get("type") == "text").strip()
    t = re.sub(r"^```(?:json)?\s*|\s*```$", "", text)
    starts = [i for i in (t.find("{"), t.find("[")) if i >= 0]
    if starts:
        t = t[min(starts):]
    try:
        return json.JSONDecoder().raw_decode(t)[0]
    except ValueError:
        raise Api(502, "Claude AI ផ្តល់លទ្ធផលមិនមែន JSON")


class Api(Exception):
    def __init__(self, status, msg):
        super().__init__(msg)
        self.status, self.msg = status, msg


class Handler(BaseHTTPRequestHandler):
    server_version = "WorkflowTracker/1.0"

    def log_message(self, fmt, *args):
        if self.command != "GET":
            sys.stderr.write("%s  %s  %s\n" % (datetime.now().strftime("%H:%M:%S"), self.address_string(), fmt % args))

    def send_json(self, obj, status=200, headers=None):
        b = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(b)))
        self.send_header("Cache-Control", "no-store")
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(b)

    def raw_body(self, limit):
        n = int(self.headers.get("Content-Length") or 0)
        if n <= 0:
            raise Api(400, "ឯកសារទទេ")
        if n > limit:
            raise Api(413, "ឯកសារធំពេក")
        return self.rfile.read(n)

    def body(self, limit=2_000_000):
        n = int(self.headers.get("Content-Length") or 0)
        if n > limit:
            raise Api(413, "ទិន្នន័យធំពេក")
        if n and "application/json" not in (self.headers.get("Content-Type") or ""):
            raise Api(415, "ត្រូវការ JSON")
        raw = self.rfile.read(n) if n else b"{}"
        try:
            return json.loads(raw.decode("utf-8") or "{}")
        except ValueError:
            raise Api(400, "JSON មិនត្រឹមត្រូវ")

    def current_user(self, con):
        c = SimpleCookie(self.headers.get("Cookie") or "")
        if "wsid" not in c:
            return None
        return con.execute("SELECT u.* FROM sessions s JOIN users u ON u.id=s.user_id "
                           "WHERE s.token=? AND s.expires_at>? AND u.active=1", (c["wsid"].value, now())).fetchone()

    def need(self, con, tab=None, level="view", admin=False, download=False):
        u = self.current_user(con)
        if not u:
            raise Api(401, "សូមចូលប្រព័ន្ធ")
        if admin and u["role"] != "admin":
            raise Api(403, "សម្រាប់អ្នកគ្រប់គ្រងប៉ុណ្ណោះ")
        p = perms_of(u)
        if tab:
            have = p.get(tab, "none")
            if level == "edit" and have != "edit":
                raise Api(403, "អ្នកមិនមានសិទ្ធិកែប្រែផ្នែកនេះទេ")
            if level == "view" and have == "none":
                raise Api(403, "អ្នកមិនមានសិទ្ធិមើលផ្នែកនេះទេ")
        if download and not p.get("download"):
            raise Api(403, "អ្នកមិនមានសិទ្ធិទាញយកទិន្នន័យទេ")
        return u

    def send_file(self, fp, name):
        ctype = mimetypes.guess_type(name)[0] or "application/octet-stream"
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(os.path.getsize(fp)))
        safe = re.sub(r"[^A-Za-z0-9._-]", "_", name)
        disp = "inline" if ctype == "application/pdf" or ctype.startswith("image/") else "attachment"
        self.send_header("Content-Disposition", "%s; filename=\"%s\"; filename*=UTF-8''%s" % (disp, safe, quote(name)))
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Content-Security-Policy", "sandbox")
        self.end_headers()
        with open(fp, "rb") as fh:
            while True:
                chunk = fh.read(1 << 20)
                if not chunk:
                    break
                self.wfile.write(chunk)

    def user_pub(self, u):
        return {"id": u["id"], "username": u["username"], "name": u["name"], "role": u["role"], "perms": perms_of(u)}

    def do_GET(self):
        self.route("GET")

    def do_POST(self):
        self.route("POST")

    def do_PUT(self):
        self.route("PUT")

    def do_DELETE(self):
        self.route("DELETE")

    def route(self, method):
        path = urlparse(self.path).path
        try:
            if not path.startswith("/api/"):
                if method != "GET":
                    raise Api(405, "Method not allowed")
                return self.static(path)
            con = connect()
            try:
                self.api(method, path, con)
            finally:
                con.close()
        except Api as e:
            self.send_json({"error": e.msg}, e.status)
        except sqlite3.OperationalError as e:
            self.send_json({"error": "database រវល់ — សូមព្យាយាមម្តងទៀត (%s)" % e}, 503)
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as e:
            self.send_json({"error": "កំហុសក្នុង server ៖ %s" % e}, 500)

    def static(self, path):
        if path in ("/", ""):
            path = "/index.html"
        fp = os.path.normpath(os.path.join(STATIC, path.lstrip("/")))
        if not fp.startswith(STATIC) or not os.path.isfile(fp):
            raise Api(404, "រកមិនឃើញ")
        ctype = {".html": "text/html; charset=utf-8", ".js": "text/javascript; charset=utf-8",
                 ".css": "text/css; charset=utf-8", ".svg": "image/svg+xml"}.get(os.path.splitext(fp)[1], "application/octet-stream")
        with open(fp, "rb") as f:
            b = f.read()
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(b)))
        self.send_header("Cache-Control", "no-cache")
        self.send_header("X-Frame-Options", "DENY")
        self.end_headers()
        self.wfile.write(b)

    def api(self, method, path, con):
        # ---------------- auth
        if path == "/api/me" and method == "GET":
            u = self.current_user(con)
            has = con.execute("SELECT COUNT(*) FROM users").fetchone()[0] > 0
            return self.send_json({"user": self.user_pub(u) if u else None, "needsSetup": not has})
        if path == "/api/setup" and method == "POST":
            b = self.body()
            with write_lock:
                if con.execute("SELECT COUNT(*) FROM users").fetchone()[0] > 0:
                    raise Api(409, "ប្រព័ន្ធបានរៀបចំរួចហើយ")
                uid = self.create_user(con, b, force_admin=True)
                con.commit()
            return self.login_as(con, uid)
        if path == "/api/login" and method == "POST":
            b = self.body()
            u = con.execute("SELECT * FROM users WHERE username=? AND active=1", (str(b.get("username", "")).strip(),)).fetchone()
            if not u or not check_pw(str(b.get("password", "")), u["pw_hash"]):
                time.sleep(0.6)
                raise Api(401, "ឈ្មោះអ្នកប្រើ ឬពាក្យសម្ងាត់មិនត្រឹមត្រូវ")
            return self.login_as(con, u["id"])
        if path == "/api/logout" and method == "POST":
            c = SimpleCookie(self.headers.get("Cookie") or "")
            if "wsid" in c:
                con.execute("DELETE FROM sessions WHERE token=?", (c["wsid"].value,))
                con.commit()
            return self.send_json({"ok": True}, headers={"Set-Cookie": "wsid=; Path=/; Max-Age=0; HttpOnly; SameSite=Strict"})
        if path == "/api/password" and method == "POST":
            u = self.need(con)
            b = self.body()
            if not check_pw(str(b.get("old", "")), u["pw_hash"]):
                raise Api(400, "ពាក្យសម្ងាត់ចាស់មិនត្រឹមត្រូវ")
            if len(str(b.get("new", ""))) < 6:
                raise Api(400, "ពាក្យសម្ងាត់ថ្មីត្រូវមានយ៉ាងហោចណាស់ 6 តួ")
            con.execute("UPDATE users SET pw_hash=? WHERE id=?", (hash_pw(str(b["new"])), u["id"]))
            con.commit()
            return self.send_json({"ok": True})

        # ---------------- Claude AI
        if path == "/api/ai" and method == "GET":
            u = self.need(con)
            c = ai_conf(con)
            out = {"ok": bool(c["key"])}
            if u["role"] == "admin":
                out.update({"model": c["model"], "models": list(AI_MODELS), "env": c["env"],
                            "hint": ("…" + c["key"][-4:]) if c["key"] else ""})
            return self.send_json(out)
        if path == "/api/ai" and method == "PUT":
            self.need(con, admin=True)
            b = self.body()
            old = con.execute("SELECT value FROM settings WHERE key='claude_ai'").fetchone()
            try:
                c = json.loads(old[0]) if old else {}
            except ValueError:
                c = {}
            if "key" in b:
                k = str(b.get("key") or "").strip()
                if k and not re.match(r"^sk-ant-[\w-]{10,}$", k):
                    raise Api(400, "API key ត្រូវចាប់ផ្តើមដោយ sk-ant-")
                c["key"] = k
            if b.get("model"):
                m = str(b["model"]).strip()
                if not re.match(r"^claude-[\w.-]{2,60}$", m):
                    raise Api(400, "ឈ្មោះ Model មិនត្រឹមត្រូវ")
                c["model"] = m
            con.execute("INSERT OR REPLACE INTO settings(key,value) VALUES('claude_ai',?)", (json.dumps(c),))
            con.commit()
            return self.send_json({"ok": True})
        if path == "/api/ai" and method == "POST":
            self.need(con)
            c = ai_conf(con)
            if not c["key"]:
                raise Api(501, "មិនទាន់កំណត់ Claude AI — អ្នកគ្រប់គ្រងត្រូវបញ្ចូល API key ក្នុងផ្ទាំង «ការគ្រប់គ្រង»")
            b = self.body(40_000_000)
            imgs = [str(x) for x in (b.get("images") or []) if isinstance(x, str) and re.match(r"^[A-Za-z0-9+/=]+$", x[:200])]
            return self.send_json({"json": ai_call(c, str(b.get("prompt") or "")[:200000], imgs)})

        # ---------------- OCR (RapidOCR, optional: pip install rapidocr_onnxruntime)
        if path == "/api/ocr" and method == "GET":
            self.need(con)
            eng, err = ocr_engine()
            return self.send_json({"ok": bool(eng), "error": err or ""})
        if path == "/api/ocr" and method == "POST":
            self.need(con)
            data = self.raw_body(20_000_000)
            eng, err = ocr_engine()
            if not eng:
                raise Api(501, "មិនទាន់ដំឡើង RapidOCR — សូមដំណើរការ install-ocr.bat (ឬ pip install rapidocr_onnxruntime) រួចចាប់ផ្តើម Server ឡើងវិញ")
            try:
                return self.send_json({"lines": ocr_lines(eng, data)})
            except Exception as e:
                raise Api(500, "RapidOCR អានរូបភាពមិនបាន (%s)" % e)

        # ---------------- data
        if path == "/api/rev" and method == "GET":
            u = self.need(con)
            return self.send_json({"rev": get_rev(con), "prev": get_prev(con), "perms": perms_of(u), "role": u["role"]})
        if path == "/api/state" and method == "GET":
            u = self.need(con)
            p = perms_of(u)
            names = user_names(con)
            reqs = []
            if any(p[k] != "none" for k in ("dash", "list", "plan", "pipe", "report")):
                pf = {}
                for f in con.execute("SELECT * FROM plan_files ORDER BY id"):
                    pf.setdefault(f["request_id"], []).append({"id": str(f["id"]), "name": f["name"], "size": f["size"],
                                                               "at": f["at"], "byName": names.get(f["uploaded_by"], "")})
                for r in con.execute("SELECT * FROM requests"):
                    d = json.loads(r["data"])
                    d.update(id=str(r["id"]), version=r["version"], updatedAt=r["updated_at"],
                             updatedBy=names.get(r["updated_by"], ""), plans=pf.get(r["id"], []))
                    reqs.append(d)
            return self.send_json({"rev": get_rev(con), "requests": reqs, "lists": get_lists(con),
                                   "user": self.user_pub(u)})

        # ---------------- plan study (សិក្សាប្លង់)
        m = re.match(r"^/api/requests/(\d+)/plan$", path)
        if m and method == "PUT":
            u = self.need(con, "plan", "edit")
            rid = int(m.group(1))
            b = self.body()
            pf = b.get("data") if isinstance(b.get("data"), dict) else {}
            with write_lock:
                row = con.execute("SELECT * FROM requests WHERE id=?", (rid,)).fetchone()
                if not row:
                    raise Api(404, "សំណើនេះត្រូវបានលុបរួចហើយ")
                if int(b.get("version") or 0) != row["version"]:
                    raise Api(409, "សំណើនេះត្រូវបានកែដោយអ្នកផ្សេង ខណៈពេលអ្នកកំពុងកែ។ សូមបិទទម្រង់ ហើយបើកម្តងទៀតដើម្បីឃើញទិន្នន័យថ្មី។")
                old = json.loads(row["data"])
                new = dict(old)
                for k in ("pipes", "valves", "extras", "meters", "houses", "payer", "other"):
                    new[k] = pf.get(k)
                if "geo" in pf:
                    new["geo"] = pf.get("geo") or ""
                if "planner" in pf:
                    new["planner"] = pf.get("planner") or ""
                dates = dict(old.get("dates") or {})
                for k in ("q", "ak"):
                    if pf.get(k):
                        dates[k] = pf[k]
                    else:
                        dates.pop(k, None)
                new["dates"] = dates
                data = self.clean(new)
                ch = diff(old, data)
                if not ch:
                    return self.send_json({"id": str(rid), "version": row["version"], "unchanged": True})
                ts = now()
                con.execute("UPDATE requests SET data=?, version=version+1, updated_by=?, updated_at=? WHERE id=?",
                            (json.dumps(data, ensure_ascii=False), u["id"], ts, rid))
                con.execute("INSERT INTO history(request_id,action,user_id,at,changes) VALUES(?,?,?,?,?)",
                            (rid, "update", u["id"], ts, json.dumps(ch, ensure_ascii=False)))
                bump_rev(con)
                con.commit()
            return self.send_json({"id": str(rid), "version": row["version"] + 1})
        m = re.match(r"^/api/requests/(\d+)/files$", path)
        if m and method == "POST":
            u = self.need(con, "plan", "edit")
            rid = int(m.group(1))
            if not con.execute("SELECT 1 FROM requests WHERE id=?", (rid,)).fetchone():
                raise Api(404, "សំណើនេះត្រូវបានលុបរួចហើយ")
            name = parse_qs(urlparse(self.path).query).get("name", [""])[0].strip()
            name = re.sub(r'[\\/:*?"<>|\x00-\x1f]', "_", os.path.basename(name.replace("\\", "/")))[:150] or "plan"
            n = int(self.headers.get("Content-Length") or 0)
            if n <= 0:
                raise Api(400, "ឯកសារទទេ")
            if n > MAX_PLAN_BYTES:
                raise Api(413, "ឯកសារធំពេក (អតិបរមា 200 MB)")
            folder = os.path.join(PLAN_DIR, str(rid))
            os.makedirs(folder, exist_ok=True)
            fname = "%s_%s" % (datetime.now().strftime("%Y%m%d-%H%M%S-") + secrets.token_hex(3), name)
            fp = os.path.join(folder, fname)
            left = n
            with open(fp, "wb") as f:
                while left > 0:
                    chunk = self.rfile.read(min(1 << 20, left))
                    if not chunk:
                        break
                    f.write(chunk)
                    left -= len(chunk)
            if left > 0:
                os.remove(fp)
                raise Api(400, "ការផ្ទេរឯកសារមិនពេញលេញ")
            with write_lock:
                ts = now()
                cur = con.execute("INSERT INTO plan_files(request_id,name,size,path,uploaded_by,at) VALUES(?,?,?,?,?,?)",
                                  (rid, name, n, os.path.join(str(rid), fname), u["id"], ts))
                con.execute("INSERT INTO history(request_id,action,user_id,at,changes) VALUES(?,?,?,?,?)",
                            (rid, "update", u["id"], ts, json.dumps({"plans": [None, name]}, ensure_ascii=False)))
                bump_rev(con)
                con.commit()
            return self.send_json({"id": str(cur.lastrowid), "name": name, "size": n}, 201)
        m = re.match(r"^/api/files/(\d+)$", path)
        if m:
            fid = int(m.group(1))
            if method == "GET":
                u = self.need(con)
                p = perms_of(u)
                if p.get("plan") == "none" and p.get("list") == "none":
                    raise Api(403, "អ្នកមិនមានសិទ្ធិមើលផ្នែកនេះទេ")
                f = con.execute("SELECT * FROM plan_files WHERE id=?", (fid,)).fetchone()
                fp = f and os.path.normpath(os.path.join(PLAN_DIR, f["path"]))
                if not f or not fp.startswith(PLAN_DIR) or not os.path.isfile(fp):
                    raise Api(404, "រកមិនឃើញឯកសារ")
                return self.send_file(fp, f["name"])
            if method == "DELETE":
                u = self.need(con, "plan", "edit")
                with write_lock:
                    f = con.execute("SELECT * FROM plan_files WHERE id=?", (fid,)).fetchone()
                    if f:
                        con.execute("DELETE FROM plan_files WHERE id=?", (fid,))
                        con.execute("INSERT INTO history(request_id,action,user_id,at,changes) VALUES(?,?,?,?,?)",
                                    (f["request_id"], "update", u["id"], now(), json.dumps({"plans": [f["name"], None]}, ensure_ascii=False)))
                        bump_rev(con)
                        con.commit()
                        try:
                            os.remove(os.path.normpath(os.path.join(PLAN_DIR, f["path"])))
                        except OSError:
                            pass
                return self.send_json({"ok": True})

        m = re.match(r"^/api/requests(?:/(\d+))?(/history)?$", path)
        if m:
            rid = int(m.group(1)) if m.group(1) else None
            if m.group(2) and method == "GET":
                self.need(con, "list", "view")
                names = user_names(con)
                return self.send_json({"history": [
                    {"action": r["action"], "at": r["at"], "user": names.get(r["user_id"], ""), "changes": json.loads(r["changes"] or "{}")}
                    for r in con.execute("SELECT * FROM history WHERE request_id=? ORDER BY id DESC LIMIT 100", (rid,))]})
            if method == "POST" and rid is None:
                u = self.need(con, "list", "edit")
                data = keep_plan(u, self.clean(self.body()), {})
                with write_lock:
                    ts = now()
                    cur = con.execute("INSERT INTO requests(data,version,created_by,created_at,updated_by,updated_at) VALUES(?,?,?,?,?,?)",
                                      (json.dumps(data, ensure_ascii=False), 1, u["id"], ts, u["id"], ts))
                    con.execute("INSERT INTO history(request_id,action,user_id,at,changes) VALUES(?,?,?,?,?)",
                                (cur.lastrowid, "create", u["id"], ts, "{}"))
                    bump_rev(con)
                    con.commit()
                return self.send_json({"id": str(cur.lastrowid), "version": 1}, 201)
            if method == "PUT" and rid:
                u = self.need(con, "list", "edit")
                b = self.body()
                data = self.clean(b.get("data"))
                with write_lock:
                    row = con.execute("SELECT * FROM requests WHERE id=?", (rid,)).fetchone()
                    if not row:
                        raise Api(404, "សំណើនេះត្រូវបានលុបរួចហើយ")
                    if int(b.get("version") or 0) != row["version"]:
                        raise Api(409, "សំណើនេះត្រូវបានកែដោយអ្នកផ្សេង ខណៈពេលអ្នកកំពុងកែ។ សូមបិទទម្រង់ ហើយបើកម្តងទៀតដើម្បីឃើញទិន្នន័យថ្មី។")
                    data = keep_plan(u, data, json.loads(row["data"]))
                    ch = diff(json.loads(row["data"]), data)
                    if not ch:
                        return self.send_json({"id": str(rid), "version": row["version"], "unchanged": True})
                    ts = now()
                    con.execute("UPDATE requests SET data=?, version=version+1, updated_by=?, updated_at=? WHERE id=?",
                                (json.dumps(data, ensure_ascii=False), u["id"], ts, rid))
                    con.execute("INSERT INTO history(request_id,action,user_id,at,changes) VALUES(?,?,?,?,?)",
                                (rid, "update", u["id"], ts, json.dumps(ch, ensure_ascii=False)))
                    bump_rev(con)
                    con.commit()
                return self.send_json({"id": str(rid), "version": row["version"] + 1})
            if method == "DELETE" and rid:
                u = self.need(con, "list", "edit")
                with write_lock:
                    row = con.execute("SELECT * FROM requests WHERE id=?", (rid,)).fetchone()
                    if row:
                        con.execute("INSERT INTO history(request_id,action,user_id,at,changes) VALUES(?,?,?,?,?)",
                                    (rid, "delete", u["id"], now(), row["data"]))
                        con.execute("DELETE FROM requests WHERE id=?", (rid,))
                        bump_rev(con)
                        con.commit()
                return self.send_json({"ok": True})

        # ---------------- project files (ឯកសារគម្រោង)
        m = re.match(r"^/api/projects/([A-Za-z0-9_-]{1,40})/files$", path)
        if m and method == "POST":
            gid = m.group(1)
            q = parse_qs(urlparse(self.path).query)
            cat = q.get("cat", ["proj"])[0]
            if cat not in ("proj", "impl", "asbuilt"):
                raise Api(400, "ប្រភេទឯកសារមិនត្រឹមត្រូវ")
            u = self.need(con)
            pp = perms_of(u)
            if not (pp.get("proj") == "edit" or (cat in ("impl", "asbuilt") and pp.get("impl") == "edit")):
                raise Api(403, "អ្នកមិនមានសិទ្ធិដាក់ឯកសារនៅផ្នែកនេះទេ")
            if not con.execute("SELECT 1 FROM projects WHERE id=?", (gid,)).fetchone():
                raise Api(404, "គម្រោងនេះត្រូវបានលុបរួចហើយ")
            name = q.get("name", [""])[0].strip()
            name = re.sub(r'[\\/:*?"<>|\x00-\x1f]', "_", os.path.basename(name.replace("\\", "/")))[:150] or "file"
            n = int(self.headers.get("Content-Length") or 0)
            if n <= 0:
                raise Api(400, "ឯកសារទទេ")
            if n > MAX_PLAN_BYTES:
                raise Api(413, "ឯកសារធំពេក (អតិបរមា 200 MB)")
            folder = os.path.join(PFILE_DIR, gid)
            os.makedirs(folder, exist_ok=True)
            fname = "%s_%s_%s" % (cat, datetime.now().strftime("%Y%m%d-%H%M%S-") + secrets.token_hex(3), name)
            fp = os.path.join(folder, fname)
            left = n
            with open(fp, "wb") as f:
                while left > 0:
                    chunk = self.rfile.read(min(1 << 20, left))
                    if not chunk:
                        break
                    f.write(chunk)
                    left -= len(chunk)
            if left > 0:
                os.remove(fp)
                raise Api(400, "ការផ្ទេរឯកសារមិនពេញលេញ")
            with write_lock:
                cur = con.execute("INSERT INTO project_files(group_id,cat,name,size,path,uploaded_by,at) VALUES(?,?,?,?,?,?,?)",
                                  (gid, cat, name, n, os.path.join(gid, fname), u["id"], now()))
                bump_prev(con)
                con.commit()
            return self.send_json({"id": str(cur.lastrowid), "name": name, "size": n}, 201)
        m = re.match(r"^/api/pfiles/(\d+)$", path)
        if m:
            fid = int(m.group(1))
            u = self.need(con)
            pp = perms_of(u)
            f = con.execute("SELECT * FROM project_files WHERE id=?", (fid,)).fetchone()
            if method == "GET":
                if pp.get("proj") == "none" and pp.get("impl") == "none":
                    raise Api(403, "អ្នកមិនមានសិទ្ធិមើលផ្នែកនេះទេ")
                fp = f and os.path.normpath(os.path.join(PFILE_DIR, f["path"]))
                if not f or not fp.startswith(os.path.normpath(PFILE_DIR)) or not os.path.isfile(fp):
                    raise Api(404, "រកមិនឃើញឯកសារ")
                return self.send_file(fp, f["name"])
            if method == "DELETE":
                if f:
                    if not (pp.get("proj") == "edit" or (f["cat"] in ("impl", "asbuilt") and pp.get("impl") == "edit")):
                        raise Api(403, "អ្នកមិនមានសិទ្ធិលុបឯកសារនេះទេ")
                    with write_lock:
                        con.execute("DELETE FROM project_files WHERE id=?", (fid,))
                        bump_prev(con)
                        con.commit()
                    try:
                        os.remove(os.path.normpath(os.path.join(PFILE_DIR, f["path"])))
                    except OSError:
                        pass
                return self.send_json({"ok": True})

        m = re.match(r"^/api/projects(?:/([A-Za-z0-9_-]{1,40}))?$", path)
        if m:
            pid = m.group(1)
            if method == "GET" and not pid:
                u = self.need(con)
                pp = perms_of(u)
                if pp.get("proj", "none") == "none" and pp.get("impl", "none") == "none":
                    raise Api(403, "អ្នកមិនមានសិទ្ធិមើលផ្នែកនេះទេ")
                names = user_names(con)
                groups = []
                pf = {}
                for f in con.execute("SELECT * FROM project_files ORDER BY id"):
                    pf.setdefault(f["group_id"], []).append({"id": str(f["id"]), "cat": f["cat"], "name": f["name"], "size": f["size"],
                                                             "at": f["at"], "byName": names.get(f["uploaded_by"], "")})
                for r in con.execute("SELECT * FROM projects"):
                    d = json.loads(r["data"])
                    d["files"] = pf.get(r["id"], [])
                    d["updatedAt"] = r["updated_at"]
                    d["updatedBy"] = names.get(r["updated_by"], "")
                    groups.append({"id": r["id"], "version": r["version"], "data": d})
                return self.send_json({"groups": groups, "prev": get_prev(con)})
            if method == "POST" and not pid:
                u = self.need(con, "proj", "edit")
                try:
                    data = clean_project(self.body(limit=5_000_000).get("data"))
                except ValueError as e:
                    raise Api(400, str(e))
                gid = "g" + secrets.token_hex(6)
                with write_lock:
                    con.execute("INSERT INTO projects(id,data,version,updated_by,updated_at) VALUES(?,?,1,?,?)",
                                (gid, json.dumps(data, ensure_ascii=False), u["id"], now()))
                    bump_prev(con)
                    con.commit()
                return self.send_json({"id": gid, "version": 1}, 201)
            if method == "PUT" and pid:
                u = self.need(con)
                pp = perms_of(u)
                impl_only = pp.get("proj") != "edit"
                if impl_only and pp.get("impl") != "edit":
                    raise Api(403, "អ្នកមិនមានសិទ្ធិកែប្រែផ្នែកនេះទេ")
                b = self.body(limit=5_000_000)
                try:
                    data = clean_project(b.get("data"))
                except ValueError as e:
                    raise Api(400, str(e))
                with write_lock:
                    row = con.execute("SELECT * FROM projects WHERE id=?", (pid,)).fetchone()
                    if not row:
                        raise Api(404, "គម្រោងនេះត្រូវបានលុបរួចហើយ")
                    if b.get("version") is not None and int(b["version"]) != row["version"]:
                        raise Api(409, "គម្រោងនេះត្រូវបានកែដោយអ្នកផ្សេង ខណៈពេលអ្នកកំពុងកែ។ សូមបិទ ហើយបើកម្តងទៀត ដើម្បីឃើញទិន្នន័យថ្មី។")
                    if impl_only and not impl_change_only(json.loads(row["data"]), data):
                        raise Api(403, "សិទ្ធិ «ការអនុវត្តគម្រោង» អាចកែបានតែស្ថានភាពអនុវត្ត ថ្ងៃអនុវត្តរួចរាល់ និងកំណត់សម្គាល់ប៉ុណ្ណោះ")
                    con.execute("UPDATE projects SET data=?, version=version+1, updated_by=?, updated_at=? WHERE id=?",
                                (json.dumps(data, ensure_ascii=False), u["id"], now(), pid))
                    bump_prev(con)
                    con.commit()
                return self.send_json({"id": pid, "version": row["version"] + 1})
            if method == "DELETE" and pid:
                self.need(con, "proj", "edit")
                with write_lock:
                    con.execute("DELETE FROM projects WHERE id=?", (pid,))
                    bump_prev(con)
                    con.commit()
                return self.send_json({"ok": True})

        if path == "/api/lists" and method == "PUT":
            self.need(con, "settings", "edit")
            b = self.body()
            if not isinstance(b, dict) or not all(isinstance(v, list) for v in b.values()):
                raise Api(400, "ទិន្នន័យបញ្ជីមិនត្រឹមត្រូវ")
            clean = {k: [str(x).strip()[:200] for x in v if str(x).strip()][:(5000 if k == "village" else 400)] for k, v in b.items() if re.match(r"^[a-z]{2,20}$", k)}
            con.execute("INSERT OR REPLACE INTO settings(key,value) VALUES('lists',?)", (json.dumps(clean, ensure_ascii=False),))
            bump_rev(con)
            con.commit()
            return self.send_json({"ok": True})

        # ---------------- users (admin)
        if path == "/api/users" and method == "GET":
            self.need(con, admin=True)
            return self.send_json({"users": [dict(id=r["id"], username=r["username"], name=r["name"], role=r["role"],
                                                  active=bool(r["active"]), perms=perms_of(r))
                                             for r in con.execute("SELECT * FROM users ORDER BY id")]})
        if path == "/api/users" and method == "POST":
            self.need(con, admin=True)
            with write_lock:
                uid = self.create_user(con, self.body())
                con.commit()
            return self.send_json({"id": uid}, 201)
        m = re.match(r"^/api/users/(\d+)$", path)
        if m and method == "PUT":
            me = self.need(con, admin=True)
            uid = int(m.group(1))
            b = self.body()
            t = con.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()
            if not t:
                raise Api(404, "រកអ្នកប្រើមិនឃើញ")
            role = b.get("role", t["role"])
            active = 1 if b.get("active", bool(t["active"])) else 0
            if role not in ("admin", "user"):
                raise Api(400, "តួនាទីមិនត្រឹមត្រូវ")
            if uid == me["id"] and (role != "admin" or not active):
                raise Api(400, "អ្នកមិនអាចដកសិទ្ធិអ្នកគ្រប់គ្រងពីខ្លួនឯងបានទេ")
            perms = clean_perms(b["perms"]) if "perms" in b else perms_of(t)
            name = str(b.get("name", t["name"])).strip()[:80] or t["name"]
            con.execute("UPDATE users SET role=?, active=?, perms=?, name=? WHERE id=?",
                        (role, active, json.dumps(perms), name, uid))
            if b.get("password"):
                if len(str(b["password"])) < 6:
                    raise Api(400, "ពាក្យសម្ងាត់ត្រូវមានយ៉ាងហោចណាស់ 6 តួ")
                con.execute("UPDATE users SET pw_hash=? WHERE id=?", (hash_pw(str(b["password"])), uid))
                con.execute("DELETE FROM sessions WHERE user_id=?", (uid,))
            if not active:
                con.execute("DELETE FROM sessions WHERE user_id=?", (uid,))
            bump_rev(con)
            con.commit()
            return self.send_json({"ok": True})
        if m and method == "DELETE":
            me = self.need(con, admin=True)
            uid = int(m.group(1))
            t = con.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()
            if not t:
                raise Api(404, "រកអ្នកប្រើមិនឃើញ")
            if uid == me["id"]:
                raise Api(400, "អ្នកមិនអាចលុបគណនីខ្លួនឯងបានទេ")
            if t["role"] == "admin" and con.execute("SELECT COUNT(*) FROM users WHERE role='admin' AND active=1 AND id<>?", (uid,)).fetchone()[0] == 0:
                raise Api(400, "ត្រូវមានអ្នកគ្រប់គ្រងយ៉ាងហោចណាស់ម្នាក់")
            with write_lock:
                r = con.execute("SELECT value FROM settings WHERE key='deleted_users'").fetchone()
                gone = json.loads(r[0]) if r else {}
                gone[str(uid)] = t["name"] + " (បានលុប)"
                con.execute("INSERT OR REPLACE INTO settings(key,value) VALUES('deleted_users',?)", (json.dumps(gone, ensure_ascii=False),))
                con.execute("DELETE FROM sessions WHERE user_id=?", (uid,))
                con.execute("DELETE FROM users WHERE id=?", (uid,))
                bump_rev(con)
                con.commit()
            return self.send_json({"ok": True})

        # ---------------- backups
        if path == "/api/backups" and method == "GET":
            self.need(con, admin=True)
            items = []
            for f in sorted((f for f in os.listdir(BACKUP_DIR) if f.endswith(".json")), reverse=True):
                fp = os.path.join(BACKUP_DIR, f)
                mm = re.match(r"workflow-(\d{8})-(\d{6})-([\w-]+)\.json$", f)
                try:
                    with open(fp, encoding="utf-8") as fh:
                        jj = json.load(fh)
                    cnt = len(jj.get("requests", []))
                    pcnt = len(jj["projects"]) if isinstance(jj.get("projects"), list) else None
                    counts = jj.get("counts") or snap_counts(jj.get("requests") or [], jj.get("projects") or [])
                except Exception:
                    cnt = pcnt = counts = None
                items.append({"file": f, "at": "%s-%s-%sT%s:%s" % (mm.group(1)[:4], mm.group(1)[4:6], mm.group(1)[6:], mm.group(2)[:2], mm.group(2)[2:4]) if mm else "",
                              "tag": mm.group(3) if mm else "", "count": cnt, "pcount": pcnt, "counts": counts, "size": os.path.getsize(fp)})
            return self.send_json({"backups": items, "folder": BACKUP_DIR})
        if path == "/api/backup" and method == "POST":
            self.need(con, admin=True)
            save_prefs(con, (self.body(limit=20_000_000) or {}).get("prefs"))
            return self.send_json({"ok": True, "file": backup("manual")})
        m = re.match(r"^/api/backups/(workflow-\d{8}-\d{6}-[\w-]+\.json)$", path)
        if m and method == "GET":
            self.need(con, admin=True)
            fp = os.path.join(BACKUP_DIR, m.group(1))
            if not os.path.isfile(fp):
                raise Api(404, "រកមិនឃើញឯកសារបម្រុងទុក")
            with open(fp, "rb") as f:
                b = f.read()
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Disposition", 'attachment; filename="%s"' % m.group(1))
            self.send_header("Content-Length", str(len(b)))
            self.end_headers()
            self.wfile.write(b)
            return
        if path == "/api/backup-full.zip" and method == "GET":
            # full backup: database + JSON snapshot + plan files + project files (PDF / images)
            self.need(con, admin=True)
            tmpdir = tempfile.mkdtemp(prefix="wfbk-")
            try:
                dbcopy = os.path.join(tmpdir, "workflow.db")
                out = sqlite3.connect(dbcopy)
                with out:
                    con.backup(out)
                out.close()
                zpath = os.path.join(tmpdir, "full.zip")
                with zipfile.ZipFile(zpath, "w", zipfile.ZIP_DEFLATED) as z:
                    z.write(dbcopy, "data/workflow.db")
                    z.writestr("backup.json", json.dumps(snapshot(con), ensure_ascii=False))
                    for base, arc in ((PLAN_DIR, "data/plans"), (PFILE_DIR, "data/project-files")):
                        for root, _dirs, files in os.walk(base):
                            for fn in files:
                                fp = os.path.join(root, fn)
                                z.write(fp, os.path.join(arc, os.path.relpath(fp, base)).replace(os.sep, "/"))
                    z.writestr("README.txt", "WorkflowTracker full backup %s\r\n- data/workflow.db : database (requests, projects, users, settings)\r\n- data/plans, data/project-files : attached files\r\n- backup.json : same data as JSON (can be restored from the admin page)\r\nTo restore everything: stop the server, copy the data folder over the server's data folder, start the server.\r\n" % datetime.now().isoformat(timespec="seconds"))
                size = os.path.getsize(zpath)
                self.send_response(200)
                self.send_header("Content-Type", "application/zip")
                self.send_header("Content-Disposition", 'attachment; filename="workflow-full-backup-%s.zip"' % datetime.now().strftime("%Y%m%d-%H%M"))
                self.send_header("Content-Length", str(size))
                self.end_headers()
                with open(zpath, "rb") as fh:
                    while True:
                        chunk = fh.read(1 << 20)
                        if not chunk:
                            break
                        self.wfile.write(chunk)
            finally:
                for root, dirs, files in os.walk(tmpdir, topdown=False):
                    for fn in files:
                        os.remove(os.path.join(root, fn))
                    for dn in dirs:
                        os.rmdir(os.path.join(root, dn))
                os.rmdir(tmpdir)
            return
        if path == "/api/backup.json" and method == "GET":
            self.need(con, download=True)
            b = json.dumps(snapshot(con), ensure_ascii=False).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Disposition", 'attachment; filename="workflow-backup-%s.json"' % datetime.now().strftime("%Y%m%d"))
            self.send_header("Content-Length", str(len(b)))
            self.end_headers()
            self.wfile.write(b)
            return
        if path == "/api/projects-clear" and method == "POST":
            u = self.need(con, admin=True)
            j = self.body()
            if j.get("confirm") != "CLEAR":
                raise Api(400, "សូមបញ្ជាក់ការលុប")
            before = backup("before-clear-projects")
            with write_lock:
                n = con.execute("SELECT COUNT(*) FROM projects").fetchone()[0]
                con.execute("DELETE FROM projects")
                bump_prev(con)
                con.commit()
            return self.send_json({"ok": True, "deleted": n, "backup": os.path.basename(before) if before else ""})
        if path == "/api/all-clear" and method == "POST":
            # every tab: requests (+ plan study) and projects (+ branch implementation); keeps users, settings, backups and attached files
            u = self.need(con, admin=True)
            j = self.body()
            if j.get("confirm") != "CLEAR-ALL":
                raise Api(400, "សូមបញ្ជាក់ការលុប")
            before = backup("before-clear-all")
            with write_lock:
                nr = con.execute("SELECT COUNT(*) FROM requests").fetchone()[0]
                np_ = con.execute("SELECT COUNT(*) FROM projects").fetchone()[0]
                con.execute("DELETE FROM requests")
                con.execute("DELETE FROM history")
                con.execute("DELETE FROM projects")
                bump_rev(con)
                bump_prev(con)
                con.commit()
            return self.send_json({"ok": True, "requests": nr, "projects": np_, "backup": os.path.basename(before) if before else ""})
        if path == "/api/restore" and method == "POST":
            u = self.need(con, admin=True)
            j = self.body(limit=50_000_000)
            if j.get("format") != "wf-backup" or not isinstance(j.get("requests"), list):
                raise Api(400, "ឯកសារនេះមិនមែនជាឯកសារបម្រុងទុករបស់ប្រព័ន្ធនេះទេ")
            items = []
            for x in j["requests"]:
                try:
                    items.append((x.get("id"), clean_request(x.get("data") or {})))
                except ValueError as e:
                    raise Api(400, "ទិន្នន័យក្នុងឯកសារបម្រុងទុកមិនត្រឹមត្រូវ ៖ %s" % e)
            before = backup("before-restore")
            with write_lock:
                ts = now()
                con.execute("DELETE FROM requests")
                used = set()
                for xid, d in items:
                    xid = int(xid) if str(xid or "").isdigit() and int(xid) not in used else None
                    if xid:
                        used.add(xid)
                    cur = con.execute("INSERT INTO requests(id,data,version,created_by,created_at,updated_by,updated_at) VALUES(?,?,?,?,?,?,?)",
                                      (xid, json.dumps(d, ensure_ascii=False), 1, u["id"], ts, u["id"], ts))
                    con.execute("INSERT INTO history(request_id,action,user_id,at,changes) VALUES(?,?,?,?,?)",
                                (cur.lastrowid, "restore", u["id"], ts, "{}"))
                if isinstance(j.get("projects"), list):
                    con.execute("DELETE FROM projects")
                    for g in j["projects"]:
                        try:
                            pd = clean_project(g.get("data"))
                        except ValueError:
                            continue
                        gid = str(g.get("id") or "g" + secrets.token_hex(6))[:40]
                        con.execute("INSERT OR REPLACE INTO projects(id,data,version,updated_by,updated_at) VALUES(?,?,1,?,?)",
                                    (gid, json.dumps(pd, ensure_ascii=False), u["id"], ts))
                    bump_prev(con)
                if isinstance(j.get("lists"), dict):
                    con.execute("INSERT OR REPLACE INTO settings(key,value) VALUES('lists',?)", (json.dumps(j["lists"], ensure_ascii=False),))
                nu = restore_users(con, j.get("users"), u)
                save_prefs(con, j.get("prefs"), commit=False)
                if isinstance(j.get("deletedUsers"), dict):
                    con.execute("INSERT OR REPLACE INTO settings(key,value) VALUES('deleted_users',?)", (json.dumps(j["deletedUsers"], ensure_ascii=False),))
                bump_rev(con)
                con.commit()
            return self.send_json({"ok": True, "count": len(items), "users": nu, "before": before})

        raise Api(404, "រកមិនឃើញ")

    def clean(self, b):
        try:
            return clean_request(b)
        except ValueError as e:
            raise Api(400, str(e))

    def create_user(self, con, b, force_admin=False):
        username = str(b.get("username", "")).strip()
        name = str(b.get("name", "")).strip()[:80]
        pw = str(b.get("password", ""))
        role = "admin" if force_admin else b.get("role", "user")
        if not re.match(r"^[A-Za-z0-9._-]{3,30}$", username):
            raise Api(400, "ឈ្មោះអ្នកប្រើ ៖ អក្សរឡាតាំង លេខ . _ - ចំនួន 3–30 តួ")
        if not name:
            raise Api(400, "សូមបញ្ចូលឈ្មោះពេញ")
        if len(pw) < 6:
            raise Api(400, "ពាក្យសម្ងាត់ត្រូវមានយ៉ាងហោចណាស់ 6 តួ")
        if role not in ("admin", "user"):
            raise Api(400, "តួនាទីមិនត្រឹមត្រូវ")
        if con.execute("SELECT 1 FROM users WHERE username=?", (username,)).fetchone():
            raise Api(409, "ឈ្មោះអ្នកប្រើនេះមានរួចហើយ")
        cur = con.execute("INSERT INTO users(username,name,role,perms,pw_hash,created_at) VALUES(?,?,?,?,?,?)",
                          (username, name, role, json.dumps(clean_perms(b.get("perms"))), hash_pw(pw), now()))
        return cur.lastrowid

    def login_as(self, con, uid):
        t = secrets.token_urlsafe(32)
        exp = (datetime.now() + timedelta(days=SESSION_DAYS)).strftime("%Y-%m-%dT%H:%M:%S")
        con.execute("DELETE FROM sessions WHERE expires_at<?", (now(),))
        con.execute("INSERT INTO sessions(token,user_id,expires_at) VALUES(?,?,?)", (t, uid, exp))
        con.commit()
        u = con.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()
        return self.send_json({"user": self.user_pub(u)},
                              headers={"Set-Cookie": "wsid=%s; Path=/; Max-Age=%d; HttpOnly; SameSite=Strict" % (t, SESSION_DAYS * 86400)})


def lan_ips():
    ips = set()
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("10.255.255.255", 1))
        ips.add(s.getsockname()[0])
        s.close()
    except OSError:
        pass
    try:
        for info in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET):
            ips.add(info[4][0])
    except OSError:
        pass
    return sorted(i for i in ips if not i.startswith("127."))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8080)
    ap.add_argument("--host", default="0.0.0.0")
    a = ap.parse_args()
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8")
        except Exception:
            pass
    try:
        init_db()
    except Exception as e:
        print("[ERROR] មិនអាចបើក database ៖", e)
        print("សូមពិនិត្យថា folder នេះមិនស្ថិតក្នុង zip ឬ folder ដែលបានចាក់សោ (read-only)។")
        raise
    threading.Thread(target=backup_loop, daemon=True).start()
    try:
        srv = ThreadingHTTPServer((a.host, a.port), Handler)
    except OSError as e:
        print("[ERROR] មិនអាចបើក port %d ៖ %s" % (a.port, e))
        print("Port នេះប្រហែលជាកំពុងប្រើដោយកម្មវិធីផ្សេង ឬ server បានបើករួចហើយក្នុងបង្អួចផ្សេង។")
        print("សាកល្បង ៖  py server.py --port 8090")
        sys.exit(1)
    print("=" * 62)
    print(" ប្រព័ន្ធគ្រប់គ្រងលំហូរឯកសារ កំពុងដំណើរការ")
    print(" លើកុំព្យូទ័រនេះ ៖  http://localhost:%d" % a.port)
    for ip in lan_ips():
        print(" សម្រាប់អ្នកផ្សេងក្នុងការិយាល័យ ៖  http://%s:%d" % (ip, a.port))
    print(" ទិន្នន័យ ៖ %s" % DB_PATH)
    print(" កុំបិទបង្អួចនេះ ពេលកំពុងប្រើ។ ចុច Ctrl+C ដើម្បីបិទ server។")
    print("=" * 62)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\n បានបិទ server។")


if __name__ == "__main__":
    main()
