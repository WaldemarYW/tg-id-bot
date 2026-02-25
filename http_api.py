import os
import re
import sqlite3
from pathlib import Path

from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware

API_KEY = (os.getenv("API_KEY", "") or "").strip()
DB_PATH = (os.getenv("TG_DB_PATH", "") or "").strip()
if not DB_PATH:
    DB_PATH = str((Path(__file__).parent / "messages.db").resolve())

ALLOWED_ORIGINS_RAW = os.getenv("ALLOWED_ORIGINS", "*")
TEN_DIGITS = re.compile(r"^\d{10}$")

raw_origins = (ALLOWED_ORIGINS_RAW or "").strip()
if not raw_origins or raw_origins == "*":
    allow_origins = ["*"]
    allow_credentials = False
else:
    allow_origins = [o.strip() for o in raw_origins.split(",") if o.strip()]
    allow_credentials = True
    if not allow_origins:
        allow_origins = ["*"]
        allow_credentials = False

app = FastAPI(title="TG ID Bot Count API", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_credentials=allow_credentials,
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)


def auth(authorization: str | None = Header(default=None)):
    if not API_KEY:
        return True
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Unauthorized")
    token = authorization.split(" ", 1)[1].strip()
    if token != API_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")
    return True


def get_conn() -> sqlite3.Connection:
    if not os.path.exists(DB_PATH):
        raise HTTPException(status_code=500, detail="TG DB file not found")
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


@app.get("/api/health")
def health():
    return {"ok": True, "db_path": DB_PATH}


@app.get("/api/tg/count")
def count_by_male_id(male_id: str, _=Depends(auth)):
    male_id = str(male_id or "").strip()
    if not TEN_DIGITS.match(male_id):
        raise HTTPException(status_code=400, detail="male_id must be exactly 10 digits")

    conn = get_conn()
    try:
        row = conn.execute(
            """
            SELECT COUNT(DISTINCT m.id) AS c
            FROM messages m
            JOIN message_male_ids mm ON mm.message_id_ref = m.id
            WHERE mm.male_id = ?
            """,
            (male_id,),
        ).fetchone()
        count = int(row["c"] if row and row["c"] is not None else 0)
        return {"ok": True, "male_id": male_id, "count": count}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"DB error: {exc}")
    finally:
        conn.close()
