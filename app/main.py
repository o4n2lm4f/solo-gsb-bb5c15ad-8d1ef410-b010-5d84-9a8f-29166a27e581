import json
import sqlite3
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .chain import GENESIS_HASH, record_hash
from .db import get_conn, init_db

app = FastAPI(title="Webhook Event Receiver & Replay")


@app.on_event("startup")
def startup() -> None:
    init_db()


# ---------- models ----------

class EventIn(BaseModel):
    event_id: str = Field(..., min_length=1, max_length=255)
    occurred_at: datetime
    payload: dict[str, Any]


class ReplayIn(BaseModel):
    event_ids: list[str] = Field(..., min_length=1, max_length=1000)


# ---------- helpers ----------

def normalize_dt(dt: datetime) -> str:
    """归一化为 UTC ISO8601 字符串, 保证可按字典序做时间范围比较。"""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def row_to_event(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "seq": row["id"],  # 首次接收顺序号
        "event_id": row["event_id"],
        "occurred_at": row["occurred_at"],
        "payload": json.loads(row["payload"]),
        "received_at": row["received_at"],
    }


# ---------- endpoints ----------

@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/events", status_code=201)
def create_event(event: EventIn):
    """接收单个事件, 按 event_id 幂等: 重复投递不产生重复记录, 也不改变哈希链。"""
    received_at = utc_now()
    occurred_at = normalize_dt(event.occurred_at)
    try:
        with get_conn() as conn:
            # BEGIN IMMEDIATE 提前获取写锁, 保证"读链头 -> 写入"在同一事务内原子完成
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT hash FROM events ORDER BY id DESC LIMIT 1"
            ).fetchone()
            prev_hash = row["hash"] if row else GENESIS_HASH
            cur = conn.execute(
                "INSERT INTO events (event_id, occurred_at, payload, received_at)"
                " VALUES (?, ?, ?, ?)",
                (
                    event.event_id,
                    occurred_at,
                    json.dumps(event.payload, ensure_ascii=False),
                    received_at,
                ),
            )
            seq = cur.lastrowid
            conn.execute(
                "UPDATE events SET prev_hash = ?, hash = ? WHERE id = ?",
                (
                    prev_hash,
                    record_hash(
                        seq=seq,
                        event_id=event.event_id,
                        occurred_at=occurred_at,
                        payload=event.payload,
                        received_at=received_at,
                        prev_hash=prev_hash,
                    ),
                    seq,
                ),
            )
    except sqlite3.IntegrityError:
        # event_id 已存在 -> 幂等返回, 不写入重复记录, 链保持不变
        return JSONResponse(
            status_code=200,
            content={"status": "duplicate", "event_id": event.event_id},
        )
    return {"status": "created", "event_id": event.event_id}


@app.get("/events")
def list_events(
    from_: Optional[datetime] = Query(None, alias="from", description="occurred_at 下界(含)"),
    to: Optional[datetime] = Query(None, description="occurred_at 上界(含)"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
):
    """按 occurred_at 时间范围分页查询。"""
    clauses, params = [], []
    if from_ is not None:
        clauses.append("occurred_at >= ?")
        params.append(normalize_dt(from_))
    if to is not None:
        clauses.append("occurred_at <= ?")
        params.append(normalize_dt(to))
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""

    with get_conn() as conn:
        total = conn.execute(
            f"SELECT COUNT(*) AS c FROM events {where}", params
        ).fetchone()["c"]
        rows = conn.execute(
            f"SELECT * FROM events {where} ORDER BY occurred_at, id LIMIT ? OFFSET ?",
            (*params, page_size, (page - 1) * page_size),
        ).fetchall()

    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "items": [row_to_event(r) for r in rows],
    }


@app.post("/replay")
def replay(req: ReplayIn):
    """批量重放: 命中的事件按首次接收顺序输出, 未命中的标记为 skipped。"""
    # 请求内去重, 保证重复 id 不产生重复输出
    requested = list(dict.fromkeys(req.event_ids))

    found: dict[str, dict[str, Any]] = {}
    with get_conn() as conn:
        placeholders = ",".join("?" * len(requested))
        rows = conn.execute(
            f"SELECT * FROM events WHERE event_id IN ({placeholders}) ORDER BY id",
            requested,
        ).fetchall()
    for r in rows:
        found[r["event_id"]] = row_to_event(r)

    results: list[dict[str, Any]] = []
    # 命中部分: 严格按首次接收顺序(seq)输出
    for event in sorted(found.values(), key=lambda e: e["seq"]):
        results.append({"event_id": event["event_id"], "status": "replayed", "event": event})
    # 未命中部分: 按请求顺序追加, 标记 skipped
    for eid in requested:
        if eid not in found:
            results.append({"event_id": eid, "status": "skipped", "reason": "not_found"})

    replayed = sum(1 for r in results if r["status"] == "replayed")
    return {
        "replayed": replayed,
        "skipped": len(results) - replayed,
        "results": results,
    }


def _chain_break(checked: int, seq: int, event_id: str, reason: str) -> dict[str, Any]:
    return {
        "verified": False,
        "checked": checked,
        "first_break": {"seq": seq, "event_id": event_id, "reason": reason},
    }


@app.get("/integrity")
def integrity():
    """从最早记录开始校验哈希链: 序号连续性、前后哈希衔接、记录内容哈希。"""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT id, event_id, occurred_at, payload, received_at, prev_hash, hash"
            " FROM events ORDER BY id"
        ).fetchall()

    checked = 0
    prev_seq: Optional[int] = None
    prev_hash = GENESIS_HASH
    for row in rows:
        seq = row["id"]
        # 序号连续性: 相邻记录 seq 必须逐一递增
        if prev_seq is not None and seq != prev_seq + 1:
            return _chain_break(checked, seq, row["event_id"], "seq_gap")
        # 前后哈希衔接: prev_hash 必须等于上一条记录的 hash(首条为创世空串)
        if row["prev_hash"] is None or row["prev_hash"] != prev_hash:
            return _chain_break(checked, seq, row["event_id"], "prev_hash_mismatch")
        # 事件字段: 按固定 JSON 规范重算哈希并与存储值比对
        if row["hash"] is None or row["hash"] != record_hash(
            seq=seq,
            event_id=row["event_id"],
            occurred_at=row["occurred_at"],
            payload=json.loads(row["payload"]),
            received_at=row["received_at"],
            prev_hash=row["prev_hash"],
        ):
            return _chain_break(checked, seq, row["event_id"], "hash_mismatch")
        checked += 1
        prev_seq = seq
        prev_hash = row["hash"]

    return {"verified": True, "checked": checked, "first_break": None}
