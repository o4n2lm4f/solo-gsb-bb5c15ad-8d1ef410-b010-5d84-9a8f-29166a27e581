"""消费游标模块: 客户端按 consumer_id 以"租约 + 确认"方式顺序消费事件流。

- pull: 原子租用游标之后一段尚未确认的连续事件, 返回 lease_token;
  租约未过期时重复拉取返回同一批次, 过期后该批次可重新投递(新 token)。
- ack: 仅接受当前租约 token, 把游标推进到本批次末尾; 过期、伪造或旧 token 返回 409。
"""
import secrets
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from .db import get_conn, row_to_event

router = APIRouter(prefix="/consumers", tags=["consumers"])


class PullIn(BaseModel):
    limit: int = Field(50, ge=1, le=500)
    lease_seconds: int = Field(30, ge=1, le=3600)


class AckIn(BaseModel):
    lease_token: str = Field(..., min_length=1, max_length=255)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _get_consumer(conn: sqlite3.Connection, consumer_id: str) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM consumers WHERE consumer_id = ?", (consumer_id,)
    ).fetchone()


def _fetch_range(conn: sqlite3.Connection, start_seq: int, end_seq: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM events WHERE id BETWEEN ? AND ? ORDER BY id",
        (start_seq, end_seq),
    ).fetchall()
    return [row_to_event(r) for r in rows]


def _batch_response(consumer_id: str, cursor: int, token: Optional[str],
                    expires_at: Optional[str], events: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "consumer_id": consumer_id,
        "cursor": cursor,  # 本批次之前已确认到的 seq
        "lease_token": token,
        "lease_expires_at": expires_at,
        "count": len(events),
        "events": events,
    }


@router.post("/{consumer_id}/pull")
def pull(consumer_id: str, req: PullIn):
    """拉取游标之后的事件批次。

    未过期租约存在时返回同一批次(幂等); 否则在同一事务内租用新批次。
    空队列返回空批次且不创建租约。
    """
    now = _utc_now()
    with get_conn() as conn:
        # BEGIN IMMEDIATE 提前获取写锁, 保证"检查租约 -> 建立租约"原子完成
        conn.execute("BEGIN IMMEDIATE")
        consumer = _get_consumer(conn, consumer_id)
        if consumer is None:
            # 首次出现的 consumer 从 seq=1 开始(cursor=0)
            conn.execute(
                "INSERT INTO consumers (consumer_id, cursor) VALUES (?, 0)",
                (consumer_id,),
            )
            consumer = _get_consumer(conn, consumer_id)

        # 租约未过期 -> 重复拉取必须返回同一批次(同一 token, 不延长过期时间)
        if consumer["lease_token"] is not None and \
                datetime.fromisoformat(consumer["lease_expires_at"]) > now:
            events = _fetch_range(conn, consumer["lease_start_seq"], consumer["lease_end_seq"])
            return _batch_response(
                consumer_id, consumer["cursor"],
                consumer["lease_token"], consumer["lease_expires_at"], events,
            )

        # 无租约或已过期 -> 从游标后原子租用一段连续事件
        start_seq = consumer["cursor"] + 1
        rows = conn.execute(
            "SELECT * FROM events WHERE id >= ? ORDER BY id LIMIT ?",
            (start_seq, req.limit),
        ).fetchall()

        if not rows:
            # 空队列: 返回空批次, 不创建租约
            return _batch_response(consumer_id, consumer["cursor"], None, None, [])

        end_seq = rows[-1]["id"]
        token = secrets.token_hex(16)
        expires_at = (now + timedelta(seconds=req.lease_seconds)).isoformat()
        conn.execute(
            "UPDATE consumers SET lease_token = ?, lease_start_seq = ?,"
            " lease_end_seq = ?, lease_expires_at = ? WHERE consumer_id = ?",
            (token, start_seq, end_seq, expires_at, consumer_id),
        )
        return _batch_response(
            consumer_id, consumer["cursor"], token, expires_at,
            [row_to_event(r) for r in rows],
        )


@router.post("/{consumer_id}/ack")
def ack(consumer_id: str, req: AckIn):
    """确认当前租约批次, 把游标推进到本批次末尾。

    仅接受当前租约 token; 过期、伪造或旧 token 一律返回 409, 禁止越序确认。
    """
    now = _utc_now()
    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        consumer = _get_consumer(conn, consumer_id)
        if consumer is None or consumer["lease_token"] is None:
            raise HTTPException(status_code=409, detail="no_active_lease")
        if datetime.fromisoformat(consumer["lease_expires_at"]) <= now:
            raise HTTPException(status_code=409, detail="lease_expired")
        if not secrets.compare_digest(req.lease_token, consumer["lease_token"]):
            raise HTTPException(status_code=409, detail="lease_token_mismatch")

        new_cursor = consumer["lease_end_seq"]
        conn.execute(
            "UPDATE consumers SET cursor = ?, lease_token = NULL, lease_start_seq = NULL,"
            " lease_end_seq = NULL, lease_expires_at = NULL WHERE consumer_id = ?",
            (new_cursor, consumer_id),
        )
    return {"status": "acked", "consumer_id": consumer_id, "cursor": new_cursor}
