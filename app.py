import asyncio
import json
import os
import io
import random
import time
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple

import aiohttp
import uvicorn
from fastapi import FastAPI, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic_settings import BaseSettings
from pydantic import Field
from telethon import TelegramClient, events
from telethon.errors import ChannelPrivateError, FloodWaitError, ChatAdminRequiredError
from telethon.tl.types import Message, MessageMediaPhoto, MessageMediaDocument
from telethon.tl.functions.channels import JoinChannelRequest, LeaveChannelRequest
from telethon.tl.functions.messages import GetHistoryRequest

# ---------- Config (.env) ----------

class Cfg(BaseSettings):
    # Telegram API
    API_ID: int
    API_HASH: str

    # Callback
    CALLBACK_URL: str = ""
    CALLBACK_BEARER: str = ""

    # API security
    API_BEARER: str = ""

    # Scan/poll
    SCAN_INTERVAL_SEC: int = 60
    BATCH_MAX: int = 50
    BACKOFF_MIN_MS: int = 150
    BACKOFF_MAX_MS: int = 600

    # Session rescan + join throttle
    SESS_RESCAN_SEC: int = 30
    JOIN_INTERVAL_SEC: int = 300
    JOIN_JITTER_MS: int = 5000

    # Alerts
    TELEGRAM_ALERT_BOT_TOKEN: str = ""
    TELEGRAM_ALERT_CHAT_ID: str = ""
    TELEGRAM_ALERT_TOPIC_ID: Optional[int] = None

    # Media
    INCLUDE_MEDIA: bool = Field(default=False)
    MEDIA_MAX_MB: float = Field(default=20.0)

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

cfg = Cfg()

# ---------- FastAPI ----------

app = FastAPI(title="tg-pool")

def require_bearer(req: Request):
    if not cfg.API_BEARER:
        return
    auth = req.headers.get("authorization", "")
    if not auth.startswith("Bearer ") or auth[7:] != cfg.API_BEARER:
        raise HTTPException(status_code=401, detail="Invalid token")

# ---------- Paths & State ----------

ROOT = Path("/opt/tg-pool")
SESS_DIR = ROOT / "sessions"
STATE_FILE = ROOT / "state.db"

SESS_DIR.mkdir(parents=True, exist_ok=True)

_state_lock = asyncio.Lock()
_state: Dict[str, Any] = {
    "channels": {}  # name -> {chat_id, session_index, last_id}
}
if STATE_FILE.exists():
    try:
        _state.update(json.loads(STATE_FILE.read_text("utf-8")))
    except Exception:
        pass

async def save_state():
    async with _state_lock:
        STATE_FILE.write_text(json.dumps(_state, ensure_ascii=False, indent=2), "utf-8")

# ---------- Sessions ----------

class SessionWrap:
    def __init__(self, index: int, path: Path):
        self.index = index
        self.path = path
        self.client: Optional[TelegramClient] = None
        self.online: bool = False
        self.next_join_ts: float = 0.0

_sessions_lock = asyncio.Lock()
_sessions: List[SessionWrap] = []  # stable order
_session_by_path: Dict[str, SessionWrap] = {}

def list_session_files() -> List[Path]:
    return sorted(SESS_DIR.glob("*.session"))

async def start_session(sw: SessionWrap):
    if sw.client:
        return
    sw.client = TelegramClient(str(sw.path.with_suffix("")), cfg.API_ID, cfg.API_HASH)
    await sw.client.connect()
    if not await sw.client.is_user_authorized():
        # không login -> bỏ qua
        sw.online = False
        await alert(f"❌ Session mới chưa authorized: {sw.path}")
        return
    sw.online = True
    print(f"[+] Session[{sw.index}] sẵn sàng: {sw.path}")

async def stop_session(sw: SessionWrap):
    try:
        if sw.client:
            await sw.client.disconnect()
    except Exception:
        pass
    sw.client = None
    sw.online = False

async def rescan_sessions():
    """Add/remove sessions based on files."""
    async with _sessions_lock:
        files = list_session_files()
        paths = {str(p): p for p in files}
        # remove
        for p in list(_session_by_path.keys()):
            if p not in paths:
                sw = _session_by_path.pop(p)
                try:
                    await stop_session(sw)
                finally:
                    _sessions.remove(sw)
                    await alert(f"🗑️ Session bị gỡ: {p}")
        # add
        existing = set(_session_by_path.keys())
        for p in files:
            sp = str(p)
            if sp in existing:
                continue
            sw = SessionWrap(len(_sessions), p)
            _sessions.append(sw)
            _session_by_path[sp] = sw
            try:
                await start_session(sw)
            except Exception:
                sw.online = False

# ---------- Alert to Telegram ----------

async def alert(text: str):
    if not (cfg.TELEGRAM_ALERT_BOT_TOKEN and cfg.TELEGRAM_ALERT_CHAT_ID):
        return
    url = f"https://api.telegram.org/bot{cfg.TELEGRAM_ALERT_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": cfg.TELEGRAM_ALERT_CHAT_ID, "text": text, "disable_web_page_preview": True}
    if cfg.TELEGRAM_ALERT_TOPIC_ID:
        payload["message_thread_id"] = int(cfg.TELEGRAM_ALERT_TOPIC_ID)
    try:
        async with aiohttp.ClientSession() as s:
            await s.post(url, json=payload, timeout=15)
    except Exception:
        pass

# ---------- Utilities ----------

def choose_session_for_new_channel() -> Optional[SessionWrap]:
    # pick session online with min current assigned channels count
    counts = {sw.index: 0 for sw in _sessions if sw.online}
    for ch in _state["channels"].values():
        idx = ch.get("session_index")
        if idx in counts:
            counts[idx] += 1
    best_idx = None
    best_count = 10**9
    for sw in _sessions:
        if not sw.online:
            continue
        c = counts.get(sw.index, 0)
        if c < best_count:
            best_count = c
            best_idx = sw.index
    return next((s for s in _sessions if s.index == best_idx), None)

async def ensure_join(sw: SessionWrap, channel: str):
    """Join channel with throttle."""
    now = time.time()
    if now < sw.next_join_ts:
        await asyncio.sleep(sw.next_join_ts - now)
    # jitter
    jitter = random.uniform(0, cfg.JOIN_JITTER_MS / 1000.0)
    await asyncio.sleep(jitter)
    try:
        ent = await sw.client.get_entity(channel)
        # nếu chưa là member, JoinChannelRequest sẽ đảm bảo
        await sw.client(JoinChannelRequest(ent))
    except ChannelPrivateError:
        # kênh private không join được bằng username
        pass
    except ChatAdminRequiredError:
        pass
    except FloodWaitError as e:
        await alert(f"⏳ FloodWait {int(e.seconds)}s khi join {channel} (sess {sw.index})")
        await asyncio.sleep(e.seconds)
    except Exception:
        # ignore
        pass
    # set next join time
    sw.next_join_ts = time.time() + cfg.JOIN_INTERVAL_SEC

def last_id_of(name: str) -> int:
    info = _state["channels"].get(name) or {}
    return int(info.get("last_id") or 0)

def set_last_id(name: str, mid: int):
    if name in _state["channels"]:
        _state["channels"][name]["last_id"] = int(mid)

def _is_allowed_doc(doc) -> bool:
    mt = (getattr(doc, "mime_type", "") or "").lower()
    if not mt:
        return False
    if mt.startswith("image/") or mt.startswith("video/"):
        return True
    return False

# ---------- Callback building (JSON or multipart with media) ----------

MEDIA_MAX_BYTES = int(cfg.MEDIA_MAX_MB * 1024 * 1024)

async def build_payload_and_files(client: TelegramClient, msg: Message, channel_username: Optional[str]):
    payload = {
        "source": "telegram",
        "channel": (channel_username or "").lstrip("@"),
        "message": {
            "chat_id": msg.chat_id,
            "message_id": msg.id,
            "date": msg.date.isoformat() if getattr(msg, "date", None) else None,
            "text": msg.message or "",
            "views": getattr(msg, "views", None),
            "forwards": getattr(msg, "forwards", None),
            "reply_to_msg_id": getattr(getattr(msg, "reply_to", None), "reply_to_msg_id", None),
        },
        "media": []
    }
    if channel_username:
        payload["post_url"] = f"https://t.me/{(channel_username or '').lstrip('@')}/{msg.id}"

    if not cfg.INCLUDE_MEDIA:
        return payload, None

    files: List[Tuple[str, Tuple[str, bytes, str]]] = []
    mi = 0

    if isinstance(msg.media, MessageMediaPhoto):
        buf = io.BytesIO()
        try:
            await client.download_media(msg, file=buf)
            data = buf.getvalue()
            if data and len(data) <= MEDIA_MAX_BYTES:
                fname = f"photo_{msg.id}.jpg"
                payload["media"].append({
                    "type": "photo",
                    "mime": "image/jpeg",
                    "file_name": fname,
                    "size": len(data)
                })
                files.append((f"media{mi}", (fname, data, "image/jpeg"))); mi += 1
        except Exception:
            pass

    elif isinstance(msg.media, MessageMediaDocument) and msg.media.document:
        doc = msg.media.document
        if _is_allowed_doc(doc):
            mime = (doc.mime_type or "application/octet-stream").lower()
            name = None
            for a in (doc.attributes or []):
                if hasattr(a, "file_name") and a.file_name:
                    name = a.file_name
                    break
            if not name:
                name = f"{'video' if mime.startswith('video/') else 'image'}_{msg.id}"
            buf = io.BytesIO()
            try:
                await client.download_media(msg, file=buf)
                data = buf.getvalue()
                if data and len(data) <= MEDIA_MAX_BYTES:
                    payload["media"].append({
                        "type": "video" if mime.startswith("video/") else "image",
                        "mime": mime,
                        "file_name": name,
                        "size": len(data)
                    })
                    files.append((f"media{mi}", (name, data, mime))); mi += 1
            except Exception:
                pass

    return payload, (files if files else None)

async def send_callback(payload: dict, files: Optional[List[Tuple[str, Tuple[str, bytes, str]]]]):
    if not cfg.CALLBACK_URL:
        return
    headers = {}
    if cfg.CALLBACK_BEARER:
        headers["Authorization"] = f"Bearer {cfg.CALLBACK_BEARER}"
    async with aiohttp.ClientSession() as s:
        if files:
            form = aiohttp.FormData()
            form.add_field("json", json.dumps(payload), content_type="application/json")
            for fn, (fname, data, mime) in files:
                form.add_field(fn, data, filename=fname, content_type=mime)
            await s.post(cfg.CALLBACK_URL, data=form, headers=headers, timeout=30)
        else:
            await s.post(cfg.CALLBACK_URL, json=payload, headers=headers, timeout=30)

# ---------- Poll loop ----------

async def poll_loop():
    while True:
        try:
            # build map session_index -> [channels]
            assign: Dict[int, List[str]] = {}
            for name, meta in _state["channels"].items():
                idx = int(meta.get("session_index", -1))
                assign.setdefault(idx, []).append(name)

            tasks = []
            for sw in list(_sessions):
                if not sw.online or not sw.client:
                    continue
                chs = assign.get(sw.index, [])
                if not chs:
                    continue
                tasks.append(_poll_one_session(sw, chs))
            if tasks:
                await asyncio.gather(*tasks)
        except Exception as e:
            # keep looping
            pass
        await asyncio.sleep(cfg.SCAN_INTERVAL_SEC)

async def _poll_one_session(sw: SessionWrap, ch_names: List[str]):
    client = sw.client
    for name in ch_names:
        await asyncio.sleep(random.uniform(cfg.BACKOFF_MIN_MS/1000.0, cfg.BACKOFF_MAX_MS/1000.0))
        try:
            entity = await client.get_entity(name)
            # get new messages strictly after last_id
            last_id = last_id_of(name)
            # Using GetHistoryRequest to control min_id
            res = await client(GetHistoryRequest(
                peer=entity,
                offset_id=0,
                offset_date=None,
                add_offset=0,
                limit=cfg.BATCH_MAX,
                max_id=0,
                min_id=last_id,
                hash=0
            ))
            messages: List[Message] = list(res.messages or [])
            if not messages:
                continue
            # sort ascending by id
            messages.sort(key=lambda m: m.id or 0)
            # push each
            for m in messages:
                if not m.id or m.id <= last_id:
                    continue
                # build payload (+ media)
                payload, files = await build_payload_and_files(client, m, name)
                await send_callback(payload, files)
                set_last_id(name, m.id)
            await save_state()
        except FloodWaitError as e:
            await alert(f"⏳ FloodWait {int(e.seconds)}s khi đọc {name} (sess {sw.index})")
            await asyncio.sleep(e.seconds)
        except ChannelPrivateError:
            # cannot access -> skip
            continue
        except Exception:
            # ignore single channel errors
            continue

# ---------- Session monitor loop ----------

async def monitor_sessions():
    while True:
        try:
            # rescan files, add/remove sessions
            await rescan_sessions()
            # check authorizations
            for sw in list(_sessions):
                if not sw.client:
                    continue
                try:
                    ok = await sw.client.is_user_authorized()
                except Exception:
                    ok = False
                if not ok and sw.online:
                    sw.online = False
                    await alert(f"❌ Session die/not authorized: {sw.path}")
                    # reassign channels that were on this session
                    # they will be migrated lazily on next /channel calls or rescheduling
                    # (no-op here, keep mapping; next choose_session_for_new_channel will skip offline)
        except Exception:
            pass
        await asyncio.sleep(cfg.SESS_RESCAN_SEC)

# ---------- API ----------

@app.get("/status")
async def status(_: Any = Depends(require_bearer)):
    async with _sessions_lock:
        sess = [
            {"index": sw.index, "online": sw.online, "path": f"sessions/{Path(sw.path).name}"}
            for sw in _sessions
        ]
    chs = []
    for name, meta in _state["channels"].items():
        chs.append({
            "name": name,
            "chat_id": meta.get("chat_id"),
            "session_index": meta.get("session_index"),
            "last_id": meta.get("last_id"),
            "session_path": str(f"sessions/{Path(_sessions[meta['session_index']].path).name}") if isinstance(meta.get("session_index"), int) and 0 <= meta["session_index"] < len(_sessions) else None
        })
    return {"ok": True, "sessions": sess, "channels": chs}

@app.get("/")
async def root():
    return JSONResponse({"ok": False, "hint": "see /status"})

@app.get("/channel")
async def add_channel(chanel: str = Query(..., alias="chanel"), _: Any = Depends(require_bearer)):
    name = chanel.lstrip("@")
    # pick session
    sw = choose_session_for_new_channel()
    if not sw:
        raise HTTPException(503, "No online session available")
    try:
        await ensure_join(sw, name)
        # fetch last message id to set last_id baseline
        ent = await sw.client.get_entity(name)
        hist = await sw.client(GetHistoryRequest(
            peer=ent, offset_id=0, offset_date=None, add_offset=0,
            limit=1, max_id=0, min_id=0, hash=0
        ))
        last = 0
        if hist.messages:
            last = hist.messages[0].id or 0
        _state["channels"][name] = {
            "chat_id": getattr(ent, "id", None),
            "session_index": sw.index,
            "last_id": last
        }
        await save_state()
        return {"ok": True, "channel": name, "session_index": sw.index, "last_id": last}
    except FloodWaitError as e:
        await alert(f"⏳ FloodWait {int(e.seconds)}s khi join {name} (sess {sw.index})")
        raise HTTPException(429, f"FloodWait {int(e.seconds)}s")
    except Exception as e:
        raise HTTPException(500, f"join/add failed: {e}")

@app.get("/delete")
async def delete_channel(chanel: str = Query(..., alias="chanel"), _: Any = Depends(require_bearer)):
    name = chanel.lstrip("@")
    meta = _state["channels"].get(name)
    if not meta:
        return {"ok": True, "removed": False}
    idx = int(meta.get("session_index", -1))
    sw = next((s for s in _sessions if s.index == idx and s.client), None)
    try:
        if sw and sw.client:
            ent = await sw.client.get_entity(name)
            await sw.client(LeaveChannelRequest(ent))
    except Exception:
        pass
    _state["channels"].pop(name, None)
    await save_state()
    return {"ok": True, "removed": True}

# ---------- Lifespan ----------

@app.on_event("startup")
async def on_startup():
    await rescan_sessions()
    # background tasks
    asyncio.create_task(monitor_sessions())
    asyncio.create_task(poll_loop())

@app.on_event("shutdown")
async def on_shutdown():
    async with _sessions_lock:
        for sw in _sessions:
            try:
                await stop_session(sw)
            except Exception:
                pass

# ---------- Main ----------

if __name__ == "__main__":
    # Bind public (0.0.0.0). Nếu muốn chỉ nội bộ: host="127.0.0.1"
    uvicorn.run("app:app", host="0.0.0.0", port=8080, reload=False, log_level="info")
