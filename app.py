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
from telethon import TelegramClient
from telethon.errors import ChannelPrivateError, FloodWaitError, ChatAdminRequiredError
from telethon.tl.types import Message, MessageMediaPhoto, MessageMediaDocument
from telethon.tl.functions.channels import JoinChannelRequest, LeaveChannelRequest
from telethon.tl.functions.messages import GetHistoryRequest, GetMessagesRequest

# --- LLM (OpenAI-only with multi-key router) ---
from llm_openai import LLMOpenAI

# ---------- Config (.env) ----------
class Cfg(BaseSettings):
    # Telegram API
    API_ID: int
    API_HASH: str

    # Callback to Worker
    CALLBACK_URL: str = ""
    CALLBACK_BEARER: str = ""

    # API security
    API_BEARER: str = ""

    # Scan/poll (producer)
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

    # Defaults for preprocessing (can override per channel)
    FILTER_LINKS_DEFAULT: bool = Field(default=True)
    FILTER_CAPTION_DEFAULT: bool = Field(default=True)

    # Queue/worker
    QUEUE_MAXSIZE: int = 1000
    WORKERS: int = 5
    SEND_RETRY: int = 3
    SEND_RETRY_DELAY_MS: int = 400
    SEND_BACKOFF: float = 2.0

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
    "channels": {}  # name -> {chat_id, session_index, last_id, user_prompt, strip_links, strip_caption}
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
_sessions: List[SessionWrap] = []
_session_by_path: Dict[str, SessionWrap] = {}

def list_session_files() -> List[Path]:
    return sorted(SESS_DIR.glob("*.session"))

async def start_session(sw: SessionWrap):
    if sw.client:
        return
    sw.client = TelegramClient(str(sw.path.with_suffix("")), cfg.API_ID, cfg.API_HASH)
    await sw.client.connect()
    if not await sw.client.is_user_authorized():
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

# ---------- Alerts ----------
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

# ---------- Utils ----------
def choose_session_for_new_channel() -> Optional[SessionWrap]:
    counts = {sw.index: 0 for sw in _sessions if sw.online}
    for ch in _state["channels"].values():
        idx = ch.get("session_index")
        if idx in counts:
            counts[idx] += 1
    best_idx, best_count = None, 10**9
    for sw in _sessions:
        if not sw.online:
            continue
        c = counts.get(sw.index, 0)
        if c < best_count:
            best_count = c
            best_idx = sw.index
    return next((s for s in _sessions if s.index == best_idx), None)

async def ensure_join(sw: SessionWrap, channel: str):
    now = time.time()
    if now < sw.next_join_ts:
        await asyncio.sleep(sw.next_join_ts - now)
    await asyncio.sleep(random.uniform(0, cfg.JOIN_JITTER_MS / 1000.0))
    try:
        ent = await sw.client.get_entity(channel)
        await sw.client(JoinChannelRequest(ent))
    except ChannelPrivateError:
        pass
    except ChatAdminRequiredError:
        pass
    except FloodWaitError as e:
        await alert(f"⏳ FloodWait {int(e.seconds)}s khi join {channel} (sess {sw.index})")
        await asyncio.sleep(e.seconds)
    except Exception:
        pass
    sw.next_join_ts = time.time() + cfg.JOIN_INTERVAL_SEC

def last_id_of(name: str) -> int:
    info = _state["channels"].get(name) or {}
    return int(info.get("last_id") or 0)

def set_last_id(name: str, mid: int):
    if name in _state["channels"]:
        _state["channels"][name]["last_id"] = int(mid)

def _is_allowed_doc(doc) -> bool:
    mt = (getattr(doc, "mime_type", "") or "").lower()
    return bool(mt and (mt.startswith("image/") or mt.startswith("video/")))

# ---------- Preprocess helpers ----------
import re

def strip_links_keep_lines(s: str) -> str:
    s = (s or "").replace("\r\n", "\n")
    s = re.sub(r"https?://[^\s)>\]]+", "", s, flags=re.I)
    s = "\n".join(line.rstrip() for line in s.split("\n"))
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()

def remove_tail_caption(s: str) -> str:
    lines = (s or "").replace("\r\n","\n").split("\n")
    promo = re.compile(r"^(source|nguồn|cre|credit|credits|follow|subscribe|join|contact|liên hệ|kết nối)\b", re.I)
    linkish = re.compile(r"(t\.me/|telegram\.me/|@[\w_]{3,})", re.I)
    cut = len(lines); removed = 0
    for i in range(len(lines)-1, -1, -1):
        ln = (lines[i] or "").strip()
        if not ln:
            cut = i; removed += 1; continue
        if promo.search(ln) or linkish.search(ln):
            cut = i; removed += 1; continue
        break
    return "\n".join(lines[:cut]).strip()

def remove_boilerplate(s: str) -> str:
    bad = [r"quick\s*recap", r"^tl;?dr\b", r"^summary\b", r"^recap\b", r"^highlights?\b", r"^mmo101\b"]
    lines = (s or "").replace("\r\n","\n").split("\n")
    kept = []
    for ln in lines:
        t = ln.strip()
        if t and any(re.search(rx, t, flags=re.I) for rx in bad):
            continue
        kept.append(ln)
    out = "\n".join(kept)
    out = out.replace("\n\n\n","\n\n").strip()
    return out

# ---------- Callback building ----------
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
                payload["media"].append({"type":"photo","mime":"image/jpeg","file_name":fname,"size":len(data)})
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
                    payload["media"].append({"type":"video" if mime.startswith("video/") else "image","mime":mime,"file_name":name,"size":len(data)})
                    files.append((f"media{mi}", (name, data, mime))); mi += 1
            except Exception:
                pass

    return payload, (files if files else None)

async def send_callback(payload: dict, files: Optional[List[Tuple[str, Tuple[str, bytes, str]]]]) -> bool:
    """Gửi về Worker với retry/backoff nhẹ để chống nghẽn."""
    if not cfg.CALLBACK_URL:
        return False

    headers = {}
    if cfg.CALLBACK_BEARER:
        headers["Authorization"] = f"Bearer {cfg.CALLBACK_BEARER}"

    delay = cfg.SEND_RETRY_DELAY_MS / 1000.0
    for attempt in range(cfg.SEND_RETRY):
        try:
            async with aiohttp.ClientSession() as s:
                if files:
                    form = aiohttp.FormData()
                    form.add_field("json", json.dumps(payload), content_type="application/json")
                    for fn, (fname, data, mime) in files:
                        form.add_field(fn, data, filename=fname, content_type=mime)
                    r = await s.post(cfg.CALLBACK_URL, data=form, headers=headers, timeout=60)
                else:
                    r = await s.post(cfg.CALLBACK_URL, json=payload, headers=headers, timeout=60)
                if 200 <= r.status < 300:
                    return True
        except Exception:
            pass
        if attempt < cfg.SEND_RETRY - 1:
            await asyncio.sleep(delay)
            delay *= cfg.SEND_BACKOFF
    return False

# ---------- LLM instance & Queue workers ----------
_llm = LLMOpenAI()
_queue: asyncio.Queue = asyncio.Queue(maxsize=cfg.QUEUE_MAXSIZE)

async def worker_loop(worker_id: int):
    """Consumer: lấy job từ queue → tải message → preprocess → GPT → callback → cập nhật last_id."""
    while True:
        name, sess_idx, msg_id = await _queue.get()
        try:
            sw = next((s for s in _sessions if s.index == sess_idx and s.client and s.online), None)
            if not sw:
                continue
            client = sw.client

            # Re-fetch message by ID để tránh lệ thuộc object cũ
            ent = await client.get_entity(name)
            full = await client(GetMessagesRequest(id=[msg_id]))
            msg = full.messages[0] if (getattr(full, "messages", None)) else None
            if not msg:
                continue

            payload, files = await build_payload_and_files(client, msg, name)

            # Per-channel config
            ch_cfg = _state["channels"].get(name) or {}
            user_prompt = (ch_cfg.get("user_prompt") or "").strip()
            use_strip_links = bool(ch_cfg.get("strip_links") if ch_cfg.get("strip_links") is not None else cfg.FILTER_LINKS_DEFAULT)
            use_strip_caption = bool(ch_cfg.get("strip_caption") if ch_cfg.get("strip_caption") is not None else cfg.FILTER_CAPTION_DEFAULT)

            raw_text = payload.get("message", {}).get("text") or ""

            pre = raw_text
            if use_strip_caption:
                pre = remove_tail_caption(pre)
            if use_strip_links:
                pre = strip_links_keep_lines(pre)
            pre = remove_boilerplate(pre)

            sys_extra = os.getenv("LLM_SYS_EXTRA", "")

            rewritten, gpt_info = await _llm.rewrite(pre, user_prompt=user_prompt, sys_extra=sys_extra)

            payload["message"]["text"] = rewritten or pre
            payload.setdefault("meta", {})["gpt_info"] = gpt_info
            payload["meta"]["pre_flags"] = {"strip_links": use_strip_links, "strip_caption": use_strip_caption}

            ok = await send_callback(payload, files)
            if ok:
                set_last_id(name, msg_id)
                await save_state()
        except Exception:
            # không chặn worker; nuốt lỗi job đơn lẻ
            pass
        finally:
            _queue.task_done()

# ---------- Producer: poll loop ----------
async def poll_loop():
    while True:
        try:
            assign: Dict[int, List[str]] = {}
            for name, meta in _state["channels"].items():
                idx = int(meta.get("session_index", -1))
                assign.setdefault(idx, []).append(name)

            for sw in list(_sessions):
                if not sw.online or not sw.client:
                    continue
                chs = assign.get(sw.index, [])
                for name in chs:
                    await asyncio.sleep(random.uniform(cfg.BACKOFF_MIN_MS/1000.0, cfg.BACKOFF_MAX_MS/1000.0))
                    try:
                        entity = await sw.client.get_entity(name)
                        last_id = last_id_of(name)
                        res = await sw.client(GetHistoryRequest(
                            peer=entity, offset_id=0, offset_date=None, add_offset=0,
                            limit=cfg.BATCH_MAX, max_id=0, min_id=last_id, hash=0
                        ))
                        messages: List[Message] = list(res.messages or [])
                        if not messages:
                            continue
                        messages.sort(key=lambda m: m.id or 0)

                        for m in messages:
                            if not m.id or m.id <= last_id:
                                continue
                            # Đưa vào hàng đợi; backpressure nếu đầy
                            await _queue.put((name, sw.index, m.id))
                    except FloodWaitError as e:
                        await alert(f"⏳ FloodWait {int(e.seconds)}s khi đọc {name} (sess {sw.index})")
                        await asyncio.sleep(e.seconds)
                    except ChannelPrivateError:
                        continue
                    except Exception:
                        continue
        except Exception:
            pass
        await asyncio.sleep(cfg.SCAN_INTERVAL_SEC)

# ---------- Session monitor ----------
async def monitor_sessions():
    while True:
        try:
            await rescan_sessions()
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
        except Exception:
            pass
        await asyncio.sleep(cfg.SESS_RESCAN_SEC)

# ---------- API ----------
@app.get("/status")
async def status(_: Any = Depends(require_bearer)):
    async with _sessions_lock:
        sess = [{"index": sw.index, "online": sw.online, "path": f"sessions/{Path(sw.path).name}"} for sw in _sessions]
    chs = []
    for name, meta in _state["channels"].items():
        chs.append({
            "name": name,
            "chat_id": meta.get("chat_id"),
            "session_index": meta.get("session_index"),
            "last_id": meta.get("last_id"),
            "user_prompt": meta.get("user_prompt", ""),
            "strip_links": meta.get("strip_links", None),
            "strip_caption": meta.get("strip_caption", None),
            "session_path": str(f"sessions/{Path(_sessions[meta['session_index']].path).name}") if isinstance(meta.get("session_index"), int) and 0 <= meta["session_index"] < len(_sessions) else None
        })
    qstats = {"size": _queue.qsize(), "maxsize": cfg.QUEUE_MAXSIZE}
    return {"ok": True, "sessions": sess, "channels": chs, "queue": qstats}

@app.get("/")
async def root():
    return JSONResponse({"ok": False, "hint": "see /status"})

@app.get("/channel")
async def add_channel(
    chanel: str = Query(..., alias="chanel"),
    prompt: Optional[str] = Query(None),
    strip_links: Optional[int] = Query(None, description="0|1"),
    strip_caption: Optional[int] = Query(None, description="0|1"),
    _: Any = Depends(require_bearer)
):
    name = chanel.lstrip("@")

    # nếu tồn tại → update cấu hình
    if name in _state["channels"]:
        if prompt is not None:
            _state["channels"][name]["user_prompt"] = str(prompt)
        if strip_links is not None:
            _state["channels"][name]["strip_links"] = 1 if int(strip_links) else 0
        if strip_caption is not None:
            _state["channels"][name]["strip_caption"] = 1 if int(strip_caption) else 0
        await save_state()
        return {"ok": True, "channel": name, "exists": True,
                "session_index": _state["channels"][name].get("session_index"),
                "last_id": _state["channels"][name].get("last_id")}

    # pick session
    sw = choose_session_for_new_channel()
    if not sw:
        raise HTTPException(503, "No online session available")
    try:
        await ensure_join(sw, name)
        ent = await sw.client.get_entity(name)
        hist = await sw.client(GetHistoryRequest(peer=ent, offset_id=0, offset_date=None, add_offset=0,
                                                 limit=1, max_id=0, min_id=0, hash=0))
        last = hist.messages[0].id if (hist.messages and hist.messages[0].id) else 0
        _state["channels"][name] = {
            "chat_id": getattr(ent, "id", None),
            "session_index": sw.index,
            "last_id": int(last),
            "user_prompt": str(prompt) if prompt is not None else "",
            "strip_links": (1 if int(strip_links) else 0) if strip_links is not None else None,
            "strip_caption": (1 if int(strip_caption) else 0) if strip_caption is not None else None
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
    # Khởi tạo N worker consumer
    for i in range(max(1, cfg.WORKERS)):
        asyncio.create_task(worker_loop(i))
    # Background producers/monitors
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
    uvicorn.run("app:app", host="0.0.0.0", port=8080, reload=False, log_level="info")
