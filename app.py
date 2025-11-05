# app.py — VPS poll-only -> callback worker /post-new
import asyncio, json, os, io, random, re, time
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple

import aiohttp
from fastapi import FastAPI, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic_settings import BaseSettings
from pydantic import Field
from telethon import TelegramClient
from telethon.errors import ChannelPrivateError, FloodWaitError, ChatAdminRequiredError
from telethon.tl.types import Message, MessageMediaPhoto, MessageMediaDocument
from telethon.tl.functions.channels import JoinChannelRequest, LeaveChannelRequest
from telethon.tl.functions.messages import GetHistoryRequest
import uvicorn

# ============ Config (.env) ============
class Cfg(BaseSettings):
    # Telegram API
    API_ID: int
    API_HASH: str

    # Worker callback (điểm Worker /post-new)
    CALLBACK_URL: str = ""          # ví dụ: https://telegram.faker.today/post-new
    CALLBACK_BEARER: str = ""       # Bearer kèm khi gửi về Worker

    # Bảo vệ API VPS
    API_BEARER: str = ""            # Bearer để gọi /channel, /delete, /status

    # Polling & throttle
    SCAN_INTERVAL_SEC: int = 60
    BATCH_MAX: int = 50
    BACKOFF_MIN_MS: int = 150
    BACKOFF_MAX_MS: int = 600

    # Session/Join
    SESS_RESCAN_SEC: int = 30
    JOIN_INTERVAL_SEC: int = 300
    JOIN_JITTER_MS: int = 5000

    # Media
    INCLUDE_MEDIA: bool = Field(default=True)
    MEDIA_MAX_MB: float = Field(default=20.0)

    # Bind host/port
    BIND_HOST: str = Field(default="0.0.0.0")
    BIND_PORT: int = Field(default=8080)

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

cfg = Cfg()

# ============ FastAPI ============
app = FastAPI(title="tg-pool (poll-only)")

def require_bearer(req: Request):
    if not cfg.API_BEARER:
        return
    auth = req.headers.get("authorization", "")
    if not auth.startswith("Bearer ") or auth[7:] != cfg.API_BEARER:
        raise HTTPException(status_code=401, detail="Invalid token")

# ============ State & sessions ============
ROOT = Path("/opt/tg-pool")
SESS_DIR = ROOT / "sessions"
STATE_FILE = ROOT / "state.db"
SESS_DIR.mkdir(parents=True, exist_ok=True)

_state_lock = asyncio.Lock()
_state: Dict[str, Any] = {"channels": {}}  # name -> {chat_id, session_index, last_id}
if STATE_FILE.exists():
    try:
        _state.update(json.loads(STATE_FILE.read_text("utf-8")))
    except Exception:
        pass

async def save_state():
    async with _state_lock:
        STATE_FILE.write_text(json.dumps(_state, ensure_ascii=False, indent=2), "utf-8")

class SessionWrap:
    def __init__(self, index: int, path: Path):
        self.index = index
        self.path = path
        self.client: Optional[TelegramClient] = None
        self.online: bool = False
        self.next_join_ts: float = 0.0

_sessions: List[SessionWrap] = []
_session_by_path: Dict[str, SessionWrap] = {}
_sessions_lock = asyncio.Lock()

def list_session_files() -> List[Path]:
    return sorted(SESS_DIR.glob("*.session"))

async def start_session(sw: SessionWrap):
    if sw.client:
        return
    sw.client = TelegramClient(str(sw.path.with_suffix("")), cfg.API_ID, cfg.API_HASH)
    await sw.client.connect()
    if not await sw.client.is_user_authorized():
        sw.online = False
        print(f"[!] Session NOT authorized: {sw.path.name}")
        return
    sw.online = True
    print(f"[+] Session[{sw.index}] ready: {sw.path.name}")

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
                    print(f"[-] Session removed: {Path(p).name}")
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

def choose_session_for_new_channel() -> Optional[SessionWrap]:
    counts = {sw.index: 0 for sw in _sessions if sw.online}
    for ch in _state["channels"].values():
        idx = ch.get("session_index")
        if isinstance(idx, int) and idx in counts:
            counts[idx] += 1
    best_idx, best_count = None, 10**9
    for sw in _sessions:
        if not sw.online:
            continue
        c = counts.get(sw.index, 0)
        if c < best_count:
            best_count, best_idx = c, sw.index
    return next((s for s in _sessions if s.index == best_idx), None)

async def ensure_join(sw: SessionWrap, channel: str):
    now = time.time()
    if now < sw.next_join_ts:
        await asyncio.sleep(sw.next_join_ts - now)
    jitter = random.uniform(0, cfg.JOIN_JITTER_MS / 1000.0)
    await asyncio.sleep(jitter)
    try:
        ent = await sw.client.get_entity(channel)
        await sw.client(JoinChannelRequest(ent))
    except (ChannelPrivateError, ChatAdminRequiredError):
        pass
    except FloodWaitError as e:
        print(f"[FW] join {channel}: wait {e.seconds}s")
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

# ============ Build payload/files ============
MEDIA_MAX_BYTES = int(cfg.MEDIA_MAX_MB * 1024 * 1024)

def _strip_urls_keep_lines(s: str) -> str:
    s = (s or "")
    s = re.sub(r"\r\n?", "\n", s)
    s = re.sub(r"https?://[^\s)>\]]+", "", s, flags=re.I)
    s = re.sub(r"[ \t]+\n", "\n", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()

async def build_payload_and_files(client: TelegramClient, msg: Message, channel_username: Optional[str]):
    payload = {
        "source": "telegram",
        "channel": (channel_username or "").lstrip("@"),
        "post_url": f"https://t.me/{(channel_username or '').lstrip('@')}/{msg.id}" if channel_username else "",
        "message": {
            "chat_id": msg.chat_id,
            "message_id": msg.id,
            "grouped_id": getattr(msg, "grouped_id", None),
            "date": msg.date.isoformat() if getattr(msg, "date", None) else None,
            "text": msg.message or "",
            "views": getattr(msg, "views", None),
            "forwards": getattr(msg, "forwards", None),
            "reply_to_msg_id": getattr(getattr(msg, "reply_to", None), "reply_to_msg_id", None),
        },
        "media": []
    }

    files: List[Tuple[str, Tuple[str, bytes, str]]] = []

    if not cfg.INCLUDE_MEDIA:
        return payload, None

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
                files.append((f"media0", (fname, data, "image/jpeg")))
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
                    files.append((f"media0", (name, data, mime)))
            except Exception:
                pass

    # Dọn text để đỡ gửi rác về Worker (giữ bố cục)
    payload["message"]["text"] = _strip_urls_keep_lines(payload["message"]["text"])
    return payload, (files if files else None)

# ============ Gửi về Worker ============
async def send_callback(payload: dict, files: Optional[List[Tuple[str, Tuple[str, bytes, str]]]]):
    if not cfg.CALLBACK_URL:
        print("[!] CALLBACK_URL empty, skip")
        return
    headers = {}
    if cfg.CALLBACK_BEARER:
        headers["Authorization"] = f"Bearer {cfg.CALLBACK_BEARER}"
    async with aiohttp.ClientSession() as s:
        if files:
            form = aiohttp.FormData()
            form.add_field("json", json.dumps(payload, ensure_ascii=False), content_type="application/json")
            for fn, (fname, data, mime) in files:
                form.add_field(fn, data, filename=fname, content_type=mime)
            await s.post(cfg.CALLBACK_URL, data=form, headers=headers, timeout=60)
        else:
            await s.post(cfg.CALLBACK_URL, json=payload, headers=headers, timeout=60)

# ============ Poll loop (gom album theo grouped_id) ============
async def poll_loop():
    while True:
        try:
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
        except Exception:
            pass
        await asyncio.sleep(cfg.SCAN_INTERVAL_SEC)

async def _poll_one_session(sw: SessionWrap, ch_names: List[str]):
    client = sw.client
    for name in ch_names:
        await asyncio.sleep(random.uniform(cfg.BACKOFF_MIN_MS/1000.0, cfg.BACKOFF_MAX_MS/1000.0))
        try:
            ent = await client.get_entity(name)
            last_id = last_id_of(name)
            res = await client(GetHistoryRequest(
                peer=ent, offset_id=0, offset_date=None, add_offset=0,
                limit=cfg.BATCH_MAX, max_id=0, min_id=last_id, hash=0
            ))
            messages: List[Message] = list(res.messages or [])
            if not messages:
                continue
            messages.sort(key=lambda m: m.id or 0)

            groups: Dict[int, List[Message]] = {}
            singles: List[Message] = []
            for m in messages:
                if not m.id or m.id <= last_id:
                    continue
                gid = getattr(m, "grouped_id", None)
                if gid:
                    groups.setdefault(gid, []).append(m)
                else:
                    singles.append(m)

            # Album groups -> 1 callback duy nhất
            for gid, gid_msgs in groups.items():
                gid_msgs.sort(key=lambda x: x.id or 0)
                # pick message có text dài nhất làm primary caption
                primary = max(gid_msgs, key=lambda x: len(x.message or ""), default=gid_msgs[0])

                payload, files = await build_payload_and_files(client, primary, name)
                url_meta = []
                bin_files = [] if not files else list(files)

                for gm in gid_msgs:
                    p, f = await build_payload_and_files(client, gm, name)
                    for mi in (p.get("media") or []):
                        url_meta.append(mi)
                    if f:
                        bin_files.extend(f)

                # unique theo (file_name|mime)
                seen = set()
                uniq = []
                for mi in url_meta:
                    key = f"{mi.get('file_name') or ''}|{mi.get('mime') or ''}"
                    if key in seen: continue
                    seen.add(key)
                    uniq.append(mi)
                payload["media"] = uniq

                await send_callback(payload, bin_files if bin_files else None)
                set_last_id(name, gid_msgs[-1].id)

            # Single posts
            for m in singles:
                payload, files = await build_payload_and_files(client, m, name)
                await send_callback(payload, files)
                set_last_id(name, m.id)

            await save_state()

        except FloodWaitError as e:
            print(f"[FW] read {name}: wait {e.seconds}s")
            await asyncio.sleep(e.seconds)
        except ChannelPrivateError:
            continue
        except Exception:
            continue

# ============ API ============
@app.get("/status")
async def status(_: Any = Depends(require_bearer)):
    async with _sessions_lock:
        sess = [{"index": sw.index, "online": sw.online, "path": f"sessions/{sw.path.name}"} for sw in _sessions]
    chs = []
    for name, meta in _state["channels"].items():
        chs.append({
            "name": name,
            "chat_id": meta.get("chat_id"),
            "session_index": meta.get("session_index"),
            "last_id": meta.get("last_id"),
        })
    return {"ok": True, "sessions": sess, "channels": chs}

@app.get("/")
async def root():
    return JSONResponse({"ok": False, "hint": "see /status"})

@app.get("/channel")
async def add_channel(
    chanel: str = Query(..., alias="chanel"),
    source: str = Query("", alias="source"),   # optional, chỉ để log/hiển thị
    _: Any = Depends(require_bearer)
):
    name = chanel.lstrip("@")
    sw = choose_session_for_new_channel()
    if not sw:
        raise HTTPException(503, "No online session available")
    try:
        await ensure_join(sw, name)
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

# ============ Startup / Shutdown ============
@app.on_event("startup")
async def on_startup():
    await rescan_sessions()
    asyncio.create_task(poll_loop())
    asyncio.create_task(_monitor_sessions())

async def _monitor_sessions():
    while True:
        try:
            await rescan_sessions()
            # nếu session bị deauth, chỉ log & để add_channel lần sau tự cân lại
            for sw in list(_sessions):
                if not sw.client:
                    continue
                try:
                    ok = await sw.client.is_user_authorized()
                except Exception:
                    ok = False
                if not ok and sw.online:
                    sw.online = False
                    print(f"[!] Session offline/deauth: {sw.path.name}")
        except Exception:
            pass
        await asyncio.sleep(cfg.SESS_RESCAN_SEC)

@app.on_event("shutdown")
async def on_shutdown():
    async with _sessions_lock:
        for sw in _sessions:
            try:
                await stop_session(sw)
            except Exception:
                pass

if __name__ == "__main__":
    uvicorn.run("app:app", host=cfg.BIND_HOST, port=cfg.BIND_PORT, reload=False, log_level="info")
