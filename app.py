import os, glob, asyncio, random, traceback
from typing import Dict, Optional, List, Tuple
from datetime import datetime
from contextlib import asynccontextmanager

import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

import aiosqlite
from fastapi import FastAPI, Query, HTTPException, Request, Header, Depends
from fastapi.responses import JSONResponse
from pydantic_settings import BaseSettings, SettingsConfigDict
from telethon import TelegramClient, events, functions
from telethon.errors import FloodWaitError, RPCError
from telethon.tl.types import Message
import aiohttp

DB_PATH = "state.db"

# ------------------- Config (.env) -------------------
class Cfg(BaseSettings):
    API_ID: int
    API_HASH: str

    CALLBACK_URL: str
    CALLBACK_BEARER: Optional[str] = None

    # security for HTTP API (optional)
    API_BEARER: Optional[str] = None

    # scan + throttling
    SCAN_INTERVAL_SEC: int = 60
    BATCH_MAX: int = 50
    BACKOFF_MIN_MS: int = 150
    BACKOFF_MAX_MS: int = 600

    # session rescan + throttled joining
    SESS_RESCAN_SEC: int = 30
    JOIN_INTERVAL_SEC: int = 300     # 5 phút
    JOIN_JITTER_MS: int = 5000       # thêm 0-5s ngẫu nhiên

    # alert to telegram bot (optional)
    TELEGRAM_ALERT_BOT_TOKEN: Optional[str] = None
    TELEGRAM_ALERT_CHAT_ID: Optional[str] = None

    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=True,
    )

cfg = Cfg()

# ------------------- Auth for HTTP API -------------------
async def require_bearer(authorization: str = Header(default="")):
    token = cfg.API_BEARER
    if not token:
        return
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Bearer")
    if authorization.split(" ", 1)[1].strip() != token:
        raise HTTPException(status_code=403, detail="Invalid token")

# ------------------- Globals -------------------
clients: List[Optional[TelegramClient]] = []
session_files: List[str] = []
db: aiosqlite.Connection = None

# cache: chat_id -> (name, sess_idx) ;   name -> (chat_id, sess_idx)
managed_chats: Dict[int, Tuple[str, int]] = {}
managed_names: Dict[str, Tuple[int, int]] = {}

# join queue per session (join chậm 1 kênh/5 phút)
join_queues: Dict[int, asyncio.Queue[str]] = {}
join_workers_started: Dict[int, bool] = {}

# alert dedupe
session_alerted: Dict[str, bool] = {}  # sess_path -> alerted?

# ------------------- DB helpers -------------------
async def db_init():
    global db
    db = await aiosqlite.connect(DB_PATH)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS channels (
            name TEXT PRIMARY KEY,
            chat_id INTEGER NOT NULL,
            session_index INTEGER NOT NULL,
            last_id INTEGER NOT NULL DEFAULT 0,
            session_path TEXT NOT NULL DEFAULT ''
        )
    """)
    await db.commit()

async def db_get_channel(name: str):
    async with db.execute("SELECT name, chat_id, session_index, last_id, session_path FROM channels WHERE name=?", (name,)) as cur:
        return await cur.fetchone()

async def db_get_all_channels():
    rows = []
    async with db.execute("SELECT name, chat_id, session_index, last_id, session_path FROM channels") as cur:
        async for r in cur:
            rows.append(r)
    return rows

async def db_upsert_channel(name: str, chat_id: int, sess_idx: int, last_id: int, sess_path: str):
    await db.execute("""
      INSERT INTO channels(name, chat_id, session_index, last_id, session_path)
      VALUES (?, ?, ?, ?, ?)
      ON CONFLICT(name) DO UPDATE SET
        chat_id=excluded.chat_id,
        session_index=excluded.session_index,
        last_id=excluded.last_id,
        session_path=excluded.session_path
    """, (name, chat_id, sess_idx, last_id, sess_path))
    await db.commit()

async def db_update_last_id(name: str, last_id: int):
    await db.execute("UPDATE channels SET last_id=? WHERE name=?", (last_id, name))
    await db.commit()

async def db_update_mapping(name: str, sess_idx: int, sess_path: str):
    await db.execute("UPDATE channels SET session_index=?, session_path=? WHERE name=?", (sess_idx, sess_path, name))
    await db.commit()

async def db_delete_channel(name: str):
    await db.execute("DELETE FROM channels WHERE name=?", (name,))
    await db.commit()

# ------------------- Utils -------------------
def normalize_name(s: str) -> str:
    s = (s or "").strip()
    if s.startswith("@"): s = s[1:]
    return s

def jitter_s(min_ms: int, max_ms: int) -> float:
    return random.uniform(min_ms/1000, max_ms/1000)

def pick_least_loaded_session(assignments: List[Tuple[str,int,int,int,str]]) -> int:
    counts = [0]*len(clients)
    for _,_,sess_idx,_,_ in assignments:
        if 0 <= sess_idx < len(counts):
            counts[sess_idx] += 1
    best_idx, best_cnt = None, 1<<30
    for i,cnt in enumerate(counts):
        if clients[i] is None:
            continue
        if cnt < best_cnt:
            best_cnt, best_idx = cnt, i
    if best_idx is None:
        raise RuntimeError("Không có session nào sẵn sàng.")
    return best_idx

async def http_post_json(url: str, payload: dict, headers: dict = None, timeout: int = 20):
    h = {"Content-Type": "application/json"}
    if headers: h.update(headers)
    async with aiohttp.ClientSession() as sess:
        async with sess.post(url, headers=h, json=payload, timeout=timeout) as resp:
            return resp.status, await resp.text()

async def alert(text: str):
    if not (cfg.TELEGRAM_ALERT_BOT_TOKEN and cfg.TELEGRAM_ALERT_CHAT_ID):
        return
    url = f"https://api.telegram.org/bot{cfg.TELEGRAM_ALERT_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": cfg.TELEGRAM_ALERT_CHAT_ID,
        "text": text,
        "disable_web_page_preview": True
    }
    # gửi vào Topic nếu có
    if os.getenv("TELEGRAM_ALERT_TOPIC_ID") or getattr(cfg, "TELEGRAM_ALERT_TOPIC_ID", None):
        try:
            topic_id = int(os.getenv("TELEGRAM_ALERT_TOPIC_ID") or cfg.TELEGRAM_ALERT_TOPIC_ID)
            payload["message_thread_id"] = topic_id
        except Exception:
            pass

    try:
        async with aiohttp.ClientSession() as s:
            await s.post(url, json=payload, timeout=15)
    except Exception:
        pass

# ------------------- Join queue (throttled) -------------------
def ensure_join_worker(sess_idx: int):
    if sess_idx not in join_queues:
        join_queues[sess_idx] = asyncio.Queue()
    if not join_workers_started.get(sess_idx):
        asyncio.create_task(join_worker(sess_idx))
        join_workers_started[sess_idx] = True

async def join_worker(sess_idx: int):
    q = join_queues[sess_idx]
    while True:
        try:
            nm = await q.get()
            cli = clients[sess_idx] if 0 <= sess_idx < len(clients) else None
            if cli is None:
                # session hiện không online -> đẩy lại và chờ
                await asyncio.sleep(5)
                await q.put(nm)
                continue
            try:
                entity = None
                try:
                    entity = await cli.get_entity(nm)
                except Exception:
                    entity = None
                if entity is None:
                    await cli(functions.channels.JoinChannelRequest(nm))
                    entity = await cli.get_entity(nm)

                row = await db_get_channel(nm)
                if row:
                    name, chat_id, _, last_id, _sess_path = row
                    await db_update_mapping(name, sess_idx, session_files[sess_idx])
                    managed_chats[chat_id] = (name, sess_idx)
                    managed_names[name] = (chat_id, sess_idx)
                    print(f"[JoinQueue] {nm} -> session[{sess_idx}] OK")
                    await alert(f"✅ Đã join lại kênh {nm} bằng session[{sess_idx}]")
            except Exception as e:
                print(f"[JoinQueue WARN] {nm} @sess[{sess_idx}]: {e}")
                await alert(f"⚠️ Join {nm} @sess[{sess_idx}] lỗi: {e}")

            sleep_s = cfg.JOIN_INTERVAL_SEC + random.uniform(0, cfg.JOIN_JITTER_MS/1000)
            await asyncio.sleep(sleep_s)
        except Exception as e:
            print("[join_worker] ERR:", e)
            await asyncio.sleep(3)

# ------------------- Telethon handlers -------------------
async def on_new_message(event: events.NewMessage.Event, sess_idx: int):
    try:
        m: Message = event.message
        chat_id = m.chat_id
        if chat_id not in managed_chats:
            return
        ch_name, assigned_idx = managed_chats[chat_id]
        if assigned_idx != sess_idx:
            return
        row = await db_get_channel(ch_name)
        if not row:
            return
        _, _, _, last_id, _ = row
        if m.id <= last_id:
            return
        await db_update_last_id(ch_name, m.id)
        headers = {}
        if cfg.CALLBACK_BEARER:
            headers["Authorization"] = f"Bearer {cfg.CALLBACK_BEARER}"
        st, body = await http_post_json(cfg.CALLBACK_URL, serialize_message(m, ch_name), headers=headers)
        if st >= 300:
            print(f"[CALLBACK] {st} {body}")
        await asyncio.sleep(jitter_s(cfg.BACKOFF_MIN_MS, cfg.BACKOFF_MAX_MS))
    except FloodWaitError as e:
        print(f"[FloodWait] sess={sess_idx} {e.seconds}s")
        await asyncio.sleep(e.seconds + 1)
    except Exception as e:
        print("[on_new_message] error:", e)
        traceback.print_exc()

async def register_handlers(sess_idx: int):
    cli = clients[sess_idx]
    if cli is None: return
    @cli.on(events.NewMessage)
    async def _handler(event):
        await on_new_message(event, sess_idx)

# ------------------- Periodic catch-up -------------------
async def periodic_scan():
    await asyncio.sleep(2)
    while True:
        try:
            rows = await db_get_all_channels()
            per_session: Dict[int, List[str]] = {}
            for name, chat_id, sess_idx, last_id, _p in rows:
                per_session.setdefault(sess_idx, []).append(name)

            for sess_idx, names in per_session.items():
                cli = clients[sess_idx]
                if cli is None:
                    continue
                for nm in names:
                    try:
                        entity = await cli.get_entity(nm)
                        msgs = await cli.get_messages(entity, limit=cfg.BATCH_MAX)
                        if not msgs:
                            continue
                        msgs.sort(key=lambda x: x.id)
                        row = await db_get_channel(nm)
                        if not row:
                            continue
                        _, _, _, last_id, _ = row
                        new_msgs = [m for m in msgs if m.id > last_id]
                        for m in new_msgs:
                            await db_update_last_id(nm, m.id)
                            headers = {}
                            if cfg.CALLBACK_BEARER:
                                headers["Authorization"] = f"Bearer {cfg.CALLBACK_BEARER}"
                            st, body = await http_post_json(cfg.CALLBACK_URL, serialize_message(m, nm), headers=headers)
                            if st >= 300:
                                print(f"[CALLBACK] {st} {body}")
                            await asyncio.sleep(jitter_s(cfg.BACKOFF_MIN_MS, cfg.BACKOFF_MAX_MS))
                        await asyncio.sleep(jitter_s(cfg.BACKOFF_MIN_MS, cfg.BACKOFF_MAX_MS))
                    except FloodWaitError as e:
                        print(f"[Scan FloodWait] sess={sess_idx} {nm} {e.seconds}s")
                        await asyncio.sleep(e.seconds + 1)
                    except Exception as e:
                        print(f"[Scan ERR] sess={sess_idx} {nm}: {e}")
                        await asyncio.sleep(0.2)
            await asyncio.sleep(cfg.SCAN_INTERVAL_SEC)
        except Exception as e:
            print("[periodic_scan] ERR:", e)
            await asyncio.sleep(3)

# ------------------- Session bootstrap -------------------
async def start_sessions():
    global session_files
    session_files = sorted(glob.glob(os.path.join("sessions", "*.session")))
    if not session_files:
        print("[*] Không tìm thấy file .session trong ./sessions — API vẫn chạy, nhưng chưa có session.")

    for i, sess_path in enumerate(session_files):
        cli = TelegramClient(sess_path, cfg.API_ID, cfg.API_HASH)
        await cli.connect()
        if not await cli.is_user_authorized():
            print(f"[*] {sess_path}: CHƯA ĐĂNG NHẬP → skip")
            await alert(f"❌ Session not authorized: {sess_path}")
            await cli.disconnect()
            clients.append(None)
            session_alerted[sess_path] = True
            continue
        clients.append(cli)
        await register_handlers(i)
        ensure_join_worker(i)
        print(f"[+] Session[{i}] sẵn sàng: {sess_path}")

    # map lại theo session_path nếu index lệch
    rows = await db_get_all_channels()
    for name, chat_id, sess_idx, _last, sess_path in rows:
        if sess_path and sess_path in session_files:
            new_idx = session_files.index(sess_path)
            if new_idx != sess_idx:
                await db_update_mapping(name, new_idx, sess_path)
                sess_idx = new_idx
        managed_chats[chat_id] = (name, sess_idx)
        managed_names[name] = (chat_id, sess_idx)
        ensure_join_worker(sess_idx)

# ------------------- Monitor sessions (health + alert) -------------------
async def monitor_sessions():
    while True:
        try:
            for i, sess_path in enumerate(session_files):
                cli = clients[i] if i < len(clients) else None
                ok = (cli is not None)
                if ok:
                    try:
                        if not await cli.is_user_authorized():
                            ok = False
                    except Exception:
                        ok = False
                if not ok:
                    if not session_alerted.get(sess_path):
                        await alert(f"❌ Session die hoặc mất quyền: {sess_path} (idx {i}) — cần thay thế.")
                        session_alerted[sess_path] = True
                else:
                    # reset alert flag nếu đã hồi
                    session_alerted[sess_path] = False
            await asyncio.sleep(60)
        except Exception:
            await asyncio.sleep(5)

# ------------------- Rescan sessions + Reassign -------------------
async def rescan_sessions_loop():
    await asyncio.sleep(5)
    known = set(session_files)
    while True:
        try:
            current_list = sorted(glob.glob(os.path.join("sessions", "*.session")))
            current = set(current_list)
            added = list(current - known)
            removed = list(known - current)

            # Added sessions
            for sess_path in added:
                i = len(clients)
                cli = TelegramClient(sess_path, cfg.API_ID, cfg.API_HASH)
                await cli.connect()
                if not await cli.is_user_authorized():
                    print(f"[*] {sess_path}: CHƯA ĐĂNG NHẬP (bỏ).")
                    await alert(f"❌ Session mới chưa authorized: {sess_path}")
                    await cli.disconnect()
                    continue
                clients.append(cli)
                session_files.append(sess_path)
                await register_handlers(i)
                ensure_join_worker(i)
                print(f"[+] Session[{i}] thêm mới: {sess_path}")
                await alert(f"🆕 Thêm session mới: {sess_path} (idx {i})")

            # Removed sessions
            for dead_path in removed:
                if dead_path in session_files:
                    idx = session_files.index(dead_path)
                    try:
                        if clients[idx] is not None:
                            await clients[idx].disconnect()
                    except: pass
                    clients[idx] = None
                    print(f"[-] Session[{idx}] đã gỡ: {dead_path}")
                    await alert(f"🗑️ Session bị gỡ: {dead_path} (idx {idx})")

                    rows = await db_get_all_channels()
                    victims = [r for r in rows if r[2] == idx]  # (name, chat_id, sess_idx, last_id, sess_path)

                    preferred_idx = None
                    if len(added) == 1 and added[0] in session_files:
                        preferred_idx = session_files.index(added[0])

                    for name, chat_id, _sess_idx, _last_id, _path_db in victims:
                        try:
                            if preferred_idx is not None and clients[preferred_idx] is not None:
                                new_idx = preferred_idx
                            else:
                                alive_rows = [r for r in rows if clients[r[2]] is not None and r[2] != idx]
                                new_idx = pick_least_loaded_session(alive_rows)

                            ensure_join_worker(new_idx)
                            await join_queues[new_idx].put(name)
                            await db_update_mapping(name, new_idx, session_files[new_idx])
                            managed_chats[chat_id] = (name, new_idx)
                            managed_names[name] = (chat_id, new_idx)
                            print(f"[Reassign queued] {name} -> session[{new_idx}]")
                            await alert(f"🔁 Chuyển {name} -> session[{new_idx}], sẽ join chậm.")
                        except Exception as e:
                            print(f"[Reassign ERR] {name}: {e}")
                            await alert(f"❗ Reassign {name} lỗi: {e}")

            known = set(session_files := current_list)
            await asyncio.sleep(cfg.SESS_RESCAN_SEC)
        except Exception as e:
            print("[rescan_sessions_loop] ERR:", e)
            await asyncio.sleep(3)

# ------------------- FastAPI lifespan -------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    await db_init()
    await start_sessions()
    asyncio.create_task(periodic_scan())
    asyncio.create_task(rescan_sessions_loop())
    asyncio.create_task(monitor_sessions())
    yield
    try:
        if db: await db.close()
        for c in clients:
            if c: await c.disconnect()
    except:
        pass

app = FastAPI(title="TG Pool Poller", version="1.4", lifespan=lifespan)

# ------------------- HTTP API -------------------
@app.get("/channel")
async def add_channel(name: str = Query(..., alias="chanel"),
                      _dep: None = Depends(require_bearer)):
    nm = normalize_name(name)
    if await db_get_channel(nm):
        row = await db_get_channel(nm)
        return {"ok": True, "msg": "exists", "channel": nm, "session_index": row[2]}

    rows = await db_get_all_channels()
    sess_idx = pick_least_loaded_session(rows)
    cli = clients[sess_idx]
    if cli is None:
        raise HTTPException(503, "Không có session sẵn sàng (chưa đăng nhập).")

    try:
        entity = None
        try:
            entity = await cli.get_entity(nm)
        except Exception:
            entity = None
        if entity is None:
            await cli(functions.channels.JoinChannelRequest(nm))
            entity = await cli.get_entity(nm)
    except RPCError as e:
        raise HTTPException(400, f"Join/get entity lỗi: {e.__class__.__name__}")
    except Exception as e:
        raise HTTPException(400, f"Lỗi khác khi join/get entity: {str(e)}")

    chat_id = int(getattr(entity, "id", 0) or 0)
    if not chat_id:
        raise HTTPException(400, "Không lấy được chat_id.")

    msgs = await cli.get_messages(entity, limit=1)
    last_id = msgs[0].id if msgs else 0

    sess_path = session_files[sess_idx]
    await db_upsert_channel(nm, chat_id, sess_idx, last_id, sess_path)
    managed_chats[chat_id] = (nm, sess_idx)
    managed_names[nm] = (chat_id, sess_idx)

    return {"ok": True, "channel": nm, "chat_id": chat_id, "session_index": sess_idx, "last_id": last_id}

@app.get("/delete")
async def delete_channel(channel: str = Query(..., alias="chanel"),
                         _dep: None = Depends(require_bearer)):
    nm = normalize_name(channel)
    row = await db_get_channel(nm)
    if not row:
        return {"ok": True, "msg": "not_found"}
    _, chat_id, sess_idx, _last, _path = row

    cli = clients[sess_idx] if 0 <= sess_idx < len(clients) else None
    if cli is not None:
        try:
            await cli(functions.channels.LeaveChannelRequest(channel=nm))
        except Exception as e:
            print(f"[Leave WARN] {nm}: {e}")

    await db_delete_channel(nm)
    managed_chats.pop(chat_id, None)
    managed_names.pop(nm, None)
    return {"ok": True, "msg": "deleted"}

@app.get("/status")
async def status(_dep: None = Depends(require_bearer)):
    rows = await db_get_all_channels()
    return {
        "ok": True,
        "sessions": [
            {"index": i, "online": (clients[i] is not None), "path": session_files[i] if i < len(session_files) else None}
            for i in range(len(session_files))
        ],
        "channels": [
            {"name": r[0], "chat_id": r[1], "session_index": r[2], "last_id": r[3], "session_path": r[4]}
            for r in rows
        ]
    }

@app.exception_handler(Exception)
async def on_any_exception(request: Request, exc: Exception):
    print("[HTTP ERR]", request.url, repr(exc))
    return JSONResponse(status_code=500, content={"ok": False, "error": str(exc)})

# ------------------- Entrypoint -------------------
if __name__ == "__main__":
    import uvicorn
    # Bind 0.0.0.0 nếu bạn muốn gọi từ ngoài; an toàn hơn: 127.0.0.1 + SSH tunnel/Cloudflare Tunnel
    uvicorn.run("app:app", host="0.0.0.0", port=8080, reload=False, log_level="info")
