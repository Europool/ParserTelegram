import asyncio
import json
import re
import random
from datetime import datetime
from io import BytesIO
from typing import List, Set, Optional

import aiohttp
import aiosqlite
from fastapi import FastAPI, Request, Form, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from openpyxl import Workbook
from telethon import TelegramClient
from telethon.errors import (
    SessionPasswordNeededError,
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
)
from telethon.tl.functions.contacts import SearchRequest

# CONFIG
DB_PATH = "data.db"
SESSION_NAME = "session"

REQUEST_DELAY = 0.2
RETRY_DELAY = 0.3

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/123 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4) AppleWebKit/605.1.15 Safari/605.1.15",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1) AppleWebKit/605.1.15 Mobile Safari/604.1",
    "Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36 Chrome/122 Mobile Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) Gecko Firefox/124.0",
]

# APP
app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates("templates")

progress = {"running": False, "total": 0, "done": 0}
live_log: List[str] = []

#  PIPELINE STATE 
pipeline_lock = asyncio.Lock()
pipeline_task: Optional[asyncio.Task] = None
pipeline_cancelled = False
pipeline_started_by_user = False 

# TELEGRAM
tg_client: Optional[TelegramClient] = None
tg_lock = asyncio.Lock()

# UTILS
def log(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    live_log.append(f"[{ts}] {msg}")
    del live_log[:-300]

def headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8",
        "Accept": "text/html,application/xhtml+xml",
    }

# DB
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        );
        CREATE TABLE IF NOT EXISTS channels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE,
            link TEXT,
            title TEXT,
            members INTEGER,
            type TEXT,
            created_at TEXT
        );
        """)
        await db.commit()

async def get_setting(key: str):
    async with aiosqlite.connect(DB_PATH) as db:
        r = await db.execute("SELECT value FROM settings WHERE key=?", (key,))
        row = await r.fetchone()
        return row[0] if row else None

async def set_setting(key: str, value: Optional[str]):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO settings(key,value) VALUES (?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
        await db.commit()

# TELEGRAM CLIENT
async def get_tg():
    global tg_client
    async with tg_lock:
        if tg_client and tg_client.is_connected():
            return tg_client

        api_id = await get_setting("api_id")
        api_hash = await get_setting("api_hash")
        if not api_id or not api_hash:
            return None

        tg_client = TelegramClient(
            SESSION_NAME,
            int(api_id),
            api_hash,
            auto_reconnect=True,
            sequential_updates=True,
        )
        await tg_client.connect()
        return tg_client

async def telegram_ready():
    c = await get_tg()
    return bool(c and await c.is_user_authorized())

# WS
class WS:
    def __init__(self):
        self.clients: Set[WebSocket] = set()

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.clients.add(ws)

    async def push(self):
        async with aiosqlite.connect(DB_PATH) as db:
            cur = await db.execute(
                "SELECT username, link, title, members, type "
                "FROM channels ORDER BY members DESC"
            )
            rows = await cur.fetchall()

        payload = json.dumps({
            "progress": progress,
            "log": live_log[-50:],
            "telegram_ready": await telegram_ready(),
            "telegram_code_sent": bool(await get_setting("phone_code_hash")),
            "rows": rows,
        })

        for c in list(self.clients):
            try:
                await c.send_text(payload)
            except:
                self.clients.discard(c)

ws = WS()

@app.on_event("startup")
async def startup():
    await init_db()
    asyncio.create_task(ws_pulse())

@app.on_event("shutdown")
async def shutdown():
    if tg_client:
        await tg_client.disconnect()

async def ws_pulse():
    while True:
        await asyncio.sleep(0.8)
        await ws.push()

# TG AUTH
@app.post("/tg/send_code")
async def tg_send_code(api_id: str = Form(...), api_hash: str = Form(...), phone: str = Form(...)):
    await set_setting("api_id", api_id)
    await set_setting("api_hash", api_hash)
    await set_setting("phone", phone)

    client = await get_tg()
    log("Telegram: отправка кода")
    res = await client.send_code_request(phone)
    await set_setting("phone_code_hash", res.phone_code_hash)
    return {"ok": True}

@app.post("/tg/confirm")
async def tg_confirm(code: str = Form(...), password: Optional[str] = Form(None)):
    client = await get_tg()
    phone = await get_setting("phone")
    h = await get_setting("phone_code_hash")

    try:
        await client.sign_in(phone=phone, code=code, phone_code_hash=h)
    except PhoneCodeExpiredError:
        await set_setting("phone_code_hash", None)
        log("Telegram: код истёк")
        raise HTTPException(400)
    except PhoneCodeInvalidError:
        raise HTTPException(400)
    except SessionPasswordNeededError:
        await client.sign_in(password=password)

    await set_setting("phone_code_hash", None)
    log("Telegram: авторизация успешна")
    return {"ok": True}

# PARSER
def parse_html(html: str):
    title = None
    members = 0
    kind = "channel"

    t = re.search(r'og:title" content="([^"]+)"', html)
    if t:
        title = t.group(1)

    m = re.search(r'([\d\s]+)\s+(subscribers|members)', html)
    if m:
        members = int(m.group(1).replace(" ", ""))
        kind = "group" if m.group(2) == "members" else "channel"

    if "joinchat" in html:
        kind = "group"

    return title, members, kind

async def fetch_html(session, username):
    async with session.get(f"https://t.me/{username}", headers=headers()) as r:
        html = await r.text()
        return html if len(html) > 500 else None

async def check_username(username, session, db):
    log(f"Проверка @{username}")

    html = await fetch_html(session, username)
    if not html:
        await asyncio.sleep(RETRY_DELAY)
        html = await fetch_html(session, username)

    if not html:
        progress["done"] += 1
        return

    title, members, kind = parse_html(html)

    updates, params = [], []
    if title:
        updates.append("title=?")
        params.append(title)
    if members > 0:
        updates.append("members=?")
        params.append(members)
    if kind:
        updates.append("type=?")
        params.append(kind)

    if not updates:
        progress["done"] += 1
        return

    cur = await db.execute("SELECT id FROM channels WHERE username=?", (username,))
    exists = await cur.fetchone()

    if exists:
        await db.execute(
            f"UPDATE channels SET {', '.join(updates)} WHERE username=?",
            (*params, username),
        )
    else:
        await db.execute(
            "INSERT INTO channels(username, link, title, members, type, created_at) "
            "VALUES (?,?,?,?,?,?)",
            (
                username,
                f"https://t.me/{username}",
                title,
                members,
                kind,
                datetime.utcnow().isoformat(),
            ),
        )

    await db.commit()
    progress["done"] += 1
    await asyncio.sleep(REQUEST_DELAY)

# PIPELINE
async def search_tg(queries):
    client = await get_tg()
    found = set()
    for q in queries:
        log(f"Поиск: {q}")
        res = await client(SearchRequest(q=q, limit=50))
        for c in res.chats:
            if c.username:
                found.add(c.username.lower())
    return found

async def pipeline(queries):
    global pipeline_cancelled

    if not pipeline_started_by_user:
        log("Pipeline заблокирован")
        return

    async with pipeline_lock:
        progress.update({"running": True, "done": 0, "total": 0})
        log("Pipeline: start")

        usernames = await search_tg(queries)
        progress["total"] = len(usernames)

        async with aiohttp.ClientSession() as session:
            async with aiosqlite.connect(DB_PATH) as db:
                for u in usernames:
                    if pipeline_cancelled:
                        log("Pipeline остановлен")
                        break
                    await check_username(u, session, db)
                    await ws.push()

        progress["running"] = False
        log("Pipeline finished")

# RECHECK
@app.post("/recheck_empty")
async def recheck_empty():
    global pipeline_cancelled
    pipeline_cancelled = False

    async with pipeline_lock:
        async with aiosqlite.connect(DB_PATH) as db:
            cur = await db.execute(
                "SELECT username FROM channels WHERE members=0 OR title IS NULL"
            )
            names = [r[0] for r in await cur.fetchall()]

        if not names:
            log("Перепроверка: пустых нет")
            return {"ok": True}

        progress.update({"running": True, "done": 0, "total": len(names)})
        log("Перепроверка пустых: start")

        async with aiohttp.ClientSession() as session:
            async with aiosqlite.connect(DB_PATH) as db:
                for u in names:
                    if pipeline_cancelled:
                        log("Перепроверка остановлена")
                        break
                    await check_username(u, session, db)
                    await ws.push()

        progress["running"] = False
        log("Перепроверка пустых: finished")
    return {"ok": True}

# CLEAR
@app.post("/clear_results")
async def clear_results():
    global pipeline_cancelled, pipeline_task, pipeline_started_by_user

    pipeline_cancelled = True
    pipeline_started_by_user = False

    async with pipeline_lock:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("DELETE FROM channels")
            await db.commit()

        progress.update({"running": False, "done": 0, "total": 0})
        live_log.clear()
        log("Результаты очищены")
        await ws.push()

    pipeline_task = None
    return {"ok": True}

# WEB
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse(
        "index.html",
        {"request": request, "progress": progress}
    )

@app.post("/run")
async def run(queries: str = Form(...)):
    global pipeline_task, pipeline_cancelled, pipeline_started_by_user

    pipeline_started_by_user = True
    pipeline_cancelled = False

    q = [x.strip() for x in queries.splitlines() if x.strip()]
    pipeline_task = asyncio.create_task(pipeline(q))
    return {"ok": True}

@app.websocket("/ws")
async def ws_route(wsck: WebSocket):
    await ws.connect(wsck)
    try:
        while True:
            await wsck.receive_text()
    except WebSocketDisconnect:
        pass

@app.get("/export/excel")
async def export_excel():
    wb = Workbook()
    wsx = wb.active
    wsx.append(["Username", "Link", "Title", "Members", "Type"])

    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT username, link, title, members, type "
            "FROM channels ORDER BY members DESC"
        )
        for row in await cur.fetchall():
            wsx.append(row)

    bio = BytesIO()
    wb.save(bio)
    bio.seek(0)

    return StreamingResponse(
        bio,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=telegram_channels.xlsx"}
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)