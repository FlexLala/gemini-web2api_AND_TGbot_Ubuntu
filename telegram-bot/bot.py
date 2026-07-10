#!/usr/bin/env python3
r"""
Gemini Telegram Bot
- Работает через локальный gemini-web2api (Германия -> Gemini)
- Доступ только по разрешению админа
- LaTeX-safe HTML форматирование
- Инлайн-режим (только экономная модель)
- Админ-статистика без чтения запросов
"""

import os
import sys
import sqlite3
import html
import re
import json
import time
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any

import httpx
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InlineQueryResultArticle,
    InputTextMessageContent,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    InlineQueryHandler,
    ContextTypes,
    filters,
)

# ─── Configuration ───────────────────────────────────────────────────────────

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
GEMINI_API_URL = os.getenv("GEMINI_API_URL", "http://gemini-api:8081/v1/chat/completions").strip()
INLINE_MODEL = os.getenv("INLINE_MODEL", "gemini-3.5-flash").strip()
DEFAULT_MODEL = os.getenv("DEFAULT_MODEL", "gemini-3.5-flash").strip()
API_KEY = os.getenv("GEMINI_API_KEY", "").strip()
MAX_HISTORY = 20  # сообщений (10 пар)
IGNORE_TTL_SEC = 300  # 5 минут

if not BOT_TOKEN:
    raise SystemExit("TELEGRAM_BOT_TOKEN не задан в .env")
if not ADMIN_ID:
    raise SystemExit("ADMIN_ID не задан в .env")

# ─── Logging ─────────────────────────────────────────────────────────────────

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ─── Database ────────────────────────────────────────────────────────────────

DB_PATH = "/app/data/bot.db"

def _conn() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    return sqlite3.connect(DB_PATH)

def init_db() -> None:
    conn = _conn()
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            allowed INTEGER DEFAULT 0,
            blocked INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS conversations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            role TEXT,
            content TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS stats (
            user_id INTEGER PRIMARY KEY,
            requests INTEGER DEFAULT 0,
            tokens_prompt INTEGER DEFAULT 0,
            tokens_completion INTEGER DEFAULT 0,
            last_used TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()

def ensure_user(user_id: int, username: Optional[str], first_name: Optional[str], allowed: int = 0) -> None:
    conn = _conn()
    c = conn.cursor()
    c.execute("SELECT 1 FROM users WHERE user_id = ?", (user_id,))
    if not c.fetchone():
        c.execute(
            "INSERT INTO users (user_id, username, first_name, allowed) VALUES (?, ?, ?, ?)",
            (user_id, username or "", first_name or "", allowed),
        )
        c.execute(
            "INSERT OR IGNORE INTO stats (user_id, requests, tokens_prompt, tokens_completion) VALUES (?, 0, 0, 0)",
            (user_id,),
        )
    else:
        c.execute(
            "UPDATE users SET username = ?, first_name = ? WHERE user_id = ?",
            (username or "", first_name or "", user_id),
        )
    conn.commit()
    conn.close()

def get_user(user_id: int) -> Optional[Dict[str, Any]]:
    conn = _conn()
    c = conn.cursor()
    c.execute("SELECT user_id, username, first_name, allowed, blocked FROM users WHERE user_id = ?", (user_id,))
    row = c.fetchone()
    conn.close()
    if row:
        return {
            "user_id": row[0],
            "username": row[1],
            "first_name": row[2],
            "allowed": row[3],
            "blocked": row[4],
        }
    return None

def set_allowed(user_id: int, allowed: int) -> None:
    conn = _conn()
    c = conn.cursor()
    c.execute("UPDATE users SET allowed = ? WHERE user_id = ?", (allowed, user_id))
    conn.commit()
    conn.close()

def set_blocked(user_id: int, blocked: int) -> None:
    conn = _conn()
    c = conn.cursor()
    c.execute("UPDATE users SET blocked = ? WHERE user_id = ?", (blocked, user_id))
    conn.commit()
    conn.close()

def get_history(user_id: int) -> List[Dict[str, str]]:
    conn = _conn()
    c = conn.cursor()
    c.execute(
        "SELECT role, content FROM conversations WHERE user_id = ? ORDER BY id DESC LIMIT ?",
        (user_id, MAX_HISTORY),
    )
    rows = c.fetchall()
    conn.close()
    rows.reverse()
    return [{"role": r, "content": c} for r, c in rows]

def add_message(user_id: int, role: str, content: str) -> None:
    conn = _conn()
    c = conn.cursor()
    c.execute(
        "INSERT INTO conversations (user_id, role, content) VALUES (?, ?, ?)",
        (user_id, role, content),
    )
    c.execute(
        """
        DELETE FROM conversations WHERE id IN (
            SELECT id FROM conversations WHERE user_id = ? ORDER BY id DESC LIMIT -1 OFFSET ?
        )
        """,
        (user_id, MAX_HISTORY),
    )
    conn.commit()
    conn.close()

def clear_history(user_id: int) -> None:
    conn = _conn()
    c = conn.cursor()
    c.execute("DELETE FROM conversations WHERE user_id = ?", (user_id,))
    conn.commit()
    conn.close()

def update_stats(user_id: int, prompt_len: int, completion_len: int) -> None:
    conn = _conn()
    c = conn.cursor()
    c.execute(
        """
        INSERT INTO stats (user_id, requests, tokens_prompt, tokens_completion, last_used)
        VALUES (?, 1, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            requests = requests + 1,
            tokens_prompt = tokens_prompt + excluded.tokens_prompt,
            tokens_completion = tokens_completion + excluded.tokens_completion,
            last_used = excluded.last_used
        """,
        (user_id, prompt_len // 4, completion_len // 4, datetime.utcnow().isoformat()),
    )
    conn.commit()
    conn.close()

def get_all_stats() -> List[Dict[str, Any]]:
    conn = _conn()
    c = conn.cursor()
    c.execute("""
        SELECT u.user_id, u.username, u.first_name, u.allowed, u.blocked,
               COALESCE(s.requests, 0), COALESCE(s.tokens_prompt, 0), COALESCE(s.tokens_completion, 0)
        FROM users u
        LEFT JOIN stats s ON u.user_id = s.user_id
        ORDER BY s.requests DESC
    """)
    rows = c.fetchall()
    conn.close()
    return [
        {
            "user_id": r[0],
            "username": r[1],
            "first_name": r[2],
            "allowed": r[3],
            "blocked": r[4],
            "requests": r[5],
            "tokens_prompt": r[6],
            "tokens_completion": r[7],
        }
        for r in rows
    ]

# ─── Ignore list (in-memory, 5 min TTL) ──────────────────────────────────────

_ignore_cache: Dict[int, float] = {}

def is_ignored(user_id: int) -> bool:
    ts = _ignore_cache.get(user_id, 0)
    if time.time() - ts < IGNORE_TTL_SEC:
        return True
    if user_id in _ignore_cache:
        del _ignore_cache[user_id]
    return False

def add_ignore(user_id: int) -> None:
    _ignore_cache[user_id] = time.time()

# ─── LaTeX / HTML formatting ─────────────────────────────────────────────────

def _escape(text: str) -> str:
    return html.escape(text)

def format_gemini_text(raw: str) -> str:
    r"""
    Обрабатывает текст от Gemini для Telegram HTML:
    - Выделяет <thinking>...</thinking> в сворачиваемый blockquote
    - Экранирует HTML
    - Оборачивает LaTeX \(...\), \[...\], $...$, $$...$$ в code/pre
    """
    # 1. Вырезаем thinking
    thinking_html = ""
    m = re.search(r'<thinking>(.*?)</thinking>', raw, re.DOTALL)
    text = raw
    if m:
        thinking = m.group(1).strip()
        text = re.sub(r'<thinking>.*?</thinking>', '', raw, flags=re.DOTALL, count=1).strip()
        if thinking:
            thinking_html = (
                f'<blockquote expandable><b>🧠 Рассуждение:</b>\n'
                f'{_escape(thinking)}</blockquote>\n\n'
            )

    # 2. Разбиваем текст на сегменты: plain / latex
    parts: List[str] = []
    last_end = 0
    # Группы: 2=\(...\), 3=\[...\], 4=$...$, 5=$$...$$
    pattern = re.compile(
        r'(\\\((.*?)\\\)|\\\[(.*?)\\\]|(?<!\$)\$(?!\$)(.*?)(?<!\$)\$(?!\$)|\$\$(.*?)\$\$)',
        re.DOTALL,
    )

    for match in pattern.finditer(text):
        start, end = match.span()
        if start > last_end:
            parts.append(_escape(text[last_end:start]))
        if match.group(2) is not None:
            parts.append(f'<code>{_escape(match.group(2))}</code>')
        elif match.group(3) is not None:
            parts.append(f'<pre>{_escape(match.group(3))}</pre>')
        elif match.group(4) is not None:
            parts.append(f'<code>{_escape(match.group(4))}</code>')
        elif match.group(5) is not None:
            parts.append(f'<pre>{_escape(match.group(5))}</pre>')
        last_end = end

    if last_end < len(text):
        parts.append(_escape(text[last_end:]))

    formatted = thinking_html + "".join(parts)
    if not formatted.strip():
        formatted = _escape(raw)
    return formatted

def split_message(text: str, max_len: int = 4000) -> List[str]:
    if len(text) <= max_len:
        return [text]
    chunks = []
    current = ""
    for paragraph in text.split('\n\n'):
        para = paragraph + '\n\n'
        if len(current) + len(para) > max_len:
            if current:
                chunks.append(current.rstrip())
            current = para
        else:
            current += para
    if current:
        chunks.append(current.rstrip())
    # Если абзацы слишком длинные, режем по строкам
    final = []
    for ch in chunks:
        if len(ch) <= max_len:
            final.append(ch)
            continue
        sub = ""
        for line in ch.split('\n'):
            if len(sub) + len(line) + 1 > max_len:
                final.append(sub)
                sub = line + '\n'
            else:
                sub += line + '\n'
        if sub:
            final.append(sub)
    return final

# ─── Gemini API client ───────────────────────────────────────────────────────

HEADERS = {"Content-Type": "application/json"}
if API_KEY:
    HEADERS["Authorization"] = f"Bearer {API_KEY}"

async def ask_gemini(
    user_id: int,
    prompt: str,
    model: str,
    with_history: bool = True,
) -> Dict[str, Any]:
    messages = []
    if with_history:
        messages = get_history(user_id)
    messages.append({"role": "user", "content": prompt})

    payload = {
        "model": model,
        "messages": messages,
        "stream": False,
    }

    async with httpx.AsyncClient(timeout=120.0) as client:
        resp = await client.post(GEMINI_API_URL, json=payload, headers=HEADERS)
        resp.raise_for_status()
        data = resp.json()

    # Проверка ошибок от gemini-web2api
    if "error" in data:
        raise RuntimeError(data["error"].get("message", "Unknown API error"))

    choice = data.get("choices", [{}])[0]
    answer = choice.get("message", {}).get("content") or "(пустой ответ)"

    # Сохраняем историю
    if with_history:
        add_message(user_id, "user", prompt)
        add_message(user_id, "assistant", answer)

    # Статистика
    usage = data.get("usage", {})
    update_stats(
        user_id,
        usage.get("prompt_tokens", 0) * 4,
        usage.get("completion_tokens", 0) * 4,
    )

    return {"text": answer, "model": model}

# ─── Access control ──────────────────────────────────────────────────────────

async def require_access(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    user = update.effective_user
    if not user:
        return False

    user_id = user.id
    username = user.username
    first_name = user.first_name

    ensure_user(user_id, username, first_name, allowed=1 if user_id == ADMIN_ID else 0)

    if user_id == ADMIN_ID:
        return True

    u = get_user(user_id)
    if u and u["blocked"]:
        await update.message.reply_text("⛔ Доступ заблокирован.")
        return False
    if u and u["allowed"]:
        return True

    if is_ignored(user_id):
        await update.message.reply_text("⏳ Ожидайте решения администратора.")
        return False

    # Уведомляем админа
    await notify_admin_request(update, context)
    await update.message.reply_text(
        "🔒 У вас пока нет доступа. Администратор получил уведомление и рассмотрит запрос."
    )
    return False

async def notify_admin_request(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    query_text = ""
    if update.message and update.message.text:
        query_text = update.message.text[:200]
    elif update.inline_query and update.inline_query.query:
        query_text = "[inline] " + update.inline_query.query[:200]

    text = (
        f"🔔 <b>Новый запрос на доступ</b>\n\n"
        f"ID: <code>{user.id}</code>\n"
        f"Имя: {html.escape(user.first_name or '')}\n"
        f"Юзернейм: @{html.escape(user.username or 'нет')}\n\n"
        f"Сообщение: {html.escape(query_text)}"
    )
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Разрешить", callback_data=f"allow:{user.id}"),
            InlineKeyboardButton("🚫 Игнорировать 5 мин", callback_data=f"ignore:{user.id}"),
        ]
    ])
    try:
        await context.bot.send_message(chat_id=ADMIN_ID, text=text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
    except Exception as e:
        logger.error(f"Не удалось уведомить админа: {e}")

# ─── Handlers ────────────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    ensure_user(user.id, user.username, user.first_name, allowed=1 if user.id == ADMIN_ID else 0)

    text = (
        "👋 <b>Привет!</b>\n\n"
        "Я бот для Gemini через локальный API (Германия -> Gemini).\n\n"
        "<b>Команды:</b>\n"
        "/model — выбрать модель\n"
        "/clear — очистить историю диалога\n"
        "/help — справка\n"
    )
    if user.id == ADMIN_ID:
        text += (
            "\n<b>Админ-команды:</b>\n"
            "/stats — статистика по пользователям\n"
            "/users — список пользователей\n"
            "/allow &lt;id&gt; — разрешить пользователя\n"
            "/block &lt;id&gt; — заблокировать пользователя\n"
        )
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await cmd_start(update, context)

async def cmd_model(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_access(update, context):
        return
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("⚡ Gemini 3.5 Flash (быстрый)", callback_data="model:gemini-3.5-flash")],
        [InlineKeyboardButton("🧠 Gemini 3.5 Flash Thinking (глубокий)", callback_data="model:gemini-3.5-flash-thinking")],
        [InlineKeyboardButton("🔬 Gemini 3.1 Pro", callback_data="model:gemini-3.1-pro")],
        [InlineKeyboardButton("🎯 Gemini Auto", callback_data="model:gemini-auto")],
        [InlineKeyboardButton("💡 Flash Thinking Lite", callback_data="model:gemini-3.5-flash-thinking-lite")],
        [InlineKeyboardButton("🪶 Flash Lite", callback_data="model:gemini-flash-lite")],
    ])
    await update.message.reply_text("Выберите модель:", reply_markup=keyboard)

async def cmd_clear(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_access(update, context):
        return
    clear_history(update.effective_user.id)
    await update.message.reply_text("🗑 История диалога очищена.")

async def callback_model(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    data = query.data
    if data.startswith("model:"):
        model = data.split(":", 1)[1]
        context.user_data["model"] = model
        await query.edit_message_text(f"✅ Модель установлена: <code>{html.escape(model)}</code>", parse_mode=ParseMode.HTML)

async def callback_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    data = query.data
    if not data or ":" not in data:
        return
    action, user_id_str = data.split(":", 1)
    try:
        user_id = int(user_id_str)
    except ValueError:
        return

    if action == "allow":
        set_allowed(user_id, 1)
        set_blocked(user_id, 0)
        await query.edit_message_text(f"✅ Доступ разрешён для <code>{user_id}</code>", parse_mode=ParseMode.HTML)
        try:
            await context.bot.send_message(chat_id=user_id, text="🎉 Администратор разрешил вам доступ! Можете начинать.")
        except Exception:
            pass
    elif action == "ignore":
        add_ignore(user_id)
        await query.edit_message_text(f"🚫 Запрос от <code>{user_id}</code> проигнорирован на 5 минут.", parse_mode=ParseMode.HTML)

# ─── Message handler ─────────────────────────────────────────────────────────

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_access(update, context):
        return
    user = update.effective_user
    prompt = update.message.text
    if not prompt:
        return

    model = context.user_data.get("model", DEFAULT_MODEL)
    wait_msg = await update.message.reply_text("⏳ Думаю...")

    try:
        result = await ask_gemini(user.id, prompt, model, with_history=True)
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error: {e.response.status_code} {e.response.text}")
        await wait_msg.edit_text(f"❌ Ошибка соединения с API: <code>{e.response.status_code}</code> Bad Gateway / Upstream error", parse_mode=ParseMode.HTML)
        return
    except Exception as e:
        logger.exception("Gemini error")
        await wait_msg.edit_text(f"❌ Ошибка: <code>{html.escape(str(e))}</code>", parse_mode=ParseMode.HTML)
        return

    formatted = format_gemini_text(result["text"])
    chunks = split_message(formatted)

    try:
        await wait_msg.delete()
    except Exception:
        pass

    for chunk in chunks:
        try:
            await update.message.reply_text(chunk, parse_mode=ParseMode.HTML)
        except Exception as e:
            logger.error(f"Send error: {e}")
            # Fallback: отправляем как plain text
            await update.message.reply_text(_escape(chunk))

# ─── Admin commands ──────────────────────────────────────────────────────────

async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != ADMIN_ID:
        return
    rows = get_all_stats()
    if not rows:
        await update.message.reply_text("Нет данных.")
        return
    lines = ["<b>📊 Статистика</b>\n"]
    for r in rows:
        name = r["username"] or r["first_name"] or str(r["user_id"])
        status = "🟢" if r["allowed"] else "🔴"
        if r["blocked"]:
            status = "⛔"
        lines.append(
            f"{status} <code>{r['user_id']}</code> | @{html.escape(name)} | "
            f"запросов: {r['requests']} | токенов: {r['tokens_prompt'] + r['tokens_completion']}"
        )
    text = "\n".join(lines)
    for chunk in split_message(text):
        await update.message.reply_text(chunk, parse_mode=ParseMode.HTML)

async def cmd_users(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != ADMIN_ID:
        return
    rows = get_all_stats()
    lines = ["<b>👥 Пользователи</b>\n"]
    for r in rows:
        name = r["username"] or r["first_name"] or "—"
        status = "разрешён" if r["allowed"] else "ожидание"
        if r["blocked"]:
            status = "заблокирован"
        lines.append(f"• <code>{r['user_id']}</code> | @{html.escape(name)} | {status}")
    text = "\n".join(lines)
    for chunk in split_message(text):
        await update.message.reply_text(chunk, parse_mode=ParseMode.HTML)

async def cmd_allow(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != ADMIN_ID:
        return
    if not context.args:
        await update.message.reply_text("Использование: /allow &lt;user_id&gt;", parse_mode=ParseMode.HTML)
        return
    try:
        uid = int(context.args[0])
    except ValueError:
        await update.message.reply_text("Неверный ID.")
        return
    set_allowed(uid, 1)
    set_blocked(uid, 0)
    await update.message.reply_text(f"✅ Разрешён доступ для <code>{uid}</code>", parse_mode=ParseMode.HTML)
    try:
        await context.bot.send_message(chat_id=uid, text="🎉 Вам разрешили доступ!")
    except Exception:
        pass

async def cmd_block(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != ADMIN_ID:
        return
    if not context.args:
        await update.message.reply_text("Использование: /block &lt;user_id&gt;", parse_mode=ParseMode.HTML)
        return
    try:
        uid = int(context.args[0])
    except ValueError:
        await update.message.reply_text("Неверный ID.")
        return
    set_blocked(uid, 1)
    set_allowed(uid, 0)
    await update.message.reply_text(f"⛔ Пользователь <code>{uid}</code> заблокирован.", parse_mode=ParseMode.HTML)

# ─── Inline mode ─────────────────────────────────────────────────────────────

async def inline_query(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.inline_query.query
    user = update.effective_user
    if not user or not query or len(query.strip()) < 2:
        return

    ensure_user(user.id, user.username, user.first_name, allowed=1 if user.id == ADMIN_ID else 0)

    if user.id != ADMIN_ID:
        u = get_user(user.id)
        if not u or not u["allowed"] or u["blocked"]:
            if not is_ignored(user.id):
                await notify_admin_request(update, context)
            return

    # Делаем запрос к API сразу (инлайн не даёт редактировать потом)
    try:
        result = await ask_gemini(user.id, query.strip(), INLINE_MODEL, with_history=False)
        formatted = format_gemini_text(result["text"])
        if len(formatted) > 4000:
            formatted = formatted[:3990] + "\n\n<i>...обрезано</i>"
    except httpx.HTTPStatusError as e:
        formatted = f"❌ Ошибка API: <code>{e.response.status_code}</code>"
    except Exception as e:
        formatted = f"❌ Ошибка: <code>{html.escape(str(e))}</code>"

    title = query.strip()[:50] + "..." if len(query.strip()) > 50 else query.strip()
    results = [
        InlineQueryResultArticle(
            id="gemini_inline",
            title=title,
            input_message_content=InputTextMessageContent(
                f"<b>Вопрос:</b> {html.escape(query.strip()[:200])}\n\n{formatted}",
                parse_mode=ParseMode.HTML,
            ),
            description="Нажмите, чтобы отправить ответ Gemini",
        )
    ]
    await update.inline_query.answer(results, cache_time=5, is_personal=True)

# ─── Error handler ───────────────────────────────────────────────────────────

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error(f"Exception while handling an update: {context.error}", exc_info=context.error)
    if update and update.effective_message:
        try:
            await update.effective_message.reply_text(
                f"❌ Произошла внутренняя ошибка: <code>{html.escape(str(context.error))}</code>",
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass

# ─── Main ────────────────────────────────────────────────────────────────────

def main() -> None:
    init_db()
    application = Application.builder().token(BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("help", cmd_help))
    application.add_handler(CommandHandler("model", cmd_model))
    application.add_handler(CommandHandler("clear", cmd_clear))
    application.add_handler(CommandHandler("stats", cmd_stats))
    application.add_handler(CommandHandler("users", cmd_users))
    application.add_handler(CommandHandler("allow", cmd_allow))
    application.add_handler(CommandHandler("block", cmd_block))

    application.add_handler(CallbackQueryHandler(callback_model, pattern=r"^model:"))
    application.add_handler(CallbackQueryHandler(callback_admin, pattern=r"^(allow|ignore):"))

    application.add_handler(InlineQueryHandler(inline_query))

    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    application.add_error_handler(error_handler)

    logger.info("Бот запущен. Ожидаю сообщения...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
