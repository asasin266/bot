#!/usr/bin/env python3
# coding: utf-8
"""
Анонимный чат-бот с очередью поиска, VIP, историей, жалобами, профилем, антиспамом и базовой защитой.
Требует Python 3.11+, aiogram.
Настройки (TOKEN, ADMIN_ID) из переменных окружения.
"""

import os
import re
import time
import logging
import sqlite3
from datetime import datetime
from functools import wraps
from concurrent.futures import ThreadPoolExecutor
import asyncio

from aiogram import Bot, Dispatcher, types
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardRemove
from aiogram.utils import exceptions
from aiogram.utils.executor import start_polling

# ---------------------------
# Конфигурация (берётся из окружения)
# ---------------------------
TOKEN = os.getenv("TOKEN") or os.getenv("TG_BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID") or os.getenv("TG_ADMIN_ID") or 0)
DB_PATH = os.getenv("BOT_DB_PATH", "bot_data.db")
LOG_FILE = os.getenv("BOT_LOG", "bot.log")

if not TOKEN:
    raise RuntimeError("TOKEN is not set. Set TOKEN in environment variables.")

# Безопасность / лимиты
MAX_FILE_SIZE = int(os.getenv("MAX_FILE_SIZE", 10 * 1024 * 1024))  # 10 MB
ALLOWED_DOCUMENT_EXT = {'.pdf', '.txt', '.jpg', '.jpeg', '.png', '.mp3', '.ogg', '.mp4', '.webm'}
MSG_RATE_LIMIT_PER_MIN = int(os.getenv("MSG_RATE_LIMIT_PER_MIN", 20))

# Логирование
logging.basicConfig(level=logging.INFO, filename=LOG_FILE,
                    format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------
# Aiogram init
# ---------------------------
bot = Bot(TOKEN, parse_mode='HTML')
dp = Dispatcher(bot)

# Thread pool for sqlite sync calls
executor_pool = ThreadPoolExecutor(max_workers=4)
loop = asyncio.get_event_loop()

# ---------------------------
# Простые sync-DB вызовы в executor
# ---------------------------
def _init_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY,
        username TEXT,
        name TEXT,
        sex TEXT DEFAULT 'Не выбран',
        age INTEGER DEFAULT 0,
        interests TEXT DEFAULT '',
        vip INTEGER DEFAULT 0,
        partner INTEGER DEFAULT NULL,
        banned INTEGER DEFAULT 0,
        created_at TEXT
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS search_queue (
        user_id INTEGER,
        sex_filter TEXT,
        queued_at REAL
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        direction TEXT,
        content TEXT,
        created_at REAL
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS complaints (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        from_user INTEGER,
        about_user INTEGER,
        reason TEXT,
        created_at REAL
    )""")
    conn.commit()
    conn.close()

def run_db(query, params=(), fetch=False, many=False):
    def _run():
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        cur = conn.cursor()
        if many:
            cur.executemany(query, params)
        else:
            cur.execute(query, params)
        if fetch:
            rows = cur.fetchall()
            conn.commit()
            conn.close()
            return rows
        conn.commit()
        conn.close()
        return None
    return loop.run_in_executor(executor_pool, _run)

_init_db()

# ---------------------------
# Утилиты
# ---------------------------
def sanitize_text(text: str, max_len=1000):
    if text is None:
        return ''
    text = re.sub(r'[\x00-\x08\x0b-\x0c\x0e-\x1f]', '', text)
    text = text.strip()
    return text[:max_len]

def is_admin(user_id: int):
    return user_id == ADMIN_ID

user_rates = {}  # user_id -> [timestamps]
def rate_limited(user_id: int, limit=MSG_RATE_LIMIT_PER_MIN):
    now = time.time()
    window_start = now - 60
    arr = user_rates.get(user_id, [])
    arr = [t for t in arr if t > window_start]
    if len(arr) >= limit:
        user_rates[user_id] = arr
        return True
    arr.append(now)
    user_rates[user_id] = arr
    return False

# ---------------------------
# DB helper functions (async wrappers)
# ---------------------------
async def ensure_user_record(user: types.User):
    uid = user.id
    username = sanitize_text(user.username or "")
    name = sanitize_text(user.first_name or "")
    rows = await run_db("SELECT id FROM users WHERE id = ?", (uid,), fetch=True)
    if not rows:
        created = datetime.utcnow().isoformat()
        await run_db("INSERT INTO users (id, username, name, created_at) VALUES (?, ?, ?, ?)",
                     (uid, username, name, created))
    else:
        await run_db("UPDATE users SET username = ?, name = ? WHERE id = ?",
                     (username, name, uid))

async def get_user(uid: int):
    rows = await run_db("SELECT id, username, name, sex, age, interests, vip, partner, banned FROM users WHERE id = ?",
                        (uid,), fetch=True)
    if not rows:
        return None
    row = rows[0]
    return {
        "id": row[0],
        "username": row[1],
        "name": row[2],
        "sex": row[3],
        "age": row[4],
        "interests": row[5].split(',') if row[5] else [],
        "vip": bool(row[6]),
        "partner": row[7],
        "banned": bool(row[8])
    }

async def set_partner(u1:int, u2:int):
    await run_db("UPDATE users SET partner = ? WHERE id = ?", (u2, u1))
    await run_db("UPDATE users SET partner = ? WHERE id = ?", (u1, u2))

async def clear_partner(uid:int):
    await run_db("UPDATE users SET partner = NULL WHERE id = ?", (uid,))

async def add_to_queue(uid:int, sex_filter:str):
    await run_db("INSERT INTO search_queue (user_id, sex_filter, queued_at) VALUES (?, ?, ?)",
                 (uid, sex_filter, time.time()))

async def remove_from_queue(uid:int):
    await run_db("DELETE FROM search_queue WHERE user_id = ?", (uid,))

async def pop_queue_candidate(sex_filter:str, prefer_vip=True):
    rows = await run_db("SELECT user_id FROM search_queue WHERE sex_filter = ? ORDER BY queued_at ASC", (sex_filter,), fetch=True)
    if not rows:
        return None
    candidate_ids = [r[0] for r in rows]
    if prefer_vip:
        for cid in candidate_ids:
            u = await get_user(cid)
            if u and u['vip'] and not u['partner'] and not u['banned']:
                await remove_from_queue(cid)
                return cid
    # fallback to first suitable
    for cid in candidate_ids:
        u = await get_user(cid)
        if u and not u['partner'] and not u['banned']:
            await remove_from_queue(cid)
            return cid
    return None

async def save_history(user_id:int, direction:str, content:str):
    content = sanitize_text(content, max_len=2000)
    await run_db("INSERT INTO history (user_id, direction, content, created_at) VALUES (?, ?, ?, ?)",
                 (user_id, direction, content, time.time()))
    rows = await run_db("SELECT id FROM history WHERE user_id = ? ORDER BY id DESC LIMIT 51", (user_id,), fetch=True)
    if rows and len(rows) > 50:
        keep_ids = [r[0] for r in rows[:50]]
        min_keep = min(keep_ids)
        await run_db("DELETE FROM history WHERE user_id = ? AND id < ?", (user_id, min_keep))

async def get_history(user_id:int, limit=50):
    rows = await run_db("SELECT direction, content, created_at FROM history WHERE user_id = ? ORDER BY id DESC LIMIT ?",
                        (user_id, limit), fetch=True)
    return rows or []

async def complain(from_user:int, about_user:int, reason:str):
    await run_db("INSERT INTO complaints (from_user, about_user, reason, created_at) VALUES (?, ?, ?, ?)",
                 (from_user, about_user, sanitize_text(reason, 500), time.time()))
    try:
        await bot.send_message(ADMIN_ID, f"⚠️ Жалоба: от <a href='tg://user?id={from_user}'>{from_user}</a> на <a href='tg://user?id={about_user}'>{about_user}</a>\nПричина: {sanitize_text(reason,300)}", parse_mode='HTML')
    except Exception as e:
        logger.exception("Can't notify admin of complaint: %s", e)

# ---------------------------
# Keyboards
# ---------------------------
def kb_main():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add("Поиск собеседника🔎", "Сменить пол✏️")
    kb.add("Новый собеседник♻️", "Закончить диалог❌")
    kb.add("Профиль👤", "Пожаловаться🚨")
    return kb

def kb_profile():
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(InlineKeyboardButton("Изменить пол", callback_data="edit_sex"),
           InlineKeyboardButton("Возраст", callback_data="edit_age"))
    kb.add(InlineKeyboardButton("Интересы", callback_data="edit_interests"),
           InlineKeyboardButton("Стать VIP", callback_data="become_vip"))
    return kb

def kb_dialog():
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("Следующий собеседник♻️", callback_data="next_partner"))
    kb.add(InlineKeyboardButton("Завершить чат❌", callback_data="end_chat"))
    kb.add(InlineKeyboardButton("Пожаловаться🚨", callback_data="complain_partner"))
    return kb

def kb_choose_sex():
    kb = InlineKeyboardMarkup(row_width=3)
    kb.add(InlineKeyboardButton("Мужчина", callback_data="choise_sex_Мужчина"),
           InlineKeyboardButton("Женщина", callback_data="choise_sex_Женщина"),
           InlineKeyboardButton("Любой", callback_data="choise_sex_Любой"))
    return kb

# ---------------------------
# Handlers
# ---------------------------
@dp.message_handler(commands=['start'])
async def start_handler(message: Message):
    await ensure_user_record(message.from_user)
    await message.answer("<b>💻 Главное меню</b>", reply_markup=kb_main())

@dp.message_handler(lambda m: m.text and m.text.strip() == 'Поиск собеседника🔎')
async def choose_sex(message: Message):
    u = await get_user(message.from_user.id)
    if u is None:
        await ensure_user_record(message.from_user)
        u = await get_user(message.from_user.id)
    if u.get('banned'):
        await message.answer("⛔ Вы заблокированы.")
        return
    await message.answer("❓ Кого будем искать?", reply_markup=kb_choose_sex())

@dp.callback_query_handler(lambda c: c.data and c.data.startswith("choise_sex_"))
async def on_choose_sex(callback: CallbackQuery):
    uid = callback.from_user.id
    sex = callback.data.split('_', 2)[2]
    await remove_from_queue(uid)
    await ensure_user_record(callback.from_user)
    await add_to_queue(uid, sex)
    await bot.answer_callback_query(callback.id, "Вы добавлены в очередь. Ждите собеседника...")
    # Попробуем найти кандидата (сначала same filter, потом 'Любой')
    candidate = await pop_queue_candidate(sex_filter=sex, prefer_vip=True)
    if candidate is None:
        candidate = await pop_queue_candidate(sex_filter='Любой', prefer_vip=True)
    if candidate is None:
        await bot.send_message(uid, "⏳ Поиск собеседника... Ожидание.", reply_markup=ReplyKeyboardRemove())
        return
    # проверяем валидность кандидата
    cand_user = await get_user(candidate)
    if cand_user is None or cand_user.get('banned') or cand_user.get('partner'):
        # пробуем снова (простая стратегия)
        await on_choose_sex(callback)
        return
    await set_partner(uid, candidate)
    await bot.send_message(uid, "✅ Собеседник найден. Общайтесь!", reply_markup=kb_dialog())
    await bot.send_message(candidate, "✅ Собеседник найден. Общайтесь!", reply_markup=kb_dialog())

@dp.callback_query_handler(lambda c: c.data in ('next_partner','end_chat','complain_partner'))
async def dialog_controls(callback: CallbackQuery):
    uid = callback.from_user.id
    data = callback.data
    user = await get_user(uid)
    if not user or not user.get('partner'):
        await bot.answer_callback_query(callback.id, "Вы сейчас не в чате.")
        return
    partner = user['partner']
    if data == 'end_chat':
        await clear_partner(uid)
        await clear_partner(partner)
        await bot.send_message(uid, "❌ Диалог окончен", reply_markup=kb_main())
        try:
            await bot.send_message(partner, "❌ Диалог окончен", reply_markup=kb_main())
        except Exception:
            pass
        await bot.answer_callback_query(callback.id)
    elif data == 'next_partner':
        await clear_partner(uid)
        await clear_partner(partner)
        await bot.send_message(partner, "❌ Диалог окончен", reply_markup=kb_main())
        await add_to_queue(uid, 'Любой')
        await bot.send_message(uid, "Ищем нового собеседника...", reply_markup=ReplyKeyboardRemove())
        await bot.answer_callback_query(callback.id)
    elif data == 'complain_partner':
        await complain(uid, partner, "Жалоба через кнопку")
        await bot.answer_callback_query(callback.id, "Жалоба отправлена админу. Спасибо.")

@dp.message_handler(content_types=types.ContentType.ANY)
async def message_router(message: Message):
    uid = message.from_user.id
    if rate_limited(uid):
        await message.answer("⛔ Вы отправляете сообщения слишком быстро. Подождите немного.")
        return
    await ensure_user_record(message.from_user)
    u = await get_user(uid)
    if u.get('banned'):
        await message.answer("⛔ Вы заблокированы.")
        return
    partner = u.get('partner')
    if not partner:
        # Обработка кнопок/текста вне диалога
        if message.text:
            txt = message.text.strip().lower()
            if txt in ('профиль👤','/profile'):
                await message.answer("Ваш профиль:", reply_markup=kb_profile())
                return
            if txt in ('пожаловаться🚨','/complain'):
                await message.answer("Напишите id пользователя или опишите проблему.")
                return
        return
    # В диалоге — пересылаем
    try:
        p = await get_user(partner)
        if not p or p.get('banned'):
            await message.answer("⛔ Ваш собеседник недоступен. Диалог завершён.")
            await clear_partner(uid)
            return
        if message.content_type == 'text':
            await save_history(uid, 'out', message.text)
            await save_history(partner, 'in', message.text)
            await bot.send_message(partner, message.text)
        elif message.content_type == 'photo':
            file_id = message.photo[-1].file_id
            await save_history(uid, 'out', '[photo]')
            await bot.send_photo(partner, file_id, caption=sanitize_text(message.caption or ""))
        elif message.content_type == 'voice':
            if getattr(message.voice, 'file_size', 0) and message.voice.file_size > MAX_FILE_SIZE:
                await message.answer("Файл слишком большой.")
                return
            fid = message.voice.file_id
            await save_history(uid, 'out', '[voice]')
            await bot.send_voice(partner, fid)
        elif message.content_type == 'document':
            doc = message.document
            name = doc.file_name or ""
            ext = os.path.splitext(name)[1].lower()
            if getattr(doc, 'file_size', 0) and doc.file_size > MAX_FILE_SIZE:
                await message.answer("Файл слишком большой.")
                return
            if ext not in ALLOWED_DOCUMENT_EXT:
                await message.answer("Неподдерживаемый тип файла.")
                return
            await save_history(uid, 'out', f'[document:{name}]')
            await bot.send_document(partner, doc.file_id)
        elif message.content_type == 'sticker':
            await save_history(uid, 'out', '[sticker]')
            await bot.send_sticker(partner, message.sticker.file_id)
        elif message.content_type == 'video':
            if getattr(message.video, 'file_size', 0) and message.video.file_size > MAX_FILE_SIZE:
                await message.answer("Файл слишком большой.")
                return
            await save_history(uid, 'out', '[video]')
            await bot.send_video(partner, message.video.file_id)
        else:
            await save_history(uid, 'out', f"[{message.content_type}]")
            try:
                await message.forward(partner)
            except Exception:
                await message.answer("Не удалось переслать это сообщение.")
    except exceptions.BotBlocked:
        await clear_partner(uid)
        await message.answer("❌ Ваш собеседник заблокировал бота; диалог завершён.")
    except Exception as e:
        logger.exception("Ошибка при пересылке сообщения: %s", e)
        await message.answer("Произошла ошибка при отправке сообщения.")

# ---------------------------
# Профиль и настройки
# ---------------------------
@dp.callback_query_handler(lambda c: c.data == 'edit_sex')
async def edit_sex_cb(callback: CallbackQuery):
    await bot.edit_message_text(chat_id=callback.from_user.id, message_id=callback.message.message_id,
                                text="Выберите пол", reply_markup=kb_choose_sex())

@dp.callback_query_handler(lambda c: c.data == 'become_vip')
async def become_vip(callback: CallbackQuery):
    uid = callback.from_user.id
    await run_db("UPDATE users SET vip = 1 WHERE id = ?", (uid,))
    await bot.answer_callback_query(callback.id, "Вы стали VIP (демо).")
    await bot.send_message(uid, "⭐ Вы теперь VIP!")

@dp.callback_query_handler(lambda c: c.data == 'edit_age')
async def edit_age_cb(callback: CallbackQuery):
    await bot.send_message(callback.from_user.id, "Напишите ваш возраст (числом).")

@dp.message_handler(lambda m: m.text and m.text.isdigit() and 5 <= int(m.text) <= 120)
async def set_age_handler(message: Message):
    age = int(message.text)
    await run_db("UPDATE users SET age = ? WHERE id = ?", (age, message.from_user.id))
    await message.answer(f"Возраст обновлён: {age}")

@dp.message_handler(lambda m: m.text and m.text.startswith('/setinterests '))
async def set_interests_cmd(message: Message):
    data = message.text.replace('/setinterests ', '', 1).strip()
    interests = ','.join([sanitize_text(s.strip(), 50) for s in data.split(',') if s.strip()])
    await run_db("UPDATE users SET interests = ? WHERE id = ?", (interests, message.from_user.id))
    await message.answer("Интересы обновлены.")

# ---------------------------
# Админ: декоратор и команды
# ---------------------------
def admin_only(handler):
    @wraps(handler)
    async def wrapper(message: Message):
        if not is_admin(message.from_user.id):
            await message.answer("Нет доступа.")
            return
        return await handler(message)
    return wrapper

@dp.message_handler(commands=['stats'])
@admin_only
async def cmd_stats(message: Message):
    rows = await run_db("SELECT COUNT(*) FROM users", fetch=True)
    total = rows[0][0] if rows else 0
    vip_rows = await run_db("SELECT COUNT(*) FROM users WHERE vip = 1", fetch=True)
    vip = vip_rows[0][0] if vip_rows else 0
    banned_rows = await run_db("SELECT COUNT(*) FROM users WHERE banned = 1", fetch=True)
    banned = banned_rows[0][0] if banned_rows else 0
    q_rows = await run_db("SELECT COUNT(*) FROM search_queue", fetch=True)
    queued = q_rows[0][0] if q_rows else 0
    await message.answer(f"👥 Пользователей: {total}\n⭐ VIP: {vip}\n⛔ Заблокировано: {banned}\n⏳ В очереди: {queued}")

@dp.message_handler(commands=['broadcast'])
@admin_only
async def cmd_broadcast(message: Message):
    parts = message.text.split(' ', 1)
    if len(parts) < 2:
        await message.answer("Использование: /broadcast текст")
        return
    text = parts[1]
    rows = await run_db("SELECT id FROM users", fetch=True)
    sent = 0
    for r in rows:
        uid = r[0]
        try:
            await bot.send_message(uid, f"📢 Админ: {text}")
            sent += 1
            await asyncio.sleep(0.05)
        except Exception:
            continue
    await message.answer(f"Отправлено {sent} сообщений.")

@dp.message_handler(commands=['ban'])
@admin_only
async def cmd_ban(message: Message):
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("Использование: /ban <user_id>")
        return
    try:
        uid = int(parts[1])
        await run_db("UPDATE users SET banned = 1 WHERE id = ?", (uid,))
        try:
            await bot.send_message(uid, "⛔ Вы заблокированы администратором.")
        except Exception:
            pass
        await message.answer("OK")
    except Exception as e:
        await message.answer("Ошибка: " + str(e))

@dp.message_handler(commands=['unban'])
@admin_only
async def cmd_unban(message: Message):
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("Использование: /unban <user_id>")
        return
    uid = int(parts[1])
    await run_db("UPDATE users SET banned = 0 WHERE id = ?", (uid,))
    await message.answer("OK")

@dp.message_handler(commands=['promote'])
@admin_only
async def cmd_promote(message: Message):
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("Использование: /promote <user_id>")
        return
    uid = int(parts[1])
    await run_db("UPDATE users SET vip = 1 WHERE id = ?", (uid,))
    try:
        await bot.send_message(uid, "⭐ Вам выдан VIP (администратор).")
    except Exception:
        pass
    await message.answer("OK")

@dp.message_handler(commands=['demote'])
@admin_only
async def cmd_demote(message: Message):
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("Использование: /demote <user_id>")
        return
    uid = int(parts[1])
    await run_db("UPDATE users SET vip = 0 WHERE id = ?", (uid,))
    try:
        await bot.send_message(uid, "⭐ VIP снят.")
    except Exception:
        pass
    await message.answer("OK")

# ---------------------------
# Обработка ошибок
# ---------------------------
async def except_handler(update, exception):
    try:
        uid = None
        if hasattr(update, 'message') and update.message:
            uid = update.message.chat.id
            name = update.message.chat.first_name
        else:
            name = "unknown"
        text = f"⛔ Произошла ошибка у пользователя {uid}\n\n<code>{exception}</code>"
        if ADMIN_ID:
            try:
                await bot.send_message(ADMIN_ID, text, parse_mode='HTML')
            except Exception:
                logger.exception("Не удалось отправить ошибку админу.")
        logger.exception("Error in update: %s", exception)
    except Exception as e:
        logger.exception("Failed to notify admin: %s", e)

dp.register_errors_handler(except_handler)

# ---------------------------
# Запуск
# ---------------------------
if __name__ == '__main__':
    logger.info("Bot starting...")
    start_polling(dp, skip_updates=True)
