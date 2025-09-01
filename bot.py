import os
import asyncio
import time
import csv
import hashlib
import secrets
import logging
import re
from logging.handlers import RotatingFileHandler
from typing import Dict

from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ChatType
from aiogram.filters import Command, CommandStart
from aiogram.filters.command import CommandObject
from aiogram.types import Message, CallbackQuery, ChatMemberUpdated
from aiogram.utils.keyboard import ReplyKeyboardBuilder, InlineKeyboardBuilder

from db import DB
from utils import extract_text_and_media, extract_male_ids
from i18n import t


# ========= ENV & LOGGING =========
load_dotenv()

BOT_TOKEN    = os.getenv("BOT_TOKEN")
OWNER_ID     = int(os.getenv("OWNER_ID", "0"))
LANG_DEFAULT = os.getenv("LANG", "ru")
DB_PATH      = os.getenv("DB_PATH", "./bot.db")

LOG_FILE         = os.getenv("LOG_FILE", "bot.log")
LOG_LEVEL        = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_MAX_BYTES    = int(os.getenv("LOG_MAX_BYTES", "5242880"))
LOG_BACKUP_COUNT = int(os.getenv("LOG_BACKUP_COUNT", "5"))

# PUBLIC_OPEN flag
PUBLIC_OPEN  = os.getenv("PUBLIC_OPEN", "0") == "1"

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        RotatingFileHandler(LOG_FILE, maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUP_COUNT, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# ========= DB & BOT =========
db = DB(DB_PATH)
if OWNER_ID:
    db.add_admin(OWNER_ID)
    db.add_allowed_user(OWNER_ID, username_lc="owner", added_by=OWNER_ID, credits=10**9)

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp  = Dispatcher()


# ========= ACCESS HELPERS =========
def is_superadmin(user_id: int) -> bool:
    return user_id == OWNER_ID

def is_admin(user_id: int) -> bool:
    return is_superadmin(user_id) or db.is_admin(user_id)

def is_allowed_user(user_id: int) -> bool:
    if PUBLIC_OPEN:
        return True
    return is_admin(user_id) or db.is_allowed_user(user_id)

def lang_for(user_id: int) -> str:
    row = db.conn.execute("SELECT lang FROM users WHERE user_id=?", (user_id,)).fetchone()
    if row and row["lang"] in ("ru", "uk"):
        return row["lang"]
    return LANG_DEFAULT


# ========= SIMPLE NAV (без FSM) =========
NAV_STATE: Dict[int, str] = {}
NAV_STACK: Dict[int, list] = {}

def nav_set(uid: int, state: str):
    NAV_STATE[uid] = state

def nav_push(uid: int, state: str):
    stack = NAV_STACK.get(uid, [])
    stack.append(NAV_STATE.get(uid, "root"))
    NAV_STACK[uid] = stack
    NAV_STATE[uid] = state

def nav_back(uid: int) -> str:
    stack = NAV_STACK.get(uid, [])
    if stack:
        prev = stack.pop()
        NAV_STACK[uid] = stack
        NAV_STATE[uid] = prev
        return prev
    NAV_STATE[uid] = "root"
    return "root"


# ========= REPORT FLOW (минимальный стейт) =========
# stage: None | "wait_female" | "wait_text"
REPORT_STATE: Dict[int, Dict] = {}


# ========= KEYBOARDS =========
def kb_main(uid: int):
    # Поиск → Добавить отчёт → Админ → Мои запросы → Язык
    lang = lang_for(uid)
    kb = ReplyKeyboardBuilder()
    kb.button(text=t(lang, "menu_search"))
    kb.button(text="➕ Добавить отчёт")
    if is_admin(uid):
        kb.button(text=t(lang, "menu_admin"))
    kb.button(text=t(lang, "menu_mine"))
    kb.button(text=t(lang, "menu_lang"))
    kb.adjust(2, 2, 1)
    return kb.as_markup(resize_keyboard=True)

def kb_admin(uid: int):
    # как в core-2: «👤 Админы» видно всем админам
    kb = ReplyKeyboardBuilder()
    if not PUBLIC_OPEN:
        kb.button(text="👥 Пользователи")
    kb.button(text="👤 Админы")
    kb.button(text="💬 Чаты")
    kb.button(text="📊 Статистика")
    kb.button(text="💾 Экспорт")
    kb.button(text="⬅️ Назад")
    kb.adjust(2, 2, 2)
    return kb.as_markup(resize_keyboard=True)

def kb_admin_users(uid: int):
    kb = ReplyKeyboardBuilder()
    kb.button(text="➕ Добавить пользователя")
    kb.button(text="➖ Удалить пользователя")
    kb.button(text="⬅️ Назад")
    kb.adjust(2, 1)
    return kb.as_markup(resize_keyboard=True)

def kb_admin_admins(uid: int):
    kb = ReplyKeyboardBuilder()
    kb.button(text="➕ Добавить админа")
    kb.button(text="➖ Удалить админа")
    kb.button(text="⬅️ Назад")
    kb.adjust(2, 1)
    return kb.as_markup(resize_keyboard=True)

def kb_admin_chats(uid: int):
    # Только добавить чат + назад
    kb = ReplyKeyboardBuilder()
    kb.button(text=t(lang_for(uid), "admin_add_chat"))
    kb.button(text="⬅️ Назад")
    kb.adjust(1, 1)
    return kb.as_markup(resize_keyboard=True)

def kb_admin_exports(uid: int):
    lang = lang_for(uid)
    kb = ReplyKeyboardBuilder()
    # Только суперадмину: экспорт по женскому ID и полный экспорт
    if is_superadmin(uid):
        kb.button(text=t(lang, "export_male"))
        kb.button(text=t(lang, "export_female"))
        kb.button(text=t(lang, "export_all"))
    kb.button(text=t(lang, "export_stats"))
    kb.button(text="⬅️ Назад")
    kb.adjust(2, 2)
    return kb.as_markup(resize_keyboard=True)

def kb_admin_stats(uid: int):
    lang = lang_for(uid)
    kb = ReplyKeyboardBuilder()
    kb.button(text=t(lang, "stats_my_chats"))
    kb.button(text=t(lang, "stats_my_users"))
    if is_superadmin(uid):
        kb.button(text=t(lang, "stats_all_chats"))
        kb.button(text=t(lang, "stats_all_users"))
    kb.button(text="⬅️ Назад")
    kb.adjust(2, 2, 1)
    return kb.as_markup(resize_keyboard=True)

async def show_menu(message: Message, state: str):
    uid = message.from_user.id
    if state == "root":
        await message.answer(t(lang_for(uid), "start"), reply_markup=kb_main(uid))
    elif state == "admin":
        await message.answer(t(lang_for(uid), "admin_menu"), reply_markup=kb_admin(uid))
    elif state == "admin.users":
        await message.answer("Управление пользователями", reply_markup=kb_admin_users(uid))
    elif state == "admin.admins":
        await message.answer("Управление администраторами", reply_markup=kb_admin_admins(uid))
        if not is_superadmin(uid):
            await message.answer("Только суперадмин может управлять администраторами.")
    elif state == "admin.chats":
        await message.answer("Управление чатами", reply_markup=kb_admin_chats(uid))
    elif state == "admin.exports":
        await message.answer(t(lang_for(uid), "export_menu"), reply_markup=kb_admin_exports(uid))
    else:
        await message.answer(t(lang_for(uid), "start"), reply_markup=kb_main(uid))


# ========= START / LANGUAGE =========
@dp.message(CommandStart())
async def start(message: Message, command: CommandObject):
    uid = message.from_user.id
    # upsert профиль
    db.conn.execute(
        """
        INSERT INTO users(user_id, first_name, last_name, username, lang)
        VALUES(?,?,?,?,?)
        ON CONFLICT(user_id) DO UPDATE SET
            first_name=excluded.first_name,
            last_name=excluded.last_name,
            username=excluded.username,
            updated_at=CURRENT_TIMESTAMP
        """,
        (
            uid,
            message.from_user.first_name or "",
            message.from_user.last_name or "",
            message.from_user.username or "",
            None,
        )
    )
    db.conn.commit()

    # Автоактивация по резерву username
    if not is_allowed_user(uid) and message.from_user.username:
        uname_lc = (message.from_user.username or "").lower()
        if hasattr(db, "consume_reserved_username") and db.consume_reserved_username(uname_lc):
            db.add_allowed_user(uid, uname_lc, added_by=0, credits=100)
            db.log_audit(uid, "accept_reserved_username", target=uname_lc, details="")

    nav_set(uid, "root")
    await message.answer(t(lang_for(uid), "start"), reply_markup=kb_main(uid))

@dp.message(F.text.in_({"⚙️ Админ", "⚙️ Адмін"}))
@dp.message(Command("admin"))
async def admin_entry(message: Message):
    uid = message.from_user.id
    if not is_admin(uid):
        await message.answer(t(lang_for(uid), "admin_only"))
        return
    nav_push(uid, "admin")
    await show_menu(message, "admin")

@dp.message(F.text.func(lambda s: isinstance(s, str) and ("Язык" in s or "Мова" in s)))
async def switch_lang(message: Message):
    uid = message.from_user.id
    cur = lang_for(uid)
    new = "uk" if cur == "ru" else "ru"
    db.conn.execute(
        """
        INSERT INTO users(user_id, lang) VALUES(?,?)
        ON CONFLICT(user_id) DO UPDATE SET lang=excluded.lang, updated_at=CURRENT_TIMESTAMP
        """,
        (uid, new)
    )
    db.conn.commit()
    await message.answer(t(new, "menu_lang_set"), reply_markup=kb_main(uid))


# ========= MAIN MENU ACTIONS =========
@dp.message(F.text.func(lambda s: isinstance(s, str) and ("Поиск по ID" in s or "Пошук за ID" in s)))
async def action_search_prompt(message: Message):
    await message.answer(t(lang_for(message.from_user.id), "search_enter_id"))

@dp.message(F.text.func(lambda s: isinstance(s, str) and ("Мои запросы" in s or "Мої запити" in s)))
async def my_queries(message: Message):
    uid = message.from_user.id
    logs = db.get_user_searches(uid, 10)
    lines = [f"{r['created_at']} • {r['query_type']} • {r['query_value']}" for r in logs]
    if not lines:
        await message.answer("—")
    else:
        credits_msg = ""
        if not is_admin(uid):
            credits = db.get_user_credits(uid)
            credits_msg = "\n" + t(lang_for(uid), "credits_left", credits=credits)
        await message.answer("\n".join(lines) + credits_msg)


# ========= REPORT: UI =========
@dp.message(F.text == "➕ Добавить отчёт")
async def report_start(message: Message):
    uid = message.from_user.id
    if not is_allowed_user(uid):
        if not PUBLIC_OPEN:
            await message.answer(t(lang_for(uid), "not_authorized"))
            return
    REPORT_STATE[uid] = {"stage": "wait_female"}
    await message.answer("Введите 10-значный идентификатор девушки (из названия группы).")

@dp.message(F.text == "⬅️ Назад")
async def back_button(message: Message):
    uid = message.from_user.id
    # сбрасываем возможный режим отчёта
    if uid in REPORT_STATE:
        REPORT_STATE.pop(uid, None)
    state = nav_back(uid)
    await show_menu(message, state)

# ==== ВАЖНО: точечные хендлеры отчёта (не ловят всё подряд) ====

# 1) Ждём женский ID (ровно 10 цифр), только если stage == "wait_female"
@dp.message(
    F.text.regexp(r"^\d{10}$") &
    F.func(lambda m: REPORT_STATE.get(m.from_user.id, {}).get("stage") == "wait_female")
)
async def report_wait_female(message: Message):
    uid = message.from_user.id
    fid = message.text.strip()

    row = db.conn.execute(
        "SELECT chat_id, title FROM allowed_chats WHERE female_id=? ORDER BY added_at DESC LIMIT 1",
        (fid,)
    ).fetchone()
    if not row:
        REPORT_STATE.pop(uid, None)
        await message.answer("Группа с таким женским ID не найдена или не авторизована.")
        return

    REPORT_STATE[uid] = {"stage": "wait_text", "chat_id": row["chat_id"], "female_id": fid, "title": row["title"]}
    await message.answer(f"Ок. Напишите текст отчёта одним сообщением — я отправлю его в «{row['title']}».")
    return

# 2) Ждём текст отчёта, только если stage == "wait_text"
@dp.message(
    F.text &
    F.func(lambda m: REPORT_STATE.get(m.from_user.id, {}).get("stage") == "wait_text")
)
async def report_wait_text(message: Message):
    uid = message.from_user.id
    st = REPORT_STATE.get(uid) or {}
    chat_id = st.get("chat_id")
    female_id = st.get("female_id")
    title = st.get("title") or ""

    text = (message.text or "").strip()
    if not text:
        await message.answer("Пустой отчёт не принимаю. Напишите текст отчёта.")
        return

    signer = f"@{message.from_user.username}" if message.from_user.username else f"id:{uid}"
    out_text = f"Отчёт от {signer}:\n\n{text}"

    sent = await bot.send_message(chat_id=chat_id, text=out_text)

    male_ids = extract_male_ids(out_text)
    msg_db_id = db.save_message(
        chat_id=chat_id,
        message_id=sent.message_id,
        sender_id=uid,
        sender_username=message.from_user.username or None,
        sender_first_name=message.from_user.first_name or None,
        date=sent.date.timestamp(),
        text=out_text,
        media_type="text",
        file_id="",
        is_forward=0,
    )
    db.link_male_ids(msg_db_id, male_ids)
    db.add_credits(uid, 1)
    db.log_audit(uid, "report_send", target=female_id, details=f"chat_id={chat_id}")

    REPORT_STATE.pop(uid, None)
    await message.answer(f"Отчёт отправлен в «{title}». Спасибо!")

# ========= ADMIN MENUS =========
@dp.message(F.text == "👥 Пользователи")
async def admin_users_menu(message: Message):
    uid = message.from_user.id
    if not is_admin(uid): return
    nav_push(uid, "admin.users")
    await show_menu(message, "admin.users")

@dp.message(F.text == "👤 Админы")
async def admin_admins_menu(message: Message):
    uid = message.from_user.id
    if not is_admin(uid): return  # открыть могут все админы
    nav_push(uid, "admin.admins")
    await show_menu(message, "admin.admins")

@dp.message(F.text == "💬 Чаты")
async def admin_chats_menu(message: Message):
    uid = message.from_user.id
    if not is_admin(uid): return
    nav_push(uid, "admin.chats")
    await show_menu(message, "admin.chats")

@dp.message(F.text == "📊 Статистика")
async def admin_stats_menu(message: Message):
    uid = message.from_user.id
    if not is_admin(uid):
        return
    nav_push(uid, "admin.stats")
    men, msgs, chats, females = db.count_stats()
    await message.answer(t(lang_for(uid), "stats", men=men, msgs=msgs, chats=chats, females=females))
    await message.answer(t(lang_for(uid), "stats_menu"), reply_markup=kb_admin_stats(uid))

@dp.message(F.text == "💾 Экспорт")
async def admin_exports_menu(message: Message):
    uid = message.from_user.id
    if not is_admin(uid): return
    nav_push(uid, "admin.exports")
    await show_menu(message, "admin.exports")

# Guards: restrict certain exports to superadmin only
@dp.message(F.text.in_({t("ru", "export_all"), t("uk", "export_all")}))
async def guard_export_all(message: Message):
    uid = message.from_user.id
    if not is_superadmin(uid):
        await message.answer(t(lang_for(uid), "superadmin_only"))
        return

@dp.message(F.text.in_({t("ru", "export_female"), t("uk", "export_female")}))
async def guard_export_female(message: Message):
    uid = message.from_user.id
    if not is_superadmin(uid):
        await message.answer(t(lang_for(uid), "superadmin_only"))
        return

@dp.message(F.text.in_({t("ru", "export_male"), t("uk", "export_male")}))
async def guard_export_male(message: Message):
    uid = message.from_user.id
    if not is_superadmin(uid):
        await message.answer(t(lang_for(uid), "superadmin_only"))
        return

# ======== STATS SUBACTIONS ========
@dp.message(F.text.in_({t("ru", "stats_my_chats"), t("uk", "stats_my_chats")}))
async def stats_my_chats(message: Message):
    uid = message.from_user.id
    if not is_admin(uid):
        return
    rows = db.list_chats_by_admin(uid)
    lang = lang_for(uid)
    header = t(lang, "stats_my_chats_header", count=len(rows))
    if not rows:
        await message.answer(header)
        return
    lines = [header]
    for r in rows[:50]:
        title = r["title"] or "(no title)"
        fid = r["female_id"] or "?"
        lines.append(f"• {title} (fid:{fid}) — {r['chat_id']}")
    await message.answer("\n".join(lines))

@dp.message(F.text.in_({t("ru", "stats_my_users"), t("uk", "stats_my_users")}))
async def stats_my_users(message: Message):
    uid = message.from_user.id
    if not is_admin(uid):
        return
    rows = db.list_users_by_admin(uid)
    lang = lang_for(uid)
    header = t(lang, "stats_my_users_header", count=len(rows))
    if not rows:
        await message.answer(header)
        return
    lines = [header]
    for r in rows[:100]:
        uname = r["username"] or r["username_lc"] or ""
        disp = f"@{uname}" if uname else f"id:{r['user_id']}"
        lines.append(f"• {disp} (credits:{r['credits']})")
    await message.answer("\n".join(lines))

@dp.message(F.text.in_({t("ru", "stats_all_chats"), t("uk", "stats_all_chats")}))
async def stats_all_chats(message: Message):
    uid = message.from_user.id
    if not is_superadmin(uid):
        await message.answer(t(lang_for(uid), "superadmin_only"))
        return
    admins = db.list_admins()
    if not admins:
        await message.answer("—")
        return
    chunks = []
    for a in admins:
        aid = a["user_id"]
        aname = (f"@{a['username']}" if a["username"] else (a["first_name"] or "")) or str(aid)
        block_head = t(lang_for(uid), "stats_admin_block", admin=aname, id=aid)
        rows = db.list_chats_by_admin(aid)
        lines = [block_head, f"Всего: {len(rows)}"]
        for r in rows[:30]:
            title = r["title"] or "(no title)"
            fid = r["female_id"] or "?"
            lines.append(f"• {title} (fid:{fid}) — {r['chat_id']}")
        chunks.append("\n".join(lines))
    await message.answer("\n\n".join(chunks))

@dp.message(F.text.in_({t("ru", "stats_all_users"), t("uk", "stats_all_users")}))
async def stats_all_users(message: Message):
    uid = message.from_user.id
    if not is_superadmin(uid):
        await message.answer(t(lang_for(uid), "superadmin_only"))
        return
    admins = db.list_admins()
    if not admins:
        await message.answer("—")
        return
    chunks = []
    for a in admins:
        aid = a["user_id"]
        aname = (f"@{a['username']}" if a["username"] else (a["first_name"] or "")) or str(aid)
        block_head = t(lang_for(uid), "stats_admin_block", admin=aname, id=aid)
        rows = db.list_users_by_admin(aid)
        lines = [block_head, f"Всего: {len(rows)}"]
        for r in rows[:60]:
            uname = r["username"] or r["username_lc"] or ""
            disp = f"@{uname}" if uname else f"id:{r['user_id']}"
            lines.append(f"• {disp} (credits:{r['credits']})")
        chunks.append("\n".join(lines))
    await message.answer("\n\n".join(chunks))


# ========= ADMIN ACTIONS =========
ADM_PENDING: Dict[int, str] = {}

# --- Пользователи
@dp.message(F.text == "➕ Добавить пользователя")
async def ask_add_user(message: Message):
    uid = message.from_user.id
    if not is_admin(uid): return
    ADM_PENDING[uid] = "add_user"
    await message.answer(t(lang_for(uid), "prompt_user_id"))
    await message.answer("Можете также прислать @username — добавим по нику и активируем, когда пользователь нажмёт /start.")

@dp.message(F.text == "➖ Удалить пользователя")
async def ask_del_user(message: Message):
    uid = message.from_user.id
    if not is_admin(uid): return
    ADM_PENDING[uid] = "del_user"
    await message.answer(t(lang_for(uid), "prompt_user_id"))

# --- Админы (видно всем админам; выполнять может только супер)
@dp.message(F.text == "➕ Добавить админа")
async def ask_add_admin(message: Message):
    uid = message.from_user.id
    if not is_superadmin(uid):
        await message.answer("Только суперадмин может управлять администраторами.")
        return
    ADM_PENDING[uid] = "add_admin"
    await message.answer(t(lang_for(uid), "prompt_user_id"))

@dp.message(F.text == "➖ Удалить админа")
async def ask_del_admin(message: Message):
    uid = message.from_user.id
    if not is_superadmin(uid):
        await message.answer("Только суперадмин может управлять администраторами.")
        return
    ADM_PENDING[uid] = "del_admin"
    await message.answer(t(lang_for(uid), "prompt_user_id"))

# Принять id:123...
@dp.message(F.text.regexp(r"^id:(\d{6,12})$"))
async def handle_admin_input(message: Message):
    uid = message.from_user.id
    action = ADM_PENDING.pop(uid, None)
    if not action:
        return
    target_id_str = message.text.split(":", 1)[1]
    try:
        target_id = int(target_id_str)
    except ValueError:
        await message.answer("Bad ID")
        return

    if action == "add_admin":
        if not is_superadmin(uid):
            await message.answer("Только суперадмин может управлять администраторами.")
            return
        db.add_admin(target_id)
        db.log_audit(uid, "add_admin", target=str(target_id), details="")
        await message.answer("Админ добавлен.")
    elif action == "del_admin":
        if not is_superadmin(uid):
            await message.answer("Только суперадмин может управлять администраторами.")
            return
        db.remove_admin(target_id)
        db.log_audit(uid, "remove_admin", target=str(target_id), details="")
        await message.answer("Админ удалён.")
    elif action == "add_user":
        if not is_admin(uid): return
        db.add_allowed_user(target_id, username_lc="", added_by=uid, credits=100)
        db.log_audit(uid, "add_user", target=str(target_id), details=f"by={uid}")
        await message.answer("Пользователь добавлен (100 кредитов).")
    elif action == "del_user":
        if not is_admin(uid): return
        db.remove_allowed_user(target_id)
        db.log_audit(uid, "remove_user", target=str(target_id), details=f"by={uid}")
        await message.answer("Пользователь удалён.")
    else:
        await message.answer("OK")

# Принять @username для add_user:
@dp.message(F.text.regexp(r"^@[A-Za-z0-9_]{5,32}$"))
async def handle_add_user_by_username(message: Message):
    uid = message.from_user.id
    if not is_admin(uid):
        return
    action = ADM_PENDING.get(uid)
    if action != "add_user":
        return

    uname_lc_with_at = message.text.strip().lower()
    uname_clean = uname_lc_with_at.lstrip("@")
    uname_lc = uname_clean.lower()

    # 1) если пользователь уже нажимал /start ранее — он есть в таблице users
    row = db.conn.execute(
        "SELECT user_id FROM users WHERE lower(username)=?",
        (uname_lc,)
    ).fetchone()
    if row:
        user_id_found = row["user_id"]
        db.add_allowed_user(user_id_found, uname_lc, added_by=uid, credits=100)
        db.log_audit(uid, "add_user_by_username_immediate", target=f"{uname_lc}({user_id_found})", details="")
        await message.answer("Пользователь найден по нику и добавлен (100 кредитов). Готов пользоваться поиском.")
        return

    # 2) иначе — резервируем ник, активируем при первом /start
    if hasattr(db, "reserve_username") and db.reserve_username(uname_lc, added_by=uid):
        db.log_audit(uid, "reserve_username", target=uname_lc, details="")
        await message.answer("Резерв по @username создан. Как только пользователь нажмёт /start у бота — доступ активируется и будет выдано 100 кредитов.")
    else:
        await message.answer("Не удалось создать резерв: возможно, уже существует или пользователь уже добавлен.")


# ========= CHATS =========
@dp.message(F.text.func(lambda s: isinstance(s, str) and ("Добавить чат" in s or "Додати чат" in s)))
async def add_chat_hint(message: Message):
    uid = message.from_user.id
    if not is_admin(uid):
        await message.answer(t(lang_for(uid), "add_chat_admins_only"))
        return
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    secret = "".join(secrets.choice(alphabet) for _ in range(8))
    secret_hash = hashlib.sha256(secret.encode()).hexdigest()
    db.save_auth_secret(secret_hash, created_by=uid)
    logger.info(f"Generated auth secret for user {uid}")
    await message.answer(t(lang_for(uid), "auth_secret_dm", secret=secret), parse_mode="HTML")

@dp.message(Command("authorize"))
async def authorize_group(message: Message):
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return
    uid = message.from_user.id
    lang = lang_for(uid)
    if not is_admin(uid):
        await message.reply(t(lang, "add_chat_admins_only"))
        return
    parts = message.text.strip().split(maxsplit=1)
    if len(parts) < 2:
        await message.reply(t(lang, "authorize_need_token"))
        return
    secret = parts[1].strip()
    secret_hash = hashlib.sha256(secret.encode()).hexdigest()
    row = db.pop_auth_secret(secret_hash)
    if not row:
        await message.reply(t(lang, "authorize_bad_or_expired"))
        return
    member = await bot.get_chat_member(message.chat.id, uid)
    if member.status not in ("administrator", "creator"):
        await message.reply(t(lang, "authorize_need_admin"))
        return
    title = message.chat.title or ""
    female_id = db.get_female_id_from_title(title) or "НЕИЗВЕСТНО"
    db.add_allowed_chat(message.chat.id, title, female_id, uid)
    db.log_audit(uid, "authorize_chat", target=str(message.chat.id), details=f"female_id={female_id}")
    await message.reply(t(lang, "authorize_ok", fid=female_id))

@dp.message(Command("unauthorize"))
async def unauthorize_group(message: Message):
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return
    uid = message.from_user.id
    lang = lang_for(uid)
    if not is_superadmin(uid):
        await message.reply(t(lang, "unauthorize_only_superadmin"))
        return
    db.remove_allowed_chat(message.chat.id)
    db.log_audit(uid, "unauthorize_chat", target=str(message.chat.id), details="")
    await message.reply(t(lang, "unauthorize_ok"))


# ========= SEARCH (10 цифр) =========
@dp.message(F.text.regexp(r"^\d{10}$"))
async def handle_male_search(message: Message):
    uid = message.from_user.id
    # если в режиме отчёта — не обрабатываем как поиск
    st = REPORT_STATE.get(uid)
    if st and st.get("stage") in {"wait_female", "wait_text"}:
        return

    lang = lang_for(uid)

    banned_until = db.get_user_ban(uid)
    now_ts = int(time.time())
    if banned_until and now_ts < banned_until:
        until_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(banned_until))
        await message.answer(t(lang, "banned", until=until_str))
        return
    if not db.rate_limit_allowed(uid, now_ts):
        await message.answer(t(lang, "rate_limited"))
        return
    if not is_allowed_user(uid):
        if not PUBLIC_OPEN:
            await message.answer(t(lang, "not_authorized"))
            return
    if not is_admin(uid):
        if not PUBLIC_OPEN:
            credits = db.get_user_credits(uid)
            if credits <= 0:
                await message.answer(t(lang, "no_credits"))
                return
            db.reduce_credits(uid, 1)

    male = message.text.strip()
    db.log_search(uid, "male", male)

    # автобан (не для админов)
    ts_ago = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now_ts - 60))
    row = db.conn.execute(
        "SELECT COUNT(*) AS c FROM searches WHERE user_id=? AND query_type='male' AND created_at > ?",
        (uid, ts_ago)
    ).fetchone()
    if row and row["c"] is not None and row["c"] >= 30 and not is_admin(uid):
        banned_until_ts = now_ts + 900
        db.set_user_ban(uid, banned_until_ts)
        until_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(banned_until_ts))
        await message.answer(t(lang, "banned", until=until_str))
        return

    await send_results(message, male, 0)

async def send_results(message: Message, male_id: str, offset: int):
    uid = message.from_user.id
    lang = lang_for(uid)
    total = db.count_by_male(male_id)
    rows  = db.search_by_male(male_id, limit=5, offset=offset)
    if total == 0:
        await message.answer(t(lang, "search_not_found"))
        return
    for row in rows:
        try:
            await bot.copy_message(
                chat_id=uid,
                from_chat_id=row["chat_id"],
                message_id=row["message_id"]
            )
        except Exception:
            await message.answer(row["text"] or "(no text)")
    new_offset = offset + 5
    if new_offset < total:
        kb = InlineKeyboardBuilder()
        kb.button(text=t(lang, "more"), callback_data=f"more:{male_id}:{new_offset}")
        await message.answer(f"{min(new_offset, total)}/{total}", reply_markup=kb.as_markup())
    else:
        await message.answer(f"{total}/{total}")

@dp.callback_query(F.data.startswith("more:"))
async def cb_more(call: CallbackQuery):
    try:
        _, male_id, off = call.data.split(":", 2)
        await send_results(call.message, male_id, int(off))
    finally:
        await call.answer("")


# ========= GROUP LISTENERS =========
@dp.my_chat_member()
async def on_bot_added(event: ChatMemberUpdated):
    # Auto-authorize chat when the bot is added to a group
    try:
        if event.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
            return
        old_status = getattr(event.old_chat_member, "status", None)
        new_status = getattr(event.new_chat_member, "status", None)
        # Added if transitioned from left/kicked to member/administrator
        if old_status in {"left", "kicked", None} and new_status in {"member", "administrator"}:
            inviter_id = event.from_user.id if event.from_user else 0
            title = event.chat.title or ""
            female_id = db.get_female_id_from_title(title) or "НЕИЗВЕСТНО"
            db.add_allowed_chat(event.chat.id, title, female_id, inviter_id)
            db.log_audit(inviter_id, "auto_authorize_chat_on_add", target=str(event.chat.id), details=f"female_id={female_id}")
            # Notify the chat
            lang = lang_for(inviter_id)
            try:
                await bot.send_message(event.chat.id, t(lang, "authorize_ok", fid=female_id))
            except Exception:
                pass
    except Exception as e:
        logger.exception(f"Failed to auto-authorize chat on add: {e}")

@dp.message(F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}))
async def on_group_message(message: Message):
    if db.get_allowed_chat(message.chat.id) is None:
        return
    text, media_type, file_id, is_forward = extract_text_and_media(message)
    if not text:
        return
    male_ids = extract_male_ids(text)
    if not male_ids:
        return
    msg_db_id = db.save_message(
        chat_id=message.chat.id,
        message_id=message.message_id,
        sender_id=message.from_user.id if message.from_user else None,
        sender_username=message.from_user.username if message.from_user else None,
        sender_first_name=message.from_user.first_name if message.from_user else None,
        date=message.date.timestamp(),
        text=text,
        media_type=media_type,
        file_id=file_id,
        is_forward=is_forward,
    )
    db.link_male_ids(msg_db_id, male_ids)
    if message.from_user and not is_admin(message.from_user.id):
        db.add_credits(message.from_user.id, 1)

@dp.edited_message(F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}))
async def on_group_edited(message: Message):
    if db.get_allowed_chat(message.chat.id) is None:
        return
    text, media_type, file_id, is_forward = extract_text_and_media(message)
    row = db.conn.execute(
        "SELECT id FROM messages WHERE chat_id=? AND message_id=?",
        (message.chat.id, message.message_id)
    ).fetchone()
    if not row:
        return
    msg_db_id = row["id"]
    db.update_message_text(message.chat.id, message.message_id, text or "")
    db.unlink_all_male_ids(msg_db_id)
    male_ids = extract_male_ids(text or "")
    db.link_male_ids(msg_db_id, male_ids)


# ========= MAIN =========
async def main():
    logger.info("Bot starting...")
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
