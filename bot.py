import os
import asyncio
import time
import csv
import hashlib
import secrets
import logging
import re
import html
from logging.handlers import RotatingFileHandler
from typing import Dict, Optional

from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ChatType
from aiogram.filters import Command, CommandStart
from aiogram.filters.command import CommandObject
from aiogram.types import Message, CallbackQuery, ChatMemberUpdated, ReplyKeyboardRemove, KeyboardButton
from aiogram.utils.keyboard import ReplyKeyboardBuilder, InlineKeyboardBuilder

from db import DB
from utils import extract_text_and_media, extract_male_ids, highlight_id
from i18n import t


# ========= ENV & LOGGING =========
load_dotenv()

BOT_TOKEN    = os.getenv("BOT_TOKEN")
OWNER_ID     = int(os.getenv("OWNER_ID", "0"))
OWNER_IDS_RAW = os.getenv("OWNER_IDS", "")
ENV_SUPERADMINS = set()
if OWNER_ID:
    ENV_SUPERADMINS.add(OWNER_ID)
for token in OWNER_IDS_RAW.split(","):
    token = token.strip()
    if not token:
        continue
    try:
        sid = int(token)
    except ValueError:
        continue
    if sid:
        ENV_SUPERADMINS.add(sid)
SUPERADMINS = set()
BOT_USERNAME = os.getenv("BOT_USERNAME", "")
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
for sid in ENV_SUPERADMINS:
    db.add_superadmin(sid, added_by=OWNER_ID or sid)
    username_label = f"owner_{sid}"
    db.add_allowed_user(sid, username_lc=username_label, added_by=sid, credits=10**9)

def refresh_superadmins():
    global SUPERADMINS
    SUPERADMINS.clear()
    for sid in db.list_superadmins():
        SUPERADMINS.add(sid)

refresh_superadmins()

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp  = Dispatcher()


# ========= ACCESS HELPERS =========
def is_superadmin(user_id: int) -> bool:
    return user_id in SUPERADMINS

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

# ========= MALE SEARCH FILTER STATE =========
MALE_SEARCH_STATE: Dict[int, Dict] = {}
TIME_FILTER_CHOICES = ["all", "24h"]
TIME_FILTER_SECONDS = {
    "24h": 24 * 3600,
}

REPORT_LOOKUP_WINDOW = 24 * 3600
REPORT_LOOKUP_PAGE = 5

# ========= LEGEND FLOW =========
LEGEND_STATE: Dict[int, Dict] = {}
LEGEND_HASHTAG = "#легенда"

# ========= USER LEGEND VIEW =========
LEGEND_VIEW_STATE: Dict[int, Dict] = {}

def legend_deep_link(female_id: str) -> Optional[str]:
    if not female_id or not BOT_USERNAME:
        return None
    return f"https://t.me/{BOT_USERNAME}?start=legend_{female_id}"

def format_legend_text(body: str, female_id: Optional[str] = None, lang: Optional[str] = None) -> str:
    clean = (body or "").strip()
    if not clean.lower().startswith(LEGEND_HASHTAG):
        clean = f"{LEGEND_HASHTAG}\n{clean}" if clean else LEGEND_HASHTAG
    link = legend_deep_link(female_id)
    if link:
        link_text = t(lang or LANG_DEFAULT, "legend_view_link")
        anchor = f'<a href="{link}">{link_text}</a>'
        if anchor not in clean:
            clean = f"{clean}\n\n{anchor}"
    return clean

async def process_legend_from_chat(message: Message, text: str):
    chat = db.get_allowed_chat(message.chat.id)
    if not chat:
        return
    female_id = chat["female_id"]
    if not female_id:
        return
    lang = lang_for(chat["added_by"] or OWNER_ID)
    prepared = format_legend_text(text, female_id, lang)
    db.upsert_female_legend(female_id, message.chat.id, prepared, message.message_id)
    db.track_legend_message(female_id, message.chat.id, message.message_id, prepared)

def time_filter_label(lang: str, time_filter: str) -> str:
    mapping = {
        "all": t(lang, "filter_period_all"),
        "24h": t(lang, "filter_period_24h"),
    }
    return mapping.get(time_filter, mapping["all"])

def female_filter_label(lang: str, female_id: Optional[str]) -> str:
    if not female_id:
        return t(lang, "filter_female_all")
    title = db.get_female_title(female_id) or ""
    if title:
        return f"{title} ({female_id})"
    return female_id

def time_filter_since(time_filter: str) -> Optional[float]:
    seconds = TIME_FILTER_SECONDS.get(time_filter)
    if not seconds:
        return None
    return time.time() - seconds


# ========= KEYBOARDS =========
def private_reply_markup(message: Message, markup):
    """Return reply keyboard only in private chats; remove it elsewhere."""
    if message.chat.type == ChatType.PRIVATE:
        return markup
    return ReplyKeyboardRemove()

def kb_main(uid: int):
    # Поиск → Добавить отчёт → Админ → Мои запросы → Язык
    lang = lang_for(uid)
    kb = ReplyKeyboardBuilder()
    if is_admin(uid) or db.is_allowed_user(uid):
        kb.button(text=t(lang, "menu_search"))
    kb.button(text="➕ Добавить отчёт")
    kb.button(text=t(lang, "menu_legend_view"))
    kb.button(text=t(lang, "menu_extra"))
    if is_admin(uid):
        kb.button(text=t(lang, "menu_admin_panel"))
    kb.adjust(2, 2, 1, 1)
    return kb.as_markup(resize_keyboard=True)

def kb_extra(uid: int):
    lang = lang_for(uid)
    limited_user = (not is_admin(uid)) and (not db.is_allowed_user(uid))
    kb = ReplyKeyboardBuilder()
    kb.button(text=t(lang, "menu_lang"))
    if limited_user:
        kb.button(text=t(lang, "menu_support"))
    kb.button(text="⬅️ Назад")
    kb.adjust(1, 1, 1)
    return kb.as_markup(resize_keyboard=True)

def kb_admin(uid: int):
    kb = ReplyKeyboardBuilder()
    row = [KeyboardButton(text="👥 Пользователи")]
    if is_superadmin(uid):
        row.append(KeyboardButton(text=t(lang_for(uid), "menu_superadmin_panel")))
    kb.row(*row)
    kb.row(KeyboardButton(text="💬 Чаты"))
    kb.row(KeyboardButton(text="⬅️ Назад"))
    return kb.as_markup(resize_keyboard=True)

def kb_admin_legend(uid: int):
    kb = ReplyKeyboardBuilder()
    kb.button(text="➕ Добавить легенду")
    kb.button(text="✏️ Редактировать легенду")
    kb.button(text="⬅️ Назад")
    kb.adjust(1, 1, 1)
    return kb.as_markup(resize_keyboard=True)

def kb_admin_users(uid: int):
    kb = ReplyKeyboardBuilder()
    kb.button(text="➕ Добавить пользователя")
    kb.button(text="📂 Мои пользователи")
    kb.button(text="⬅️ Назад")
    kb.adjust(1, 1, 1)
    return kb.as_markup(resize_keyboard=True)

def kb_admin_admins(uid: int):
    kb = ReplyKeyboardBuilder()
    kb.button(text="➕ Добавить админа")
    if is_superadmin(uid):
        kb.button(text="Все админы")
        kb.button(text="Лимиты гостей")
    if uid == OWNER_ID:
        kb.button(text="⚙️ Суперадмины")
    kb.button(text="⬅️ Назад")
    kb.adjust(2, 1, 1)
    return kb.as_markup(resize_keyboard=True)

def kb_admin_chats(uid: int):
    # Только добавить чат + назад
    kb = ReplyKeyboardBuilder()
    kb.button(text="📂 Мои чаты")
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
        await message.answer(
            t(lang_for(uid), "start"),
            reply_markup=private_reply_markup(message, kb_main(uid)),
        )
    elif state == "admin":
        await message.answer(
            t(lang_for(uid), "admin_menu"),
            reply_markup=private_reply_markup(message, kb_admin(uid)),
        )
    elif state == "admin.users":
        await message.answer(
            "Управление пользователями",
            reply_markup=private_reply_markup(message, kb_admin_users(uid)),
        )
    elif state == "admin.admins":
        await message.answer(
            "Управление администраторами",
            reply_markup=private_reply_markup(message, kb_admin_admins(uid)),
        )
        if not is_superadmin(uid):
            await message.answer("Только суперадмин может управлять администраторами.")
    elif state == "admin.chats":
        await message.answer(
            "Управление чатами\nДобавьте бота в нужный чат, что бы связать чат с ботом.",
            reply_markup=private_reply_markup(message, kb_admin_chats(uid)),
        )
    elif state == "admin.legend":
        await message.answer(
            "Легенда: выберите действие.",
            reply_markup=private_reply_markup(message, kb_admin_legend(uid)),
        )
    elif state == "admin.exports":
        await message.answer(
            t(lang_for(uid), "export_menu"),
            reply_markup=private_reply_markup(message, kb_admin_exports(uid)),
        )
    elif state == "extra":
        # Build and show user status inside the extra menu
        lang = lang_for(uid)
        is_admin_flag = is_admin(uid)
        is_allowed_flag = db.is_allowed_user(uid)
        role = ""
        access = ""
        if is_superadmin(uid):
            role = "Суперадмин" if lang == "ru" else "Суперадмін"
            access = "есть" if lang == "ru" else "є"
        elif is_admin_flag:
            role = "Админ" if lang == "ru" else "Адмін"
            access = "есть" if lang == "ru" else "є"
        elif not is_allowed_flag:
            role = t(lang, "limited_status")
            access = t(lang, "limited_access")
        else:
            role = "Пользователь" if lang == "ru" else "Користувач"
            access = "есть" if lang == "ru" else "є"
        credits_line = ""
        banned_line = ""
        banned_until = db.get_user_ban(uid)
        if banned_until:
            until_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(banned_until))
            banned_line = ("\nБлокировка до: " if lang == "ru" else "\nБлокування до: ") + until_str
        status_title = t(lang, "extra_title")
        # Show used/left quotas (для ограниченных — по настраиваемым лимитам; для остальных — used и ∞)
        now_ts = int(time.time())
        cutoff = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now_ts - 24*3600))
        row_s = db.conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM searches
            WHERE user_id=?
              AND query_type IN ('male', 'report_female')
              AND created_at > ?
            """,
            (uid, cutoff)
        ).fetchone()
        used_search = (row_s["c"] if row_s and row_s["c"] is not None else 0)
        row_r = db.conn.execute(
            "SELECT COUNT(*) AS c FROM audit_log WHERE actor_id=? AND action='report_send' AND ts > ?",
            (uid, cutoff)
        ).fetchone()
        used_reports = (row_r["c"] if row_r and row_r["c"] is not None else 0)
        if not is_admin_flag and not is_allowed_flag:
            limit_s = db.get_setting_int('guest_limit_search', 50)
            limit_r = db.get_setting_int('guest_limit_report', 5)
            left_s, left_r = max(0, limit_s - used_search), max(0, limit_r - used_reports)
        else:
            limit_s = limit_r = "∞"
            left_s = left_r = "∞"
        quota_lines = (
            "\n" + t(lang, "limited_search_used", used=used_search, limit=limit_s)
            + "\n" + t(lang, "limited_report_used", used=used_reports, limit=limit_r)
        )
        id_line = "\n" + t(lang, "extra_your_id", id=uid)
        status = f"{status_title}\nСтатус: {role}\nДоступ: {access}{banned_line}{quota_lines}{id_line}"
        await message.answer(status, reply_markup=private_reply_markup(message, kb_extra(uid)))
    else:
        await message.answer(
            t(lang_for(uid), "start"),
            reply_markup=private_reply_markup(message, kb_main(uid)),
        )


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
    await message.answer(
        t(lang_for(uid), "start"),
        reply_markup=private_reply_markup(message, kb_main(uid)),
    )
    payload = (command.args or "").strip() if command else ""
    if payload:
        await handle_start_payload(message, payload)

async def handle_start_payload(message: Message, payload: str):
    payload = (payload or "").strip()
    if payload.lower().startswith("legend_"):
        female_id = payload.split("_", 1)[1] if "_" in payload else ""
        if re.fullmatch(r"\d{10}", female_id):
            await send_report_lookup_results(message.chat.id, message.from_user.id, female_id, 0)

@dp.message(F.text.in_({t("ru", "menu_admin_panel"), t("uk", "menu_admin_panel")}))
@dp.message(Command("admin"))
async def admin_entry(message: Message):
    uid = message.from_user.id
    if not is_admin(uid):
        await message.answer(t(lang_for(uid), "admin_only"))
        return
    nav_push(uid, "admin")
    await show_menu(message, "admin")

## (removed) separate superadmin panel entry via main menu button

@dp.message(F.text.in_({t("ru", "menu_lang"), t("uk", "menu_lang")}))
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
    await message.answer(
        t(new, "menu_lang_set"),
        reply_markup=private_reply_markup(message, kb_main(uid)),
    )


# ========= MAIN MENU ACTIONS =========
@dp.message(F.text.func(lambda s: isinstance(s, str) and ("Поиск по ID" in s or "Пошук за ID" in s)))
async def action_search_prompt(message: Message):
    uid = message.from_user.id
    # Прерываем режим отчёта, если он был активен
    if REPORT_STATE.get(uid):
        REPORT_STATE.pop(uid, None)
    await message.answer(t(lang_for(uid), "search_enter_id"))

@dp.message(F.text.in_({t("ru", "menu_legend_view"), t("uk", "menu_legend_view")}))
async def legend_view_start(message: Message):
    uid = message.from_user.id
    LEGEND_VIEW_STATE[uid] = {"stage": "wait_female"}
    await message.answer(t(lang_for(uid), "legend_view_prompt"))

@dp.message(F.text.in_({t("ru", "menu_support"), t("uk", "menu_support")}))
async def support_info(message: Message):
    await message.answer(t(lang_for(message.from_user.id), "support_text"))

@dp.message(F.text.in_({t("ru", "menu_extra"), t("uk", "menu_extra")}))
async def extra_menu(message: Message):
    uid = message.from_user.id
    nav_push(uid, "extra")
    await show_menu(message, "extra")

## Removed: "Мои запросы" feature and handler

## Removed: отдельная кнопка показа Telegram ID (ID теперь в блоке Информация)


# ========= REPORT: UI =========
@dp.message(F.text == "➕ Добавить отчёт")
async def report_start(message: Message):
    uid = message.from_user.id
    # Разрешаем запуск отчёта всем: для ограниченных лимит проверяется в следующем шаге
    REPORT_STATE[uid] = {"stage": "wait_female"}
    await message.answer("Введите 10-значный идентификатор девушки (из названия группы).")

@dp.message(
    F.text.regexp(r"^\d{10}$") &
    F.func(lambda m: LEGEND_VIEW_STATE.get(m.from_user.id, {}).get("stage") == "wait_female")
)
async def legend_view_wait_female(message: Message):
    uid = message.from_user.id
    lang = lang_for(uid)
    female_id = message.text.strip()
    now_ts = int(time.time())
    if not is_admin(uid) and not db.is_allowed_user(uid):
        ts_ago_24h = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now_ts - 24*3600))
        row_q = db.conn.execute(
            "SELECT COUNT(*) AS c FROM searches WHERE user_id=? AND query_type='legend_view' AND created_at > ?",
            (uid, ts_ago_24h)
        ).fetchone()
        lim_leg = db.get_setting_int('guest_limit_legend', 10)
        if row_q and row_q["c"] is not None and row_q["c"] >= lim_leg:
            LEGEND_VIEW_STATE.pop(uid, None)
            await message.answer(t(lang, "legend_view_limit", limit=lim_leg))
            return
    legend = db.get_female_legend(female_id)
    if not legend:
        LEGEND_VIEW_STATE.pop(uid, None)
        await message.answer(t(lang, "legend_view_not_found", fid=female_id))
        return
    row = db.conn.execute(
        "SELECT title FROM allowed_chats WHERE female_id=? ORDER BY added_at DESC LIMIT 1",
        (female_id,)
    ).fetchone()
    title = (row["title"] if row else "") or female_id
    db.log_search(uid, "legend_view", female_id)
    text = format_legend_text(legend["content"], female_id, lang)
    LEGEND_VIEW_STATE.pop(uid, None)
    await message.answer(f"{t(lang, 'legend_view_title', title=title)}\n\n{text}")

@dp.message(F.text == "⬅️ Назад")
async def back_button(message: Message):
    uid = message.from_user.id
    # сбрасываем возможный режим отчёта
    REPORT_STATE.pop(uid, None)
    LEGEND_STATE.pop(uid, None)
    LEGEND_VIEW_STATE.pop(uid, None)
    MALE_SEARCH_STATE.pop(uid, None)
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

    # Restricted guests: daily limit (configured) for reports
    if not is_admin(uid) and not db.is_allowed_user(uid):
        now_ts = int(time.time())
        ts_ago_24h = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now_ts - 24*3600))
        row_q = db.conn.execute(
            "SELECT COUNT(*) AS c FROM audit_log WHERE actor_id=? AND action='report_send' AND ts > ?",
            (uid, ts_ago_24h)
        ).fetchone()
        lim_r = db.get_setting_int('guest_limit_report', 5)
        if row_q and row_q["c"] is not None and row_q["c"] >= lim_r:
            await message.answer(t(lang_for(uid), "limited_report_quota", limit=lim_r))
            REPORT_STATE.pop(uid, None)
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
    # credits removed
    db.log_audit(uid, "report_send", target=female_id, details=f"chat_id={chat_id}")

    REPORT_STATE.pop(uid, None)
    await message.answer(f"Отчёт отправлен в «{title}». Спасибо!")

# ========= ADMIN MENUS =========
@dp.message(F.text == "Легенда")
async def admin_legend_menu(message: Message):
    uid = message.from_user.id
    if not is_admin(uid):
        return
    LEGEND_STATE.pop(uid, None)
    nav_push(uid, "admin.legend")
    await show_menu(message, "admin.legend")

@dp.message(F.text == "➕ Добавить легенду")
async def legend_add_prompt(message: Message):
    uid = message.from_user.id
    if not is_admin(uid):
        return
    LEGEND_STATE[uid] = {"mode": "add", "stage": "wait_female"}
    await message.answer(
        "Введите 10-значный женский ID, для которого нужно добавить легенду.",
        reply_markup=private_reply_markup(message, kb_admin_legend(uid)),
    )

@dp.message(F.text == "✏️ Редактировать легенду")
async def legend_edit_prompt(message: Message):
    uid = message.from_user.id
    if not is_admin(uid):
        return
    LEGEND_STATE[uid] = {"mode": "edit", "stage": "wait_female"}
    await message.answer(
        "Введите 10-значный женский ID, чтобы редактировать легенду.",
        reply_markup=private_reply_markup(message, kb_admin_legend(uid)),
    )

@dp.message(
    F.text.regexp(r"^\d{10}$") &
    F.func(lambda m: LEGEND_STATE.get(m.from_user.id, {}).get("stage") == "wait_female")
)
async def legend_wait_female(message: Message):
    uid = message.from_user.id
    if not is_admin(uid):
        return
    st = LEGEND_STATE.get(uid) or {}
    mode = st.get("mode")
    female_id = message.text.strip()
    if not mode:
        LEGEND_STATE.pop(uid, None)
        await message.answer("Состояние не определено. Нажмите «Легенда» ещё раз.")
        return
    chat_row = db.conn.execute(
        "SELECT chat_id, title FROM allowed_chats WHERE female_id=? ORDER BY added_at DESC LIMIT 1",
        (female_id,)
    ).fetchone()
    if not chat_row:
        await message.answer("Для этой девушки не найден авторизованный чат. Добавьте чат и попробуйте снова.")
        return
    legend_row = db.get_female_legend(female_id)
    if mode == "add" and legend_row:
        await message.answer("Легенда для этой девушки уже существует. Используйте режим редактирования.")
        return
    if mode == "edit" and not legend_row:
        await message.answer("Легенда ещё не создана. Сначала добавьте её через режим добавления.")
        return
    LEGEND_STATE[uid] = {
        "mode": mode,
        "stage": "wait_text",
        "female_id": female_id,
        "chat_id": chat_row["chat_id"],
        "chat_title": chat_row["title"] or "",
        "previous_content": (legend_row["content"] if legend_row else ""),
    }
    title = chat_row["title"] or f"id:{chat_row['chat_id']}"
    if mode == "edit" and legend_row:
        preview = (legend_row["content"] or "").strip()
        if len(preview) > 1500:
            preview = preview[:1500] + "…"
        await message.answer(
            f"Текущий текст легенды для {female_id}:\n\n{preview or '(пусто)'}\n\nОтправьте новый текст одним сообщением.",
            reply_markup=private_reply_markup(message, kb_admin_legend(uid)),
        )
    else:
        await message.answer(
            f"Чат «{title}» найден. Отправьте текст легенды одним сообщением.",
            reply_markup=private_reply_markup(message, kb_admin_legend(uid)),
        )

@dp.message(
    F.text &
    F.func(lambda m: LEGEND_STATE.get(m.from_user.id, {}).get("stage") == "wait_text")
)
async def legend_wait_text(message: Message):
    uid = message.from_user.id
    if not is_admin(uid):
        return
    st = LEGEND_STATE.get(uid) or {}
    body = (message.text or "").strip()
    if not body:
        await message.answer("Пустой текст легенды не принимаю. Отправьте содержимое ещё раз.")
        return
    chat_id = st.get("chat_id")
    female_id = st.get("female_id")
    mode = st.get("mode")
    if not chat_id or not female_id:
        LEGEND_STATE.pop(uid, None)
        await message.answer("Не удалось определить чат. Начните заново через «Легенда».")
        return
    previous_content = (st.get("previous_content") or "").strip()
    if mode == "edit" and body == previous_content:
        await message.answer("Текст не изменился. Введите другой вариант или нажмите «⬅️ Назад».")
        return
    prepared_text = format_legend_text(body, female_id, lang_for(uid))
    try:
        sent = await bot.send_message(chat_id=chat_id, text=prepared_text, disable_web_page_preview=True)
    except Exception as exc:
        logger.exception("Не удалось отправить легенду для %s: %s", female_id, exc)
        await message.answer("Не удалось отправить сообщение в группу. Проверьте, что бот админ и не заблокирован.")
        return
    db.upsert_female_legend(female_id, chat_id, body, sent.message_id)
    db.log_audit(uid, "legend_add" if mode == "add" else "legend_edit", target=female_id, details=f"chat_id={chat_id}")
    LEGEND_STATE.pop(uid, None)
    title = st.get("chat_title") or f"id:{chat_id}"
    status = "добавлена" if mode == "add" else "обновлена"
    response_text = f"Легенда для {female_id} {status} и отправлена в «{title}»."
    await message.answer(
        response_text,
        reply_markup=private_reply_markup(message, kb_admin_legend(uid)),
    )

@dp.message(
    F.func(lambda m: MALE_SEARCH_STATE.get(m.from_user.id, {}).get("stage") in {"wait_female_filter", "wait_female_manual"})
)
async def male_search_wait_female_filter(message: Message):
    uid = message.from_user.id
    lang = lang_for(uid)
    state = MALE_SEARCH_STATE.get(uid)
    if not state:
        return
    stage = state.get("stage")
    if stage not in {"wait_female_filter", "wait_female_manual"}:
        return
    text = (message.text or "").strip()
    female_filter = None
    if text and re.fullmatch(r"\d{10}", text):
        female_filter = text
    state["female_filter"] = female_filter
    if stage == "wait_female_filter":
        state["stage"] = "wait_period_filter"
        await message.answer(t(lang, "male_filter_prompt_period"), reply_markup=build_period_prompt_kb(state["male_id"], lang))
    else:
        state["stage"] = None
        time_filter = state.get("time_filter", "all")
        await send_results(message, state["male_id"], 0, user_id=uid, female_filter=female_filter, time_filter=time_filter)

@dp.message(F.text == "👥 Пользователи")
async def admin_users_menu(message: Message):
    uid = message.from_user.id
    if not is_admin(uid): return
    nav_push(uid, "admin.users")
    await show_menu(message, "admin.users")
    # Also show quick entry to "Мои пользователи"
    await message.answer("Выберите действие или откройте 📂 Мои пользователи.")

@dp.message(F.text == "📂 Мои пользователи")
async def show_my_users(message: Message):
    uid = message.from_user.id
    if not is_admin(uid):
        return
    await _close_prev_paged(uid)
    kb, total, page = build_my_users_kb(uid, page=0)
    caption = f"Ваши пользователи: {total}" if lang_for(uid) == "ru" else f"Ваші користувачі: {total}"
    sent = await message.answer(caption, reply_markup=kb)
    PAGED_MSG[uid] = sent.message_id

@dp.message(F.text.in_({"👑 Панель суперадмина", "👤 Админы", "👑 Панель суперадміна"}))
async def admin_admins_menu(message: Message):
    uid = message.from_user.id
    if not is_superadmin(uid):
        await message.answer("Только суперадмин может управлять администраторами.")
        return
    nav_push(uid, "admin.admins")
    await show_menu(message, "admin.admins")

@dp.message(F.text == "Лимиты гостей")
async def guest_limits_menu(message: Message):
    uid = message.from_user.id
    if not is_superadmin(uid):
        await message.answer("Только суперадмин может менять лимиты.")
        return
    ls = db.get_setting_int('guest_limit_search', 50)
    lr = db.get_setting_int('guest_limit_report', 5)
    ll = db.get_setting_int('guest_limit_legend', 10)
    text = (
        "Текущие лимиты для ограниченных пользователей:\n"
        f"• Поиск в сутки: {ls}\n"
        f"• Отчёты в сутки: {lr}\n"
        f"• Легенды в сутки: {ll}\n\n"
        "Выберите действие кнопками ниже или отправьте:\n"
        "поиск: 100 — для лимита поиска\n"
        "отчёты: 10 — для лимита отчётов\n"
        "легенды: 10 — для лимита легенд"
    )
    kb = build_guest_limits_kb(ls, lr, ll)
    await message.answer(text, reply_markup=kb)

@dp.message(F.text.regexp(r"(?i)^\s*(поиск|отч[её]ты|легенд[аы])\s*[:=]\s*(\d{1,4})\s*$"))
async def guest_limits_set(message: Message):
    uid = message.from_user.id
    if not is_superadmin(uid):
        return
    m = re.match(r"(?i)^\s*(поиск|отч[её]ты|легенд[аы])\s*[:=]\s*(\d{1,4})\s*$", message.text.strip())
    if not m:
        return
    kind = m.group(1).lower()
    val = int(m.group(2))
    val = max(0, min(100000, val))
    if kind.startswith("поиск"):
        db.set_setting_int('guest_limit_search', val)
        await message.answer(f"Лимит поиска для ограниченных установлен: {val} в сутки.")
    elif kind.startswith("отч"):
        db.set_setting_int('guest_limit_report', val)
        await message.answer(f"Лимит отчётов для ограниченных установлен: {val} в сутки.")
    else:
        db.set_setting_int('guest_limit_legend', val)
        await message.answer(f"Лимит легенд для ограниченных установлен: {val} в сутки.")

@dp.callback_query(F.data.regexp(r"^gl([srl]):(noop|[+\-]\d+)$"))
async def cb_guest_limits_delta(call: CallbackQuery):
    try:
        _, tail = call.data.split(":", 1)
    except Exception:
        await call.answer("")
        return
    kind = call.data[2]  # 's', 'r', or 'l'
    op = tail
    uid = call.from_user.id
    if not is_superadmin(uid):
        await call.answer("Нет прав", show_alert=True)
        return
    if kind == 's':
        key = 'guest_limit_search'
        default = 50
    elif kind == 'r':
        key = 'guest_limit_report'
        default = 5
    else:
        key = 'guest_limit_legend'
        default = 10
    cur = db.get_setting_int(key, default)
    if op == 'noop':
        await call.answer("")
        return
    try:
        delta = int(op)
    except Exception:
        delta = 0
    new_val = max(0, min(100000, cur + delta))
    db.set_setting_int(key, new_val)
    ls = db.get_setting_int('guest_limit_search', 50)
    lr = db.get_setting_int('guest_limit_report', 5)
    ll = db.get_setting_int('guest_limit_legend', 10)
    text = (
        "Текущие лимиты для ограниченных пользователей:\n"
        f"• Поиск в сутки: {ls}\n"
        f"• Отчёты в сутки: {lr}\n"
        f"• Легенды в сутки: {ll}\n\n"
        "Выберите действие кнопками ниже или отправьте:\n"
        "поиск: 100 — для лимита поиска\n"
        "отчёты: 10 — для лимита отчётов\n"
        "легенды: 10 — для лимита легенд"
    )
    kb = build_guest_limits_kb(ls, lr, ll)
    try:
        await call.message.edit_text(text, reply_markup=kb)
    except Exception:
        try:
            await call.message.edit_reply_markup(reply_markup=kb)
        except Exception:
            pass
    await call.answer("Сохранено")

@dp.callback_query(F.data == "gl:back")
async def cb_guest_limits_back(call: CallbackQuery):
    uid = call.from_user.id
    if not is_superadmin(uid):
        await call.answer("Нет прав", show_alert=True)
        return
    try:
        await call.message.delete()
    except Exception:
        pass
    await call.answer("")
    await bot.send_message(uid, "Управление администраторами", reply_markup=kb_admin_admins(uid))

## (removed) list_all_admins handler and button

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
    await message.answer(
        t(lang_for(uid), "stats_menu"),
        reply_markup=private_reply_markup(message, kb_admin_stats(uid)),
    )

@dp.message(F.text.in_({"💾 Экспорт", "🧩 Дополнительно"}))
async def admin_exports_menu(message: Message):
    uid = message.from_user.id
    if not is_admin(uid): return
    nav_push(uid, "admin.exports")
    # Доп. информация для раздела: общее число отправленных сообщений пользователем
    total_my_msgs = db.count_messages_by_user(uid) if hasattr(db, "count_messages_by_user") else 0
    lines = [
        (f"Отправленных сообщений: {total_my_msgs}" if lang_for(uid) == "ru" else f"Надісланих повідомлень: {total_my_msgs}")
    ]
    if is_admin(uid):
        try:
            users_cnt = db.count_users_by_admin(uid)
            chats_cnt = db.count_chats_by_admin(uid)
            if lang_for(uid) == "ru":
                lines.append(f"Моих пользователей: {users_cnt}")
                lines.append(f"Моих чатов: {chats_cnt}")
            else:
                lines.append(f"Мої користувачі: {users_cnt}")
                lines.append(f"Мої чати: {chats_cnt}")
        except Exception:
            pass
    await message.answer("\n".join(lines))
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
        lines.append(f"• {disp}")
    await message.answer("\n".join(lines))

# Срабатывает ТОЛЬКО когда пользователь в меню статистики
@dp.message(
    F.text.in_({t("ru", "stats_all_chats"), t("uk", "stats_all_chats")}) &
    F.func(lambda m: NAV_STATE.get(m.from_user.id) == "admin.stats")
)
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

@dp.message(
    F.text.in_({t("ru", "stats_all_users"), t("uk", "stats_all_users")}) &
    F.func(lambda m: NAV_STATE.get(m.from_user.id) == "admin.stats")
)
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
        lines.append(f"• {disp}")
        chunks.append("\n".join(lines))
    await message.answer("\n\n".join(chunks))


# ========= ADMIN ACTIONS =========
ADM_PENDING: Dict[int, str] = {}
PAGED_MSG: Dict[int, int] = {}
ADMIN_PICK_MODE: Dict[int, str] = {}
# Сохраняем страницу списка "Все админы", с которой был выбран конкретный админ,
# чтобы уметь возвращаться из разделов админа обратно в его подменю с корректной кнопкой
# "⬅ Список админов" (на нужную страницу).
ADMIN_FROM_PAGE: Dict[int, Dict[int, int]] = {}

async def _close_prev_paged(uid: int):
    msg_id = PAGED_MSG.pop(uid, None)
    if msg_id:
        try:
            await bot.delete_message(uid, msg_id)
        except Exception:
            pass

# ===== Helper: build inline keyboard for listing admin's users
def build_my_users_kb(uid: int, page: int = 0, page_size: int = 10):
    rows = db.list_users_by_admin(uid)
    total = len(rows)
    total_pages = max(1, (total + page_size - 1) // page_size)
    page = min(max(0, page), total_pages - 1)
    start = page * page_size
    end = min(total, start + page_size)

    kb = InlineKeyboardBuilder()
    for r in rows[start:end]:
        uname = r["username"] or r["username_lc"] or ""
        disp = f"@{uname}" if uname else f"id:{r['user_id']}"
        kb.button(text=disp, callback_data=f"mui:{r['user_id']}:{page}")
    prev_page = max(0, page - 1)
    next_page = min(total_pages - 1, page + 1)
    kb.adjust(1)
    nav = InlineKeyboardBuilder()
    nav.button(text="«", callback_data=f"mup:{prev_page}")
    nav.button(text=f"{page+1}/{total_pages}", callback_data=f"mup:{page}")
    nav.button(text="»", callback_data=f"mup:{next_page}")
    kb.row(*nav.buttons)
    close = InlineKeyboardBuilder()
    close.button(text="✖ Закрыть", callback_data="muc:close")
    kb.row(*close.buttons)
    return kb.as_markup(), total, page

# (удалено) Пагинация для раздела удаления чатов — больше не используется

# ===== Helper: build inline keyboard for listing "my chats" with message counts
def build_my_chats_kb(uid: int, page: int = 0, page_size: int = 10):
    rows = db.list_chats_by_admin(uid)
    total = len(rows)
    total_pages = max(1, (total + page_size - 1) // page_size)
    page = min(max(0, page), total_pages - 1)
    start = page * page_size
    end = min(total, start + page_size)

    kb = InlineKeyboardBuilder()
    for r in rows[start:end]:
        title = (r["title"] or "(no title)").strip()
        fid = r["female_id"] or "?"
        text = f"{title} • {fid}"
        if len(text) > 64:
            text = text[:61] + "…"
        kb.button(text=text, callback_data=f"mci:{r['chat_id']}:{page}")
    # Single navigation row + close
    total_pages = max(1, (total + page_size - 1) // page_size)
    prev_page = max(0, page - 1)
    next_page = min(total_pages - 1, page + 1)
    kb.adjust(1)
    nav = InlineKeyboardBuilder()
    nav.button(text="«", callback_data=f"mcp:{prev_page}")
    nav.button(text=f"{page+1}/{total_pages}", callback_data=f"mcp:{page}")
    nav.button(text="»", callback_data=f"mcp:{next_page}")
    kb.row(*nav.buttons)
    close = InlineKeyboardBuilder()
    close.button(text="✖ Закрыть", callback_data="mcc:close")
    kb.row(*close.buttons)
    return kb.as_markup(), total, page

# ===== Helper: list admins for superadmin browse
def build_admins_list_kb(page: int = 0, page_size: int = 10, pick_prefix: str = "admi"):
    admins = db.list_admins()
    total = len(admins)
    total_pages = max(1, (total + page_size - 1) // page_size)
    page = min(max(0, page), total_pages - 1)
    start = page * page_size
    end = min(total, start + page_size)

    kb = InlineKeyboardBuilder()
    for a in admins[start:end]:
        aid = a["user_id"]
        uname = a["username"]
        name = (f"@{uname}" if uname else (a["first_name"] or "")) or str(aid)
        text = f"{name} — id:{aid}"
        if len(text) > 60:
            text = text[:57] + "…"
        kb.button(text=text, callback_data=f"{pick_prefix}:{aid}:{page}")
    prev_page = max(0, page - 1)
    next_page = min(total_pages - 1, page + 1)
    kb.adjust(1)
    nav = InlineKeyboardBuilder()
    nav.button(text="«", callback_data=f"admp:{prev_page}")
    nav.button(text=f"{page+1}/{total_pages}", callback_data=f"admp:{page}")
    nav.button(text="»", callback_data=f"admp:{next_page}")
    kb.row(*nav.buttons)
    # Back to previous submenu (only for pages after the first)
    if page > 0:
        back = InlineKeyboardBuilder()
        back.button(text="⬅ Назад", callback_data="admb:back")
        kb.row(*back.buttons)
    close = InlineKeyboardBuilder()
    close.button(text="✖ Закрыть", callback_data="admc:close")
    kb.row(*close.buttons)
    return kb.as_markup(), total, page

# ===== Helper: list chats for a specific admin (superadmin view)
def build_admin_chats_kb(admin_id: int, page: int = 0, page_size: int = 10):
    rows = db.list_chats_by_admin(admin_id)
    total = len(rows)
    total_pages = max(1, (total + page_size - 1) // page_size)
    page = min(max(0, page), total_pages - 1)
    start = page * page_size
    end = min(total, start + page_size)

    kb = InlineKeyboardBuilder()
    for r in rows[start:end]:
        title = (r["title"] or "(no title)").strip()
        fid = r["female_id"] or "?"
        text = f"{title} • {fid}"
        if len(text) > 64:
            text = text[:61] + "…"
        kb.button(text=text, callback_data=f"adci:{r['chat_id']}:{admin_id}:{page}")
    prev_page = max(0, page - 1)
    next_page = min(total_pages - 1, page + 1)
    kb.adjust(1)
    nav = InlineKeyboardBuilder()
    nav.button(text="«", callback_data=f"adcp:{admin_id}:{prev_page}")
    nav.button(text=f"{page+1}/{total_pages}", callback_data=f"adcp:{admin_id}:{page}")
    nav.button(text="»", callback_data=f"adcp:{admin_id}:{next_page}")
    kb.row(*nav.buttons)
    # Кнопка Назад в подменю выбранного админа
    back = InlineKeyboardBuilder()
    back.button(text="⬅ Назад", callback_data=f"admsb:{admin_id}")
    kb.row(*back.buttons)
    close = InlineKeyboardBuilder()
    close.button(text="✖ Закрыть", callback_data="admc:close")
    kb.row(*close.buttons)
    return kb.as_markup(), total, page

# Users of a given admin (for superadmin view)
def build_admin_users_kb(admin_id: int, page: int = 0, page_size: int = 10):
    rows = db.list_users_by_admin(admin_id)
    total = len(rows)
    total_pages = max(1, (total + page_size - 1) // page_size)
    page = min(max(0, page), total_pages - 1)
    start = page * page_size
    end = min(total, start + page_size)

    kb = InlineKeyboardBuilder()
    for r in rows[start:end]:
        uname = r["username"] or r["username_lc"] or ""
        disp = f"@{uname}" if uname else f"id:{r['user_id']}"
        kb.button(text=disp, callback_data=f"adui:{r['user_id']}:{admin_id}:{page}")
    prev_page = max(0, page - 1)
    next_page = min(total_pages - 1, page + 1)
    kb.adjust(1)
    nav = InlineKeyboardBuilder()
    nav.button(text="«", callback_data=f"adup:{admin_id}:{prev_page}")
    nav.button(text=f"{page+1}/{total_pages}", callback_data=f"adup:{admin_id}:{page}")
    nav.button(text="»", callback_data=f"adup:{admin_id}:{next_page}")
    kb.row(*nav.buttons)
    # Кнопка Назад в подменю выбранного админа
    back = InlineKeyboardBuilder()
    back.button(text="⬅ Назад", callback_data=f"admsb:{admin_id}")
    kb.row(*back.buttons)
    close = InlineKeyboardBuilder()
    close.button(text="✖ Закрыть", callback_data="admc:close")
    kb.row(*close.buttons)
    return kb.as_markup(), total, page

# ===== Helper: keyboard for guest limits editing (superadmin)
def build_guest_limits_kb(limit_search: int, limit_report: int, limit_legend: int):
    kb = InlineKeyboardBuilder()
    # Search limit controls
    kb.button(text=f"Поиск: {limit_search}", callback_data="gls:noop")
    kb.button(text="-10", callback_data="gls:-10")
    kb.button(text="-1", callback_data="gls:-1")
    kb.button(text="+1", callback_data="gls:+1")
    kb.button(text="+10", callback_data="gls:+10")
    kb.adjust(1, 4)
    # Report limit controls
    kb.button(text=f"Отчёты: {limit_report}", callback_data="glr:noop")
    kb.button(text="-10", callback_data="glr:-10")
    kb.button(text="-1", callback_data="glr:-1")
    kb.button(text="+1", callback_data="glr:+1")
    kb.button(text="+10", callback_data="glr:+10")
    kb.adjust(1, 4)
    # Legend limit controls
    kb.button(text=f"Легенды: {limit_legend}", callback_data="gll:noop")
    kb.button(text="-10", callback_data="gll:-10")
    kb.button(text="-1", callback_data="gll:-1")
    kb.button(text="+1", callback_data="gll:+1")
    kb.button(text="+10", callback_data="gll:+10")
    kb.adjust(1, 4)
    # Back
    kb.button(text="⬅ Назад", callback_data="gl:back")
    kb.adjust(1)
    return kb.as_markup()

def build_period_prompt_kb(male_id: str, lang: str):
    kb = InlineKeyboardBuilder()
    for code in TIME_FILTER_CHOICES:
        kb.button(text=time_filter_label(lang, code), callback_data=f"mftime:{male_id}:{code}:init")
    kb.adjust(1)
    return kb.as_markup()

def build_female_prompt_kb(male_id: str, lang: str):
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang, "male_filter_enter_button"), callback_data=f"mffask:{male_id}")
    kb.button(text=t(lang, "male_filter_all_button"), callback_data=f"mfself:{male_id}:-")
    kb.adjust(1)
    return kb.as_markup()

# --- Пользователи
@dp.message(F.text == "➕ Добавить пользователя")
async def ask_add_user(message: Message):
    uid = message.from_user.id
    if not is_admin(uid): return
    ADM_PENDING[uid] = "add_user"
    await message.answer("Введите числовой Telegram ID пользователя (только цифры).")

## Removed: old entry point for deleting user via plain ID

# --- Админы (видно всем админам; выполнять может только супер)
@dp.message(F.text == "➕ Добавить админа")
async def ask_add_admin(message: Message):
    uid = message.from_user.id
    if not is_superadmin(uid):
        await message.answer("Только суперадмин может управлять администраторами.")
        return
    ADM_PENDING[uid] = "add_admin"
    await message.answer("Введите числовой Telegram ID администратора (только цифры).")

@dp.message(F.text == "➖ Удалить админа")
async def ask_del_admin(message: Message):
    uid = message.from_user.id
    if not is_superadmin(uid):
        await message.answer("Только суперадмин может управлять администраторами.")
        return
    ADM_PENDING[uid] = "del_admin"
    await message.answer(t(lang_for(uid), "prompt_user_id"))

@dp.message(F.text == "⚙️ Суперадмины")
async def superadmin_manage_menu(message: Message):
    uid = message.from_user.id
    if uid != OWNER_ID:
        await message.answer("Только владелец может управлять суперадминами.")
        return
    sms = db.list_superadmins()
    lines = ["Текущие суперадмины:"]
    for sid in sms:
        mark = "👑 " if sid == OWNER_ID else ""
        lines.append(f"{mark}id:{sid}")
    lines.append("\nКоманды:\n• \"add id:123\" — добавить\n• \"del id:123\" — удалить")
    await message.answer("\n".join(lines))
    ADM_PENDING[uid] = "superadmin_select"

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
        await message.answer("Пользователь добавлен.")
    elif action == "add_superadmin":
        if uid != OWNER_ID:
            await message.answer("Только владелец может управлять суперадминами.")
            return
        db.add_superadmin(target_id, added_by=uid)
        db.add_allowed_user(target_id, username_lc="", added_by=uid, credits=10**9)
        refresh_superadmins()
        await message.answer("Суперадмин добавлен.")
    elif action == "del_superadmin":
        if uid != OWNER_ID:
            await message.answer("Только владелец может управлять суперадминами.")
            return
        if target_id == OWNER_ID:
            await message.answer("Нельзя удалить владельца.")
            return
        if target_id not in SUPERADMINS:
            await message.answer("Этот пользователь не является суперадмином.")
            return
        db.remove_superadmin(target_id)
        refresh_superadmins()
        await message.answer("Суперадмин удалён.")
    # 'del_user' flow removed in favor of inline deletion in "Мои пользователи"
    else:
        await message.answer("OK")

# Принять только цифры для add_user (только когда активен режим добавления)
@dp.message(
    F.text.regexp(r"^\d{6,12}$") &
    F.func(lambda m: ADM_PENDING.get(m.from_user.id) == "add_user")
)
async def handle_add_user_by_id_digits(message: Message):
    uid = message.from_user.id
    if not is_admin(uid):
        return
    target_id_str = message.text.strip()
    try:
        target_id = int(target_id_str)
    except ValueError:
        await message.answer("Неверный ID")
        return
    db.add_allowed_user(target_id, username_lc="", added_by=uid, credits=100)
    db.log_audit(uid, "add_user", target=str(target_id), details=f"by={uid}")
    ADM_PENDING.pop(uid, None)
    await message.answer("Пользователь добавлен.")

# Принять только цифры для add_admin (только когда активен режим добавления админа)
@dp.message(
    F.text.regexp(r"^\d{6,12}$") &
    F.func(lambda m: ADM_PENDING.get(m.from_user.id) in {"add_admin", "add_superadmin"})
)
async def handle_add_admin_by_id_digits(message: Message):
    uid = message.from_user.id
    action = ADM_PENDING.get(uid)
    if action not in {"add_admin", "add_superadmin"}:
        return
    if action == "add_admin" and not is_superadmin(uid):
        return
    if action == "add_superadmin" and uid != OWNER_ID:
        return
    target_id_str = message.text.strip()
    try:
        target_id = int(target_id_str)
    except ValueError:
        await message.answer("Неверный ID")
        return
    if action == "add_admin":
        db.add_admin(target_id)
        db.log_audit(uid, "add_admin", target=str(target_id), details="by_digits")
        await message.answer("Админ добавлен.")
    else:
        db.add_superadmin(target_id, added_by=uid)
        db.add_allowed_user(target_id, username_lc="", added_by=uid, credits=10**9)
        refresh_superadmins()
        await message.answer("Суперадмин добавлен.")
    ADM_PENDING.pop(uid, None)


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

## (удалено) отдельный раздел удаления чатов

@dp.message(F.text == "📂 Мои чаты")
async def show_my_chats(message: Message):
    uid = message.from_user.id
    if not is_admin(uid):
        return
    await _close_prev_paged(uid)
    kb, total, page = build_my_chats_kb(uid, page=0)
    caption = f"Ваши чаты: {total}" if total else "У вас нет добавленных чатов."
    sent = await message.answer(caption, reply_markup=kb)
    PAGED_MSG[uid] = sent.message_id

@dp.message(F.text.in_({"Все админы", "📚 Чаты всех админов"}))
async def show_admins_list(message: Message):
    uid = message.from_user.id
    if not is_superadmin(uid):
        return
    await _close_prev_paged(uid)
    kb, total, page = build_admins_list_kb(page=0)
    caption = "Админы:" if total else "Админов нет."
    sent = await message.answer(caption, reply_markup=kb)
    PAGED_MSG[uid] = sent.message_id

@dp.message(F.text == "Все пользователи")
async def show_all_users_by_admin(message: Message):
    uid = message.from_user.id
    if not is_superadmin(uid):
        return
    await _close_prev_paged(uid)
    # mark pick mode so that selecting admin opens users directly
    ADMIN_PICK_MODE[uid] = "users"
    kb, total, page = build_admins_list_kb(page=0, pick_prefix="admi")
    caption = "Админы:" if total else "Админов нет."
    sent = await message.answer(caption, reply_markup=kb)
    PAGED_MSG[uid] = sent.message_id

## (удалено) коллбеки dcp/dc/dcY/dcN — не используются

@dp.callback_query(F.data.regexp(r"^mcp:(\d+)$"))
async def cb_my_chats_page(call: CallbackQuery):
    try:
        _, page_str = call.data.split(":", 1)
        page = int(page_str)
    except Exception:
        await call.answer("")
        return
    uid = call.from_user.id
    if not is_admin(uid):
        await call.answer("Нет прав", show_alert=True)
        return
    kb, total, cur_page = build_my_chats_kb(uid, page=page)
    caption = f"Ваши чаты: {total}" if total else "У вас нет добавленных чатов."
    try:
        await call.message.edit_text(caption, reply_markup=kb)
    except Exception:
        try:
            await call.message.edit_reply_markup(reply_markup=kb)
        except Exception:
            pass
    await call.answer("")
    PAGED_MSG[call.from_user.id] = call.message.message_id

@dp.callback_query(F.data.regexp(r"^mci:(-?\d+):(\d+)$"))
async def cb_my_chats_item(call: CallbackQuery):
    try:
        _, chat_id_str, page_str = call.data.split(":", 2)
        chat_id = int(chat_id_str)
        page = int(page_str)
    except Exception:
        await call.answer("")
        return
    uid = call.from_user.id
    row = db.conn.execute("SELECT title, female_id, added_by FROM allowed_chats WHERE chat_id=?", (chat_id,)).fetchone()
    title = (row["title"] if row else "?") or "(no title)"
    fid = (row["female_id"] if row else "?") or "?"
    total_msgs = db.count_messages_in_chat(chat_id)
    unique_males = db.count_unique_males_in_chat(chat_id)
    text = f"Чат: {title} • {fid} — {chat_id}\nСообщений: {total_msgs}\nУникальных мужчин: {unique_males}"
    kb = InlineKeyboardBuilder()
    # Показать кнопку удаления внутри карточки чата
    if row and row["added_by"] == uid:
        kb.button(text="🗑 Удалить чат", callback_data=f"mcd:{chat_id}:{page}")
    kb.button(text="⬅ Назад", callback_data=f"mcp:{page}")
    kb.button(text="✖ Закрыть", callback_data="mcc:close")
    kb.adjust(2, 1)
    try:
        await call.message.edit_text(text, reply_markup=kb.as_markup())
    except Exception:
        try:
            await call.message.edit_reply_markup(reply_markup=kb.as_markup())
        except Exception:
            pass
    await call.answer("")
    PAGED_MSG[call.from_user.id] = call.message.message_id

@dp.callback_query(F.data.regexp(r"^mcd:(-?\d+):(\d+)$"))
async def cb_my_chat_delete_confirm(call: CallbackQuery):
    try:
        _, chat_id_str, page_str = call.data.split(":", 2)
        chat_id = int(chat_id_str)
        page = int(page_str)
    except Exception:
        await call.answer("")
        return
    uid = call.from_user.id
    row = db.get_allowed_chat(chat_id)
    if not row or row["added_by"] != uid:
        await call.answer("Можно удалять только свои чаты", show_alert=True)
        return
    title = (row["title"] if row else "?") or "(no title)"
    fid = (row["female_id"] if row else "?") or "?"
    kb = InlineKeyboardBuilder()
    kb.button(text="Да", callback_data=f"mcdY:{chat_id}:{page}")
    kb.button(text="Нет", callback_data=f"mci:{chat_id}:{page}")
    kb.adjust(2)
    try:
        await call.message.edit_text(f"Удалить чат: {title} • {fid} — {chat_id}?", reply_markup=kb.as_markup())
    except Exception:
        try:
            await call.message.edit_reply_markup(reply_markup=kb.as_markup())
        except Exception:
            pass
    await call.answer("")
    PAGED_MSG[call.from_user.id] = call.message.message_id

@dp.callback_query(F.data.regexp(r"^mcdY:(-?\d+):(\d+)$"))
async def cb_my_chat_delete_yes(call: CallbackQuery):
    try:
        _, chat_id_str, page_str = call.data.split(":", 2)
        chat_id = int(chat_id_str)
        page = int(page_str)
    except Exception:
        await call.answer("")
        return
    uid = call.from_user.id
    row = db.get_allowed_chat(chat_id)
    if not row or row["added_by"] != uid:
        await call.answer("Можно удалять только свои чаты", show_alert=True)
        return
    title = (row["title"] if row else "?") or "(no title)"
    fid = (row["female_id"] if row else "?") or "?"
    db.remove_allowed_chat(chat_id)
    db.log_audit(uid, "unauthorize_my_chat_from_card", target=str(chat_id), details="from_my_chats")
    try:
        await bot.send_message(uid, f"Удалён чат: {title} • {fid} — {chat_id}")
    except Exception:
        pass
    # Вернуться к той же странице списка «Мои чаты»
    kb, total, cur_page = build_my_chats_kb(uid, page=page)
    caption = f"Ваши чаты: {total}" if total else "У вас нет добавленных чатов."
    try:
        await call.message.edit_text(caption, reply_markup=kb)
    except Exception:
        try:
            await call.message.edit_reply_markup(reply_markup=kb)
        except Exception:
            pass
    await call.answer("Удалено")
    PAGED_MSG[call.from_user.id] = call.message.message_id

@dp.callback_query(F.data == "mcc:close")
async def cb_my_chats_close(call: CallbackQuery):
    try:
        await call.message.delete()
    except Exception:
        pass
    await call.answer("")
    if PAGED_MSG.get(call.from_user.id) == call.message.message_id:
        PAGED_MSG.pop(call.from_user.id, None)

# ===== Users pagination (admin-only)
@dp.callback_query(F.data.regexp(r"^mup:(\d+)$"))
async def cb_my_users_page(call: CallbackQuery):
    try:
        _, page_str = call.data.split(":", 1)
        page = int(page_str)
    except Exception:
        await call.answer("")
        return
    uid = call.from_user.id
    if not is_admin(uid):
        await call.answer("Нет прав", show_alert=True)
        return
    kb, total, cur_page = build_my_users_kb(uid, page=page)
    caption = f"Ваши пользователи: {total}" if lang_for(uid) == "ru" else f"Ваші користувачі: {total}"
    try:
        await call.message.edit_text(caption, reply_markup=kb)
    except Exception:
        try:
            await call.message.edit_reply_markup(reply_markup=kb)
        except Exception:
            pass
    await call.answer("")
    PAGED_MSG[uid] = call.message.message_id

@dp.callback_query(F.data.regexp(r"^mui:(\d+):(\d+)$"))
async def cb_my_users_item(call: CallbackQuery):
    try:
        _, user_id_str, page_str = call.data.split(":", 2)
        user_id = int(user_id_str)
        page = int(page_str)
    except Exception:
        await call.answer("")
        return
    uid = call.from_user.id
    if not is_admin(uid):
        await call.answer("Нет прав", show_alert=True)
        return
    # Fetch user info
    row = db.conn.execute(
        "SELECT au.user_id, au.credits, au.added_by, u.username, u.first_name, u.last_name FROM allowed_users au LEFT JOIN users u ON u.user_id=au.user_id WHERE au.user_id=?",
        (user_id,)
    ).fetchone()
    if not row:
        await call.answer("Пользователь не найден", show_alert=True)
        return
    if not is_superadmin(uid) and row["added_by"] != uid:
        await call.answer("Только свои пользователи", show_alert=True)
        return
    uname = row["username"] or ""
    name = (row["first_name"] or "")
    title = (f"@{uname}" if uname else name).strip() or f"id:{user_id}"
    msgs = db.count_messages_by_user(user_id)
    chats = db.list_user_chats(user_id)
    # Build text
    lines = [f"Пользователь: {title} (id:{user_id})", f"Сообщений: {msgs}"]
    if chats:
        lines.append("Чаты:")
        for c in chats[:20]:
            t = c["title"] or "(no title)"
            fid = c["female_id"] or "?"
            lines.append(f"• {t} (fid:{fid}) — {c['chat_id']}")
        if len(chats) > 20:
            lines.append(f"…и ещё {len(chats)-20}")
    text = "\n".join(lines)
    # Build keyboard
    kb = InlineKeyboardBuilder()
    # Allow delete only for owner admin or superadmin
    if is_superadmin(uid) or row["added_by"] == uid:
        kb.button(text="🗑 Удалить пользователя", callback_data=f"mud:{user_id}:{page}")
    kb.button(text="⬅ Назад", callback_data=f"mup:{page}")
    kb.button(text="✖ Закрыть", callback_data="muc:close")
    kb.adjust(1, 2)
    try:
        await call.message.edit_text(text, reply_markup=kb.as_markup())
    except Exception:
        try:
            await call.message.edit_reply_markup(reply_markup=kb.as_markup())
        except Exception:
            pass
    await call.answer("")
    PAGED_MSG[uid] = call.message.message_id

@dp.callback_query(F.data.regexp(r"^mud:(\d+):(\d+)$"))
async def cb_my_user_delete_confirm(call: CallbackQuery):
    try:
        _, user_id_str, page_str = call.data.split(":", 2)
        user_id = int(user_id_str)
        page = int(page_str)
    except Exception:
        await call.answer("")
        return
    uid = call.from_user.id
    row = db.conn.execute("SELECT added_by FROM allowed_users WHERE user_id=?", (user_id,)).fetchone()
    if not row:
        await call.answer("Пользователь не найден", show_alert=True)
        return
    if not is_superadmin(uid) and row["added_by"] != uid:
        await call.answer("Можно удалять только своих пользователей", show_alert=True)
        return
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Да, удалить", callback_data=f"mudY:{user_id}:{page}")
    kb.button(text="↩ Нет", callback_data=f"mup:{page}")
    kb.adjust(1)
    try:
        await call.message.edit_text(f"Удалить пользователя id:{user_id}?", reply_markup=kb.as_markup())
    except Exception:
        try:
            await call.message.edit_reply_markup(reply_markup=kb.as_markup())
        except Exception:
            pass
    await call.answer("")

@dp.callback_query(F.data.regexp(r"^mudY:(\d+):(\d+)$"))
async def cb_my_user_delete_yes(call: CallbackQuery):
    try:
        _, user_id_str, page_str = call.data.split(":", 2)
        user_id = int(user_id_str)
        page = int(page_str)
    except Exception:
        await call.answer("")
        return
    uid = call.from_user.id
    row = db.conn.execute("SELECT added_by FROM allowed_users WHERE user_id=?", (user_id,)).fetchone()
    if row and (is_superadmin(uid) or row["added_by"] == uid):
        db.remove_allowed_user(user_id)
        db.log_audit(uid, "remove_user_from_panel", target=str(user_id), details="via_my_users")
        try:
            await bot.send_message(uid, f"Пользователь удалён: id:{user_id}")
        except Exception:
            pass
    kb, total, cur_page = build_my_users_kb(uid, page=page)
    caption = f"Ваши пользователи: {total}" if lang_for(uid) == "ru" else f"Ваші користувачі: {total}"
    try:
        await call.message.edit_text(caption, reply_markup=kb)
    except Exception:
        try:
            await call.message.edit_reply_markup(reply_markup=kb)
        except Exception:
            pass
    await call.answer("Удалено")

@dp.callback_query(F.data == "muc:close")
async def cb_my_users_close(call: CallbackQuery):
    try:
        await call.message.delete()
    except Exception:
        pass
    await call.answer("")
    if PAGED_MSG.get(call.from_user.id) == call.message.message_id:
        PAGED_MSG.pop(call.from_user.id, None)

## (удалено) закрытие старой пагинации удаления чатов

# ===== Superadmin: browse admins -> their chats -> stats/delete =====
@dp.callback_query(F.data.regexp(r"^admp:(\d+)$"))
async def cb_admins_page(call: CallbackQuery):
    try:
        _, page_str = call.data.split(":", 1)
        page = int(page_str)
    except Exception:
        await call.answer("")
        return
    uid = call.from_user.id
    if not is_superadmin(uid):
        await call.answer("Нет прав", show_alert=True)
        return
    kb, total, cur_page = build_admins_list_kb(page=page)
    caption = "Админы:" if total else "Админов нет."
    try:
        await call.message.edit_text(caption, reply_markup=kb)
    except Exception:
        try:
            await call.message.edit_reply_markup(reply_markup=kb)
        except Exception:
            pass
    await call.answer("")
    PAGED_MSG[uid] = call.message.message_id

@dp.callback_query(F.data == "admb:back")
async def cb_admins_back(call: CallbackQuery):
    # Go back to the previous submenu (admin.admins) instead of the first page
    uid = call.from_user.id
    if not is_superadmin(uid):
        await call.answer("Нет прав", show_alert=True)
        return
    try:
        await call.message.delete()
    except Exception:
        pass
    await call.answer("")
    if PAGED_MSG.get(uid) == getattr(call.message, 'message_id', None):
        PAGED_MSG.pop(uid, None)
    # Show the "Управление администраторами" submenu
    try:
        await bot.send_message(uid, "Управление администраторами", reply_markup=kb_admin_admins(uid))
    except Exception:
        pass

@dp.callback_query(F.data.regexp(r"^admi:(\d+):(\d+)$"))
async def cb_admin_pick(call: CallbackQuery):
    try:
        _, admin_id_str, from_page = call.data.split(":", 2)
        admin_id = int(admin_id_str)
    except Exception:
        await call.answer("")
        return
    uid = call.from_user.id
    if not is_superadmin(uid):
        await call.answer("Нет прав", show_alert=True)
        return
    # Сохранить страницу списка админов, с которой выбирали этого админа
    try:
        fp = int(from_page)
    except Exception:
        fp = 0
    d = ADMIN_FROM_PAGE.get(uid, {})
    d[admin_id] = fp
    ADMIN_FROM_PAGE[uid] = d
    # If pick mode requests users directly, open users list; else show submenu
    mode = ADMIN_PICK_MODE.pop(uid, None)
    if mode == "users":
        kb, total, page = build_admin_users_kb(admin_id=admin_id, page=0)
        caption = f"Пользователи админа id:{admin_id}: {total}" if total else "У этого админа нет пользователей."
        try:
            await call.message.edit_text(caption, reply_markup=kb)
        except Exception:
            try:
                await call.message.edit_reply_markup(reply_markup=kb)
            except Exception:
                pass
        await call.answer("")
        PAGED_MSG[uid] = call.message.message_id
        return
    else:
        # Show submenu for the chosen admin
        kb = InlineKeyboardBuilder()
        kb.button(text="Чаты админа", callback_data=f"adms:chats:{admin_id}:0")
        kb.button(text="Пользователи админа", callback_data=f"adms:users:{admin_id}:0")
        kb.button(text="🗑 Удалить админа", callback_data=f"admd:{admin_id}:{from_page}")
        kb.button(text="⬅ Список админов", callback_data=f"admp:{from_page}")
        kb.button(text="✖ Закрыть", callback_data="admc:close")
        kb.adjust(1)
        caption = f"Админ id:{admin_id} — выберите раздел"
        try:
            await call.message.edit_text(caption, reply_markup=kb.as_markup())
        except Exception:
            try:
                await call.message.edit_reply_markup(reply_markup=kb.as_markup())
            except Exception:
                pass
        await call.answer("")
        PAGED_MSG[uid] = call.message.message_id

@dp.callback_query(F.data.regexp(r"^adms:(chats|users):(\d+):(\d+)$"))
async def cb_admin_subsection(call: CallbackQuery):
    try:
        _, section, admin_id_str, page_str = call.data.split(":", 3)
        admin_id = int(admin_id_str)
        page = int(page_str)
    except Exception:
        await call.answer("")
        return
    uid = call.from_user.id
    if not is_superadmin(uid):
        await call.answer("Нет прав", show_alert=True)
        return
    if section == "chats":
        kb, total, cur_page = build_admin_chats_kb(admin_id=admin_id, page=page)
        caption = f"Чаты админа id:{admin_id}: {total}" if total else "У этого админа нет чатов."
    else:
        kb, total, cur_page = build_admin_users_kb(admin_id=admin_id, page=page)
        caption = f"Пользователи админа id:{admin_id}: {total}" if total else "У этого админа нет пользователей."
    try:
        await call.message.edit_text(caption, reply_markup=kb)
    except Exception:
        try:
            await call.message.edit_reply_markup(reply_markup=kb)
        except Exception:
            pass
    await call.answer("")
    PAGED_MSG[uid] = call.message.message_id

@dp.callback_query(F.data.regexp(r"^admsb:(\d+)$"))
async def cb_admin_submenu_back(call: CallbackQuery):
    # Вернуться в подменю выбранного админа
    try:
        _, admin_id_str = call.data.split(":", 1)
        admin_id = int(admin_id_str)
    except Exception:
        await call.answer("")
        return
    uid = call.from_user.id
    if not is_superadmin(uid):
        await call.answer("Нет прав", show_alert=True)
        return
    from_page = ADMIN_FROM_PAGE.get(uid, {}).get(admin_id, 0)
    kb = InlineKeyboardBuilder()
    kb.button(text="Чаты админа", callback_data=f"adms:chats:{admin_id}:0")
    kb.button(text="Пользователи админа", callback_data=f"adms:users:{admin_id}:0")
    kb.button(text="🗑 Удалить админа", callback_data=f"admd:{admin_id}:{from_page}")
    kb.button(text="⬅ Список админов", callback_data=f"admp:{from_page}")
    kb.button(text="✖ Закрыть", callback_data="admc:close")
    kb.adjust(1)
    caption = f"Админ id:{admin_id} — выберите раздел"
    try:
        await call.message.edit_text(caption, reply_markup=kb.as_markup())
    except Exception:
        try:
            await call.message.edit_reply_markup(reply_markup=kb.as_markup())
        except Exception:
            pass
    await call.answer("")
    PAGED_MSG[uid] = call.message.message_id

@dp.callback_query(F.data.regexp(r"^adcp:(\d+):(\d+)$"))
async def cb_admin_chats_page(call: CallbackQuery):
    try:
        _, admin_id_str, page_str = call.data.split(":", 2)
        admin_id = int(admin_id_str)
        page = int(page_str)
    except Exception:
        await call.answer("")
        return
    uid = call.from_user.id
    if not is_superadmin(uid):
        await call.answer("Нет прав", show_alert=True)
        return
    kb, total, cur_page = build_admin_chats_kb(admin_id=admin_id, page=page)
    caption = f"Чаты админа id:{admin_id}: {total}" if total else "У этого админа нет чатов."
    try:
        await call.message.edit_text(caption, reply_markup=kb)
    except Exception:
        try:
            await call.message.edit_reply_markup(reply_markup=kb)
        except Exception:
            pass
    await call.answer("")
    PAGED_MSG[uid] = call.message.message_id

@dp.callback_query(F.data.regexp(r"^adci:(-?\d+):(\d+):(\d+)$"))
async def cb_admin_chat_item(call: CallbackQuery):
    try:
        _, chat_id_str, admin_id_str, page_str = call.data.split(":", 3)
        chat_id = int(chat_id_str)
        admin_id = int(admin_id_str)
        page = int(page_str)
    except Exception:
        await call.answer("")
        return
    uid = call.from_user.id
    if not is_superadmin(uid):
        await call.answer("Нет прав", show_alert=True)
        return
    row = db.conn.execute("SELECT title, female_id, added_by FROM allowed_chats WHERE chat_id=?", (chat_id,)).fetchone()
    title = (row["title"] if row else "?") or "(no title)"
    fid = (row["female_id"] if row else "?") or "?"
    total_msgs = db.count_messages_in_chat(chat_id)
    unique_males = db.count_unique_males_in_chat(chat_id)
    text = f"Чат: {title} • {fid} — {chat_id}\nСообщений: {total_msgs}\nУникальных мужчин: {unique_males}"
    kb = InlineKeyboardBuilder()
    kb.button(text="🗑 Удалить чат", callback_data=f"adcd:{chat_id}:{admin_id}:{page}")
    kb.button(text="⬅ Назад", callback_data=f"adcp:{admin_id}:{page}")
    kb.button(text="✖ Закрыть", callback_data="admc:close")
    kb.adjust(2, 1)
    try:
        await call.message.edit_text(text, reply_markup=kb.as_markup())
    except Exception:
        try:
            await call.message.edit_reply_markup(reply_markup=kb.as_markup())
        except Exception:
            pass
    await call.answer("")
    PAGED_MSG[uid] = call.message.message_id

@dp.callback_query(F.data.regexp(r"^adcd:(-?\d+):(\d+):(\d+)$"))
async def cb_admin_chat_delete_confirm(call: CallbackQuery):
    try:
        _, chat_id_str, admin_id_str, page_str = call.data.split(":", 3)
        chat_id = int(chat_id_str)
        admin_id = int(admin_id_str)
        page = int(page_str)
    except Exception:
        await call.answer("")
        return
    uid = call.from_user.id
    if not is_superadmin(uid):
        await call.answer("Нет прав", show_alert=True)
        return
    row = db.get_allowed_chat(chat_id)
    title = (row["title"] if row else "?") or "(no title)"
    fid = (row["female_id"] if row else "?") or "?"
    kb = InlineKeyboardBuilder()
    kb.button(text="Да", callback_data=f"adcdY:{chat_id}:{admin_id}:{page}")
    kb.button(text="Нет", callback_data=f"adci:{chat_id}:{admin_id}:{page}")
    kb.adjust(2)
    try:
        await call.message.edit_text(f"Удалить чат: {title} • {fid} — {chat_id}?", reply_markup=kb.as_markup())
    except Exception:
        try:
            await call.message.edit_reply_markup(reply_markup=kb.as_markup())
        except Exception:
            pass
    await call.answer("")
    PAGED_MSG[uid] = call.message.message_id

@dp.callback_query(F.data.regexp(r"^adcdY:(-?\d+):(\d+):(\d+)$"))
async def cb_admin_chat_delete_yes(call: CallbackQuery):
    try:
        _, chat_id_str, admin_id_str, page_str = call.data.split(":", 3)
        chat_id = int(chat_id_str)
        admin_id = int(admin_id_str)
        page = int(page_str)
    except Exception:
        await call.answer("")
        return
    uid = call.from_user.id
    if not is_superadmin(uid):
        await call.answer("Нет прав", show_alert=True)
        return
    info = db.get_allowed_chat(chat_id)
    title = (info["title"] if info else "?") or "(no title)"
    fid = (info["female_id"] if info else "?") or "?"
    db.remove_allowed_chat(chat_id)
    db.log_audit(uid, "unauthorize_chat_via_admin_browse", target=str(chat_id), details=f"admin_id={admin_id}")
    try:
        await bot.send_message(uid, f"Удалён чат: {title} • {fid} — {chat_id}")
    except Exception:
        pass
    kb, total, cur_page = build_admin_chats_kb(admin_id=admin_id, page=page)
    caption = f"Чаты админа id:{admin_id}: {total}" if total else "У этого админа нет чатов."
    try:
        await call.message.edit_text(caption, reply_markup=kb)
    except Exception:
        try:
            await call.message.edit_reply_markup(reply_markup=kb)
        except Exception:
            pass
    await call.answer("Удалено")
    PAGED_MSG[uid] = call.message.message_id

@dp.callback_query(F.data == "admc:close")
async def cb_admins_close(call: CallbackQuery):
    try:
        await call.message.delete()
    except Exception:
        pass
    await call.answer("")
    if PAGED_MSG.get(call.from_user.id) == call.message.message_id:
        PAGED_MSG.pop(call.from_user.id, None)
    if PAGED_MSG.get(call.from_user.id) == call.message.message_id:
        PAGED_MSG.pop(call.from_user.id, None)


# ========= SEARCH (10 цифр) =========
@dp.message(F.text.regexp(r"^\d{10}$"))
async def handle_male_search(message: Message):
    uid = message.from_user.id
    # если в режиме отчёта — не обрабатываем как поиск
    st = REPORT_STATE.get(uid)
    if st and st.get("stage") in {"wait_female", "wait_text"}:
        return
    legend_view_st = LEGEND_VIEW_STATE.get(uid)
    if legend_view_st and legend_view_st.get("stage") == "wait_female":
        return

    lang = lang_for(uid)

    # If a female ID is entered by mistake, show number of reports for that female
    fid_candidate = message.text.strip()
    try:
        is_ten_digits = bool(re.fullmatch(r"\d{10}", fid_candidate))
    except Exception:
        is_ten_digits = False
    if is_ten_digits:
        row_f = db.conn.execute(
            "SELECT 1 FROM allowed_chats WHERE female_id=? LIMIT 1",
            (fid_candidate,)
        ).fetchone()
        if row_f:
            # count reports from audit_log
            cnt = db.conn.execute(
                "SELECT COUNT(*) AS c FROM audit_log WHERE action='report_send' AND target=?",
                (fid_candidate,)
            ).fetchone()["c"]
            # Log as female search
            db.log_search(uid, "female", fid_candidate)
            await message.answer(t(lang, "female_reports_count", fid=fid_candidate, count=cnt))
            return

    banned_until = db.get_user_ban(uid)
    now_ts = int(time.time())
    if banned_until and now_ts < banned_until:
        until_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(banned_until))
        await message.answer(t(lang, "banned", until=until_str))
        return
    if not db.rate_limit_allowed(uid, now_ts):
        await message.answer(t(lang, "rate_limited"))
        return
    # Restricted guests: allow with daily quotas
    if not is_admin(uid) and not db.is_allowed_user(uid):
        # limit: configured searches per 24h
        ts_ago_24h = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now_ts - 24*3600))
        row_q = db.conn.execute(
            "SELECT COUNT(*) AS c FROM searches WHERE user_id=? AND query_type='male' AND created_at > ?",
            (uid, ts_ago_24h)
        ).fetchone()
        lim_s = db.get_setting_int('guest_limit_search', 50)
        if row_q and row_q["c"] is not None and row_q["c"] >= lim_s:
            await message.answer(t(lang, "limited_search_quota", limit=lim_s))
            return
    # credits mechanic removed: no checks or reductions

    male = message.text.strip()
    db.log_search(uid, "male", male)
    MALE_SEARCH_STATE[uid] = {
        "male_id": male,
        "female_filter": None,
        "time_filter": "all",
        "stage": "wait_female_filter",
    }
    await message.answer(
        t(lang, "male_filter_prompt_female"),
        reply_markup=build_female_prompt_kb(male, lang)
    )

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

    # Wait for filters before returning results

async def send_results(message: Message, male_id: str, offset: int, user_id: Optional[int] = None,
                       female_filter: Optional[str] = None, time_filter: str = "all"):
    chat_id = message.chat.id
    uid = user_id or (message.from_user.id if message.from_user else chat_id)
    lang = lang_for(uid)
    if time_filter not in TIME_FILTER_CHOICES:
        time_filter = "all"
    since_ts = time_filter_since(time_filter)
    total = db.count_by_male(male_id, female_id=female_filter, since_ts=since_ts)
    if total == 0:
        await bot.send_message(chat_id, t(lang, "search_not_found"))
        return
    if offset >= total:
        offset = 0
    rows  = db.search_by_male(male_id, limit=5, offset=offset, female_id=female_filter, since_ts=since_ts)
    state = MALE_SEARCH_STATE.setdefault(uid, {})
    state["male_id"] = male_id
    state["female_filter"] = female_filter
    state["time_filter"] = time_filter
    state["stage"] = None
    for row in rows:
        text = row["text"] or ""
        media_type = row["media_type"] or None
        file_id = row["file_id"] or None
        ts_raw = row["date"]
        ts_val = float(ts_raw) if isinstance(ts_raw, (int, float)) else float(ts_raw or 0)
        try:
            ts_fmt = time.strftime("%Y-%m-%d %H:%M", time.localtime(ts_val))
        except Exception:
            ts_fmt = "—"
        female_tag = row["female_id"] or ""
        header = f"🗓 <b>{ts_fmt}</b>"
        if female_tag:
            header += f" • {female_tag}"
        formatted = highlight_id(text, male_id)
        body = header + "\n" + (formatted or (text or "(no text)"))
        try:
            if media_type == "photo" and file_id:
                await bot.send_photo(chat_id=chat_id, photo=file_id, caption=body)
            elif media_type == "video" and file_id:
                await bot.send_video(chat_id=chat_id, video=file_id, caption=body)
            elif media_type == "audio" and file_id:
                await bot.send_audio(chat_id=chat_id, audio=file_id, caption=body)
            elif media_type == "voice" and file_id:
                await bot.send_voice(chat_id=chat_id, voice=file_id, caption=body)
            elif media_type == "document" and file_id:
                await bot.send_document(chat_id=chat_id, document=file_id, caption=body)
            else:
                await bot.send_message(chat_id=chat_id, text=body)
        except Exception:
            await bot.send_message(chat_id=chat_id, text=body)
    new_offset = offset + len(rows)
    female_label = female_filter_label(lang, female_filter)
    time_label = time_filter_label(lang, time_filter)
    summary = f"{min(new_offset, total)}/{total}\n" + t(lang, "filter_summary", female=female_label, period=time_label)
    filt_token = female_filter or "-"
    kb = InlineKeyboardBuilder()
    buttons = []
    if new_offset < total:
        buttons.append(("more", f"more:{male_id}:{new_offset}:{filt_token}:{time_filter}", t(lang, "more")))
    buttons.append(("filter", f"mfilt:{male_id}:{filt_token}:{time_filter}", t(lang, "filter_button")))
    if buttons:
        if len(buttons) == 2:
            kb.button(text=buttons[0][2], callback_data=buttons[0][1])
            kb.button(text=buttons[1][2], callback_data=buttons[1][1])
            kb.adjust(2)
        else:
            kb.button(text=buttons[0][2], callback_data=buttons[0][1])
            kb.adjust(1)
        markup = kb.as_markup()
    else:
        markup = None
    await bot.send_message(chat_id, summary, reply_markup=markup)

async def send_report_lookup_results(chat_id: int, user_id: int, female_id: str, offset: int):
    lang = lang_for(user_id)
    since_ts = time.time() - REPORT_LOOKUP_WINDOW
    total = db.count_reports_by_female(female_id, since_ts)
    if total == 0:
        if offset == 0:
            await bot.send_message(chat_id, t(lang, "report_search_empty", fid=female_id))
        else:
            await bot.send_message(chat_id, t(lang, "report_search_no_more"))
        return
    if offset >= total:
        await bot.send_message(chat_id, t(lang, "report_search_no_more"))
        return
    rows = db.get_reports_by_female(female_id, since_ts, REPORT_LOOKUP_PAGE, offset)
    if not rows:
        await bot.send_message(chat_id, t(lang, "report_search_no_more"))
        return
    for row in rows:
        text = (row["text"] or "").strip()
        base_text = text or "(no text)"
        male_ids = []
        if row["male_ids"]:
            raw_ids = [mid for mid in row["male_ids"].split(",") if mid]
            male_ids = list(dict.fromkeys(raw_ids))
        if male_ids:
            formatted = highlight_id(base_text, male_ids[0])
        else:
            formatted = html.escape(base_text)
        ts_raw = row["date"]
        ts_val = float(ts_raw) if isinstance(ts_raw, (int, float)) else float(ts_raw or 0)
        try:
            ts_fmt = time.strftime("%Y-%m-%d %H:%M", time.localtime(ts_val))
        except Exception:
            ts_fmt = "—"
        header = f"🗓 <b>{ts_fmt}</b> • {female_id}"
        body = f"{header}\n{formatted}"
        await bot.send_message(chat_id, body)
    new_offset = offset + len(rows)
    if new_offset < total:
        kb = InlineKeyboardBuilder()
        kb.button(text=t(lang, "more"), callback_data=f"rep_more:{female_id}:{new_offset}")
        await bot.send_message(chat_id, f"{min(new_offset, total)}/{total}", reply_markup=kb.as_markup())
    else:
        await bot.send_message(chat_id, f"{total}/{total}")

# ========= COUNT-ONLY QUICK CHECK =========
# Triggers on: /count 1234567890, "count 1234567890", "проверить 1234567890", "перевірити 1234567890"
@dp.message(F.text.regexp(r"^(?:/count|count|проверить|перевірити)\s+(\d{10})$", flags=re.IGNORECASE))
async def handle_count_only(message: Message):
    uid = message.from_user.id
    lang = lang_for(uid)
    m = re.match(r"^(?:/count|count|проверить|перевірити)\s+(\d{10})$", message.text.strip(), flags=re.IGNORECASE)
    male_id = m.group(1) if m else None
    if not male_id:
        await message.answer("Bad ID")
        return
    total = db.count_by_male(male_id)
    if lang == "uk":
        await message.answer(f"Повідомлень з ID {male_id}: {total}")
    else:
        await message.answer(f"Сообщений с ID {male_id}: {total}")

@dp.callback_query(F.data.startswith("more:"))
async def cb_more(call: CallbackQuery):
    try:
        parts = call.data.split(":")
        male_id = parts[1]
        offset = int(parts[2])
        female_filter = None
        time_filter = "all"
        if len(parts) >= 5:
            female_filter = None if parts[3] == "-" else parts[3]
            time_filter = parts[4]
        await send_results(call.message, male_id, offset, user_id=call.from_user.id,
                           female_filter=female_filter, time_filter=time_filter)
    finally:
        await call.answer("")

async def show_filter_menu(uid: int, male_id: str, female_token: str, time_filter: str):
    lang = lang_for(uid)
    state = MALE_SEARCH_STATE.setdefault(uid, {})
    state["male_id"] = male_id
    if "female_filter" not in state:
        state["female_filter"] = None if female_token == "-" else female_token
    if time_filter not in TIME_FILTER_CHOICES:
        time_filter = "all"
    if "time_filter" not in state:
        state["time_filter"] = time_filter
    kb = InlineKeyboardBuilder()
    current_time = state.get("time_filter", "all")
    kb.button(text=t(lang, "male_filter_enter_button"), callback_data=f"mffask:{male_id}")
    kb.button(text=t(lang, "male_filter_all_button"), callback_data=f"mfself:{male_id}:-")
    kb.adjust(1, 1)
    for code in TIME_FILTER_CHOICES:
        prefix = "✅ " if current_time == code else ""
        kb.button(text=prefix + time_filter_label(lang, code), callback_data=f"mftime:{male_id}:{code}")
    kb.button(text=t(lang, "filter_close"), callback_data="mfclose")
    kb.adjust(1)
    text = t(lang, "filter_menu_title", male=male_id)
    old_menu_id = state.get("filter_menu_id")
    if old_menu_id:
        try:
            await bot.delete_message(uid, old_menu_id)
        except Exception:
            pass
    sent = await bot.send_message(uid, text, reply_markup=kb.as_markup())
    state["filter_menu_id"] = sent.message_id

@dp.callback_query(F.data.regexp(r"^mfilt:(\d{10}):([^:]+):([a-z0-9]+)$"))
async def cb_filter_menu(call: CallbackQuery):
    match = re.match(r"^mfilt:(\d{10}):([^:]+):([a-z0-9]+)$", call.data or "")
    if not match:
        await call.answer("")
        return
    male_id, female_token, time_filter = match.groups()
    await show_filter_menu(call.from_user.id, male_id, female_token, time_filter)
    await call.answer("")

@dp.callback_query(F.data.regexp(r"^mffask:(\d{10})$"))
async def cb_filter_female_prompt(call: CallbackQuery):
    match = re.match(r"^mffask:(\d{10})$", call.data or "")
    if not match:
        await call.answer("")
        return
    male_id = match.group(1)
    uid = call.from_user.id
    lang = lang_for(uid)
    state = MALE_SEARCH_STATE.setdefault(uid, {"male_id": male_id})
    state["male_id"] = male_id
    state["stage"] = "wait_female_manual"
    try:
        await call.message.delete()
    except Exception:
        pass
    state.pop("filter_menu_id", None)
    await bot.send_message(uid, t(lang, "male_filter_prompt_female"))
    await call.answer("")

@dp.callback_query(F.data.regexp(r"^mfself:(\d{10}):(-)$"))
async def cb_filter_female_all(call: CallbackQuery):
    match = re.match(r"^mfself:(\d{10}):(-)$", call.data or "")
    if not match:
        await call.answer("")
        return
    male_id = match.group(1)
    uid = call.from_user.id
    lang = lang_for(uid)
    state = MALE_SEARCH_STATE.setdefault(uid, {"male_id": male_id})
    state["male_id"] = male_id
    state["female_filter"] = None
    stage = state.get("stage")
    try:
        await call.message.delete()
    except Exception:
        pass
    state.pop("filter_menu_id", None)
    if stage == "wait_female_filter":
        state["stage"] = "wait_period_filter"
        await bot.send_message(uid, t(lang, "male_filter_prompt_period"), reply_markup=build_period_prompt_kb(male_id, lang))
    else:
        state["stage"] = None
        await send_results(call.message, male_id, 0, user_id=uid, female_filter=None, time_filter=state.get("time_filter", "all"))
    await call.answer("")
@dp.callback_query(F.data == "mfclose")
async def cb_filter_close(call: CallbackQuery):
    uid = call.from_user.id
    state = MALE_SEARCH_STATE.get(uid)
    if state:
        state.pop("filter_menu_id", None)
    try:
        await call.message.delete()
    except Exception:
        pass
    await call.answer("")

@dp.callback_query(F.data.regexp(r"^mftime:(\d{10}):([a-z0-9]+)(?::(init))?$"))
async def cb_filter_set_time(call: CallbackQuery):
    match = re.match(r"^mftime:(\d{10}):([a-z0-9]+)(?::(init))?$", call.data or "")
    if not match:
        await call.answer("")
        return
    male_id, time_filter, init_flag = match.groups()
    uid = call.from_user.id
    if time_filter not in TIME_FILTER_CHOICES:
        time_filter = "all"
    state = MALE_SEARCH_STATE.setdefault(uid, {"male_id": male_id})
    state["male_id"] = male_id
    state["time_filter"] = time_filter
    female_filter = state.get("female_filter")
    stage = state.get("stage")
    if stage == "wait_period_filter":
        state["stage"] = None
        try:
            await call.message.delete()
        except Exception:
            pass
        await send_results(call.message, male_id, 0, user_id=uid, female_filter=female_filter, time_filter=time_filter)
    else:
        try:
            await call.message.delete()
        except Exception:
            pass
        state.pop("filter_menu_id", None)
        await send_results(call.message, male_id, 0, user_id=uid, female_filter=female_filter, time_filter=time_filter)
    await call.answer("")

@dp.callback_query(F.data.regexp(r"^rep_more:(\d{10}):(\d+)$"))
async def cb_rep_more(call: CallbackQuery):
    data = call.data or ""
    match = re.match(r"^rep_more:(\d{10}):(\d+)$", data)
    if not match:
        await call.answer("")
        return
    female_id = match.group(1)
    offset = int(match.group(2))
    chat_id = call.message.chat.id if call.message else call.from_user.id
    await send_report_lookup_results(chat_id, call.from_user.id, female_id, offset)
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
            # Only bot admins may authorize chats automatically
            if not is_admin(inviter_id):
                try:
                    await bot.send_message(event.chat.id, t(lang_for(inviter_id or OWNER_ID), "chat_not_authorized"))
                except Exception:
                    pass
                db.log_audit(inviter_id, "auto_authorize_denied_non_admin", target=str(event.chat.id), details="")
                return
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
    if LEGEND_HASHTAG.lower() in text.lower():
        await process_legend_from_chat(message, text)
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
    # credits removed

@dp.callback_query(F.data.regexp(r"^adup:(\d+):(\d+)$"))
async def cb_admin_users_page(call: CallbackQuery):
    try:
        _, admin_id_str, page_str = call.data.split(":", 2)
        admin_id = int(admin_id_str)
        page = int(page_str)
    except Exception:
        await call.answer("")
        return
    uid = call.from_user.id
    if not is_superadmin(uid):
        await call.answer("Нет прав", show_alert=True)
        return
    kb, total, cur_page = build_admin_users_kb(admin_id=admin_id, page=page)
    caption = f"Пользователи админа id:{admin_id}: {total}" if total else "У этого админа нет пользователей."
    try:
        await call.message.edit_text(caption, reply_markup=kb)
    except Exception:
        try:
            await call.message.edit_reply_markup(reply_markup=kb)
        except Exception:
            pass
    await call.answer("")
    PAGED_MSG[uid] = call.message.message_id

@dp.callback_query(F.data.regexp(r"^adui:(\d+):(\d+):(\d+)$"))
async def cb_admin_user_item(call: CallbackQuery):
    try:
        _, user_id_str, admin_id_str, page_str = call.data.split(":", 3)
        user_id = int(user_id_str)
        admin_id = int(admin_id_str)
        page = int(page_str)
    except Exception:
        await call.answer("")
        return
    uid = call.from_user.id
    if not is_superadmin(uid):
        await call.answer("Нет прав", show_alert=True)
        return
    row = db.conn.execute(
        "SELECT au.user_id, au.added_by, u.username, u.first_name, u.last_name FROM allowed_users au LEFT JOIN users u ON u.user_id=au.user_id WHERE au.user_id=?",
        (user_id,)
    ).fetchone()
    if not row:
        await call.answer("Пользователь не найден", show_alert=True)
        return
    uname = row["username"] or ""
    title = (f"@{uname}" if uname else (row["first_name"] or "")).strip() or f"id:{user_id}"
    msgs = db.count_messages_by_user(user_id)
    chats = db.list_user_chats(user_id)
    lines = [f"Пользователь: {title} (id:{user_id})", f"Сообщений: {msgs}", "Чаты:"]
    for c in chats[:20]:
        t = c["title"] or "(no title)"
        fid = c["female_id"] or "?"
        lines.append(f"• {t} (fid:{fid}) — {c['chat_id']}")
    if len(chats) > 20:
        lines.append(f"…и ещё {len(chats)-20}")
    text = "\n".join(lines)
    kb = InlineKeyboardBuilder()
    kb.button(text="🗑 Удалить пользователя", callback_data=f"adud:{user_id}:{admin_id}:{page}")
    kb.button(text="⬅ Назад", callback_data=f"adms:users:{admin_id}:{page}")
    kb.button(text="✖ Закрыть", callback_data="admc:close")
    kb.adjust(1, 2)
    try:
        await call.message.edit_text(text, reply_markup=kb.as_markup())
    except Exception:
        try:
            await call.message.edit_reply_markup(reply_markup=kb.as_markup())
        except Exception:
            pass
    await call.answer("")
 
@dp.callback_query(F.data.regexp(r"^adud:(\d+):(\d+):(\d+)$"))
async def cb_admin_user_delete_confirm(call: CallbackQuery):
    try:
        _, user_id_str, admin_id_str, page_str = call.data.split(":", 3)
        user_id = int(user_id_str)
        admin_id = int(admin_id_str)
        page = int(page_str)
    except Exception:
        await call.answer("")
        return
    uid = call.from_user.id
    if not is_superadmin(uid):
        await call.answer("Нет прав", show_alert=True)
        return
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Да, удалить", callback_data=f"adudY:{user_id}:{admin_id}:{page}")
    kb.button(text="↩ Нет", callback_data=f"adui:{user_id}:{admin_id}:{page}")
    kb.adjust(1)
    try:
        await call.message.edit_text(f"Удалить пользователя id:{user_id}?", reply_markup=kb.as_markup())
    except Exception:
        try:
            await call.message.edit_reply_markup(reply_markup=kb.as_markup())
        except Exception:
            pass
    await call.answer("")

@dp.callback_query(F.data.regexp(r"^adudY:(\d+):(\d+):(\d+)$"))
async def cb_admin_user_delete_yes(call: CallbackQuery):
    try:
        _, user_id_str, admin_id_str, page_str = call.data.split(":", 3)
        user_id = int(user_id_str)
        admin_id = int(admin_id_str)
        page = int(page_str)
    except Exception:
        await call.answer("")
        return
    uid = call.from_user.id
    if not is_superadmin(uid):
        await call.answer("Нет прав", show_alert=True)
        return
    db.remove_allowed_user(user_id)
    db.log_audit(uid, "remove_user_from_all_users_panel", target=str(user_id), details=f"admin_id={admin_id}")
    try:
        await bot.send_message(uid, f"Пользователь удалён: id:{user_id}")
    except Exception:
        pass
    kb, total, cur_page = build_admin_users_kb(admin_id=admin_id, page=page)
    caption = f"Пользователи админа id:{admin_id}: {total}" if total else "У этого админа нет пользователей."
    try:
        await call.message.edit_text(caption, reply_markup=kb)
    except Exception:
        try:
            await call.message.edit_reply_markup(reply_markup=kb)
        except Exception:
            pass
    await call.answer("Удалено")

@dp.callback_query(F.data.regexp(r"^admd:(\d+):(\d+)$"))
async def cb_admin_delete_confirm(call: CallbackQuery):
    try:
        _, admin_id_str, page_str = call.data.split(":", 2)
        admin_id = int(admin_id_str)
        page = int(page_str)
    except Exception:
        await call.answer("")
        return
    uid = call.from_user.id
    if not is_superadmin(uid):
        await call.answer("Нет прав", show_alert=True)
        return
    if admin_id in SUPERADMINS:
        await call.answer("Нельзя удалить суперадмина", show_alert=True)
        return
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Да, удалить", callback_data=f"admdY:{admin_id}:{page}")
    kb.button(text="↩ Нет", callback_data=f"admp:{page}")
    kb.adjust(1)
    try:
        await call.message.edit_text(
            f"Удалить админа id:{admin_id}? Это действие необратимо.",
            reply_markup=kb.as_markup(),
        )
    except Exception:
        # Попробуем обновить только клавиатуру; если не получится — пришлём новое сообщение
        try:
            await call.message.edit_reply_markup(reply_markup=kb.as_markup())
        except Exception:
            try:
                sent = await call.message.answer(
                    f"Удалить админа id:{admin_id}? Это действие необратимо.",
                    reply_markup=kb.as_markup(),
                )
                PAGED_MSG[call.from_user.id] = sent.message_id
            except Exception:
                pass
    await call.answer("")

# Fallback: catch any admd:* payload (in case of unexpected page value)
@dp.callback_query(F.data.startswith("admd:"))
async def cb_admin_delete_confirm_fallback(call: CallbackQuery):
    try:
        _, admin_id_str, page_str = call.data.split(":", 2)
        int(admin_id_str)  # validate
    except Exception:
        await call.answer("")
        return
    # Delegate to main handler by reusing logic
    return await cb_admin_delete_confirm(call)

@dp.callback_query(F.data.regexp(r"^admdY:(\d+):(\d+)$"))
async def cb_admin_delete_yes(call: CallbackQuery):
    try:
        _, admin_id_str, page_str = call.data.split(":", 2)
        admin_id = int(admin_id_str)
        page = int(page_str)
    except Exception:
        await call.answer("")
        return
    uid = call.from_user.id
    if not is_superadmin(uid) or admin_id in SUPERADMINS:
        await call.answer("Нет прав", show_alert=True)
        return
    db.remove_admin(admin_id)
    db.log_audit(uid, "remove_admin_from_panel", target=str(admin_id), details="via_all_admins")
    try:
        await bot.send_message(uid, f"Админ удалён: id:{admin_id}")
    except Exception:
        pass
    kb, total, cur_page = build_admins_list_kb(page=page)
    caption = "Админы:" if total else "Админов нет."
    try:
        await call.message.edit_text(caption, reply_markup=kb)
    except Exception:
        try:
            await call.message.edit_reply_markup(reply_markup=kb)
        except Exception:
            pass
    await call.answer("Удалено")

# Fallback for confirm yes
@dp.callback_query(F.data.startswith("admdY:"))
async def cb_admin_delete_yes_fallback(call: CallbackQuery):
    try:
        _, admin_id_str, page_str = call.data.split(":", 2)
        int(admin_id_str); int(page_str)
    except Exception:
        await call.answer("")
        return
    return await cb_admin_delete_yes(call)

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
