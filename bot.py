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
from aiogram.types import Message, CallbackQuery, ChatMemberUpdated, ReplyKeyboardRemove
from aiogram.utils.keyboard import ReplyKeyboardBuilder, InlineKeyboardBuilder

from db import DB
from utils import extract_text_and_media, extract_male_ids, highlight_id
from i18n import t


# ========= ENV & LOGGING =========
load_dotenv()

BOT_TOKEN    = os.getenv("BOT_TOKEN")
OWNER_ID     = int(os.getenv("OWNER_ID", "0"))
OWNER_IDS_ENV = os.getenv("OWNER_IDS", "").replace(" ", "")
OWNER_IDS = set()
if OWNER_IDS_ENV:
    try:
        OWNER_IDS = {int(x) for x in OWNER_IDS_ENV.split(",") if x}
    except ValueError:
        OWNER_IDS = set()
if OWNER_ID:
    OWNER_IDS.add(OWNER_ID)
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
for _sid in OWNER_IDS:
    try:
        db.add_admin(_sid)
        db.add_allowed_user(_sid, username_lc="owner", added_by=_sid, credits=10**9)
    except Exception as e:
        logger.warning(f"Cannot bootstrap superadmin {_sid}: {e}")

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp  = Dispatcher()


# ========= ACCESS HELPERS =========
def is_superadmin(user_id: int) -> bool:
    return user_id in OWNER_IDS

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
def private_reply_markup(message: Message, markup):
    """Return reply keyboard only in private chats; remove it elsewhere."""
    if message.chat.type == ChatType.PRIVATE:
        return markup
    return ReplyKeyboardRemove()

def kb_main(uid: int):
    # Поиск → Добавить отчёт → Админ → Мои запросы → Язык
    lang = lang_for(uid)
    kb = ReplyKeyboardBuilder()
    kb.button(text=t(lang, "menu_search"))
    kb.button(text="➕ Добавить отчёт")
    if is_admin(uid):
        kb.button(text=t(lang, "menu_admin_panel"))
    kb.button(text=t(lang, "menu_extra"))
    # Кнопка поддержки видна только ограниченным пользователям
    if not is_admin(uid) and not db.is_allowed_user(uid):
        kb.button(text=t(lang, "menu_support"))
    kb.adjust(2, 2, 1)
    return kb.as_markup(resize_keyboard=True)

def kb_extra(uid: int):
    lang = lang_for(uid)
    kb = ReplyKeyboardBuilder()
    kb.button(text=t(lang, "menu_lang"))
    kb.button(text="⬅️ Назад")
    kb.adjust(1, 1)
    return kb.as_markup(resize_keyboard=True)

def kb_admin(uid: int):
    # как в core-2: «👤 Админы» видно всем админам
    kb = ReplyKeyboardBuilder()
    # Кнопка пользователей всегда доступна в панели админа
    kb.button(text="👥 Пользователи")
    if is_superadmin(uid):
        kb.button(text=t(lang_for(uid), "menu_superadmin_panel"))
    kb.button(text="💬 Чаты")
    # Убрали «Статистика» и «Дополнительно»
    kb.button(text="⬅️ Назад")
    kb.adjust(2, 1)
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
    kb.button(text="⬅️ Назад")
    kb.adjust(2, 1)
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
            "SELECT COUNT(*) AS c FROM searches WHERE user_id=? AND query_type='male' AND created_at > ?",
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

@dp.message(F.text.in_({"👑 Панель суперадмина", "👤 Админы"}))
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
    text = (
        "Текущие лимиты для ограниченных пользователей:\n"
        f"• Поиск в сутки: {ls}\n"
        f"• Отчёты в сутки: {lr}\n\n"
        "Выберите действие кнопками ниже или отправьте:\n"
        "поиск: 100 — для лимита поиска\n"
        "отчёты: 10 — для лимита отчётов"
    )
    kb = build_guest_limits_kb(ls, lr)
    await message.answer(text, reply_markup=kb)

@dp.message(F.text.regexp(r"(?i)^\s*(поиск|отч[её]ты)\s*[:=]\s*(\d{1,4})\s*$"))
async def guest_limits_set(message: Message):
    uid = message.from_user.id
    if not is_superadmin(uid):
        return
    m = re.match(r"(?i)^\s*(поиск|отч[её]ты)\s*[:=]\s*(\d{1,4})\s*$", message.text.strip())
    if not m:
        return
    kind = m.group(1).lower()
    val = int(m.group(2))
    val = max(0, min(100000, val))
    if kind.startswith("поиск"):
        db.set_setting_int('guest_limit_search', val)
        await message.answer(f"Лимит поиска для ограниченных установлен: {val} в сутки.")
    else:
        db.set_setting_int('guest_limit_report', val)
        await message.answer(f"Лимит отчётов для ограниченных установлен: {val} в сутки.")

@dp.callback_query(F.data.regexp(r"^gl([sr]):(noop|[+\-]\d+)$"))
async def cb_guest_limits_delta(call: CallbackQuery):
    try:
        _, tail = call.data.split(":", 1)
    except Exception:
        await call.answer("")
        return
    kind = call.data[2]  # 's' or 'r'
    op = tail
    uid = call.from_user.id
    if not is_superadmin(uid):
        await call.answer("Нет прав", show_alert=True)
        return
    key = 'guest_limit_search' if kind == 's' else 'guest_limit_report'
    default = 50 if kind == 's' else 5
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
    text = (
        "Текущие лимиты для ограниченных пользователей:\n"
        f"• Поиск в сутки: {ls}\n"
        f"• Отчёты в сутки: {lr}\n\n"
        "Выберите действие кнопками ниже или отправьте:\n"
        "поиск: 100 — для лимита поиска\n"
        "отчёты: 10 — для лимита отчётов"
    )
    kb = build_guest_limits_kb(ls, lr)
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
def build_guest_limits_kb(limit_search: int, limit_report: int):
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
    # Back
    kb.button(text="⬅ Назад", callback_data="gl:back")
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
    F.func(lambda m: ADM_PENDING.get(m.from_user.id) == "add_admin")
)
async def handle_add_admin_by_id_digits(message: Message):
    uid = message.from_user.id
    if not is_superadmin(uid):
        return
    target_id_str = message.text.strip()
    try:
        target_id = int(target_id_str)
    except ValueError:
        await message.answer("Неверный ID")
        return
    db.add_admin(target_id)
    db.log_audit(uid, "add_admin", target=str(target_id), details="by_digits")
    ADM_PENDING.pop(uid, None)
    await message.answer("Админ добавлен.")


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
        text = row["text"] or ""
        media_type = row["media_type"] or None
        file_id = row["file_id"] or None
        formatted = highlight_id(text, male_id)
        try:
            if media_type == "photo" and file_id:
                await bot.send_photo(chat_id=uid, photo=file_id, caption=formatted or None)
            elif media_type == "video" and file_id:
                await bot.send_video(chat_id=uid, video=file_id, caption=formatted or None)
            elif media_type == "audio" and file_id:
                await bot.send_audio(chat_id=uid, audio=file_id, caption=formatted or None)
            elif media_type == "voice" and file_id:
                await bot.send_voice(chat_id=uid, voice=file_id, caption=formatted or None)
            elif media_type == "document" and file_id:
                await bot.send_document(chat_id=uid, document=file_id, caption=formatted or None)
            else:
                # text-only or unknown media: send as plain message
                await bot.send_message(chat_id=uid, text=formatted or (text or "(no text)"))
        except Exception:
            # Fallback to text only
            await message.answer(formatted or (text or "(no text)"))
    new_offset = offset + 5
    if new_offset < total:
        kb = InlineKeyboardBuilder()
        kb.button(text=t(lang, "more"), callback_data=f"more:{male_id}:{new_offset}")
        await message.answer(f"{min(new_offset, total)}/{total}", reply_markup=kb.as_markup())
    else:
        await message.answer(f"{total}/{total}")

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
    if admin_id == OWNER_ID:
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
    if not is_superadmin(uid) or admin_id == OWNER_ID:
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
