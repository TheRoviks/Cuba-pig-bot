import logging
from datetime import datetime

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.types import CallbackQuery, Message

from database import QuoteDatabase
from keyboards.inline import admin_chats_keyboard, admin_panel_keyboard
from utils.formatters import (
    format_admin_chat_details,
    format_admin_chats,
    format_admin_health,
    format_admin_stats,
    format_admin_users,
)
from utils.rate_limit import rate_limiter
from utils.security import ensure_admin_callback, ensure_admin_message


# Роутер глобальной админки.
router = Router()
logger = logging.getLogger(__name__)


def log_usage(db: QuoteDatabase, message: Message, command_name: str, payload: dict | None = None) -> None:
    try:
        db.log_command_usage(
            user_id=message.from_user.id if message.from_user else None,
            chat_id=message.chat.id,
            command_name=command_name,
            is_private=True,
            payload=payload,
        )
    except Exception:
        logger.exception("admin usage log failed command=%s", command_name)


def log_admin_action(
    db: QuoteDatabase,
    admin_user_id: int,
    action_name: str,
    *,
    target_chat_id: int | None = None,
    target_user_id: int | None = None,
    payload: dict | None = None,
) -> None:
    try:
        db.log_admin_action(
            admin_user_id=admin_user_id,
            action_name=action_name,
            target_chat_id=target_chat_id,
            target_user_id=target_user_id,
            payload=payload,
        )
    except Exception:
        logger.exception("admin audit log failed action=%s", action_name)


# Достаём аргументы после команды.
def get_args(message: Message) -> str:
    text = (message.text or "").strip()
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        return ""
    return parts[1].strip()


# Приводим uptime к короткому читабельному виду.
def format_uptime(started_at: datetime) -> str:
    delta = datetime.now() - started_at
    total_seconds = int(delta.total_seconds())
    hours, rem = divmod(total_seconds, 3600)
    minutes, seconds = divmod(rem, 60)
    return f"{hours}h {minutes}m {seconds}s"


# Простая антиспам-проверка для тяжёлых команд.
async def hit_cooldown_message(message: Message, key: str, cooldown: float) -> bool:
    remaining = rate_limiter.hit(message.from_user.id if message.from_user else None, key, cooldown)
    if remaining > 0:
        await message.answer(f"Не так быстро 😅 Подожди {remaining:.1f} сек.")
        return True
    return False


# Та же история для callback.
async def hit_cooldown_callback(callback: CallbackQuery, key: str, cooldown: float) -> bool:
    remaining = rate_limiter.hit(callback.from_user.id if callback.from_user else None, key, cooldown)
    if remaining > 0:
        await callback.answer(f"Подожди {remaining:.1f} сек.", show_alert=True)
        return True
    return False


@router.message(Command(commands=["admin"]))
async def admin_panel(message: Message, db: QuoteDatabase) -> None:
    if not await ensure_admin_message(message, "admin-panel"):
        return

    log_usage(db, message, "admin")
    if message.from_user:
        log_admin_action(db, message.from_user.id, "open_panel")
    logger.info("admin-open-panel user_id=%s", message.from_user.id if message.from_user else None)
    await message.answer("Открыл админку 👑", reply_markup=admin_panel_keyboard())


@router.message(Command(commands=["admin_chats"]))
async def admin_chats(message: Message, db: QuoteDatabase) -> None:
    if not await ensure_admin_message(message, "admin-chats"):
        return
    if await hit_cooldown_message(message, "admin_chats", 1.5):
        return

    try:
        log_usage(db, message, "admin_chats")
        if message.from_user:
            log_admin_action(db, message.from_user.id, "view_all_chats")
        chats = db.get_all_chats_with_stats(limit=40)
        logger.info("admin-chats user_id=%s count=%s", message.from_user.id if message.from_user else None, len(chats))
        await message.answer(
            format_admin_chats(chats),
            reply_markup=admin_chats_keyboard(chats),
        )
    except Exception:
        logger.exception("admin_chats failed")
        await message.answer("База сейчас ворчит 🤔 Попробуй ещё раз чуть позже")


@router.message(Command(commands=["admin_stats"]))
async def admin_stats(
    message: Message,
    db: QuoteDatabase,
) -> None:
    if not await ensure_admin_message(message, "admin-stats"):
        return
    if await hit_cooldown_message(message, "admin_stats", 2.0):
        return

    try:
        log_usage(db, message, "admin_stats")
        if message.from_user:
            log_admin_action(db, message.from_user.id, "view_global_stats")
        stats = db.get_global_stats()
        logger.info("admin-stats user_id=%s", message.from_user.id if message.from_user else None)
        await message.answer(format_admin_stats(stats), reply_markup=admin_panel_keyboard())
    except Exception:
        logger.exception("admin_stats failed")
        await message.answer("Не смог собрать общую статистику 🤔")


@router.message(Command(commands=["admin_chat"]))
async def admin_chat(message: Message, db: QuoteDatabase) -> None:
    if not await ensure_admin_message(message, "admin-chat"):
        return
    if await hit_cooldown_message(message, "admin_chat", 2.0):
        return

    args = get_args(message)
    if not args:
        await message.answer("Нужен chat_id после команды.\nНапример: /admin_chat -1001234567890")
        return

    try:
        chat_id = int(args)
    except ValueError:
        await message.answer("chat_id должен быть числом 🤔")
        return

    try:
        log_usage(db, message, "admin_chat", {"chat_id": chat_id})
        if message.from_user:
            log_admin_action(db, message.from_user.id, "view_chat_stats", target_chat_id=chat_id)
        chat = db.get_chat_stats(chat_id)
        if not chat:
            await message.answer("Такого чата не вижу 🤔")
            return

        latest_quotes = db.get_latest_quotes(chat_id, limit=5)
        top_quoted = db.get_top_quoted(chat_id, limit=5)
        top_savers = db.get_top_savers_in_chat(chat_id, limit=5)
        logger.info("admin-chat user_id=%s chat_id=%s", message.from_user.id if message.from_user else None, chat_id)
        await message.answer(
            format_admin_chat_details(chat, latest_quotes, top_quoted, top_savers),
            reply_markup=admin_panel_keyboard(),
        )
    except Exception:
        logger.exception("admin_chat failed")
        await message.answer("Не получилось открыть статистику по чату 🤔")


@router.message(Command(commands=["admin_users"]))
async def admin_users(message: Message, db: QuoteDatabase) -> None:
    if not await ensure_admin_message(message, "admin-users"):
        return
    if await hit_cooldown_message(message, "admin_users", 1.5):
        return

    try:
        log_usage(db, message, "admin_users")
        if message.from_user:
            log_admin_action(db, message.from_user.id, "view_users_stats")
        top_savers = db.get_top_global_savers(5)
        top_quoted = db.get_top_global_quoted(5)
        top_memberships = db.get_top_users_by_chat_membership(5)
        logger.info("admin-users user_id=%s", message.from_user.id if message.from_user else None)
        await message.answer(
            format_admin_users(top_savers, top_quoted, top_memberships),
            reply_markup=admin_panel_keyboard(),
        )
    except Exception:
        logger.exception("admin_users failed")
        await message.answer("Не смог собрать пользователей 🤔")


@router.message(Command(commands=["admin_health"]))
async def admin_health(
    message: Message,
    db: QuoteDatabase,
    started_at: datetime,
    project_version: str,
) -> None:
    if not await ensure_admin_message(message, "admin-health"):
        return
    if await hit_cooldown_message(message, "admin_health", 1.0):
        return

    try:
        log_usage(db, message, "admin_health")
        if message.from_user:
            log_admin_action(db, message.from_user.id, "view_health")
        health = db.get_admin_health_stats()
        health["uptime"] = format_uptime(started_at)
        health["project_version"] = project_version
        health["polling_status"] = "running"
        logger.info("admin-health user_id=%s", message.from_user.id if message.from_user else None)
        await message.answer(
            format_admin_health(health),
            reply_markup=admin_panel_keyboard(),
        )
    except Exception:
        logger.exception("admin_health failed")
        await message.answer("Не смог получить health-статус 🤔")


@router.callback_query(F.data == "adm:panel")
async def callback_admin_panel(callback: CallbackQuery, db: QuoteDatabase) -> None:
    if not await ensure_admin_callback(callback, "adm-panel"):
        return

    await callback.message.answer("Открыл админку 👑", reply_markup=admin_panel_keyboard())
    if callback.from_user:
        log_admin_action(db, callback.from_user.id, "open_panel_inline")
    await callback.answer()


@router.callback_query(F.data == "adm:stats")
async def callback_admin_stats(callback: CallbackQuery, db: QuoteDatabase) -> None:
    if not await ensure_admin_callback(callback, "adm-stats"):
        return
    if await hit_cooldown_callback(callback, "admin_stats", 2.0):
        return

    try:
        if callback.from_user:
            log_admin_action(db, callback.from_user.id, "view_global_stats_inline")
        stats = db.get_global_stats()
        await callback.message.answer(format_admin_stats(stats), reply_markup=admin_panel_keyboard())
        await callback.answer("Собрал статистику 📊")
    except Exception:
        logger.exception("callback_admin_stats failed")
        await callback.answer("Не смог собрать статистику 🤔", show_alert=True)


@router.callback_query(F.data == "adm:chats")
async def callback_admin_chats(callback: CallbackQuery, db: QuoteDatabase) -> None:
    if not await ensure_admin_callback(callback, "adm-chats"):
        return
    if await hit_cooldown_callback(callback, "admin_chats", 1.5):
        return

    try:
        if callback.from_user:
            log_admin_action(db, callback.from_user.id, "view_all_chats_inline")
        chats = db.get_all_chats_with_stats(limit=40)
        await callback.message.answer(
            format_admin_chats(chats),
            reply_markup=admin_chats_keyboard(chats),
        )
        await callback.answer("Лови список чатов 🏘")
    except Exception:
        logger.exception("callback_admin_chats failed")
        await callback.answer("Не смог открыть список чатов 🤔", show_alert=True)


@router.callback_query(F.data == "adm:users")
async def callback_admin_users(callback: CallbackQuery, db: QuoteDatabase) -> None:
    if not await ensure_admin_callback(callback, "adm-users"):
        return
    if await hit_cooldown_callback(callback, "admin_users", 1.5):
        return

    try:
        if callback.from_user:
            log_admin_action(db, callback.from_user.id, "view_users_stats_inline")
        text = format_admin_users(
            db.get_top_global_savers(5),
            db.get_top_global_quoted(5),
            db.get_top_users_by_chat_membership(5),
        )
        await callback.message.answer(text, reply_markup=admin_panel_keyboard())
        await callback.answer("Собрал пользователей 👥")
    except Exception:
        logger.exception("callback_admin_users failed")
        await callback.answer("Не смог собрать пользователей 🤔", show_alert=True)


@router.callback_query(F.data == "adm:health")
async def callback_admin_health(
    callback: CallbackQuery,
    db: QuoteDatabase,
    started_at: datetime,
    project_version: str,
) -> None:
    if not await ensure_admin_callback(callback, "adm-health"):
        return
    if await hit_cooldown_callback(callback, "admin_health", 1.0):
        return

    try:
        if callback.from_user:
            log_admin_action(db, callback.from_user.id, "view_health_inline")
        health = db.get_admin_health_stats()
        health["uptime"] = format_uptime(started_at)
        health["project_version"] = project_version
        health["polling_status"] = "running"
        await callback.message.answer(format_admin_health(health), reply_markup=admin_panel_keyboard())
        await callback.answer("Health ок 👌")
    except Exception:
        logger.exception("callback_admin_health failed")
        await callback.answer("Не смог получить health 🤔", show_alert=True)


@router.callback_query(F.data.startswith("adm:chat:"))
async def callback_admin_chat(callback: CallbackQuery, db: QuoteDatabase) -> None:
    if not await ensure_admin_callback(callback, "adm-chat"):
        return
    if await hit_cooldown_callback(callback, "admin_chat", 2.0):
        return

    try:
        chat_id = int(callback.data.split(":", maxsplit=2)[2])
    except (IndexError, ValueError):
        await callback.answer("Кривой chat_id 🤔", show_alert=True)
        return

    try:
        if callback.from_user:
            log_admin_action(db, callback.from_user.id, "view_chat_stats_inline", target_chat_id=chat_id)
        chat = db.get_chat_stats(chat_id)
        if not chat:
            await callback.answer("Такой чат не найден 🤔", show_alert=True)
            return

        text = format_admin_chat_details(
            chat,
            db.get_latest_quotes(chat_id, 5),
            db.get_top_quoted(chat_id, 5),
            db.get_top_savers_in_chat(chat_id, 5),
        )
        await callback.message.answer(text, reply_markup=admin_panel_keyboard())
        await callback.answer("Открыл чат 🔍")
    except Exception:
        logger.exception("callback_admin_chat failed")
        await callback.answer("Не смог открыть статистику чата 🤔", show_alert=True)


@router.callback_query(F.data == "adm:close")
async def callback_admin_close(callback: CallbackQuery) -> None:
    if not await ensure_admin_callback(callback, "adm-close"):
        return

    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        logger.exception("callback_admin_close failed")

    await callback.answer("Панель закрыта")
