import logging
import sqlite3
from datetime import datetime
from html import escape

from aiogram import Bot, F, Router
from aiogram.enums import ChatType
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command
from aiogram.types import CallbackQuery, Message

from database import QuoteDatabase
from keyboards.inline import quote_actions_keyboard
from utils.formatters import format_quote, format_quotes_catalog, format_top_list
from utils.rate_limit import rate_limiter


router = Router()
logger = logging.getLogger(__name__)


def log_usage(db: QuoteDatabase, message: Message, command_name: str, payload: dict | None = None) -> None:
    try:
        db.log_command_usage(
            user_id=message.from_user.id if message.from_user else None,
            chat_id=message.chat.id,
            command_name=command_name,
            is_private=message.chat.type == ChatType.PRIVATE,
            payload=payload,
        )
    except Exception:
        logger.exception("usage log failed command=%s", command_name)


def log_quote_event(
    db: QuoteDatabase,
    quote_id: int,
    chat_id: int,
    actor_user_id: int | None,
    event_type: str,
    payload: dict | None = None,
) -> None:
    try:
        db.log_quote_event(
            quote_id=quote_id,
            chat_id=chat_id,
            actor_user_id=actor_user_id,
            event_type=event_type,
            event_payload=payload,
        )
    except Exception:
        logger.exception("quote event log failed event=%s quote_id=%s", event_type, quote_id)


def get_args(message: Message) -> str:
    text = (message.text or "").strip()
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        return ""
    return parts[1].strip()


def parse_limit(raw_value: str, default: int = 5, max_limit: int = 10) -> int:
    if not raw_value:
        return default

    try:
        value = int(raw_value)
    except ValueError:
        return default

    if value < 1:
        return default

    return min(value, max_limit)


def is_group_context(message: Message) -> bool:
    return message.chat.type in {ChatType.GROUP, ChatType.SUPERGROUP}


async def ensure_group_context(message: Message) -> bool:
    if is_group_context(message):
        return True

    await message.answer("Эта команда работает только в группе 😅")
    return False


def get_message_author_data(message: Message) -> tuple[int | None, str, str | None]:
    if message.from_user:
        full_name = message.from_user.full_name.strip() or "Nameless hero"
        return message.from_user.id, full_name, message.from_user.username

    if message.sender_chat:
        title = message.sender_chat.title or "Anonymous chat"
        return message.sender_chat.id, title, message.sender_chat.username

    return None, "Unknown", None


async def is_admin(bot: Bot, chat_id: int, user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(chat_id, user_id)
    except TelegramBadRequest:
        return False
    except Exception:
        logger.exception("Failed to check admin rights")
        return False

    return str(member.status) in {"administrator", "creator"}


async def bot_is_admin(bot: Bot, chat_id: int) -> bool:
    me = await bot.get_me()
    return await is_admin(bot, chat_id, me.id)


async def send_quote_message(target: Message, quote: dict) -> None:
    await target.answer(
        format_quote(quote),
        reply_markup=quote_actions_keyboard(quote["id"]),
    )


async def show_quotes_list(message: Message, quotes: list[dict]) -> None:
    for quote in quotes:
        await send_quote_message(message, quote)


async def hit_cooldown(message: Message, key: str, cooldown: float) -> bool:
    remaining = rate_limiter.hit(message.from_user.id if message.from_user else None, key, cooldown)
    if remaining > 0:
        await message.answer(f"Не так быстро 😅 Подожди {remaining:.1f} сек.")
        return True
    return False


@router.message(Command(commands=["save_quote"]))
async def save_quote(message: Message, db: QuoteDatabase) -> None:
    log_usage(db, message, "save_quote")

    if not await ensure_group_context(message):
        return

    try:
        chat_id = message.chat.id
        reply = message.reply_to_message
        if not reply:
            hint = "Ответь на сообщение и потом зови меня 😎"

            try:
                has_admin = await bot_is_admin(message.bot, chat_id)
            except Exception:
                has_admin = False

            if not has_admin:
                hint += (
                    "\n\nПохоже, Telegram не дал мне доступ к исходному сообщению."
                    "\nОбычно это бывает, когда у меня нет админки или мешает privacy mode."
                    "\nВыдай мне права администратора в чате или отключи privacy mode через BotFather."
                )

            await message.answer(hint)
            return

        quote_text = (reply.text or "").strip()
        if not quote_text:
            await message.answer("Не-а 😅 Так не сработает. Нужен обычный текст")
            return

        if len(quote_text) < 3:
            await message.answer("Слишком коротко 🤔 Дай хоть нормальную фразу")
            return

        quoted_user_id, quoted_name, quoted_username = get_message_author_data(reply)
        saved_by_user_id, saved_by_name, _ = get_message_author_data(message)

        duplicate = db.find_duplicate(
            chat_id=chat_id,
            original_message_id=reply.message_id,
            quote_text=quote_text,
            quoted_user_id=quoted_user_id,
        )
        if duplicate:
            await message.answer(
                f"Похоже, это уже лежит у Кабанчика 👀\n"
                f"Такая цитата записана как #{duplicate['id']}"
            )
            return

        try:
            quote_id = db.add_quote(
                chat_id=chat_id,
                quote_text=quote_text,
                quoted_user_id=quoted_user_id,
                quoted_name=quoted_name,
                quoted_username=quoted_username,
                saved_by_user_id=saved_by_user_id,
                saved_by_name=saved_by_name,
                created_at=datetime.now().isoformat(timespec="seconds"),
                original_message_id=reply.message_id,
            )
        except sqlite3.IntegrityError:
            already_saved = db.find_duplicate(
                chat_id=chat_id,
                original_message_id=reply.message_id,
                quote_text=quote_text,
                quoted_user_id=quoted_user_id,
            )
            if already_saved:
                await message.answer(
                    f"Эта цитата уже есть у Кабанчика 😎\n"
                    f"Она записана как #{already_saved['id']}"
                )
                return
            raise

        log_quote_event(
            db,
            quote_id=quote_id,
            chat_id=chat_id,
            actor_user_id=message.from_user.id if message.from_user else None,
            event_type="created",
            payload={"original_message_id": reply.message_id},
        )
        await message.answer(f"Готово 👌 Кабанчик всё записал как цитату #{quote_id}")
    except Exception:
        logger.exception("save_quote failed")
        await message.answer("Что-то пошло не так 🤔 Попробуй ещё раз чуть позже")


@router.message(Command(commands=["q", "quote", "quote_random"]))
async def random_quote(message: Message, db: QuoteDatabase) -> None:
    log_usage(db, message, "quote_random")

    if not await ensure_group_context(message):
        return

    if await hit_cooldown(message, "random_quote", 1.2):
        return

    try:
        quote = db.get_random_quote(message.chat.id)
        if not quote:
            await message.answer("Пока пусто 🤔 Цитат тут ещё нет")
            return

        log_quote_event(
            db,
            quote_id=quote["id"],
            chat_id=message.chat.id,
            actor_user_id=message.from_user.id if message.from_user else None,
            event_type="shown_random",
        )
        await send_quote_message(message, quote)
    except Exception:
        logger.exception("random_quote failed")
        await message.answer("Не смог достать цитату 😅")


@router.message(Command(commands=["quote_id"]))
async def quote_by_id(message: Message, db: QuoteDatabase) -> None:
    log_usage(db, message, "quote_id", {"args": get_args(message)})

    if not await ensure_group_context(message):
        return

    args = get_args(message)
    if not args:
        await message.answer("Напиши id после команды 😅\nНапример: /quote_id 15")
        return

    try:
        quote_id = int(args)
    except ValueError:
        await message.answer("Нужен нормальный числовой id 🤔")
        return

    try:
        quote = db.get_quote_by_id(chat_id=message.chat.id, quote_id=quote_id)
        if not quote:
            await message.answer("Такой цитаты в этом чате не вижу 👀")
            return

        log_quote_event(
            db,
            quote_id=quote["id"],
            chat_id=message.chat.id,
            actor_user_id=message.from_user.id if message.from_user else None,
            event_type="shown_by_id",
        )
        await send_quote_message(message, quote)
    except Exception:
        logger.exception("quote_by_id failed")
        await message.answer("Не получилось открыть цитату 😅")


@router.message(Command(commands=["quotes_count"]))
async def quotes_count(message: Message, db: QuoteDatabase) -> None:
    if not await ensure_group_context(message):
        return

    try:
        total = db.count_quotes(message.chat.id)
        if total == 0:
            await message.answer("Пока пусто 🤔 Цитат тут ещё нет")
            return

        await message.answer(f"В этом чате у Кабанчика уже <b>{total}</b> цитат 📜")
    except Exception:
        logger.exception("quotes_count failed")
        await message.answer("Я запутался в подсчётах 😵")


@router.message(Command(commands=["top_quoted"]))
async def top_quoted(message: Message, db: QuoteDatabase) -> None:
    if not await ensure_group_context(message):
        return

    try:
        top = db.get_top_quoted(message.chat.id, limit=7)
        if not top:
            await message.answer("Пока пусто 🤔 Цитат тут ещё нет")
            return

        await message.answer(format_top_list(top))
    except Exception:
        logger.exception("top_quoted failed")
        await message.answer("Не смог собрать топ 😅")


@router.message(Command(commands=["my_quotes"]))
async def my_quotes(message: Message, db: QuoteDatabase) -> None:
    if message.chat.type == ChatType.PRIVATE:
        await message.answer("В личке лучше использовать /my_quotes_global или /my_quotes_chat 😎")
        return

    if not await ensure_group_context(message):
        return

    if not message.from_user:
        await message.answer("Не понимаю, кто ты в этом сообщении 😅")
        return

    try:
        quotes = db.get_quotes_by_saved_user(
            chat_id=message.chat.id,
            user_id=message.from_user.id,
            limit=5,
        )
        if not quotes:
            await message.answer("Ты пока ничего не сохранял 🤔")
            return

        await message.answer("Вот твои последние сохранёнки 📜")
        await show_quotes_list(message, quotes)
    except Exception:
        logger.exception("my_quotes failed")
        await message.answer("Не смог показать твои цитаты 😅")


@router.message(Command(commands=["latest_quotes"]))
async def latest_quotes(message: Message, db: QuoteDatabase) -> None:
    log_usage(db, message, "latest_quotes", {"args": get_args(message)})

    if not await ensure_group_context(message):
        return

    args = get_args(message)
    limit = parse_limit(args, default=5, max_limit=10)

    if await hit_cooldown(message, "latest_quotes", 1.5):
        return

    try:
        quotes = db.get_latest_quotes(message.chat.id, limit=limit)
        if not quotes:
            await message.answer("Пока пусто 🤔 Цитат тут ещё нет")
            return

        await message.answer(f"Свежак из цитатника, последние {len(quotes)} шт. 📅")
        await show_quotes_list(message, quotes)
    except Exception:
        logger.exception("latest_quotes failed")
        await message.answer("Не получилось открыть последние цитаты 😅")


@router.message(Command(commands=["all_quotes"]))
async def all_quotes(message: Message, db: QuoteDatabase) -> None:
    log_usage(db, message, "all_quotes")

    if not await ensure_group_context(message):
        return

    if await hit_cooldown(message, "all_quotes", 2.0):
        return

    try:
        quotes = db.get_all_quotes(message.chat.id)
        if not quotes:
            await message.answer("Пока пусто 🤔 В этом чате ещё нет цитат")
            return

        chunks = format_quotes_catalog(quotes)
        if not chunks:
            await message.answer("Не смог собрать список цитат 😅")
            return

        for index, chunk in enumerate(chunks, start=1):
            suffix = f"\n\nВсего цитат: {len(quotes)}" if index == len(chunks) else ""
            await message.answer(chunk + suffix)
    except Exception:
        logger.exception("all_quotes failed")
        await message.answer("Не получилось показать все цитаты 😅")


@router.message(Command(commands=["search_quote"]))
async def search_quote(message: Message, db: QuoteDatabase) -> None:
    log_usage(db, message, "search_quote", {"query": get_args(message)})

    if not await ensure_group_context(message):
        return

    query = get_args(message)
    if not query:
        await message.answer("Напиши текст для поиска 🔎\nНапример: /search_quote компилируюсь")
        return

    if await hit_cooldown(message, "search_quote", 1.5):
        return

    try:
        quotes = db.search_quotes(message.chat.id, query_text=query, limit=5)
        if not quotes:
            await message.answer("Ничего не нашёл 🤔")
            return

        log_quote_event(
            db,
            quote_id=quotes[0]["id"],
            chat_id=message.chat.id,
            actor_user_id=message.from_user.id if message.from_user else None,
            event_type="searched",
            payload={"query": query},
        )
        await message.answer(f"Нашёл кое-что по запросу: <b>{escape(query)}</b> 🔎")
        await show_quotes_list(message, quotes)
    except Exception:
        logger.exception("search_quote failed")
        await message.answer("Поиск что-то застрял в болоте 😅")


@router.message(Command(commands=["quote_user"]))
async def quote_user(message: Message, db: QuoteDatabase) -> None:
    if not await ensure_group_context(message):
        return

    args = get_args(message)

    quoted_user_id = None
    quoted_username = None
    name_part = None

    if message.reply_to_message:
        quoted_user_id, _, _ = get_message_author_data(message.reply_to_message)
    elif args:
        if args.startswith("@"):
            quoted_username = args.lstrip("@")
        else:
            try:
                quoted_user_id = int(args)
            except ValueError:
                name_part = args
    else:
        await message.answer(
            "Кого искать? 😅\nМожно так: /quote_user @username\n"
            "Или ответь на сообщение и вызови команду"
        )
        return

    try:
        quotes = db.get_quotes_by_person(
            chat_id=message.chat.id,
            quoted_user_id=quoted_user_id,
            quoted_username=quoted_username,
            name_part=name_part,
            limit=5,
        )
        if not quotes:
            await message.answer("По этому человеку пока ничего нет 🤔")
            return

        await message.answer("Нашёл цитаты этого персонажа 😎")
        await show_quotes_list(message, quotes)
    except Exception:
        logger.exception("quote_user failed")
        await message.answer("Не получилось найти цитаты 😅")


@router.message(Command(commands=["delete_quote"]))
async def delete_quote(message: Message, db: QuoteDatabase) -> None:
    log_usage(db, message, "delete_quote", {"args": get_args(message)})

    if not await ensure_group_context(message):
        return

    args = get_args(message)
    if not args:
        await message.answer("Напиши id после команды 🗑\nНапример: /delete_quote 15")
        return

    try:
        quote_id = int(args)
    except ValueError:
        await message.answer("Тут нужен именно числовой id 🤔")
        return

    if not message.from_user:
        await message.answer("Не могу проверить права на удаление 😅")
        return

    try:
        chat_id = message.chat.id
        quote = db.get_quote_by_id(chat_id=chat_id, quote_id=quote_id)
        if not quote:
            await message.answer("Такой цитаты в этом чате нет 🤔")
            return

        can_delete = quote["saved_by_user_id"] == message.from_user.id
        if not can_delete:
            can_delete = await is_admin(message.bot, chat_id, message.from_user.id)

        if not can_delete:
            await message.answer("Удалять может админ или тот, кто сохранил цитату 🗑")
            return

        deleted = db.delete_quote(
            chat_id=chat_id,
            quote_id=quote_id,
            deleted_by_user_id=message.from_user.id,
        )
        if not deleted:
            await message.answer("Странно... цитата уже исчезла 🤔")
            return

        log_quote_event(
            db,
            quote_id=quote_id,
            chat_id=chat_id,
            actor_user_id=message.from_user.id,
            event_type="deleted",
            payload={"source": "command"},
        )
        await message.answer(f"Цитата #{quote_id} удалена 🗑")
    except Exception:
        logger.exception("delete_quote failed")
        await message.answer("Не получилось удалить цитату 😅")


@router.callback_query(F.data.startswith("quote_more:"))
async def callback_more_quote(callback: CallbackQuery, db: QuoteDatabase) -> None:
    if not callback.message:
        await callback.answer()
        return

    if callback.message.chat.type not in {ChatType.GROUP, ChatType.SUPERGROUP}:
        await callback.answer("Эта кнопка работает только в группе", show_alert=True)
        return

    chat_id = callback.message.chat.id

    try:
        current_id = int(callback.data.split(":", maxsplit=1)[1])
    except (IndexError, ValueError):
        await callback.answer("Кнопка сломалась 🤔", show_alert=True)
        return

    try:
        quote = db.get_random_quote(chat_id=chat_id, exclude_id=current_id)
        if not quote:
            quote = db.get_quote_by_id(chat_id=chat_id, quote_id=current_id)

        if not quote:
            await callback.answer("Пока больше нечего показать 🤔", show_alert=True)
            return

        log_quote_event(
            db,
            quote_id=quote["id"],
            chat_id=chat_id,
            actor_user_id=callback.from_user.id if callback.from_user else None,
            event_type="shown_random",
            payload={"source": "inline_more"},
        )
        await callback.message.answer(
            format_quote(quote),
            reply_markup=quote_actions_keyboard(quote["id"]),
        )
        await callback.answer("Лови ещё одну 🎲")
    except Exception:
        logger.exception("callback_more_quote failed")
        await callback.answer("Не смог вытащить цитату 😅", show_alert=True)


@router.callback_query(F.data.startswith("quote_del:"))
async def callback_delete_quote(callback: CallbackQuery, db: QuoteDatabase) -> None:
    if not callback.message or not callback.from_user:
        await callback.answer("Не могу проверить права 😅", show_alert=True)
        return

    if callback.message.chat.type not in {ChatType.GROUP, ChatType.SUPERGROUP}:
        await callback.answer("Эта кнопка работает только в группе", show_alert=True)
        return

    chat_id = callback.message.chat.id
    try:
        quote_id = int(callback.data.split(":", maxsplit=1)[1])
    except (IndexError, ValueError):
        await callback.answer("Кривой id 🤔", show_alert=True)
        return

    try:
        quote = db.get_quote_by_id(chat_id=chat_id, quote_id=quote_id)
        if not quote:
            await callback.answer("Цитата уже исчезла или она не из этого чата", show_alert=True)
            try:
                await callback.message.edit_reply_markup(reply_markup=None)
            except TelegramBadRequest:
                pass
            return

        can_delete = quote["saved_by_user_id"] == callback.from_user.id
        if not can_delete:
            can_delete = await is_admin(callback.bot, chat_id, callback.from_user.id)

        if not can_delete:
            await callback.answer(
                "Удалять может админ или тот, кто её сохранил",
                show_alert=True,
            )
            return

        deleted = db.delete_quote(
            chat_id=chat_id,
            quote_id=quote_id,
            deleted_by_user_id=callback.from_user.id,
        )
        if not deleted:
            await callback.answer("Цитата уже удалена", show_alert=True)
            return

        log_quote_event(
            db,
            quote_id=quote_id,
            chat_id=chat_id,
            actor_user_id=callback.from_user.id,
            event_type="deleted",
            payload={"source": "inline"},
        )
        try:
            await callback.message.delete()
        except TelegramBadRequest:
            await callback.message.edit_reply_markup(reply_markup=None)

        await callback.answer("Цитата удалена 🗑")
    except Exception:
        logger.exception("callback_delete_quote failed")
        await callback.answer("Не получилось удалить цитату 😅", show_alert=True)
