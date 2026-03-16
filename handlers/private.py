from html import escape

from aiogram import F, Router
from aiogram.enums import ChatType
from aiogram.filters import Command
from aiogram.types import CallbackQuery, Message

from database import QuoteDatabase
from keyboards.inline import (
    private_chat_actions_keyboard,
    private_chat_picker_keyboard,
    private_dashboard_keyboard,
    private_summary_keyboard,
)
from utils.security import is_global_admin
from utils.formatters import (
    format_quotes_catalog,
    format_private_chat_quotes,
    format_private_quotes_summary,
    format_shared_chats,
)


# Этот роутер только для лички с ботом.
router = Router()


# Достаём аргументы после команды.
def get_args(message: Message) -> str:
    text = (message.text or "").strip()
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        return ""
    return parts[1].strip()


# Превращаем аргумент в доступный чат пользователя.
def resolve_chat_arg(
    raw_value: str,
    shared_chats: list[dict],
) -> dict | None:
    if not raw_value:
        return None

    try:
        numeric_value = int(raw_value)
    except ValueError:
        return None

    # Сначала пробуем трактовать аргумент как настоящий chat_id.
    for chat in shared_chats:
        if chat["chat_id"] == numeric_value:
            return chat

    # Если не нашли, пробуем трактовать как номер из списка.
    if 1 <= numeric_value <= len(shared_chats):
        return shared_chats[numeric_value - 1]

    return None


@router.message(F.chat.type == ChatType.PRIVATE, Command(commands=["my_chats"]))
async def my_chats(message: Message, db: QuoteDatabase) -> None:
    # В личке показываем только общие чаты пользователя и бота.
    if not message.from_user:
        await message.answer("Не могу понять, кто ты 😅")
        return

    shared_chats = db.get_shared_chats(message.from_user.id)
    if not shared_chats:
        await message.answer(
            "Пока не вижу общих чатов 🤔\n"
            "Добавь меня в группу и напиши там что-нибудь, тогда я смогу её показать",
            reply_markup=private_dashboard_keyboard(is_admin=is_global_admin(message.from_user.id)),
        )
        return

    await message.answer(
        format_shared_chats(shared_chats),
        reply_markup=private_chat_picker_keyboard(shared_chats),
    )


@router.message(F.chat.type == ChatType.PRIVATE, Command(commands=["my_quotes_global"]))
async def my_quotes_global(message: Message, db: QuoteDatabase) -> None:
    # Тут показываем только цитаты текущего пользователя и только по доступным чатам.
    if not message.from_user:
        await message.answer("Не могу понять, кто ты 😅")
        return

    summary = db.get_user_quote_summary(message.from_user.id)
    if not summary:
        await message.answer(
            "Пока не вижу твоих цитат в доступных чатах 🤔\n"
            "Сначала надо, чтобы тебя кто-то красиво процитировал",
            reply_markup=private_dashboard_keyboard(is_admin=is_global_admin(message.from_user.id)),
        )
        return

    await message.answer(
        format_private_quotes_summary(summary),
        reply_markup=private_summary_keyboard(summary),
    )


@router.message(F.chat.type == ChatType.PRIVATE, Command(commands=["my_quotes_chat"]))
async def my_quotes_chat(message: Message, db: QuoteDatabase) -> None:
    # Показываем цитаты пользователя из одного выбранного чата.
    if not message.from_user:
        await message.answer("Не могу понять, кто ты 😅")
        return

    shared_chats = db.get_shared_chats(message.from_user.id)
    if not shared_chats:
        await message.answer(
            "Пока не вижу общих чатов 🤔\n"
            "Добавь меня в группу и напиши там что-нибудь",
            reply_markup=private_dashboard_keyboard(is_admin=is_global_admin(message.from_user.id)),
        )
        return

    raw_arg = get_args(message)
    if not raw_arg:
        await message.answer(
            "Выбери чат кнопкой ниже или напиши:\n"
            "/my_quotes_chat 1\n"
            "/my_quotes_chat -1001234567890",
            reply_markup=private_chat_picker_keyboard(shared_chats),
        )
        return

    selected_chat = resolve_chat_arg(raw_arg, shared_chats)
    if not selected_chat:
        await message.answer(
            "Этот чат недоступен 🤔\n"
            "Можно смотреть только те чаты, где мы с тобой реально пересекались",
            reply_markup=private_chat_picker_keyboard(shared_chats),
        )
        return

    quotes = db.get_user_quotes_in_chat(
        user_id=message.from_user.id,
        chat_id=selected_chat["chat_id"],
        limit=7,
    )
    if not quotes:
        await message.answer(
            f"В чате <b>{escape(selected_chat['title'])}</b> пока не вижу твоих цитат 🤔"
        )
        return

    await message.answer(format_private_chat_quotes(selected_chat, quotes))


@router.message(F.chat.type == ChatType.PRIVATE, Command(commands=["all_quotes_chat"]))
async def all_quotes_chat(message: Message, db: QuoteDatabase) -> None:
    if not message.from_user:
        await message.answer("Не могу понять, кто ты 😅")
        return

    shared_chats = db.get_shared_chats(message.from_user.id)
    if not shared_chats:
        await message.answer(
            "Пока не вижу общих чатов 🤔\n"
            "Добавь меня в группу и напиши там что-нибудь",
            reply_markup=private_dashboard_keyboard(is_admin=is_global_admin(message.from_user.id)),
        )
        return

    raw_arg = get_args(message)
    if not raw_arg:
        await message.answer(
            "Выбери чат кнопкой ниже или напиши:\n"
            "/all_quotes_chat 1\n"
            "/all_quotes_chat -1001234567890",
            reply_markup=private_chat_picker_keyboard(shared_chats),
        )
        return

    selected_chat = resolve_chat_arg(raw_arg, shared_chats)
    if not selected_chat:
        await message.answer(
            "Этот чат недоступен 🤔\n"
            "Можно смотреть только те чаты, где мы с тобой реально пересекались",
            reply_markup=private_chat_picker_keyboard(shared_chats),
        )
        return

    quotes = db.get_accessible_chat_quotes(
        user_id=message.from_user.id,
        chat_id=selected_chat["chat_id"],
    )
    if not quotes:
        await message.answer(
            f"В чате <b>{escape(selected_chat['title'])}</b> пока нет цитат 🤔",
            reply_markup=private_chat_actions_keyboard(selected_chat["chat_id"]),
        )
        return

    chunks = format_quotes_catalog(
        quotes,
        heading=f"📚 Все цитаты из чата <b>{escape(selected_chat['title'])}</b>:",
        continuation_heading=f"📚 Продолжение цитат из чата <b>{escape(selected_chat['title'])}</b>:",
    )
    for index, chunk in enumerate(chunks, start=1):
        reply_markup = private_chat_actions_keyboard(selected_chat["chat_id"]) if index == len(chunks) else None
        await message.answer(
            chunk + (f"\n\nВсего цитат: {len(quotes)}" if index == len(chunks) else ""),
            reply_markup=reply_markup,
        )


@router.callback_query(F.message.chat.type == ChatType.PRIVATE, F.data == "priv:my_chats")
async def callback_my_chats(callback: CallbackQuery, db: QuoteDatabase) -> None:
    # Кнопка для списка общих чатов.
    if not callback.from_user or not callback.message:
        await callback.answer()
        return

    shared_chats = db.get_shared_chats(callback.from_user.id)
    if not shared_chats:
        await callback.answer("Пока не вижу общих чатов 🤔", show_alert=True)
        return

    await callback.message.answer(
        format_shared_chats(shared_chats),
        reply_markup=private_chat_picker_keyboard(shared_chats),
    )
    await callback.answer("Вот список чатов 📂")


@router.callback_query(F.message.chat.type == ChatType.PRIVATE, F.data == "priv:my_quotes_global")
async def callback_my_quotes_global(callback: CallbackQuery, db: QuoteDatabase) -> None:
    # Кнопка для сводки по всем доступным чатам.
    if not callback.from_user or not callback.message:
        await callback.answer()
        return

    summary = db.get_user_quote_summary(callback.from_user.id)
    if not summary:
        await callback.answer("Пока не вижу твоих цитат 🤔", show_alert=True)
        return

    await callback.message.answer(
        format_private_quotes_summary(summary),
        reply_markup=private_summary_keyboard(summary),
    )
    await callback.answer("Собрал твою сводку 💬")


@router.callback_query(F.message.chat.type == ChatType.PRIVATE, F.data == "priv:pick_chat")
async def callback_pick_chat(callback: CallbackQuery, db: QuoteDatabase) -> None:
    # Кнопка-помощник для выбора нужного чата.
    if not callback.from_user or not callback.message:
        await callback.answer()
        return

    shared_chats = db.get_shared_chats(callback.from_user.id)
    if not shared_chats:
        await callback.answer("Общих чатов пока нет 🤔", show_alert=True)
        return

    await callback.message.answer(
        "Выбирай чат 👇",
        reply_markup=private_chat_picker_keyboard(shared_chats),
    )
    await callback.answer("Лови список 🔎")


@router.callback_query(F.message.chat.type == ChatType.PRIVATE, F.data.startswith("priv:chat_menu:"))
async def callback_chat_menu(callback: CallbackQuery, db: QuoteDatabase) -> None:
    if not callback.from_user or not callback.message:
        await callback.answer()
        return

    try:
        chat_id = int(callback.data.split(":", maxsplit=2)[2])
    except (IndexError, ValueError):
        await callback.answer("Кривой chat_id 🤔", show_alert=True)
        return

    shared_chat = db.get_shared_chat(callback.from_user.id, chat_id)
    if not shared_chat:
        await callback.answer(
            "Этот чат тебе недоступен 👀\nНельзя смотреть чужие данные",
            show_alert=True,
        )
        return

    await callback.message.answer(
        f"Чат <b>{escape(shared_chat['title'])}</b>\n"
        "Можно открыть только твои цитаты или весь цитатник этого чата.",
        reply_markup=private_chat_actions_keyboard(chat_id),
    )
    await callback.answer("Открыл чат 📂")


@router.callback_query(F.message.chat.type == ChatType.PRIVATE, F.data.startswith("priv:chat_quotes:"))
async def callback_chat_quotes(callback: CallbackQuery, db: QuoteDatabase) -> None:
    # Отдаём цитаты пользователя из выбранного чата, но только если доступ подтверждён.
    if not callback.from_user or not callback.message:
        await callback.answer()
        return

    try:
        chat_id = int(callback.data.split(":", maxsplit=2)[2])
    except (IndexError, ValueError):
        await callback.answer("Кривой chat_id 🤔", show_alert=True)
        return

    shared_chat = db.get_shared_chat(callback.from_user.id, chat_id)
    if not shared_chat:
        await callback.answer(
            "Этот чат тебе недоступен 👀\nНельзя смотреть чужие данные",
            show_alert=True,
        )
        return

    quotes = db.get_user_quotes_in_chat(
        user_id=callback.from_user.id,
        chat_id=chat_id,
        limit=7,
    )
    if not quotes:
        await callback.answer("В этом чате твоих цитат пока нет 🤔", show_alert=True)
        return

    await callback.message.answer(
        format_private_chat_quotes(shared_chat, quotes),
        reply_markup=private_chat_actions_keyboard(chat_id),
    )
    await callback.answer("Показываю цитаты 📜")


@router.callback_query(F.message.chat.type == ChatType.PRIVATE, F.data.startswith("priv:chat_all_quotes:"))
async def callback_chat_all_quotes(callback: CallbackQuery, db: QuoteDatabase) -> None:
    if not callback.from_user or not callback.message:
        await callback.answer()
        return

    try:
        chat_id = int(callback.data.split(":", maxsplit=2)[2])
    except (IndexError, ValueError):
        await callback.answer("Кривой chat_id 🤔", show_alert=True)
        return

    shared_chat = db.get_shared_chat(callback.from_user.id, chat_id)
    if not shared_chat:
        await callback.answer(
            "Этот чат тебе недоступен 👀\nНельзя смотреть чужие данные",
            show_alert=True,
        )
        return

    quotes = db.get_accessible_chat_quotes(
        user_id=callback.from_user.id,
        chat_id=chat_id,
    )
    if not quotes:
        await callback.answer("В этом чате пока нет цитат 🤔", show_alert=True)
        return

    chunks = format_quotes_catalog(
        quotes,
        heading=f"📚 Все цитаты из чата <b>{escape(shared_chat['title'])}</b>:",
        continuation_heading=f"📚 Продолжение цитат из чата <b>{escape(shared_chat['title'])}</b>:",
    )
    for index, chunk in enumerate(chunks, start=1):
        reply_markup = private_chat_actions_keyboard(chat_id) if index == len(chunks) else None
        await callback.message.answer(
            chunk + (f"\n\nВсего цитат: {len(quotes)}" if index == len(chunks) else ""),
            reply_markup=reply_markup,
        )

    await callback.answer("Показываю весь цитатник 📚")
