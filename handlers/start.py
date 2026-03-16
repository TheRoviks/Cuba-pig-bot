from datetime import datetime
from time import monotonic

from aiogram import F, Router
from aiogram.enums import ChatType
from aiogram.filters import Command
from aiogram.types import ChatMemberUpdated, Message

from database import QuoteDatabase
from keyboards.inline import private_dashboard_keyboard
from utils.security import is_global_admin


# Отдельный роутер для стартовых команд и приветствий.
router = Router()
WATERMARK_TEXT = "\n\nСоздано: @TheNekitttt"
JOIN_GREETING_COOLDOWN = 10.0
_last_join_greetings: dict[int, float] = {}


# Короткая справка для групп.
GROUP_HELP_TEXT = """
Вот что я умею:

/save_quote - сохранить цитату из реплая
/q - случайная цитата
/quote - тоже случайная цитата
/quote_random - еще одна случайная цитата
/quote_user - цитаты человека
/my_quotes - последние цитаты, которые добавил ты
/quotes_count - сколько цитат в этом чате
/top_quoted - кто чаще всех попадает в цитатник
/delete_quote - удалить цитату по id
/search_quote - поиск по тексту
/latest_quotes - последние сохраненные цитаты
/all_quotes - все цитаты этого чата списком
/quote_id - показать цитату по id

В личке со мной ещё есть:
/my_chats
/my_quotes_global
/my_quotes_chat
/all_quotes_chat
""".strip() + WATERMARK_TEXT


# Отдельная справка для лички.
PRIVATE_HELP_TEXT = """
В личке я показываю твой мини-кабинет 🐗

/my_chats - наши общие чаты
/my_quotes_global - сколько твоих цитат по чатам
/my_quotes_chat - твои цитаты из выбранного чата
/all_quotes_chat - все цитаты выбранного чата

Примеры:
/my_quotes_chat 1
/my_quotes_chat -1001234567890
/all_quotes_chat 1
""".strip() + WATERMARK_TEXT


# Приветствие для группы или супергруппы.
def get_group_start_text() -> str:
    return (
        "Йо, я Кабанчик Куба 🐗\n"
        "Я собираю мемные цитаты именно из этого чата 😎\n\n"
        "Ответь на сообщение и напиши /save_quote, и я всё запомню.\n"
        "Если хочешь случайную цитату, жми /q\n"
        "А если нужен список команд, есть /help"
    ) + WATERMARK_TEXT


# Приветствие для лички с ботом.
def get_private_start_text() -> str:
    return (
        "Йо, я Кабанчик Куба 🐗\n"
        "В личке у меня мини-кабинет для тебя.\n\n"
        "Тут можно посмотреть, в каких чатах мы пересекаемся, "
        "и где тебя уже растащили на цитаты 😎\n"
        "Жми кнопки ниже или используй команды /my_chats и /my_quotes_global"
    ) + WATERMARK_TEXT


# Приветствие, когда бота добавили в новый чат.
def get_join_text() -> str:
    return (
        "Всем привет 👋\n"
        "Я Кабанчик Куба 🐗 и уже засел в этом чате, чтобы собирать ваши легендарные фразы 😎\n\n"
        "Сразу маленькая просьба: лучше выдайте мне админку 👑\n"
        "Не потому что я хочу захватить чат, а чтобы мне было проще стабильно жить тут "
        "и чтобы потом можно было без боли докидывать новые фишки.\n\n"
        "Без админки я всё равно работаю, просто с ней спокойнее и удобнее.\n"
        "И да, не переживайте: у этого чата свой отдельный цитатник, "
        "из других групп сюда ничего не пролезет.\n\n"
        "Чтобы сохранить цитату, ответьте на сообщение и напишите /save_quote\n"
        "Если можете, сразу после добавления выдайте мне права администратора."
    ) + WATERMARK_TEXT


def should_send_join_greeting(chat_id: int) -> bool:
    now = monotonic()
    last_sent_at = _last_join_greetings.get(chat_id)
    if last_sent_at is not None and now - last_sent_at < JOIN_GREETING_COOLDOWN:
        return False

    _last_join_greetings[chat_id] = now
    return True


async def send_join_greeting(message: Message, db: QuoteDatabase) -> None:
    if message.chat.type not in {ChatType.GROUP, ChatType.SUPERGROUP}:
        return

    if not should_send_join_greeting(message.chat.id):
        return

    now = datetime.now().isoformat(timespec="seconds")
    chat_title = (message.chat.title or "Без названия").strip() or "Без названия"

    db.upsert_chat(
        chat_id=message.chat.id,
        title=chat_title,
        chat_type=message.chat.type,
        is_active=1,
        timestamp=now,
    )
    await message.answer(get_join_text())


# Смотрим, что бот реально стал участником чата.
def became_active_member(event: ChatMemberUpdated) -> bool:
    old_status = str(event.old_chat_member.status)
    new_status = str(event.new_chat_member.status)

    inactive_statuses = {"left", "kicked"}
    active_statuses = {"member", "administrator", "creator"}

    return old_status in inactive_statuses and new_status in active_statuses


# Смотрим, что бота убрали из чата.
def became_inactive_member(event: ChatMemberUpdated) -> bool:
    old_status = str(event.old_chat_member.status)
    new_status = str(event.new_chat_member.status)

    active_statuses = {"member", "administrator", "creator"}
    inactive_statuses = {"left", "kicked"}

    return old_status in active_statuses and new_status in inactive_statuses


@router.message(Command(commands=["start"]))
async def cmd_start(message: Message) -> None:
    # В личке показываем кабинет, в группе обычное приветствие.
    if message.chat.type == ChatType.PRIVATE:
        await message.answer(
            get_private_start_text(),
            reply_markup=private_dashboard_keyboard(is_admin=is_global_admin(message.from_user.id if message.from_user else None)),
        )
        return

    await message.answer(get_group_start_text())


@router.message(Command(commands=["help"]))
async def cmd_help(message: Message) -> None:
    # Контекстная справка: разная для лички и группы.
    if message.chat.type == ChatType.PRIVATE:
        await message.answer(
            PRIVATE_HELP_TEXT,
            reply_markup=private_dashboard_keyboard(is_admin=is_global_admin(message.from_user.id if message.from_user else None)),
        )
        return

    await message.answer(GROUP_HELP_TEXT)


@router.message(F.new_chat_members)
async def greet_when_bot_added_by_service_message(message: Message, db: QuoteDatabase) -> None:
    if message.chat.type not in {ChatType.GROUP, ChatType.SUPERGROUP}:
        return

    bot_was_added = any(member.id == message.bot.id for member in message.new_chat_members)
    if not bot_was_added:
        return

    await send_join_greeting(message, db)


@router.my_chat_member()
async def greet_on_join(event: ChatMemberUpdated, db: QuoteDatabase) -> None:
    # Личка тут не нужна, интересуют только группы.
    if event.chat.type not in {ChatType.GROUP, ChatType.SUPERGROUP}:
        return

    now = datetime.now().isoformat(timespec="seconds")
    chat_title = (event.chat.title or "Без названия").strip() or "Без названия"

    if became_active_member(event):
        # Помечаем чат активным и отправляем первое приветствие.
        db.upsert_chat(
            chat_id=event.chat.id,
            title=chat_title,
            chat_type=event.chat.type,
            is_active=1,
            timestamp=now,
        )
        if should_send_join_greeting(event.chat.id):
            await event.bot.send_message(
                chat_id=event.chat.id,
                text=get_join_text(),
            )
        return

    if became_inactive_member(event):
        # Если бота убрали, просто помечаем чат неактивным.
        db.upsert_chat(
            chat_id=event.chat.id,
            title=chat_title,
            chat_type=event.chat.type,
            is_active=0,
            timestamp=now,
        )
