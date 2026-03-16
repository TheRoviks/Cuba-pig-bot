from datetime import datetime
from typing import Any, Awaitable, Callable

from aiogram import BaseMiddleware
from aiogram.enums import ChatType
from aiogram.types import Message

from database import QuoteDatabase


class ChatActivityMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[Message, dict[str, Any]], Awaitable[Any]],
        event: Message,
        data: dict[str, Any],
    ) -> Any:
        db = data.get("db")

        if isinstance(event, Message) and isinstance(db, QuoteDatabase):
            now = datetime.now().isoformat(timespec="seconds")
            chat = event.chat

            if event.from_user:
                db.upsert_user(
                    user_id=event.from_user.id,
                    username=event.from_user.username,
                    first_name=event.from_user.first_name,
                    last_name=event.from_user.last_name,
                    full_name=event.from_user.full_name.strip() or "Nameless hero",
                    is_bot=event.from_user.is_bot,
                    timestamp=now,
                    last_private_seen_at=now if chat.type == ChatType.PRIVATE else None,
                )

            if chat.type in {ChatType.GROUP, ChatType.SUPERGROUP}:
                chat_title = (chat.title or "Untitled").strip() or "Untitled"
                db.upsert_chat(
                    chat_id=chat.id,
                    title=chat_title,
                    chat_type=chat.type,
                    is_active=1,
                    timestamp=now,
                )

                if event.from_user and not event.from_user.is_bot:
                    user_name = event.from_user.full_name.strip() or "Nameless hero"
                    db.upsert_user_chat_link(
                        user_id=event.from_user.id,
                        chat_id=chat.id,
                        user_name=user_name,
                        username=event.from_user.username,
                        timestamp=now,
                    )

        return await handler(event, data)
