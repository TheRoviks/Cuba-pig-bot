import logging

from aiogram.enums import ChatType
from aiogram.types import CallbackQuery, Message

from admins import ADMIN_USER_IDS_SET


logger = logging.getLogger(__name__)


# Проверяем, входит ли пользователь в список глобальных админов.
def is_global_admin(user_id: int | None) -> bool:
    return user_id is not None and user_id in ADMIN_USER_IDS_SET


# Базовая проверка: команда админки только в личке и только для глобального админа.
async def ensure_admin_message(message: Message, action: str) -> bool:
    user_id = message.from_user.id if message.from_user else None

    if message.chat.type != ChatType.PRIVATE:
        logger.info("admin-deny non-private action=%s user_id=%s chat_id=%s", action, user_id, message.chat.id)
        await message.answer("Админка работает только в личке 👀")
        return False

    if not is_global_admin(user_id):
        logger.info("admin-deny forbidden action=%s user_id=%s", action, user_id)
        await message.answer("Доступа нет 🤔")
        return False

    return True


# Та же проверка, но для inline callback.
async def ensure_admin_callback(callback: CallbackQuery, action: str) -> bool:
    if not callback.message:
        await callback.answer()
        return False

    user_id = callback.from_user.id if callback.from_user else None

    if callback.message.chat.type != ChatType.PRIVATE:
        logger.info("admin-callback-deny non-private action=%s user_id=%s chat_id=%s", action, user_id, callback.message.chat.id)
        await callback.answer("Админка только в личке 👀", show_alert=True)
        return False

    if not is_global_admin(user_id):
        logger.info("admin-callback-deny forbidden action=%s user_id=%s", action, user_id)
        await callback.answer("Доступа нет 🤔", show_alert=True)
        return False

    return True
