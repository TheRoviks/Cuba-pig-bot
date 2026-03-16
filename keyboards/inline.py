from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup


# Кнопки под каждой цитатой: показать ещё или удалить.
def quote_actions_keyboard(quote_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🎲 Еще цитата",
                    callback_data=f"quote_more:{quote_id}",
                ),
                InlineKeyboardButton(
                    text="🗑 Удалить",
                    callback_data=f"quote_del:{quote_id}",
                ),
            ]
        ]
    )


# Главное меню в личке с ботом.
def private_dashboard_keyboard(is_admin: bool = False) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(
                text="📂 Мои чаты",
                callback_data="priv:my_chats",
            ),
            InlineKeyboardButton(
                text="💬 Мои цитаты",
                callback_data="priv:my_quotes_global",
            ),
        ],
        [
            InlineKeyboardButton(
                text="🔎 Выбрать чат",
                callback_data="priv:pick_chat",
            )
        ],
    ]

    if is_admin:
        rows.append(
            [
                InlineKeyboardButton(
                    text="👑 Админка",
                    callback_data="adm:panel",
                )
            ]
        )

    return InlineKeyboardMarkup(inline_keyboard=rows)


# Кнопки со списком доступных чатов.
def private_chat_picker_keyboard(chats: list[dict], limit: int = 8) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []

    for index, chat in enumerate(chats[:limit], start=1):
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"{index}. {chat['title']}",
                    callback_data=f"priv:chat_menu:{chat['chat_id']}",
                )
            ]
        )

    rows.append(
        [
            InlineKeyboardButton(
                text="📂 Общие чаты",
                callback_data="priv:my_chats",
            ),
            InlineKeyboardButton(
                text="💬 Сводка",
                callback_data="priv:my_quotes_global",
            ),
        ]
    )

    return InlineKeyboardMarkup(inline_keyboard=rows)


# Кнопки под сводкой по чатам.
def private_summary_keyboard(summary: list[dict], limit: int = 8) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []

    for item in summary[:limit]:
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"📍 {item['title']}",
                    callback_data=f"priv:chat_menu:{item['chat_id']}",
                )
            ]
        )

    rows.append(
        [
            InlineKeyboardButton(
                text="🔎 Выбрать чат",
                callback_data="priv:pick_chat",
            )
        ]
    )

    return InlineKeyboardMarkup(inline_keyboard=rows)


def private_chat_actions_keyboard(chat_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="💬 Мои цитаты",
                    callback_data=f"priv:chat_quotes:{chat_id}",
                ),
                InlineKeyboardButton(
                    text="📚 Все цитаты",
                    callback_data=f"priv:chat_all_quotes:{chat_id}",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="🔎 Выбрать другой чат",
                    callback_data="priv:pick_chat",
                )
            ],
        ]
    )


# Главное меню глобальной админки.
def admin_panel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="📊 Общая статистика",
                    callback_data="adm:stats",
                ),
                InlineKeyboardButton(
                    text="🏘 Все чаты",
                    callback_data="adm:chats",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="👥 Пользователи",
                    callback_data="adm:users",
                ),
                InlineKeyboardButton(
                    text="🩺 Health",
                    callback_data="adm:health",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="🚫 Закрыть",
                    callback_data="adm:close",
                )
            ],
        ]
    )


# Несколько быстрых кнопок по чатам для админа.
def admin_chats_keyboard(chats: list[dict], limit: int = 6) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []

    for chat in chats[:limit]:
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"🏘 {chat['title']}",
                    callback_data=f"adm:chat:{chat['chat_id']}",
                )
            ]
        )

    rows.append(
        [
            InlineKeyboardButton(
                text="📊 Общая статистика",
                callback_data="adm:stats",
            ),
            InlineKeyboardButton(
                text="🚫 Закрыть",
                callback_data="adm:close",
            ),
        ]
    )

    return InlineKeyboardMarkup(inline_keyboard=rows)
