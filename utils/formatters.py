from datetime import datetime
from html import escape


# Имя пользователя форматируем безопасно, без поломки HTML.
def format_person(name: str | None, username: str | None = None) -> str:
    safe_name = escape((name or "Неизвестный").strip() or "Неизвестный")
    if username:
        return f"{safe_name} (@{escape(username)})"
    return safe_name


# Дату приводим к короткому виду YYYY-MM-DD.
def nice_date(value: str | None) -> str:
    if not value:
        return "без даты"

    try:
        dt = datetime.fromisoformat(value)
        return dt.date().isoformat()
    except ValueError:
        return value[:10]


def trim_preview(text: str, limit: int = 80) -> str:
    clean = " ".join((text or "").split())
    if len(clean) <= limit:
        return clean
    return clean[: limit - 1].rstrip() + "…"


# Собираем красивый текст одной цитаты.
def format_quote(quote: dict) -> str:
    quote_id = quote.get("id", "?")
    quote_text = escape(quote.get("quote_text", "").strip())
    quoted_name = format_person(
        quote.get("quoted_name"),
        quote.get("quoted_username"),
    )
    saved_by = escape((quote.get("saved_by_name") or "Неизвестный").strip() or "Неизвестный")
    created_at = nice_date(quote.get("created_at"))

    return (
        f"💬 Цитата #{quote_id}\n\n"
        f"\"{quote_text}\"\n\n"
        f"— {quoted_name}\n\n"
        f"📌 Добавил: {saved_by}\n"
        f"📅 {created_at}"
    )


def format_quotes_catalog(
    quotes: list[dict],
    heading: str = "📚 Все цитаты этого чата:",
    continuation_heading: str = "📚 Продолжение списка цитат:",
) -> list[str]:
    if not quotes:
        return []

    chunks: list[str] = []
    current_lines = [heading, ""]

    for position, quote in enumerate(quotes, start=1):
        quote_id = quote.get("id", "?")
        preview = escape(trim_preview(quote.get("quote_text") or ""))
        author = format_person(quote.get("quoted_name"), quote.get("quoted_username"))
        line = f"{position}. \"{preview}\" — {author} <code>[id {quote_id}]</code>"

        projected = "\n".join(current_lines + [line])
        if len(projected) > 3500 and len(current_lines) > 2:
            chunks.append("\n".join(current_lines))
            current_lines = [continuation_heading, "", line]
            continue

        current_lines.append(line)

    if current_lines:
        chunks.append("\n".join(current_lines))

    return chunks


# Делаем текст для топа цитируемых.
def format_top_list(items: list[dict]) -> str:
    lines = ["📜 Топ цитируемых в этом чате:"]

    for index, item in enumerate(items, start=1):
        person = format_person(item.get("quoted_name"), item.get("quoted_username"))
        total = item.get("total", 0)
        lines.append(f"{index}. {person} — {total}")

    return "\n".join(lines)


# Форматируем список общих чатов в личке.
def format_shared_chats(chats: list[dict]) -> str:
    lines = ["Вот где мы с тобой пересекаемся 👇", ""]

    for index, chat in enumerate(chats, start=1):
        lines.append(f"{index}. {escape(chat['title'])}")

    lines.append("")
    lines.append("Можно открыть цитаты по номеру через /my_quotes_chat 1")
    return "\n".join(lines)


# Форматируем общую сводку по цитатам пользователя.
def format_private_quotes_summary(summary: list[dict]) -> str:
    lines = ["Твои цитаты по чатам 🐗", ""]

    for item in summary:
        lines.append(f"📍 {escape(item['title'])} — {item['total']} цитат")

    return "\n".join(lines)


# Форматируем несколько последних цитат пользователя из выбранного чата.
def format_private_chat_quotes(chat: dict, quotes: list[dict]) -> str:
    lines = [
        f"Твои последние цитаты из чата <b>{escape(chat['title'])}</b> 🐗",
        "Это не весь цитатник чата, а только твои цитаты.",
        "",
    ]

    for quote in quotes:
        quote_id = quote.get("id", "?")
        quote_text = escape((quote.get("quote_text") or "").strip())
        saved_by = escape((quote.get("saved_by_name") or "Неизвестный").strip() or "Неизвестный")
        created_at = nice_date(quote.get("created_at"))
        lines.append(
            f"#{quote_id} — \"{quote_text}\"\n"
            f"📌 Добавил: {saved_by} | 📅 {created_at}"
        )

    return "\n\n".join(lines)


# Короткий статус активности чата для админки.
def admin_chat_status(is_active: int | bool) -> str:
    return "active" if is_active else "inactive"


# Форматируем общую статистику по боту.
def format_admin_stats(stats: dict) -> str:
    lines = [
        "Админ-статистика 👑",
        "",
        f"🏘 Всего чатов: {stats['total_chats']}",
        f"✅ Активных чатов: {stats['active_chats']}",
        f"👥 Замечено пользователей: {stats['total_seen_users']}",
        f"💬 Всего цитат: {stats['total_quotes']}",
        f"⏱ Цитат за 24ч: {stats['quotes_last_24h']}",
        f"📅 Цитат за 7 дней: {stats['quotes_last_7d']}",
        "",
        "Топ чатов по цитатам:",
    ]

    top_chats = stats.get("top_chats", [])
    if top_chats:
        for item in top_chats:
            lines.append(f"• {escape(item['title'])} — {item['total']}")
    else:
        lines.append("• пока пусто")

    lines.append("")
    lines.append("Топ тех, кто сохраняет:")
    top_savers = stats.get("top_savers", [])
    if top_savers:
        for item in top_savers:
            lines.append(f"• {escape(item.get('saved_by_name') or 'Неизвестный')} — {item['total']}")
    else:
        lines.append("• пока пусто")

    lines.append("")
    lines.append("Топ цитируемых глобально:")
    top_quoted = stats.get("top_quoted", [])
    if top_quoted:
        for item in top_quoted:
            lines.append(f"• {format_person(item.get('quoted_name'), item.get('quoted_username'))} — {item['total']}")
    else:
        lines.append("• пока пусто")

    return "\n".join(lines)


# Форматируем список всех чатов для админа.
def format_admin_chats(chats: list[dict]) -> str:
    lines = ["Все чаты бота 🏘", ""]

    if not chats:
        lines.append("Пока чатов нет 🤔")
        return "\n".join(lines)

    for chat in chats:
        lines.append(
            f"<b>{escape(chat['title'])}</b>\n"
            f"ID: <code>{chat['chat_id']}</code>\n"
            f"Статус: {admin_chat_status(chat['is_active'])}\n"
            f"Цитат: {chat['quote_count']}\n"
            f"Last seen: {escape(chat['last_seen_at'])}"
        )
        lines.append("")

    return "\n".join(lines).strip()


# Форматируем подробную статистику по одному чату.
def format_admin_chat_details(
    chat: dict,
    latest_quotes: list[dict],
    top_quoted: list[dict],
    top_savers: list[dict],
) -> str:
    lines = [
        "Статистика чата 🔍",
        "",
        f"<b>{escape(chat['title'])}</b>",
        f"ID: <code>{chat['chat_id']}</code>",
        f"Тип: {escape(chat['chat_type'])}",
        f"Статус: {admin_chat_status(chat['is_active'])}",
        f"Добавлен: {escape(chat['added_at'])}",
        f"Last seen: {escape(chat['last_seen_at'])}",
        f"Цитат: {chat['quote_count']}",
        f"Замечено пользователей: {chat['seen_users_count']}",
        "",
        "Последние цитаты:",
    ]

    if latest_quotes:
        for quote in latest_quotes:
            lines.append(f"• #{quote['id']} — \"{escape((quote.get('quote_text') or '').strip())}\"")
    else:
        lines.append("• пока пусто")

    lines.append("")
    lines.append("Топ цитируемых:")
    if top_quoted:
        for item in top_quoted:
            lines.append(f"• {format_person(item.get('quoted_name'), item.get('quoted_username'))} — {item['total']}")
    else:
        lines.append("• пока пусто")

    lines.append("")
    lines.append("Топ сохранявших:")
    if top_savers:
        for item in top_savers:
            lines.append(f"• {escape(item.get('saved_by_name') or 'Неизвестный')} — {item['total']}")
    else:
        lines.append("• пока пусто")

    return "\n".join(lines)


# Форматируем срез по пользователям для админки.
def format_admin_users(
    top_savers: list[dict],
    top_quoted: list[dict],
    top_memberships: list[dict],
) -> str:
    lines = ["Пользователи 👥", "", "Кто чаще сохраняет:"]

    if top_savers:
        for item in top_savers:
            lines.append(f"• {escape(item.get('saved_by_name') or 'Неизвестный')} — {item['total']}")
    else:
        lines.append("• пока пусто")

    lines.append("")
    lines.append("Кого чаще цитируют:")
    if top_quoted:
        for item in top_quoted:
            lines.append(f"• {format_person(item.get('quoted_name'), item.get('quoted_username'))} — {item['total']}")
    else:
        lines.append("• пока пусто")

    lines.append("")
    lines.append("Кто в скольких чатах замечен:")
    if top_memberships:
        for item in top_memberships:
            lines.append(
                f"• {format_person(item.get('user_name'), item.get('username'))} — {item['chat_count']} чатов"
            )
    else:
        lines.append("• пока пусто")

    return "\n".join(lines)


# Форматируем health-инфу.
def format_admin_health(health: dict) -> str:
    lines = [
        "Health 🩺",
        "",
        f"DB: {'ok' if health['db_ok'] else 'fail'}",
        f"Путь к БД: <code>{escape(health['db_path'])}</code>",
        f"Таблица chats: {health['chats_total']}",
        f"Таблица user_chat_links: {health['links_total']}",
        f"Таблица quotes: {health['quotes_total']}",
        f"Uptime: {escape(health['uptime'])}",
        f"Версия: {escape(health['project_version'])}",
        f"Polling: {escape(health['polling_status'])}",
    ]
    return "\n".join(lines)
