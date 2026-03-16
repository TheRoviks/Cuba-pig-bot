import asyncio
import logging
from datetime import datetime

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramNetworkError, TelegramUnauthorizedError
from aiogram.types import BotCommand

from config import load_config
from database import QuoteDatabase
from handlers.admin import router as admin_router
from handlers.private import router as private_router
from handlers.quotes import router as quotes_router
from handlers.start import router as start_router
from middlewares.chat_activity import ChatActivityMiddleware


# Настраиваем обычные логи для запуска и ошибок.
def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


# Выставляем список команд, чтобы они были видны в Telegram.
# Если сеть временно лежит, не валим весь бот сразу.
async def set_main_commands(bot: Bot, attempts: int = 4, base_delay: float = 3.0) -> bool:
    commands = [
        BotCommand(command="start", description="запустить Кабанчика"),
        BotCommand(command="help", description="список команд"),
        BotCommand(command="save_quote", description="сохранить цитату из реплая"),
        BotCommand(command="q", description="случайная цитата"),
        BotCommand(command="quote", description="случайная цитата"),
        BotCommand(command="quote_random", description="ещё одна случайная цитата"),
        BotCommand(command="quote_user", description="цитаты конкретного человека"),
        BotCommand(command="my_quotes", description="что ты сохранил"),
        BotCommand(command="quotes_count", description="сколько цитат в этом чате"),
        BotCommand(command="top_quoted", description="топ самых цитируемых"),
        BotCommand(command="delete_quote", description="удалить цитату по id"),
        BotCommand(command="search_quote", description="поиск по тексту"),
        BotCommand(command="latest_quotes", description="последние цитаты"),
        BotCommand(command="all_quotes", description="все цитаты этого чата"),
        BotCommand(command="quote_id", description="показать цитату по id"),
        BotCommand(command="my_chats", description="общие чаты в личке"),
        BotCommand(command="my_quotes_global", description="твои цитаты по чатам"),
        BotCommand(command="my_quotes_chat", description="твои цитаты из одного чата"),
        BotCommand(command="all_quotes_chat", description="все цитаты из одного чата"),
    ]

    for attempt in range(1, attempts + 1):
        try:
            await bot.set_my_commands(commands)
            return True
        except TelegramNetworkError as exc:
            if attempt >= attempts:
                logging.warning(
                    "Could not set bot commands after %s attempts: %s",
                    attempt,
                    exc,
                )
                return False

            delay = base_delay * attempt
            logging.warning(
                "Failed to set bot commands on attempt %s/%s: %s. Retrying in %.1f sec.",
                attempt,
                attempts,
                exc,
                delay,
            )
            await asyncio.sleep(delay)

    return False


# Главная точка запуска бота.
async def main() -> None:
    # Сначала поднимаем логи и читаем настройки из .env.
    setup_logging()
    config = load_config()

    # База создаётся автоматически при первом старте.
    db = QuoteDatabase(config.db_path)
    db.init_db()

    # ParseMode HTML нужен для аккуратного форматирования текста.
    session_kwargs: dict[str, object] = {"timeout": 120}
    if config.proxy_url:
        session_kwargs["proxy"] = config.proxy_url
        logging.info("Proxy mode enabled for Telegram connection")

    try:
        session = AiohttpSession(**session_kwargs)
    except RuntimeError as exc:
        raise RuntimeError(
            "Proxy is configured, but proxy support dependency is missing. "
            "Install requirements again."
        ) from exc

    bot = Bot(
        token=config.bot_token,
        session=session,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )

    # В диспетчер кладём базу, чтобы потом получать её в хендлерах.
    dp = Dispatcher()
    dp["db"] = db
    dp["started_at"] = datetime.now()
    dp["project_version"] = config.project_version

    # Этот middleware обновляет известные чаты и замеченных пользователей.
    dp.message.outer_middleware(ChatActivityMiddleware())

    # Подключаем роутеры: старт, личка, админка и цитаты.
    dp.include_router(start_router)
    dp.include_router(private_router)
    dp.include_router(admin_router)
    dp.include_router(quotes_router)

    try:
        commands_updated = await set_main_commands(bot)
        if commands_updated:
            logging.info("Bot commands updated successfully")
        else:
            logging.warning("Bot commands were not updated at startup")

        logging.info("Kabanchik Kuba is starting")
        await dp.start_polling(bot)
    except TelegramUnauthorizedError:
        logging.error(
            "Telegram rejected BOT_TOKEN. Check BOT_TOKEN in .env or generate a new token in BotFather."
        )
    finally:
        await bot.session.close()


if __name__ == "__main__":
    try:
        # Запускаем polling в обычном asyncio-цикле.
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("Bot stopped")
