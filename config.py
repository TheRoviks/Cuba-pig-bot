import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote

from dotenv import load_dotenv


# Корневая папка проекта.
BASE_DIR = Path(__file__).resolve().parent


# Версию можно показывать в health-статистике.
PROJECT_VERSION = "1.4.0"


# Простой контейнер для настроек.
@dataclass(slots=True)
class Config:
    bot_token: str
    db_path: str
    project_version: str
    proxy_url: str | None


def build_proxy_url() -> str | None:
    raw_proxy_url = os.getenv("PROXY_URL", "").strip()
    if raw_proxy_url:
        return raw_proxy_url

    host = os.getenv("PROXY_HOST", "").strip()
    port_raw = os.getenv("PROXY_PORT", "").strip()
    if not host and not port_raw:
        return None

    if not host:
        raise RuntimeError("PROXY_HOST is empty.")
    if not port_raw:
        raise RuntimeError("PROXY_PORT is empty.")

    try:
        port = int(port_raw)
    except ValueError as exc:
        raise RuntimeError("PROXY_PORT must be an integer.") from exc

    if port <= 0 or port > 65535:
        raise RuntimeError("PROXY_PORT must be between 1 and 65535.")

    scheme = os.getenv("PROXY_SCHEME", "socks5").strip().lower() or "socks5"
    login = os.getenv("PROXY_LOGIN", "").strip()
    password = os.getenv("PROXY_PASSWORD", "").strip()

    auth_part = ""
    if login:
        auth_part = quote(login, safe="")
        if password:
            auth_part += f":{quote(password, safe='')}"
        auth_part += "@"

    return f"{scheme}://{auth_part}{host}:{port}"


# Загружаем .env и приводим путь к базе к нормальному виду.
def load_config() -> Config:
    load_dotenv(BASE_DIR / ".env", override=True)

    bot_token = os.getenv("BOT_TOKEN", "").strip()
    db_path = os.getenv("DB_PATH", "quotes.db").strip() or "quotes.db"
    proxy_url = build_proxy_url()

    # Без токена бот просто не сможет стартовать.
    if not bot_token:
        raise RuntimeError("BOT_TOKEN is empty. Fill in .env first.")

    db_file = Path(db_path)
    # Если путь относительный, привязываем его к папке проекта.
    if not db_file.is_absolute():
        db_file = (BASE_DIR / db_file).resolve()

    return Config(
        bot_token=bot_token,
        db_path=str(db_file),
        project_version=PROJECT_VERSION,
        proxy_url=proxy_url,
    )
