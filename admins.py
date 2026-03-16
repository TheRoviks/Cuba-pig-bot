import os
from pathlib import Path

from dotenv import load_dotenv


# Отдельное место, где настраиваются глобальные админы бота.
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")


def _parse_admin_ids(raw_value: str) -> list[int]:
    admin_ids: list[int] = []

    for chunk in raw_value.split(","):
        item = chunk.strip()
        if not item:
            continue

        try:
            admin_ids.append(int(item))
        except ValueError:
            continue

    return admin_ids


# Глобальные админы задаются через .env, например: ADMIN_USER_IDS=123,456
ADMIN_USER_IDS = _parse_admin_ids(os.getenv("ADMIN_USER_IDS", ""))
ADMIN_USER_IDS_SET = set(ADMIN_USER_IDS)
