import time


# Очень простой in-memory антиспам без лишней тяжести.
class SimpleRateLimiter:
    def __init__(self) -> None:
        self._hits: dict[tuple[int, str], float] = {}

    # Возвращаем оставшееся время, если пользователь упёрся в cooldown.
    def hit(self, user_id: int | None, key: str, cooldown: float) -> float:
        if user_id is None:
            return 0.0

        now = time.monotonic()
        token = (user_id, key)
        last_time = self._hits.get(token)

        if last_time is not None:
            delta = now - last_time
            if delta < cooldown:
                return cooldown - delta

        self._hits[token] = now
        return 0.0


rate_limiter = SimpleRateLimiter()
