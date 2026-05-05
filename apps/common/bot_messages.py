from __future__ import annotations


START_MESSAGE = (
    "Привет! Я ищу пакеты по источникам registry + scraper, индексирую результаты в Memgraph "
    "и могу отвечать как с LLM, так и без генерации LLM.\n\n"
    "Как использовать:\n"
    "- Просто напиши запрос обычным текстом. По умолчанию я использую локальный cache-first поиск.\n"
    "- Если не указать /llm или /nollm, используется режим из .env: сейчас auto -> no-LLM, "
    "потому что DISABLE_LLM_ANSWERS=true.\n"
    "- /llm <запрос> - генеративно оформленный ответ, если LLM доступна.\n"
    "- /nollm <запрос> - строгий шаблонный ответ без генерации LLM.\n\n"
    "Примеры:\n"
    "- дай список всех версий PostgreSQL для debian 13\n"
    "- дай список всех пакетов PostgreSQL 17.6\n"
    "- Python 3.12 для Ubuntu limit=10\n\n"
    "Фильтры:\n"
    "- format=deb|rpm|apk|exe\n"
    "- source=<часть имени источника>\n"
    "- sort=newest|oldest|name\n"
    "- limit=10\n"
    "- show=5"
)

EMPTY_QUERY_MESSAGE = "Пустой запрос. Добавьте текст вопроса."
INIT_MESSAGE = "Бот еще инициализируется, попробуйте снова через несколько секунд."
TEMP_ERROR_MESSAGE = "Временная ошибка обработки запроса. Попробуйте снова."
