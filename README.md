<h1 align="center">RAGU: Retrieval-Augmented Graph Utility</h1>

---

<p align="center">
<img src="assets/ragu_image.jpg" alt="Логотип RAGU" width="600" />
</p>

<h4 align="center">
  <a href="https://github.com/AsphodelRem/RAGU/blob/main/LICENSE">
    <img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="RAGU распространяется по лицензии MIT." />
  </a>
  <img src="https://img.shields.io/badge/python->=3.10-blue" alt="Python >= 3.10" />
</h4>

<h4 align="center">
  <a href="#установка">Установка</a> |
  <a href="#быстрый-старт">Быстрый старт</a>
</h4>

---

## Обзор

RAGU предоставляет пайплайн для построения **графа знаний** и поиска по проиндексированным данным. Проект содержит несколько подходов к извлечению структурированных сущностей и связей из обычных текстов, чтобы на их основе можно было отвечать на вопросы по накопленному знанию.

Проект частично основан на [nano-graphrag](https://github.com/gusye1234/nano-graphrag/tree/main).

Сообщество на Hugging Face: [RaguTeam](https://huggingface.co/RaguTeam/).

---

## Демо для дипломного проекта

Текущий локальный стенд демонстрирует интеграционный сервис поиска программных пакетов вокруг RAGU, Memgraph, FastAPI, Ollama, VK и Google Sheets.

Основной runbook: [docs/LOCAL_OLLAMA_STACK.md](docs/LOCAL_OLLAMA_STACK.md)  
Безопасный шаблон конфигурации: [.env.example](.env.example)

Ключевое поведение:

- Docker Compose запускает Memgraph, FastAPI, orchestrator и опциональные сервисы Sheets/VK.
- Ollama остается локальным сервисом на хосте и отдает контейнерам OpenAI-совместимый endpoint.
- `VK_BOT_ENABLED=true|false` управляет опциональным профилем VK-бота.
- Запросы по пакетам сначала проходят через parser/registry/scraper pipeline.
- Обычные IT-вопросы про Docker, PostgreSQL, Git, API, embeddings или поведение VK-бота маршрутизируются в локальный семантический поиск по графу.
- `/llm <запрос>` берет подготовленный пакетный или графовый контекст и просит локальную LLM оформить ответ.
- `/nollm <запрос>` возвращает детерминированный шаблонный или семантический ответ без генерации LLM.
- Для обычных запросов используется настройка `.env`: `DISABLE_LLM_ANSWERS=true` включает no-LLM по умолчанию, `false` включает LLM-оформление.
- LLM не скрейпит, не открывает сайты и не ищет источники самостоятельно. Она только оформляет структурированные данные, которые уже собрал Python-код.
- Поддерживаемые форматы пакетов: `deb`, `rpm`, `exe`.
- Подготовленные IT-ответы загружаются только явной командой через `/ingest/json`.

Полезные локальные команды:

```powershell
.\scripts\start_ollama_stack.ps1
.\venv\Scripts\python.exe scripts\check_demo_health.py
.\venv\Scripts\python.exe scripts\check_google_sheets.py
.\venv\Scripts\python.exe scripts\seed_demo_it_knowledge.py --api-base-url http://127.0.0.1:8000
.\scripts\stop_ollama_stack.ps1
```

Примеры запросов для демонстрации:

```text
/nollm Python 3.12 для Ubuntu limit=10
/llm дай список всех пакетов PostgreSQL 17.6 limit=13 show=4
покажи последние версии postgresql для ubuntu 24 выведи 5
скачай Foo Package для Solaris format=deb
```

Запрос с неподдерживаемым источником должен возвращать понятное сообщение об ошибке и не должен переиспользовать старые ответы из графа как успешный результат.

---

## Установка

Рекомендуемый вариант для разработки - локальная установка из исходников:

```commandline
git clone https://github.com/AsphodelRem/RAGU.git
cd RAGU
uv pip install -e .
```

Установка из PyPI:

```bash
pip install graph_ragu
```

Если нужны локальные модели через `transformers` и смежные зависимости:

```bash
pip install graph_ragu[local]
```

---

## Быстрый старт

### Простой пример построения графа знаний

```python
import asyncio

from ragu import (
    SimpleChunker,
    KnowledgeGraph,
    BuilderArguments,
    Settings,
    ArtifactsExtractorLLM,
)
from ragu.llm import OpenAIClient
from ragu.embedder import OpenAIEmbedder

from ragu.utils.ragu_utils import read_text_from_files

# Конфигурация. Вместо констант можно загрузить значения через ragu.Env из .env.
LLM_MODEL_NAME = "openai/gpt-4o-mini"
LLM_BASE_URL = "https://api.openai.com/v1"
LLM_API_KEY = "your-api-key-here"

EMBEDDER_MODEL_NAME = "text-embedding-3-large"


async def main():
    # Настройка рабочей директории и языка.
    Settings.storage_folder = "ragu_working_dir"
    Settings.language = "russian"

    # Загрузка документов из папки.
    docs = read_text_from_files("path/to/your/data")

    # Инициализация chunker.
    chunker = SimpleChunker(max_chunk_size=1000)

    # Клиент LLM.
    client = OpenAIClient(
        model_name=LLM_MODEL_NAME,
        base_url=LLM_BASE_URL,
        api_token=LLM_API_KEY,
        max_requests_per_second=1,
        max_requests_per_minute=60,
        cache_flush_every=10,
    )

    # Извлекатель артефактов.
    artifact_extractor = ArtifactsExtractorLLM(
        client=client,
        do_validation=False
    )

    # Embedder.
    embedder = OpenAIEmbedder(
        model_name=EMBEDDER_MODEL_NAME,
        base_url=LLM_BASE_URL,
        api_token=LLM_API_KEY,
        dim=3072,
        max_requests_per_second=1,
        max_requests_per_minute=60,
        use_cache=True,
    )

    # Настройки построения графа.
    builder_settings = BuilderArguments(
        use_llm_summarization=True,
        vectorize_chunks=True,
    )

    # Построение графа знаний.
    knowledge_graph = await KnowledgeGraph(
        client=client,
        embedder=embedder,
        chunker=chunker,
        artifact_extractor=artifact_extractor,
        builder_settings=builder_settings,
    ).build_from_docs(docs)


if __name__ == "__main__":
    asyncio.run(main())
```

> Если рабочая директория уже содержит граф знаний, RAGU автоматически загрузит существующее состояние.

### Пример запроса

**Local search**  
Ищет по сущностям, найденным для запроса, и по связанному с ними контексту: relations, summaries и chunks.

```python
from ragu import LocalSearchEngine

local_search = LocalSearchEngine(
    client,
    knowledge_graph,
    embedder,
    tokenizer_model="gpt-4o-mini",
)
local_answer = await local_search.a_query("Кто написал Ромео и Джульетту?")
print(local_answer)
```

#### Global search

Формирует ответ на основе summary-сообществ графа.

```python
from ragu import GlobalSearchEngine

global_search = GlobalSearchEngine(
    client=client,
    knowledge_graph=knowledge_graph,
)
global_answer = await global_search.a_query("Ваш широкий запрос")
print(global_answer)
```

**Naive search (vector RAG):**

```python
from ragu import NaiveSearchEngine

naive_search = NaiveSearchEngine(
    client=client,
    knowledge_graph=knowledge_graph,
    embedder=embedder,
)
naive_answer = await naive_search.a_query("Ваш запрос")
print(naive_answer)
```

### Обертка query planning

`QueryPlanEngine` разбивает сложный вопрос на зависимые подзапросы, выполняет их по порядку и использует промежуточные ответы для финального результата.

```python
from ragu import QueryPlanEngine

# Оберните любой базовый engine.
planned_local = QueryPlanEngine(local_search)
result = await planned_local.a_query("Какая столица Франции?")
print(result)

planned_global = QueryPlanEngine(global_search)
result = await planned_global.a_query("Ваш широкий запрос")
print(result)

planned_naive = QueryPlanEngine(naive_search)
result = await planned_naive.a_query("Ваш запрос")
print(result)
```

---

### Расширенная конфигурация

#### Настройки builder

Пайплайн построения графа знаний настраивается через `BuilderArguments`:

```python
from ragu import BuilderArguments

builder_arguments = BuilderArguments(
    use_llm_summarization=True,  # Включить LLM-суммаризацию сущностей и связей.
    use_clustering=False,  # Применять кластеризацию перед суммаризацией. Полезно при множестве похожих сущностей.
    build_only_vector_context=False,  # Пропустить извлечение графа и построить только chunk embeddings.
    make_community_summary=True,  # Генерировать summary-сообщества.
    remove_isolated_nodes=True,  # Удалять сущности без связей.
    vectorize_chunks=True,  # Векторизовать chunks для naive/vector search.
    cluster_only_if_more_than=10000,  # Минимум сущностей для запуска кластеризации.
    max_cluster_size=128,  # Максимум сущностей в одном кластере.
)

# Передача настроек в KnowledgeGraph.
knowledge_graph = await KnowledgeGraph(
    client=client,
    embedder=embedder,
    chunker=chunker,
    artifact_extractor=artifact_extractor,
    builder_settings=builder_arguments,
).build_from_docs(docs)
```

---

### Построение графа знаний

Каждый текст корпуса обрабатывается для извлечения структурированной информации. В граф попадают:

* **Сущности** - текстовое представление, тип сущности и контекстное описание.
* **Связи** - описание отношения между двумя сущностями или класс отношения, а также уверенность/сила связи.

> **RAGU использует классы сущностей и связей из [NEREL](https://github.com/nerel-ds/NEREL).**

### Типы сущностей

| № | Тип сущности | № | Тип сущности | № | Тип сущности |
|---|---|---|---|---|---|
|1.| AGE |11.| FAMILY |21.| PENALTY |
|2.| AWARD |12.| IDEOLOGY |22.| PERCENT |
|3.| CITY |13.| LANGUAGE |23.| PERSON |
|4.| COUNTRY |14.| LAW |24.| PRODUCT |
|5.| CRIME |15.| LOCATION |25.| PROFESSION |
|6.| DATE |16.| MONEY |26.| RELIGION |
|7.| DISEASE |17.| NATIONALITY |27.| STATE_OR_PROV |
|8.| DISTRICT |18.| NUMBER |28.| TIME |
|9.| EVENT |19.| ORDINAL |29.| WORK_OF_ART |
|10.| FACILITY |20.| ORGANIZATION | | |

### Типы связей

| № | Тип связи | № | Тип связи | № | Тип связи |
|---|---|---|---|---|---|
|1.| ABBREVIATION |18.| HEADQUARTERED_IN |35.| PLACE_RESIDES_IN |
|2.| AGE_DIED_AT |19.| IDEOLOGY_OF |36.| POINT_IN_TIME |
|3.| AGE_IS |20.| INANIMATE_INVOLVED |37.| PRICE_OF |
|4.| AGENT |21.| INCOME |38.| PRODUCES |
|5.| ALTERNATIVE_NAME |22.| KNOWS |39.| RELATIVE |
|6.| AWARDED_WITH |23.| LOCATED_IN |40.| RELIGION_OF |
|7.| CAUSE_OF_DEATH |24.| MEDICAL_CONDITION |41.| SCHOOLS_ATTENDED |
|8.| CONVICTED_OF |25.| MEMBER_OF |42.| SIBLING |
|9.| DATE_DEFUNCT_IN |26.| ORGANIZES |43.| SPOUSE |
|10.| DATE_FOUNDED_IN |27.| ORIGINS_FROM |44.| START_TIME |
|11.| DATE_OF_BIRTH |28.| OWNER_OF |45.| SUBEVENT_OF |
|12.| DATE_OF_CREATION |29.| PARENT_OF |46.| SUBORDINATE_OF |
|13.| DATE_OF_DEATH |30.| PART_OF |47.| TAKES_PLACE_IN |
|14.| END_TIME |31.| PARTICIPANT_IN |48.| WORKPLACE |
|15.| EXPENDITURE |32.| PENALIZED_AS |49.| WORKS_AS |
|16.| FOUNDED_BY |33.| PLACE_OF_BIRTH | | |
|17.| HAS_CAUSE |34.| PLACE_OF_DEATH | | |

### Как выполняется извлечение

#### 1. Базовый пайплайн

Файл: `ragu/triplet/llm_artifact_extractor.py`.

Базовый пайплайн использует LLM для извлечения сущностей, связей и их описаний за один шаг.

#### 2. [RAGU-lm](https://huggingface.co/RaguTeam/RAGU-lm) для русского языка

Компактная модель Qwen-3-0.6B, дообученная на датасете NEREL.

Пайплайн работает в несколько этапов:

1. Извлекает ненормализованные сущности из текста.
2. Нормализует сущности в канонические формы.
3. Генерирует описания сущностей.
4. Извлекает связи на основе inner product между сущностями.

### Сравнение

| Модель | Датасет | F1 (сущности) | F1 (связи) |
|---|---|---|---|
| Qwen-2.5-14B-Instruct | NEREL | 0.32 | 0.69 |
| RAGU-lm (Qwen-3-0.6B) | NEREL | 0.6 | 0.71 |
| Small-model pipeline | NEREL | 0.74 | 0.75 |

---

### Настройка prompt

Все компоненты RAGU, которые используют LLM, наследуются от `RaguGenerativeModule`. Он предоставляет методы для просмотра и обновления prompt.

#### Просмотр текущих prompt

```python
from ragu import LocalSearchEngine

search_engine = LocalSearchEngine(
    client,
    knowledge_graph,
    embedder
)

# Получить все prompt, используемые search engine.
all_prompts = search_engine.get_prompts()
print(all_prompts)
# Вернет: {'local_search': RAGUInstruction(...)}

# Получить конкретный prompt.
local_search_prompt = search_engine.get_prompt("local_search")
print(local_search_prompt.messages.to_str())
# Покажет фактический текст prompt: всю беседу одной строкой.

print(local_search_prompt.pydantic_model)
# Покажет pydantic-модель ответа.
```

#### Обновление prompt

Prompt можно настроить через новый `RAGUInstruction` с собственными сообщениями:

```python
from textwrap import dedent

from ragu.common.prompts.prompt_storage import RAGUInstruction
from ragu.common.prompts.messages import ChatMessages, UserMessage, SystemMessage
from ragu.common.prompts.default_models import DefaultResponseModel

# Создание собственной prompt-инструкции.
custom_instruction = RAGUInstruction(
    messages=ChatMessages.from_messages([
        SystemMessage(content="Вы полезный ассистент, специализирующийся на академических исследованиях."),
        UserMessage(content=dedent(
            """
            Ответьте на следующий запрос, используя предоставленный контекст.

            Запрос: {{ query }}
            Контекст: {{ context }}

            Язык: {{ language }}
            """
        ))
    ]),
    pydantic_model=DefaultResponseModel,  # Ваша pydantic-модель, если она нужна.
    description="Пользовательский prompt для local search с академическим фокусом"
)

# Обновление prompt.
search_engine.update_prompt("local_search", custom_instruction)
```

---

### Участники

#### **Идея и вдохновение**

- Ivan Bondarenko - идея, smart_chunker, NER-модель, ragu-lm

#### **Основная разработка**

- Mikhail Komarov

#### **Бенчмарки и оценка**

- Roman Shuvalov
- Yanya Dement'yeva
- Alexandr Kuleshevskiy
- Nikita Kukuzey
- Stanislav Shtuka

#### **Small Models Pipeline**

- Matvey Solovyev
- Ilya Myznikov
