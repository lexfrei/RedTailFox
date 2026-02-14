# RedTailFox Orchestrator 🦊

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.9%2B-blue" alt="Python">
  <img src="https://img.shields.io/badge/Docker-required-blue" alt="Docker">
  <img src="https://img.shields.io/badge/Redis-required-red" alt="Redis">
  <img src="https://img.shields.io/badge/License-MIT-green" alt="License">
</p>

<p align="center">
  <b>EN:</b> Lightweight Python orchestration over Docker & Redis •  
  <b>RU:</b> Легковесная оркестрация Python-воркеров •  
  <b>中文:</b> 基于 Docker 和 Redis 的轻量级 Python 编排
</p>

<p align="center">
  <i>Kubernetes — для космоса. RedTailFox — для вашего сервера.</i><br>
  <i>Kubernetes is for space. RedTailFox is for your server.</i>
</p>

---

## 🎯 Зачем это?

**Если коротко:** Kubernetes слишком жирный, Celery не умеет управлять контейнерами, а вручную поднимать 100 Docker-контейнеров и следить за ними — боль.

**RedTailFox** — это ровно тот слой оркестрации, который нужен, когда у вас:
- Много однотипных воркеров (боты, парсеры, автоответчики)
- Каждый воркер — отдельный аккаунт/задача со своим конфигом
- Нужно, чтобы система чинила себя сама
- Нет отдельного DevOps, но хочется жить спокойно

---

## 🔥 Возможности

| Feature | Description |
|---------|-------------|
| **Умные слоты** | Менеджер распределяет задачи по слотам внутри контейнеров |
| **Автомасштабирование** | При нехватке слотов автоматически поднимаются новые Docker-контейнеры |
| **Автовосстановление** | Монитор проверяет heartbeat'ы и перезапускает упавшие слоты/контейнеры |
| **Graceful shutdown** | Воркеры корректно завершаются и отписываются от менеджера |
| **Хранение конфигов в Redis** | Слот можно перезапустить даже без внешних данных |
| **Мониторинг через HeadBear** | Своя легкая система метрик (опционально) |
| **Никакого YAML** | Всё настраивается переменными окружения и Python-словарями |

---

## 🏗 Как это работает

```mermaid
graph TD
    %% Входные данные
    A["📥 ЗАДАЧА В REDIS<br/><b>autoreply_queue</b>"] --> B["⚙️ МЕНЕДЖЕР<br/><i>manager.py</b>"]
    
    %% Принятие решения
    B --> C{"🔍 ЕСТЬ СВОБОДНЫЙ<br/>СЛОТ В КОНТЕЙНЕРЕ?"}
    
    %% Ветка "Нет свободных слотов"
    C -->|❌ НЕТ| D["🐳 ЗАПУСТИТЬ DOCKER КОНТЕЙНЕР<br/><i>docker_utils.py</i><br/><b>worker_N</b>"]
    D --> E["📝 ЗАРЕГИСТРИРОВАТЬ КОНТЕЙНЕР<br/>в <b>manager:active_containers</b>"]
    
    %% Ветка "Есть свободный слот"
    C -->|✅ ДА| F["📋 НАЗНАЧИТЬ СЛОТ<br/>в <b>manager:slot_to_container</b>"]
    
    %% Объединение потоков
    E --> F
    F --> G["📤 КОМАНДА 'START'<br/>в очередь <b>COMMAND_CHANNEL_N</b>"]
    
    %% Работа воркера
    G --> H["👷 ВОРКЕР В КОНТЕЙНЕРЕ<br/><i>worker.py</i><br/>запускает слот"]
    H --> I["💓 HEARTBEAT КАЖДЫЕ 3 СЕК<br/>в <b>hb:container:name</b>"]
    
    %% Мониторинг
    I --> J["🔍 МОНИТОР<br/><i>monitor.py</i><br/>проверка каждые 10 сек"]
    
    %% Проверки монитора
    J --> K{"⏳ СЛОТ<br/>НЕАКТИВЕН >600с?"}
    K -->|ДА| L["🔄 КОМАНДА <b>restart_slot</b><br/>в autoreply_queue"]
    
    J --> M{"🔇 КОНТЕЙНЕР<br/>БЕЗ HEARTBEAT >500с?"}
    M -->|ДА| N["🚨 КОМАНДА <b>restart_container</b><br/>в autoreply_queue"]
    
    J --> O{"👻 КОНТЕЙНЕР-ЗОМБИ<br/>(есть в Docker, нет в Redis)?"}
    O -->|ДА| N
    
    %% Замыкание цикла
    L --> B
    N --> B
    
    %% Отчеты от воркера
    H --> P["📊 ОТЧЕТ О СТАТУСЕ<br/>в <b>WORKER_REPORTS</b>"]
    P --> Q["📝 ЗАПИСЬ В БАЗУ ДАННЫХ<br/>в <b>db_write_requests</b>"]
    
    %% Простая легенда
    subgraph Legend [УСЛОВНЫЕ ОБОЗНАЧЕНИЯ]
        direction LR
        L1["📥 Очередь Redis"] --- L2["⚙️ Компонент"] --- L3{"Проверка"} --- L4["🔄 Действие"]
    end
    
    %% Стилизация с контрастными цветами и четким текстом
    style A fill:#f9f9c0,stroke:#000,stroke-width:2px,color:#000
    style B fill:#2196F3,stroke:#0d47a1,stroke-width:2px,color:#fff
    style C fill:#FFC107,stroke:#000,stroke-width:2px,color:#000
    style D fill:#FF9800,stroke:#000,stroke-width:2px,color:#000
    style E fill:#4CAF50,stroke:#1b5e20,stroke-width:2px,color:#fff
    style F fill:#8BC34A,stroke:#33691e,stroke-width:2px,color:#000
    style G fill:#9C27B0,stroke:#4a148c,stroke-width:2px,color:#fff
    style H fill:#4CAF50,stroke:#1b5e20,stroke-width:2px,color:#fff
    style I fill:#FF5722,stroke:#bf360c,stroke-width:2px,color:#fff
    style J fill:#F44336,stroke:#b71c1c,stroke-width:2px,color:#fff
    style K,M,O fill:#FFC107,stroke:#000,stroke-width:2px,color:#000
    style L,N fill:#FF5722,stroke:#000,stroke-width:2px,color:#fff
    style P fill:#673AB7,stroke:#311b92,stroke-width:2px,color:#fff
    style Q fill:#009688,stroke:#004d40,stroke-width:2px,color:#fff
    
    %% Пояснительные подписи
    linkStyle 0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15 stroke-width:2px,fill:none
```

### Компоненты системы

| Компонент | Что делает | Где лежит |
|-----------|------------|-----------|
| **Менеджер** | Распределяет задачи, управляет контейнерами, хранит состояние в Redis | `manager/main.py` |
| **Воркер** | Живет в Docker, запускает слоты, шлет heartbeat | `worker/main.py` |
| **Монитор** | Следит за здоровьем, инициирует рестарты | `monitor/monitor.py` |
| **HeadBear** | Собирает метрики по слотам и контейнерам | `worker/heartbeat.py` |

---

## 🚀 Быстрый старт за 5 минут

### 1. Подготовка

```bash
# Клонируем
git clone https://github.com/yourname/redtailfox-orchestrator
cd redtailfox-orchestrator

# Ставим зависимости
pip install -r requirements.txt

# Настраиваем окружение
cp .env.example .env
# Отредактируйте .env под свой Redis
```

### 2. Собираем образ воркера

```bash
docker build -t rtf-worker:latest -f worker/Dockerfile .
```

### 3. Запускаем менеджера

```bash
python manager/main.py
```

### 4. В отдельном терминале запускаем монитор

```bash
python monitor/monitor.py
```

### 5. Отправляем первую задачу

```python
import redis, json

r = redis.Redis(host='localhost', port=6379)

task = {
    "command": "start",
    "slot_id": 1,
    "config": {
        "bot": {
            "username": "my_bot",
            "check_interval": 300
        }
    }
}

r.lpush("autoreply_queue", json.dumps(task))
print("Задача отправлена!")
```

Через несколько секунд менеджер поднимет контейнер и запустит слот.

---

## 📊 Мониторинг и метрики

Каждый воркер пишет heartbeat в Redis:

```json
{
  "container": "rtf_worker_1",
  "ts": 1678901234,
  "slots": [
    {
      "slot_id": 1,
      "running": true,
      "status": "IDLE",
      "last_active": 1678901200
    }
  ]
}
```

Монитор читает эти ключи и принимает решения:
- Если `ts` старее `HEARTBEAT_TIMEOUT` → рестарт контейнера
- Если слот неактивен больше `SLOT_IDLE_TIMEOUT` → рестарт слота
- Если контейнер есть в `ACTIVE_CONTAINERS_KEY`, но heartbeat отсутствует → рестарт

---

## 🧪 Реальный пример: Instagram-автоответчик на 1000+ воркеров

Мы используем RedTailFox как ядро для коммерческого продукта — автоответчика в Instagram.

**Как это работает:**
- Каждый слот = отдельный Instagram-аккаунт
- Слот проверяет входящие, отвечает по настроенным шаблонам
- HeadBear собирает метрики по каждому аккаунту
- Монитор следит, чтобы все работало 24/7
- При падении аккаунта/контейнера — автоперезапуск

**Результат:** один сервер держит 1000+ одновременных воркеров без Kubernetes.

[Подробнее в примере →](./examples/instagram_autoreply)

---

## ⚙️ Конфигурация

Основные переменные окружения (полный список в [.env.example](.env.example)):

| Переменная | По умолчанию | Описание |
|------------|--------------|----------|
| `REDIS_HOST` | localhost | Хост Redis |
| `REDIS_PORT` | 6379 | Порт Redis |
| `MAX_SLOTS_PER_CONTAINER` | 10 | Максимум слотов в одном контейнере |
| `WORKER_IMAGE` | rtf-worker:latest | Docker-образ воркера |
| `HEARTBEAT_TIMEOUT` | 500 | Таймаут heartbeat (сек) |
| `SLOT_IDLE_TIMEOUT` | 600 | Таймаут простоя слота (сек) |
| `WORKER_TASKS_LIST` | autoreply_queue | Очередь задач для менеджера |

---

## 🧩 Структура проекта

```
redtailfox-orchestrator/
├── manager/          # Менеджер (распределение задач)
├── worker/           # Воркер (исполнение, heartbeat)
├── monitor/          # Монитор (автовосстановление)
├── common/           # Общие модули (redis, константы)
├── docs/             # Документация
├── examples/         # Примеры использования
├── .env.example      # Шаблон конфига
└── README.md         # Этот файл
```

---

## 🤝 Как участвовать

Нам нужна помощь с:
- Тестами (особенно интеграционными)
- Документацией на английском и китайском
- Поддержкой большего количества хостов (сейчас только одна нода)
- Метриками в Prometheus

Смотри [CONTRIBUTING.md](docs/CONTRIBUTING.md) и смело создавай issue/pull request.

---

## 📄 Лицензия

MIT © 2025 RedTailFox Team

---

## 📞 Контакты

- Telegram: [@rtf_labs](https://t.me/rtf_labs)
- GitHub: [github.com/yourname/redtailfox-orchestrator](https://github.com/yourname/redtailfox-orchestrator)

---

<p align="center">
  <b>Сделано с 🦊 и любовью к Python</b>
</p>
