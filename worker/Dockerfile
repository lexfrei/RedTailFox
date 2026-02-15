FROM python:3.11-slim

# Устанавливаем системные зависимости (если нужны для некоторых библиотек)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Сначала копируем только requirements, чтобы использовать кэш слоев Docker
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем остальной код
COPY . .

# Отключаем буферизацию логов Python, чтобы видеть их в docker logs сразу
ENV PYTHONUNBUFFERED=1
# Предотвращаем создание .pyc файлов внутри контейнера
ENV PYTHONDONTWRITEBYTECODE=1

# Переменные по умолчанию (могут быть переопределены из manager.py)
ENV COMMAND_CHANNEL=worker_commands
ENV EVENT_CHANNEL=worker_events

# Используем "exec form" для CMD, чтобы python получил SIGTERM напрямую
CMD ["python", "main.py"]