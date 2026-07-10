#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

echo "=========================================="
echo "  Gemini Telegram Bot — установка"
echo "=========================================="
echo ""

# ─── Проверка зависимостей ──────────────────────────────────────────────────

check_cmd() {
    if ! command -v "$1" &>/dev/null; then
        echo "❌ Ошибка: $1 не найден. Установите $1 и повторите."
        exit 1
    fi
}

check_cmd git
check_cmd docker

if docker compose version &>/dev/null; then
    COMPOSE="docker compose"
elif docker-compose version &>/dev/null; then
    COMPOSE="docker-compose"
else
    echo "❌ Ошибка: docker compose (plugin) или docker-compose не найден."
    exit 1
fi

echo "✅ Зависимости на месте (git, docker, compose)"
echo ""

# ─── Клонирование gemini-web2api ────────────────────────────────────────────

if [ ! -d "gemini-web2api/.git" ]; then
    echo "⬇️  Клонирую gemini-web2api..."
    git clone --depth 1 https://github.com/Sophomoresty/gemini-web2api.git gemini-web2api
else
    echo "🔄 gemini-web2api уже склонирован, пропускаю."
fi
echo ""

# ─── Ввод данных ────────────────────────────────────────────────────────────

read -rp "🔑 Введите TELEGRAM_BOT_TOKEN: " BOT_TOKEN
while [ -z "$BOT_TOKEN" ]; do
    echo "Токен обязателен."
    read -rp "🔑 Введите TELEGRAM_BOT_TOKEN: " BOT_TOKEN
done

read -rp "🆔 Введите ваш ADMIN_ID (числовой Telegram ID): " ADMIN_ID
while ! [[ "$ADMIN_ID" =~ ^[0-9]+$ ]]; do
    echo "ADMIN_ID должен быть числом."
    read -rp "🆔 Введите ваш ADMIN_ID: " ADMIN_ID
done

read -rp "🔌 Порт для gemini-web2api внутри контейнера [8081]: " API_PORT
API_PORT=${API_PORT:-8081}

read -rp "🔐 API-ключ для gemini-web2api [sk-gemini-bot]: " API_KEY
API_KEY=${API_KEY:-sk-gemini-bot}

read -rp "⚡ Модель для инлайн-режима [gemini-3.5-flash]: " INLINE_MODEL
INLINE_MODEL=${INLINE_MODEL:-gemini-3.5-flash}

read -rp "⚡ Модель по умолчанию [gemini-3.5-flash]: " DEFAULT_MODEL
DEFAULT_MODEL=${DEFAULT_MODEL:-gemini-3.5-flash}

echo ""
echo "📋 Проверьте данные:"
echo "   ADMIN_ID: $ADMIN_ID"
echo "   API_PORT: $API_PORT"
echo "   API_KEY:  $API_KEY"
echo "   INLINE_MODEL: $INLINE_MODEL"
echo "   DEFAULT_MODEL: $DEFAULT_MODEL"
read -rp "Продолжить? [Y/n]: " CONFIRM
CONFIRM=${CONFIRM:-Y}
if [[ ! "$CONFIRM" =~ ^[Yy]$ ]]; then
    echo "Отменено."
    exit 0
fi

# ─── Создание .env через EOF ────────────────────────────────────────────────

echo "📝 Создаю .env ..."
cat > .env << EOF
TELEGRAM_BOT_TOKEN=${BOT_TOKEN}
ADMIN_ID=${ADMIN_ID}
GEMINI_API_URL=http://gemini-api:${API_PORT}/v1/chat/completions
INLINE_MODEL=${INLINE_MODEL}
DEFAULT_MODEL=${DEFAULT_MODEL}
GEMINI_API_KEY=${API_KEY}
EOF

# ─── Создание config.json для gemini-web2api через EOF ──────────────────────

echo "📝 Создаю config.json ..."
cat > config.json << EOF
{
  "port": ${API_PORT},
  "host": "0.0.0.0",
  "retry_attempts": 3,
  "retry_delay_sec": 2,
  "request_timeout_sec": 180,
  "gemini_bl": "boq_assistant-bard-web-server_20260525.09_p0",
  "auth_user": null,
  "xsrf_token": null,
  "default_model": "${DEFAULT_MODEL}",
  "api_keys": [
    "${API_KEY}"
  ],
  "cookie_file": null,
  "proxy": null,
  "log_requests": true
}
EOF

# ─── Подготовка данных ──────────────────────────────────────────────────────

mkdir -p data
echo "✅ Директория data/ создана"

# ─── Запуск ─────────────────────────────────────────────────────────────────

echo ""
echo "🚀 Запускаю контейнеры..."
$COMPOSE up --build -d

echo ""
echo "=========================================="
echo "  ✅ Установка завершена!"
echo "=========================================="
echo ""
echo "Полезные команды:"
echo "  Просмотр логов бота:   $COMPOSE logs -f telegram-bot"
echo "  Просмотр логов API:    $COMPOSE logs -f gemini-api"
echo "  Перезапуск:            $COMPOSE restart"
echo "  Остановка:             $COMPOSE down"
echo ""
echo "Ваши токены хранятся только в .env и config.json"
echo "НЕ коммитьте их в публичный репозиторий!"
echo ""
