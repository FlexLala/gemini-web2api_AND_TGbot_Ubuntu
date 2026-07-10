#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

echo "=========================================="
echo "  Gemini Telegram Bot — обновление"
echo "=========================================="
echo ""

# ─── Проверка зависимостей ──────────────────────────────────────────────────

if docker compose version &>/dev/null; then
    COMPOSE="docker compose"
elif docker-compose version &>/dev/null; then
    COMPOSE="docker-compose"
else
    echo "❌ Ошибка: docker compose не найден."
    exit 1
fi

# ─── Git pull основного репозитория ─────────────────────────────────────────

echo "⬇️  Обновляю основной репозиторий..."
if [ -d ".git" ]; then
    git pull origin $(git rev-parse --abbrev-ref HEAD)
else
    echo "⚠️  Это не git-репозиторий. Пропускаю git pull."
fi
echo ""

# ─── Git pull gemini-web2api ────────────────────────────────────────────────

if [ -d "gemini-web2api/.git" ]; then
    echo "⬇️  Обновляю gemini-web2api..."
    cd gemini-web2api
    git pull origin $(git rev-parse --abbrev-ref HEAD)
    cd ..
else
    echo "⚠️  gemini-web2api не найден или не является git-репозиторием."
    echo "   Выполните вручную: git clone --depth 1 https://github.com/Sophomoresty/gemini-web2api.git gemini-web2api"
fi
echo ""

# ─── Проверка .env и config.json ────────────────────────────────────────────

if [ ! -f ".env" ]; then
    echo "❌ .env не найден! Запустите сначала setup.sh или создайте вручную."
    exit 1
fi

if [ ! -f "config.json" ]; then
    echo "❌ config.json не найден! Запустите сначала setup.sh или создайте вручную."
    exit 1
fi

echo "✅ .env и config.json на месте"
echo ""

# ─── Пересборка и перезапуск ────────────────────────────────────────────────

echo "🔄 Останавливаю контейнеры..."
$COMPOSE down

echo "🏗 Пересобираю и запускаю..."
$COMPOSE up --build -d

echo ""
echo "=========================================="
echo "  ✅ Обновление завершено!"
echo "=========================================="
echo ""
echo "Полезные команды:"
echo "  Логи бота:    $COMPOSE logs -f telegram-bot"
echo "  Логи API:     $COMPOSE logs -f gemini-api"
echo "  Статус:       $COMPOSE ps"
echo ""
