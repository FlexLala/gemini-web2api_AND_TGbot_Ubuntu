#!/usr/bin/env python3
r"""
Gemini Telegram Bot v6.1 — Multi-Provider + Groq
Провайдеры (по приоритету):
  1. Google AI Studio (ключи из /app/data/gemini_keys.txt, ротация)
  2. Groq (14,400 req/day, без карты, быстрый)
  3. OpenRouter (free tier)
  4. gemini-web2api (fallback для Pro)
  5. DeepSeek API (опционально)

Healthcheck: раз в час, не жрёт квоту.
"""

import os
import sys
import sqlite3
import html
import re
import json
import time as _time_module
import logging
import shutil
import base64
import io
import asyncio
import hashlib
import random
from datetime import datetime, timedelta, time as dt_time
from typing import Optional, List, Dict, Any, AsyncGenerator, Tuple

import httpx
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InlineQueryResultArticle,
    InputTextMessageContent,
)
from telegram.constants import ParseMode
from telegram.error import BadRequest, RetryAfter, NetworkError, TimedOut
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    InlineQueryHandler,
    ChosenInlineResultHandler,
    ContextTypes,
    filters,
)

try:
    import edge_tts
    EDGE_TTS_AVAILABLE = True
except ImportError:
    EDGE_TTS_AVAILABLE = False

# ─── Configuration ───────────────────────────────────────────────────────────

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
INLINE_MODEL = os.getenv("INLINE_MODEL", "groq:llama-3.3-70b-versatile").strip()
DEFAULT_MODEL = os.getenv("DEFAULT_MODEL", "groq:llama-3.3-70b-versatile").strip()
MAX_DIALOGS = 30
IGNORE_TTL_SEC = 300

# Provider endpoints
GOOGLE_API_URL = "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions"
GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"
OPENROUTER_API_URL = "https://openrouter.ai/api/v1/chat/completions"
WEB2API_URL = os.getenv("GEMINI_WEB2API_URL", "http://gemini-api:8081/v1/chat/completions").strip()
DEEPSEEK_API_URL = "https://api.deepseek.com/v1/chat/completions"

# Keys
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "").strip()
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "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").strip()
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "").strip()
GOOGLE_KEYS_PATH = "/app/data/gemini_keys.txt"

if not BOT_TOKEN:
    raise SystemExit("TELEGRAM_BOT_TOKEN не задан в .env")
if not ADMIN_ID:
    raise SystemExit("ADMIN_ID не задан в .env")

# ─── Env helpers ─────────────────────────────────────────────────────────────

def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default

def env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default

def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}

GEMINI_CONNECT_TIMEOUT = env_float("GEMINI_CONNECT_TIMEOUT", 10.0)
GEMINI_READ_TIMEOUT = env_float("GEMINI_READ_TIMEOUT", 300.0)
GEMINI_WRITE_TIMEOUT = env_float("GEMINI_WRITE_TIMEOUT", 60.0)
GEMINI_POOL_TIMEOUT = env_float("GEMINI_POOL_TIMEOUT", 30.0)
GEMINI_RETRIES = max(0, env_int("GEMINI_RETRIES", 2))
GEMINI_RETRY_READ_TIMEOUT = env_bool("GEMINI_RETRY_READ_TIMEOUT", False)
GEMINI_MAX_CONCURRENT = max(1, env_int("GEMINI_MAX_CONCURRENT", 3))
GEMINI_MAX_CONNECTIONS = max(GEMINI_MAX_CONCURRENT, env_int("GEMINI_MAX_CONNECTIONS", 10))
GEMINI_MAX_TOKENS = max(0, env_int("GEMINI_MAX_TOKENS", 4096))
GEMINI_INLINE_MAX_TOKENS = max(0, env_int("GEMINI_INLINE_MAX_TOKENS", 1400))
GEMINI_HISTORY_MESSAGES = max(2, env_int("GEMINI_HISTORY_MESSAGES", 24))
GEMINI_HISTORY_CHARS = max(2000, env_int("GEMINI_HISTORY_CHARS", 30000))
TELEGRAM_CONCURRENT_UPDATES = max(1, env_int("TELEGRAM_CONCURRENT_UPDATES", 16))
STREAMING_ENABLED = env_bool("STREAMING_ENABLED", True)
HEALTHCHECK_TIMEOUT = env_float("HEALTHCHECK_TIMEOUT", 10.0)

# ─── Logging ─────────────────────────────────────────────────────────────────

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ─── Graceful shutdown helpers ───────────────────────────────────────────────

_active_requests: int = 0
_request_lock = asyncio.Lock()

async def _increment_active() -> None:
    global _active_requests
    async with _request_lock:
        _active_requests += 1

async def _decrement_active() -> None:
    global _active_requests
    async with _request_lock:
        _active_requests -= 1

async def _wait_active_requests(timeout: float = 60.0) -> bool:
    deadline = _time_module.monotonic() + timeout
    while _time_module.monotonic() < deadline:
        async with _request_lock:
            if _active_requests <= 0:
                return True
        await asyncio.sleep(0.5)
    logger.warning("Timeout waiting for active requests")
    return False

# ─── Telegram retry wrappers ─────────────────────────────────────────────────

async def safe_send_message(bot, chat_id, text, parse_mode=None, reply_markup=None, max_retries=3):
    for attempt in range(max_retries):
        try:
            return await bot.send_message(chat_id=chat_id, text=text, parse_mode=parse_mode, reply_markup=reply_markup)
        except RetryAfter as e:
            await asyncio.sleep(e.retry_after + 0.5)
        except (NetworkError, TimedOut):
            if attempt < max_retries - 1:
                await asyncio.sleep(1.5 ** attempt)
        except BadRequest:
            raise
    return await bot.send_message(chat_id=chat_id, text=text, parse_mode=parse_mode, reply_markup=reply_markup)

async def safe_edit_message_text(msg, text, parse_mode=None, reply_markup=None, max_retries=3):
    for attempt in range(max_retries):
        try:
            return await msg.edit_text(text=text, parse_mode=parse_mode, reply_markup=reply_markup)
        except RetryAfter as e:
            await asyncio.sleep(e.retry_after + 0.5)
        except (NetworkError, TimedOut):
            if attempt < max_retries - 1:
                await asyncio.sleep(1.5 ** attempt)
        except BadRequest:
            raise
    return await msg.edit_text(text=text, parse_mode=parse_mode, reply_markup=reply_markup)

async def safe_bot_edit_message_text(bot, text, parse_mode=None, reply_markup=None, inline_message_id=None, chat_id=None, message_id=None, max_retries=3):
    for attempt in range(max_retries):
        try:
            return await bot.edit_message_text(
                text=text, parse_mode=parse_mode, reply_markup=reply_markup,
                inline_message_id=inline_message_id, chat_id=chat_id, message_id=message_id,
            )
        except RetryAfter as e:
            await asyncio.sleep(e.retry_after + 0.5)
        except (NetworkError, TimedOut):
            if attempt < max_retries - 1:
                await asyncio.sleep(1.5 ** attempt)
        except BadRequest:
            raise
    return await bot.edit_message_text(
        text=text, parse_mode=parse_mode, reply_markup=reply_markup,
        inline_message_id=inline_message_id, chat_id=chat_id, message_id=message_id,
    )

async def safe_delete_message(msg, max_retries=2):
    for attempt in range(max_retries):
        try:
            return await msg.delete()
        except Exception:
            if attempt < max_retries - 1:
                await asyncio.sleep(0.5)

# ─── Provider config ─────────────────────────────────────────────────────────

class ProviderConfig:
    def __init__(self):
        self.google_keys: List[str] = []
        self._google_key_index = 0
        self._google_lock = asyncio.Lock()
        self._load_google_keys()

    def _load_google_keys(self) -> None:
        if os.path.exists(GOOGLE_KEYS_PATH):
            try:
                with open(GOOGLE_KEYS_PATH, "r", encoding="utf-8") as f:
                    self.google_keys = [line.strip() for line in f if line.strip() and not line.strip().startswith("#")]
                logger.info("Loaded %d Google API keys", len(self.google_keys))
            except Exception as e:
                logger.error("Failed to load Google keys: %s", e)
        else:
            logger.info("No %s found, Google AI Studio disabled", GOOGLE_KEYS_PATH)

    def reload_google_keys(self) -> None:
        self._load_google_keys()

    async def get_next_google_key(self) -> Optional[str]:
        async with self._google_lock:
            if not self.google_keys:
                return None
            key = self.google_keys[self._google_key_index]
            self._google_key_index = (self._google_key_index + 1) % len(self.google_keys)
            return key

    def is_provider_available(self, provider: str) -> bool:
        if provider == "google":
            return len(self.google_keys) > 0
        if provider == "groq":
            return bool(GROQ_API_KEY)
        if provider == "openrouter":
            return bool(OPENROUTER_API_KEY)
        if provider == "web2api":
            return True
        if provider == "deepseek":
            return bool(DEEPSEEK_API_KEY)
        return False

    def get_headers(self, provider: str, key: Optional[str] = None) -> Dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if provider == "google" and key:
            headers["Authorization"] = f"Bearer {key}"
        elif provider == "groq" and GROQ_API_KEY:
            headers["Authorization"] = f"Bearer {GROQ_API_KEY}"
        elif provider == "openrouter" and OPENROUTER_API_KEY:
            headers["Authorization"] = f"Bearer {OPENROUTER_API_KEY}"
            headers["HTTP-Referer"] = "https://t.me/"
            headers["X-Title"] = "GeminiTelegramBot"
        elif provider == "deepseek" and DEEPSEEK_API_KEY:
            headers["Authorization"] = f"Bearer {DEEPSEEK_API_KEY}"
        return headers

    def get_url(self, provider: str) -> str:
        return {
            "google": GOOGLE_API_URL,
            "groq": GROQ_API_URL,
            "openrouter": OPENROUTER_API_URL,
            "web2api": WEB2API_URL,
            "deepseek": DEEPSEEK_API_URL,
        }.get(provider, WEB2API_URL)

provider_config = ProviderConfig()

# ─── Multi-Provider HTTP Client ──────────────────────────────────────────────

class ProviderError(RuntimeError):
    pass

class ProviderTimeoutError(ProviderError):
    pass

class ProviderTransportError(ProviderError):
    pass

class CircuitBreaker:
    def __init__(self, threshold: int = 5, timeout: float = 30.0):
        self.threshold = threshold
        self.timeout = timeout
        self.failures = 0
        self.state = "closed"
        self.last_failure_time: Optional[float] = None

    def can_execute(self) -> bool:
        if self.state == "closed":
            return True
        if self.state == "open":
            if self.last_failure_time and (_time_module.monotonic() - self.last_failure_time) > self.timeout:
                self.state = "half-open"
                return True
            return False
        return True

    def record_success(self) -> None:
        self.failures = 0
        self.state = "closed"

    def record_failure(self) -> None:
        self.failures += 1
        self.last_failure_time = _time_module.monotonic()
        if self.failures >= self.threshold:
            self.state = "open"

class MultiProviderClient:
    RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

    def __init__(self) -> None:
        timeout = httpx.Timeout(
            connect=GEMINI_CONNECT_TIMEOUT,
            read=GEMINI_READ_TIMEOUT,
            write=GEMINI_WRITE_TIMEOUT,
            pool=GEMINI_POOL_TIMEOUT,
        )
        limits = httpx.Limits(
            max_connections=GEMINI_MAX_CONNECTIONS,
            max_keepalive_connections=GEMINI_MAX_CONNECTIONS,
            keepalive_expiry=30.0,
        )
        self._semaphore = asyncio.Semaphore(GEMINI_MAX_CONCURRENT)
        self._client = httpx.AsyncClient(
            timeout=timeout,
            limits=limits,
            follow_redirects=True,
            trust_env=False,
        )
        self._circuit = CircuitBreaker()

    async def close(self) -> None:
        await self._client.aclose()

    @staticmethod
    def _retry_delay(attempt: int, response: Optional[httpx.Response] = None) -> float:
        if response is not None:
            retry_after = response.headers.get("Retry-After")
            if retry_after:
                try:
                    return min(max(float(retry_after), 0.5), 30.0)
                except ValueError:
                    pass
        return min(2 ** attempt, 8) + random.uniform(0.0, 0.5)

    async def _sleep_before_retry(self, attempt: int, reason: str, response: Optional[httpx.Response] = None) -> None:
        delay = self._retry_delay(attempt, response)
        logger.warning("Retry %s/%s in %.1f sec: %s", attempt + 1, GEMINI_RETRIES, delay, reason)
        await asyncio.sleep(delay)

    def _extract_answer(self, data: Dict[str, Any]) -> str:
        choices = data.get("choices", [])
        if not choices:
            return "(пустой ответ)"
        choice = choices[0]
        message = choice.get("message", {}) or {}
        content = message.get("content")
        if content:
            return str(content)
        reasoning = message.get("reasoning_content")
        if reasoning:
            return str(reasoning)
        for key in ("text", "delta", "body"):
            val = message.get(key)
            if val:
                if isinstance(val, dict):
                    val = val.get("content") or val.get("text")
                if val:
                    return str(val)
        return "(пустой ответ)"

    async def _request(self, provider: str, payload: Dict[str, Any], key: Optional[str] = None) -> Dict[str, Any]:
        if not self._circuit.can_execute():
            raise ProviderError("Circuit breaker OPEN: сервер временно недоступен. Попробуйте через минуту.")

        url = provider_config.get_url(provider)
        headers = provider_config.get_headers(provider, key)
        start = _time_module.monotonic()

        for attempt in range(GEMINI_RETRIES + 1):
            try:
                async with self._semaphore:
                    response = await self._client.post(url, json=payload, headers=headers)
            except httpx.ReadTimeout as exc:
                self._circuit.record_failure()
                can_retry = GEMINI_RETRY_READ_TIMEOUT and attempt < GEMINI_RETRIES
                if can_retry:
                    await self._sleep_before_retry(attempt, f"{provider} read timeout")
                    continue
                raise ProviderTimeoutError(f"{provider} не ответил за {GEMINI_READ_TIMEOUT:.0f} секунд") from exc
            except httpx.TransportError as exc:
                self._circuit.record_failure()
                if attempt < GEMINI_RETRIES:
                    await self._sleep_before_retry(attempt, f"{provider} {type(exc).__name__}")
                    continue
                raise ProviderTransportError(f"Не удалось подключиться к {provider}") from exc

            if response.status_code in self.RETRYABLE_STATUS_CODES:
                self._circuit.record_failure()
                if attempt < GEMINI_RETRIES:
                    await self._sleep_before_retry(attempt, f"{provider} HTTP {response.status_code}", response)
                    continue

            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                self._circuit.record_failure()
                raise

            self._circuit.record_success()

            try:
                data = response.json()
            except ValueError as exc:
                body = response.text[:500]
                raise ProviderError(f"{provider} вернул не JSON: {body}") from exc

            api_error = data.get("error")
            if api_error:
                msg = api_error.get("message") if isinstance(api_error, dict) else str(api_error)
                raise ProviderError(f"{provider} error: {msg}")

            latency_ms = int((_time_module.monotonic() - start) * 1000)
            logger.info("%s complete: latency=%dms model=%s", provider, latency_ms, payload.get("model", "?"))
            return data

        raise ProviderError(f"{provider}: неизвестная ошибка")

    async def _stream_request(self, provider: str, payload: Dict[str, Any], key: Optional[str] = None) -> AsyncGenerator[str, None]:
        payload = {**payload, "stream": True}
        if not self._circuit.can_execute():
            raise ProviderError("Circuit breaker OPEN")

        url = provider_config.get_url(provider)
        headers = provider_config.get_headers(provider, key)
        start = _time_module.monotonic()

        for attempt in range(GEMINI_RETRIES + 1):
            try:
                async with self._semaphore:
                    async with self._client.stream("POST", url, json=payload, headers=headers) as response:
                        if response.status_code in self.RETRYABLE_STATUS_CODES:
                            self._circuit.record_failure()
                            if attempt < GEMINI_RETRIES:
                                await self._sleep_before_retry(attempt, f"{provider} HTTP {response.status_code}")
                                continue
                            response.raise_for_status()

                        try:
                            response.raise_for_status()
                        except httpx.HTTPStatusError:
                            self._circuit.record_failure()
                            raise

                        self._circuit.record_success()
                        async for line in response.aiter_lines():
                            line = line.strip()
                            if line.startswith("data: "):
                                data_str = line[6:]
                                if data_str == "[DONE]":
                                    latency_ms = int((_time_module.monotonic() - start) * 1000)
                                    logger.info("%s stream: latency=%dms model=%s", provider, latency_ms, payload.get("model", "?"))
                                    return
                                try:
                                    data = json.loads(data_str)
                                    choices = data.get("choices", [])
                                    if not choices:
                                        continue
                                    delta = choices[0].get("delta", {}) or {}
                                    content = delta.get("content", "")
                                    if content:
                                        yield str(content)
                                except json.JSONDecodeError:
                                    continue
                        latency_ms = int((_time_module.monotonic() - start) * 1000)
                        logger.info("%s stream: latency=%dms model=%s", provider, latency_ms, payload.get("model", "?"))
                        return
            except httpx.ReadTimeout as exc:
                self._circuit.record_failure()
                can_retry = GEMINI_RETRY_READ_TIMEOUT and attempt < GEMINI_RETRIES
                if can_retry:
                    await self._sleep_before_retry(attempt, f"{provider} read timeout")
                    continue
                raise ProviderTimeoutError(f"{provider} не ответил за {GEMINI_READ_TIMEOUT:.0f} секунд") from exc
            except httpx.TransportError as exc:
                self._circuit.record_failure()
                if attempt < GEMINI_RETRIES:
                    await self._sleep_before_retry(attempt, f"{provider} {type(exc).__name__}")
                    continue
                raise ProviderTransportError(f"Не удалось подключиться к {provider}") from exc
            except httpx.HTTPStatusError:
                self._circuit.record_failure()
                raise

        raise ProviderError(f"{provider}: неизвестная ошибка")

    async def complete(self, provider: str, model: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        payload = {**payload, "model": model}
        errors: List[str] = []

        if provider == "google":
            for _ in range(max(1, len(provider_config.google_keys))):
                key = await provider_config.get_next_google_key()
                if not key:
                    break
                try:
                    return await self._request("google", payload, key=key)
                except (ProviderTimeoutError, ProviderTransportError, httpx.HTTPStatusError) as exc:
                    if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code == 429:
                        logger.warning("Google key rate limited, rotating...")
                        continue
                    errors.append(f"Google: {exc}")
                    break
                except ProviderError as exc:
                    errors.append(f"Google: {exc}")
                    break
        else:
            if provider_config.is_provider_available(provider):
                try:
                    return await self._request(provider, payload)
                except Exception as exc:
                    errors.append(f"{provider}: {exc}")

        # Fallback chain
        for fb_provider, fb_model in self._fallback_chain(provider, model):
            if not provider_config.is_provider_available(fb_provider):
                continue
            try:
                fb_payload = {**payload, "model": fb_model}
                return await self._request(fb_provider, fb_payload)
            except Exception as exc:
                errors.append(f"{fb_provider}: {exc}")

        raise ProviderError(f"Все провайдеры недоступны. Ошибки: {'; '.join(errors[:3])}")

    async def stream_complete(self, provider: str, model: str, payload: Dict[str, Any]) -> AsyncGenerator[str, None]:
        payload = {**payload, "model": model}

        if provider == "google":
            for _ in range(max(1, len(provider_config.google_keys))):
                key = await provider_config.get_next_google_key()
                if not key:
                    break
                try:
                    async for chunk in self._stream_request("google", payload, key=key):
                        yield chunk
                    return
                except (ProviderTimeoutError, ProviderTransportError, httpx.HTTPStatusError) as exc:
                    if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code == 429:
                        logger.warning("Google key rate limited, rotating...")
                        continue
                    raise
        else:
            if provider_config.is_provider_available(provider):
                async for chunk in self._stream_request(provider, payload):
                    yield chunk
                return

        for fb_provider, fb_model in self._fallback_chain(provider, model):
            if not provider_config.is_provider_available(fb_provider):
                continue
            fb_payload = {**payload, "model": fb_model}
            async for chunk in self._stream_request(fb_provider, fb_payload):
                yield chunk
            return

        raise ProviderError("Стриминг недоступен ни у одного провайдера")

    def _fallback_chain(self, provider: str, model: str) -> List[Tuple[str, str]]:
        """Возвращает fallback-провайдеров в порядке приоритета."""
        chain: List[Tuple[str, str]] = []
        # 1. Groq
        groq_model = _map_to_groq(model)
        if groq_model and provider != "groq":
            chain.append(("groq", groq_model))
        # 2. OpenRouter
        or_model = _map_to_openrouter(model)
        if or_model and provider != "openrouter":
            chain.append(("openrouter", or_model))
        # 3. web2api
        w2_model = _map_to_web2api(model)
        if w2_model and provider != "web2api":
            chain.append(("web2api", w2_model))
        return chain

    async def healthcheck(self, provider: str) -> Tuple[bool, int]:
        url = provider_config.get_url(provider)
        headers = provider_config.get_headers(provider)
        if provider == "google":
            key = await provider_config.get_next_google_key()
            if not key:
                return False, 0
            headers = provider_config.get_headers("google", key)

        # Для Google и Groq используем HEAD или очень лёгкий запрос
        test_model = "gemini-3.5-flash" if provider == "google" else (
            "llama-3.3-70b-versatile" if provider == "groq" else (
                "google/gemini-3.5-flash" if provider == "openrouter" else "gemini-3.5-flash"
            )
        )

        start = _time_module.monotonic()
        try:
            hc_timeout = httpx.Timeout(connect=5.0, read=HEALTHCHECK_TIMEOUT, write=5.0, pool=5.0)
            async with httpx.AsyncClient(timeout=hc_timeout, follow_redirects=True, trust_env=False) as client:
                resp = await client.post(
                    url,
                    headers=headers,
                    json={"model": test_model, "messages": [{"role": "user", "content": "ping"}], "max_tokens": 1},
                )
                ms = int((_time_module.monotonic() - start) * 1000)
                if resp.status_code == 200:
                    self._circuit.record_success()
                    return True, ms
                else:
                    self._circuit.record_failure()
                    return False, ms
        except Exception as exc:
            ms = int((_time_module.monotonic() - start) * 1000)
            logger.debug("Healthcheck %s failed: %s", provider, exc)
            self._circuit.record_failure()
            return False, ms

mp_client: Optional[MultiProviderClient] = None

async def post_init(application: Application) -> None:
    global mp_client
    mp_client = MultiProviderClient()
    logger.info("Multi-provider client started")

async def post_shutdown(application: Application) -> None:
    logger.info("Waiting for active requests to finish...")
    await _wait_active_requests(timeout=60.0)
    global mp_client
    if mp_client is not None:
        await mp_client.close()
        mp_client = None
    logger.info("Multi-provider client stopped")

def get_mp_client() -> MultiProviderClient:
    if mp_client is None:
        raise RuntimeError("MultiProviderClient not initialized")
    return mp_client

# ─── Model mapping helpers ───────────────────────────────────────────────────

def _map_to_groq(model: str) -> Optional[str]:
    mapping = {
        "gemini-3.5-flash": "llama-3.3-70b-versatile",
        "gemini-3.1-flash-lite": "llama-3.1-8b-instant",
        "gemini-2.5-pro": "llama-3.3-70b-versatile",
        "deepseek-v4-flash": "deepseek-r1-distill-llama-70b",
        "deepseek-v4-pro": "deepseek-r1-distill-llama-70b",
        "deepseek-chat": "deepseek-r1-distill-llama-70b",
        "google/gemini-3.5-flash": "llama-3.3-70b-versatile",
        "google/gemini-3.1-flash-lite": "llama-3.1-8b-instant",
        "deepseek/deepseek-chat": "deepseek-r1-distill-llama-70b",
    }
    return mapping.get(model)

def _map_to_openrouter(model: str) -> Optional[str]:
    mapping = {
        "gemini-3.5-flash": "google/gemini-3.5-flash",
        "gemini-3.1-flash-lite": "google/gemini-3.1-flash-lite",
        "gemini-2.5-pro": "google/gemini-2.5-pro",
        "llama-3.3-70b-versatile": "meta-llama/llama-3.3-70b-instruct",
        "deepseek-v4-flash": "deepseek/deepseek-chat",
        "deepseek-v4-pro": "deepseek/deepseek-chat",
        "deepseek-chat": "deepseek/deepseek-chat",
    }
    return mapping.get(model)

def _map_to_web2api(model: str) -> Optional[str]:
    mapping = {
        "gemini-3.5-flash": "gemini-3.5-flash",
        "gemini-3.1-flash-lite": "gemini-flash-lite",
        "gemini-2.5-pro": "gemini-3.1-pro",
        "gemini-3.1-pro": "gemini-3.1-pro",
        "llama-3.3-70b-versatile": None,
    }
    return mapping.get(model)

def _parse_model_slug(slug: str) -> Tuple[str, str]:
    if ":" in slug:
        provider, model = slug.split(":", 1)
        return provider, model
    return "web2api", slug

# ─── Database ────────────────────────────────────────────────────────────────

DB_PATH = "/app/data/bot.db"

def _conn() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.execute("PRAGMA busy_timeout = 30000")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn

def init_db() -> None:
    conn = _conn()
    c = conn.cursor()
    c.execute("PRAGMA journal_mode = WAL")
    c.execute("PRAGMA synchronous = NORMAL")
    c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            allowed INTEGER DEFAULT 0,
            blocked INTEGER DEFAULT 0,
            quota INTEGER DEFAULT 100,
            system_role TEXT,
            custom_prompt TEXT,
            inline_model TEXT,
            inline_prompt TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    for col, dtype in [
        ("quota", "INTEGER DEFAULT 100"), ("system_role", "TEXT"),
        ("custom_prompt", "TEXT"), ("inline_model", "TEXT"), ("inline_prompt", "TEXT"),
    ]:
        try:
            c.execute(f"ALTER TABLE users ADD COLUMN {col} {dtype}")
        except sqlite3.OperationalError:
            pass
    c.execute("""
        CREATE TABLE IF NOT EXISTS dialogs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            title TEXT,
            token_count INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            active INTEGER DEFAULT 0
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS dialog_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dialog_id INTEGER,
            role TEXT,
            content TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS stats (
            user_id INTEGER PRIMARY KEY,
            requests INTEGER DEFAULT 0,
            tokens_prompt INTEGER DEFAULT 0,
            tokens_completion INTEGER DEFAULT 0,
            last_used TIMESTAMP
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS rate_limit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            timestamp REAL
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS bot_settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    c.execute("CREATE INDEX IF NOT EXISTS idx_dialogs_user_active ON dialogs(user_id, active, updated_at)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_messages_dialog_id ON dialog_messages(dialog_id, id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_rate_limit_user_time ON rate_limit_log(user_id, timestamp)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_rate_limit_time ON rate_limit_log(timestamp)")
    defaults = [
        ("rate_limit_messages", "5"),
        ("rate_limit_window", "60"),
        ("rate_limit_ignore", "600"),
    ]
    for k, v in defaults:
        c.execute("INSERT OR IGNORE INTO bot_settings (key, value) VALUES (?, ?)", (k, v))
    conn.commit()
    conn.close()

# ─── Settings helpers ────────────────────────────────────────────────────────

def get_setting(key: str, default: str = "") -> str:
    conn = _conn()
    c = conn.cursor()
    c.execute("SELECT value FROM bot_settings WHERE key = ?", (key,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else default

def set_setting(key: str, value: str) -> None:
    conn = _conn()
    c = conn.cursor()
    c.execute(
        "INSERT INTO bot_settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, value),
    )
    conn.commit()
    conn.close()

# ─── User helpers ────────────────────────────────────────────────────────────

def ensure_user(user_id: int, username: Optional[str], first_name: Optional[str], allowed: int = 0) -> None:
    conn = _conn()
    c = conn.cursor()
    c.execute("SELECT 1 FROM users WHERE user_id = ?", (user_id,))
    if not c.fetchone():
        c.execute(
            "INSERT INTO users (user_id, username, first_name, allowed) VALUES (?, ?, ?, ?)",
            (user_id, username or "", first_name or "", allowed),
        )
        c.execute(
            "INSERT OR IGNORE INTO stats (user_id, requests, tokens_prompt, tokens_completion) VALUES (?, 0, 0, 0)",
            (user_id,),
        )
    else:
        c.execute(
            "UPDATE users SET username = ?, first_name = ? WHERE user_id = ?",
            (username or "", first_name or "", user_id),
        )
    conn.commit()
    conn.close()

def get_user(user_id: int) -> Optional[Dict[str, Any]]:
    conn = _conn()
    c = conn.cursor()
    c.execute("SELECT user_id, username, first_name, allowed, blocked, quota, system_role, custom_prompt, inline_model, inline_prompt FROM users WHERE user_id = ?", (user_id,))
    row = c.fetchone()
    conn.close()
    if row:
        return {
            "user_id": row[0], "username": row[1], "first_name": row[2],
            "allowed": row[3], "blocked": row[4], "quota": row[5],
            "system_role": row[6], "custom_prompt": row[7],
            "inline_model": row[8], "inline_prompt": row[9],
        }
    return None

def set_allowed(user_id: int, allowed: int) -> None:
    conn = _conn()
    c = conn.cursor()
    c.execute("UPDATE users SET allowed = ? WHERE user_id = ?", (allowed, user_id))
    conn.commit()
    conn.close()

def set_blocked(user_id: int, blocked: int) -> None:
    conn = _conn()
    c = conn.cursor()
    c.execute("UPDATE users SET blocked = ? WHERE user_id = ?", (blocked, user_id))
    conn.commit()
    conn.close()

def set_quota(user_id: int, quota: int) -> None:
    conn = _conn()
    c = conn.cursor()
    c.execute("UPDATE users SET quota = ? WHERE user_id = ?", (quota, user_id))
    conn.commit()
    conn.close()

def set_system_role(user_id: int, role: Optional[str]) -> None:
    conn = _conn()
    c = conn.cursor()
    c.execute("UPDATE users SET system_role = ? WHERE user_id = ?", (role, user_id))
    conn.commit()
    conn.close()

def set_custom_prompt(user_id: int, prompt: Optional[str]) -> None:
    conn = _conn()
    c = conn.cursor()
    c.execute("UPDATE users SET custom_prompt = ? WHERE user_id = ?", (prompt, user_id))
    conn.commit()
    conn.close()

def set_inline_model(user_id: int, model: Optional[str]) -> None:
    conn = _conn()
    c = conn.cursor()
    c.execute("UPDATE users SET inline_model = ? WHERE user_id = ?", (model, user_id))
    conn.commit()
    conn.close()

def set_inline_prompt(user_id: int, prompt: Optional[str]) -> None:
    conn = _conn()
    c = conn.cursor()
    c.execute("UPDATE users SET inline_prompt = ? WHERE user_id = ?", (prompt, user_id))
    conn.commit()
    conn.close()

# ─── Dialog helpers ──────────────────────────────────────────────────────────

def get_active_dialog(user_id: int) -> Optional[Dict[str, Any]]:
    conn = _conn()
    c = conn.cursor()
    c.execute(
        "SELECT id, title, token_count, created_at, updated_at FROM dialogs WHERE user_id = ? AND active = 1 ORDER BY updated_at DESC LIMIT 1",
        (user_id,),
    )
    row = c.fetchone()
    conn.close()
    if row:
        return {"id": row[0], "title": row[1], "token_count": row[2], "created_at": row[3], "updated_at": row[4]}
    return None

def create_dialog(user_id: int, title: str) -> int:
    conn = _conn()
    c = conn.cursor()
    c.execute("UPDATE dialogs SET active = 0 WHERE user_id = ?", (user_id,))
    c.execute("INSERT INTO dialogs (user_id, title, active) VALUES (?, ?, 1)", (user_id, title))
    dialog_id = c.lastrowid
    c.execute("SELECT id FROM dialogs WHERE user_id = ? ORDER BY updated_at DESC LIMIT -1 OFFSET ?", (user_id, MAX_DIALOGS))
    for old in c.fetchall():
        c.execute("DELETE FROM dialog_messages WHERE dialog_id = ?", (old[0],))
        c.execute("DELETE FROM dialogs WHERE id = ?", (old[0],))
    conn.commit()
    conn.close()
    return dialog_id

def switch_dialog(user_id: int, dialog_id: int) -> bool:
    conn = _conn()
    c = conn.cursor()
    c.execute("SELECT 1 FROM dialogs WHERE id = ? AND user_id = ?", (dialog_id, user_id))
    if not c.fetchone():
        conn.close()
        return False
    c.execute("UPDATE dialogs SET active = 0 WHERE user_id = ?", (user_id,))
    c.execute("UPDATE dialogs SET active = 1 WHERE id = ?", (dialog_id,))
    conn.commit()
    conn.close()
    return True

def get_dialogs(user_id: int) -> List[Dict[str, Any]]:
    conn = _conn()
    c = conn.cursor()
    c.execute(
        "SELECT id, title, token_count, updated_at, active FROM dialogs WHERE user_id = ? ORDER BY updated_at DESC LIMIT ?",
        (user_id, MAX_DIALOGS),
    )
    rows = c.fetchall()
    conn.close()
    return [{"id": r[0], "title": r[1], "token_count": r[2], "updated_at": r[3], "active": r[4]} for r in rows]

def get_dialog_history(dialog_id: int, limit: int = 40) -> List[Dict[str, str]]:
    conn = _conn()
    c = conn.cursor()
    c.execute(
        "SELECT role, content FROM dialog_messages WHERE dialog_id = ? ORDER BY id DESC LIMIT ?",
        (dialog_id, limit),
    )
    rows = c.fetchall()
    conn.close()
    rows.reverse()
    return [{"role": r, "content": c} for r, c in rows]

def add_dialog_message(dialog_id: int, role: str, content: str, max_msgs: int = 50) -> None:
    conn = _conn()
    c = conn.cursor()
    c.execute("INSERT INTO dialog_messages (dialog_id, role, content) VALUES (?, ?, ?)", (dialog_id, role, content))
    c.execute(
        "DELETE FROM dialog_messages WHERE id IN (SELECT id FROM dialog_messages WHERE dialog_id = ? ORDER BY id DESC LIMIT -1 OFFSET ?)",
        (dialog_id, max_msgs),
    )
    conn.commit()
    conn.close()

def update_dialog_meta(dialog_id: int, token_delta: int) -> None:
    conn = _conn()
    c = conn.cursor()
    c.execute("UPDATE dialogs SET token_count = token_count + ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?", (token_delta, dialog_id))
    conn.commit()
    conn.close()

def update_dialog_title(dialog_id: int, title: str) -> None:
    conn = _conn()
    c = conn.cursor()
    c.execute("UPDATE dialogs SET title = ? WHERE id = ?", (title, dialog_id))
    conn.commit()
    conn.close()

# ─── Stats ───────────────────────────────────────────────────────────────────

def update_stats(user_id: int, prompt_tokens: int, completion_tokens: int) -> None:
    prompt_tokens = max(0, int(prompt_tokens or 0))
    completion_tokens = max(0, int(completion_tokens or 0))
    conn = _conn()
    try:
        conn.execute(
            """
            INSERT INTO stats (user_id, requests, tokens_prompt, tokens_completion, last_used)
            VALUES (?, 1, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                requests = requests + 1,
                tokens_prompt = tokens_prompt + excluded.tokens_prompt,
                tokens_completion = tokens_completion + excluded.tokens_completion,
                last_used = excluded.last_used
            """,
            (user_id, prompt_tokens, completion_tokens, datetime.utcnow().isoformat()),
        )
        conn.commit()
    finally:
        conn.close()

def get_all_stats() -> List[Dict[str, Any]]:
    conn = _conn()
    c = conn.cursor()
    c.execute("""
        SELECT u.user_id, u.username, u.first_name, u.allowed, u.blocked, u.quota,
               COALESCE(s.requests, 0), COALESCE(s.tokens_prompt, 0), COALESCE(s.tokens_completion, 0)
        FROM users u
        LEFT JOIN stats s ON u.user_id = s.user_id
        ORDER BY s.requests DESC
    """)
    rows = c.fetchall()
    conn.close()
    return [
        {"user_id": r[0], "username": r[1], "first_name": r[2], "allowed": r[3], "blocked": r[4], "quota": r[5],
         "requests": r[6], "tokens_prompt": r[7], "tokens_completion": r[8]}
        for r in rows
    ]

# ─── Rate limit ──────────────────────────────────────────────────────────────

def check_rate_limit(user_id: int) -> tuple:
    if user_id == ADMIN_ID:
        return True, 0
    msgs = int(get_setting("rate_limit_messages", "5"))
    window = int(get_setting("rate_limit_window", "60"))
    ignore_dur = int(get_setting("rate_limit_ignore", "600"))
    now = _time_module.monotonic()
    conn = _conn()
    c = conn.cursor()
    c.execute("DELETE FROM rate_limit_log WHERE timestamp < ?", (now - window,))
    c.execute("SELECT COUNT(*) FROM rate_limit_log WHERE user_id = ? AND timestamp > ?", (user_id, now - window))
    count = c.fetchone()[0]
    if count >= msgs:
        add_ignore_custom(user_id, ignore_dur)
        conn.commit()
        conn.close()
        return False, ignore_dur
    c.execute("INSERT INTO rate_limit_log (user_id, timestamp) VALUES (?, ?)", (user_id, now))
    conn.commit()
    conn.close()
    return True, 0

# ─── Ignore list ─────────────────────────────────────────────────────────────

_ignore_until: Dict[int, float] = {}

def is_ignored(user_id: int) -> bool:
    expires_at = _ignore_until.get(user_id)
    if expires_at is None:
        return False
    if _time_module.monotonic() >= expires_at:
        _ignore_until.pop(user_id, None)
        return False
    return True

def add_ignore(user_id: int) -> None:
    add_ignore_custom(user_id, IGNORE_TTL_SEC)

def add_ignore_custom(user_id: int, seconds: int) -> None:
    _ignore_until[user_id] = _time_module.monotonic() + max(1, seconds)

# ─── LaTeX / HTML formatting ─────────────────────────────────────────────────

def split_plain_text(text: str, max_len: int = 3500) -> List[str]:
    text = (text or "").strip()
    if not text:
        return ["(пустой ответ)"]
    chunks: List[str] = []
    while len(text) > max_len:
        positions = [text.rfind("\n\n", 0, max_len + 1), text.rfind("\n", 0, max_len + 1), text.rfind(" ", 0, max_len + 1)]
        cut = max(positions)
        if cut < max_len // 2:
            cut = max_len
        chunk = text[:cut].rstrip()
        if not chunk:
            chunk = text[:max_len]
            cut = max_len
        chunks.append(chunk)
        text = text[cut:].lstrip()
    if text:
        chunks.append(text)
    return chunks

def split_html_lines(lines: List[str], max_len: int = 3500) -> List[str]:
    chunks: List[str] = []
    current: List[str] = []
    current_len = 0
    for line in lines:
        line_len = len(line)
        if current_len + line_len + 1 > max_len and current:
            chunks.append("\n".join(current))
            current = []
            current_len = 0
        current.append(line)
        current_len += line_len + 1
    if current:
        chunks.append("\n".join(current))
    return chunks

def format_gemini_text(raw: str) -> str:
    raw = str(raw or "")
    thinking_blocks = re.findall(r"<thinking>(.*?)</thinking>", raw, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<thinking>.*?</thinking>", "", raw, flags=re.DOTALL | re.IGNORECASE).strip()

    replacements: List[str] = []
    def stash(value: str) -> str:
        token = f"\uE000{len(replacements)}\uE001"
        replacements.append(value)
        return token

    def replace_code_block(match: re.Match) -> str:
        code = match.group(2) or ""
        return stash(f"<pre><code>{html.escape(code.strip())}</code></pre>")
    text = re.sub(r"```(?:([A-Za-z0-9_+.\-]+)\n)?(.*?)```", replace_code_block, text, flags=re.DOTALL)

    def replace_display_math(match: re.Match) -> str:
        value = match.group(1) if match.group(1) is not None else match.group(2)
        return stash(f"<pre>{html.escape((value or '').strip())}</pre>")
    text = re.sub(r"\\\[(.*?)\\\]|\$\$(.*?)\$\$", replace_display_math, text, flags=re.DOTALL)

    def replace_inline_math(match: re.Match) -> str:
        value = match.group(1) if match.group(1) is not None else match.group(2)
        return stash(f"<code>{html.escape((value or '').strip())}</code>")
    text = re.sub(r"\\\((.*?)\\\)|(?<!\$)\$(?!\$)(.*?)(?<!\$)\$(?!\$)", replace_inline_math, text, flags=re.DOTALL)

    text = re.sub(r"`([^`\n]+)`", lambda m: stash(f"<code>{html.escape(m.group(1))}</code>"), text)

    formatted = html.escape(text)
    formatted = re.sub(r"(?m)^#{1,6}[ \t]+(.+)$", r"<b>\1</b>", formatted)
    formatted = re.sub(r"\*\*([^*\n]+)\*\*", r"<b>\1</b>", formatted)

    for index, replacement in enumerate(replacements):
        formatted = formatted.replace(f"\uE000{index}\uE001", replacement)

    thinking_html = ""
    if thinking_blocks:
        thinking = "\n\n".join(part.strip() for part in thinking_blocks if part.strip())
        if thinking:
            thinking_html = f'<blockquote expandable><b>🧠 Рассуждение:</b>\n{html.escape(thinking)}</blockquote>\n\n'

    return thinking_html + formatted.strip()

def make_title_from_text(text: str) -> str:
    words = text.strip().split()
    title = " ".join(words[:8])[:60]
    return title or "Новый диалог"

# ─── History trimming ────────────────────────────────────────────────────────

def trim_dialog_history(messages: List[Dict[str, Any]], max_chars: int = GEMINI_HISTORY_CHARS) -> List[Dict[str, Any]]:
    result: List[Dict[str, Any]] = []
    total_chars = 0
    for message in reversed(messages):
        content = message.get("content", "")
        content_size = len(content) if isinstance(content, str) else len(json.dumps(content, ensure_ascii=False))
        if result and total_chars + content_size > max_chars:
            break
        if content_size > max_chars and not result:
            if isinstance(content, str):
                message = {**message, "content": content[-max_chars:]}
            result.append(message)
            break
        result.append(message)
        total_chars += content_size
    result.reverse()
    return result

# ─── API helpers ─────────────────────────────────────────────────────────────

async def ask_ai(
    user_id: int,
    prompt: str,
    model_slug: str,
    dialog_id: Optional[int] = None,
    image_b64: Optional[str] = None,
    doc_text: Optional[str] = None,
    system_prompt: Optional[str] = None,
) -> Dict[str, Any]:
    provider, model = _parse_model_slug(model_slug)

    messages = []
    u = get_user(user_id)
    final_system = system_prompt
    if not final_system and u and u.get("custom_prompt"):
        final_system = u["custom_prompt"]
    if not final_system and u and u.get("system_role"):
        role_text = {
            "programmer": "Ты senior-разработчик. Пиши чистый, современный код с пояснениями.",
            "translator": "Ты профессиональный переводчик. Переводи точно, сохраняя стиль.",
            "teacher": "Ты терпеливый учитель. Объясняй сложные вещи простым языком, с примерами.",
            "concise": "Отвечай максимально кратко и по делу. Без воды.",
            "creative": "Ты креативный помощник. Предлагай нестандартные идеи.",
        }.get(u["system_role"], "")
        if role_text:
            final_system = role_text
    if final_system:
        messages.append({"role": "system", "content": final_system})

    if dialog_id:
        history = await asyncio.to_thread(get_dialog_history, dialog_id, GEMINI_HISTORY_MESSAGES)
        messages.extend(trim_dialog_history(history))

    content: Any = prompt
    if doc_text:
        content = f"{prompt}\n\n[Содержимое документа]:\n{doc_text}"
    if image_b64:
        text_content = content if isinstance(content, str) else prompt
        content = [
            {"type": "text", "text": text_content},
            {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{image_b64}"}},
        ]
    messages.append({"role": "user", "content": content})

    payload = {"messages": messages, "stream": False}
    if GEMINI_MAX_TOKENS > 0:
        payload["max_tokens"] = GEMINI_MAX_TOKENS

    client = get_mp_client()
    data = await client.complete(provider, model, payload)
    answer = client._extract_answer(data)

    if dialog_id:
        add_dialog_message(dialog_id, "user", prompt if not doc_text else f"{prompt} [+документ]")
        add_dialog_message(dialog_id, "assistant", answer)
        usage = data.get("usage", {})
        update_dialog_meta(dialog_id, usage.get("prompt_tokens", 0) + usage.get("completion_tokens", 0))

    usage = data.get("usage", {})
    pt = usage.get("prompt_tokens")
    if pt is None:
        pt = max(1, len(json.dumps(messages, ensure_ascii=False)) // 4)
    ct = usage.get("completion_tokens")
    if ct is None:
        ct = max(1, len(answer) // 4)
    await asyncio.to_thread(update_stats, user_id, pt, ct)

    return {"text": answer, "model": model, "provider": provider}

async def ask_ai_inline(user_id: int, prompt: str, model_slug: str, system_prompt: Optional[str] = None) -> Dict[str, Any]:
    provider, model = _parse_model_slug(model_slug)
    payload = {"messages": [], "stream": False}
    if system_prompt:
        payload["messages"].append({"role": "system", "content": system_prompt})
    else:
        global_prompt = get_setting("global_inline_prompt", None)
        if global_prompt:
            payload["messages"].append({"role": "system", "content": global_prompt})
    payload["messages"].append({"role": "user", "content": prompt})
    if GEMINI_INLINE_MAX_TOKENS > 0:
        payload["max_tokens"] = GEMINI_INLINE_MAX_TOKENS

    client = get_mp_client()
    data = await client.complete(provider, model, payload)
    answer = client._extract_answer(data)
    usage = data.get("usage", {})
    return {"text": answer, "model": model, "provider": provider, "usage": usage}

# ─── Access control ──────────────────────────────────────────────────────────

async def require_access(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    user = update.effective_user
    if not user:
        return False
    user_id = user.id
    ensure_user(user_id, user.username, user.first_name, allowed=1 if user_id == ADMIN_ID else 0)
    if user_id == ADMIN_ID:
        return True
    u = get_user(user_id)
    if u and u["blocked"]:
        await update.message.reply_text("⛔ Доступ заблокирован.")
        return False
    if u and u["allowed"]:
        if u["quota"] > 0:
            conn = _conn()
            c = conn.cursor()
            c.execute("SELECT requests FROM stats WHERE user_id = ?", (user_id,))
            row = c.fetchone()
            conn.close()
            used = row[0] if row else 0
            if used >= u["quota"]:
                await update.message.reply_text(f"📛 Лимит запросов исчерпан ({u['quota']}). Обратитесь к администратору.")
                return False
        ok, retry = check_rate_limit(user_id)
        if not ok:
            await update.message.reply_text(f"🐢 Слишком много сообщений. Подождите {retry} секунд.")
            return False
        return True
    if is_ignored(user_id):
        await update.message.reply_text("⏳ Ожидайте решения администратора.")
        return False
    await notify_admin_request(update, context)
    await update.message.reply_text("🔒 У вас пока нет доступа. Администратор получил уведомление.")
    return False

async def notify_admin_request(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    query_text = ""
    if update.message and update.message.text:
        query_text = update.message.text[:200]
    elif update.inline_query and update.inline_query.query:
        query_text = "[inline] " + update.inline_query.query[:200]
    text = (
        f"🔔 <b>Новый запрос на доступ</b>\n\n"
        f"ID: <code>{user.id}</code>\n"
        f"Имя: {html.escape(user.first_name or '')}\n"
        f"Юзернейм: @{html.escape(user.username or 'нет')}\n\n"
        f"Сообщение: {html.escape(query_text)}"
    )
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Разрешить", callback_data=f"allow:{user.id}"),
         InlineKeyboardButton("🚫 Игнорировать 5 мин", callback_data=f"ignore:{user.id}")],
    ])
    try:
        await safe_send_message(context.bot, ADMIN_ID, text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
    except Exception as e:
        logger.error(f"Не удалось уведомить админа: {e}")

# ─── User locks ──────────────────────────────────────────────────────────────

_user_locks: Dict[int, asyncio.Lock] = {}

def get_user_lock(user_id: int) -> asyncio.Lock:
    lock = _user_locks.get(user_id)
    if lock is None:
        lock = asyncio.Lock()
        _user_locks[user_id] = lock
    return lock

# ─── TTS helpers ─────────────────────────────────────────────────────────────

VOICE_ENABLED_KEY = "voice_enabled"

def _detect_voice_lang(text: str) -> str:
    cyrillic = sum(1 for c in text if '\u0400' <= c <= '\u04ff')
    return "ru-RU-SvetlanaNeural" if cyrillic > len(text) * 0.3 else "en-US-AriaNeural"

async def _maybe_send_tts(text: str, user_id: int, context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    if not context.user_data.get(VOICE_ENABLED_KEY):
        return
    if not EDGE_TTS_AVAILABLE:
        return
    if len(text) > 3000:
        return
    try:
        voice = _detect_voice_lang(text)
        communicate = edge_tts.Communicate(text[:3000], voice)
        mp3 = io.BytesIO()
        async for chunk in communicate.stream():
            if chunk["type"] == "audio":
                mp3.write(chunk["data"])
        if mp3.tell() == 0:
            return
        mp3.seek(0)
        mp3.name = "voice.mp3"
        await context.bot.send_voice(chat_id=chat_id, voice=mp3)
    except Exception as e:
        logger.warning("TTS failed for user %s: %s", user_id, e)

# ─── Commands ────────────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    ensure_user(user.id, user.username, user.first_name, allowed=1 if user.id == ADMIN_ID else 0)
    text = (
        "👋 <b>Привет!</b>\n\n"
        "Я мульти-провайдерный AI-бот.\n\n"
        "<b>Команды:</b>\n"
        "/new — новый диалог\n"
        "/dialog — мои диалоги\n"
        "/model — выбрать модель и провайдера\n"
        "/role — выбрать роль\n"
        "/prompt — свой системный промпт\n"
        "/inlinemodel — модель для инлайна\n"
        "/inlineprompt — промпт для инлайна\n"
        "/clear — очистить активный диалог\n"
        "/export — экспорт диалога в .md\n"
        "/voice — голосовые ответы on/off\n"
        "/ping — пинг провайдеров\n"
        "/providers — статус провайдеров\n"
        "/help — справка\n"
    )
    if user.id == ADMIN_ID:
        text += (
            "\n<b>Админ-команды:</b>\n"
            "/stats — статистика\n"
            "/users — список пользователей\n"
            "/allow &lt;id&gt; — разрешить\n"
            "/block &lt;id&gt; — заблокировать\n"
            "/setquota &lt;id&gt; &lt;n&gt; — квота\n"
            "/setrate &lt;msg&gt; &lt;win&gt; &lt;ign&gt; — антиспам\n"
            "/setglobalinlineprompt — глобальный инлайн-промпт\n"
            "/reloadkeys — перечитать gemini_keys.txt\n"
        )
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await cmd_start(update, context)

async def cmd_new(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_access(update, context):
        return
    dialog_id = create_dialog(update.effective_user.id, "Новый диалог")
    await update.message.reply_text(f"✅ Создан новый диалог <code>#{dialog_id}</code>.", parse_mode=ParseMode.HTML)

async def cmd_dialogs(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_access(update, context):
        return
    user_id = update.effective_user.id
    dialogs = get_dialogs(user_id)
    if not dialogs:
        await update.message.reply_text("У вас пока нет диалогов. Начните с /new")
        return
    lines = ["<b>📁 Ваши диалоги</b>\n"]
    buttons = []
    for d in dialogs:
        active = " ✓" if d["active"] else ""
        lines.append(f"{d['id']}. {html.escape(d['title'])} (≈{d['token_count']} токенов){active}")
        buttons.append([InlineKeyboardButton(f"#{d['id']} {d['title'][:30]}", callback_data=f"switch:{d['id']}")])
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(buttons))

# ─── Model selection ─────────────────────────────────────────────────────────

MODEL_BUTTONS = [
    [InlineKeyboardButton("🚀 Llama 3.3 70B (Groq)", callback_data="model:groq:llama-3.3-70b-versatile")],
    [InlineKeyboardButton("🚀 Llama 3.1 8B (Groq)", callback_data="model:groq:llama-3.1-8b-instant")],
    [InlineKeyboardButton("🚀 DeepSeek R1 Distill (Groq)", callback_data="model:groq:deepseek-r1-distill-llama-70b")],
    [InlineKeyboardButton("⚡ Gemini 3.5 Flash (Google)", callback_data="model:google:gemini-3.5-flash")],
    [InlineKeyboardButton("🪶 Gemini 3.1 Flash Lite (Google)", callback_data="model:google:gemini-3.1-flash-lite")],
    [InlineKeyboardButton("🧠 Gemini 2.5 Pro (Google)", callback_data="model:google:gemini-2.5-pro")],
    [InlineKeyboardButton("🌐 Gemini 3.5 Flash (OpenRouter)", callback_data="model:openrouter:google/gemini-3.5-flash")],
    [InlineKeyboardButton("🌐 DeepSeek Chat (OpenRouter)", callback_data="model:openrouter:deepseek/deepseek-chat")],
    [InlineKeyboardButton("🌐 Claude Sonnet (OpenRouter)", callback_data="model:openrouter:anthropic/claude-sonnet-4")],
    [InlineKeyboardButton("🔧 Gemini 3.5 Flash (web2api)", callback_data="model:web2api:gemini-3.5-flash")],
    [InlineKeyboardButton("🔧 Gemini 3.1 Pro (web2api)", callback_data="model:web2api:gemini-3.1-pro")],
]

if DEEPSEEK_API_KEY:
    MODEL_BUTTONS.extend([
        [InlineKeyboardButton("🔥 DeepSeek V4 Flash", callback_data="model:deepseek:deepseek-v4-flash")],
        [InlineKeyboardButton("🔥 DeepSeek V4 Pro", callback_data="model:deepseek:deepseek-v4-pro")],
    ])

async def cmd_model(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_access(update, context):
        return
    await update.message.reply_text("Выберите модель:", reply_markup=InlineKeyboardMarkup(MODEL_BUTTONS))

async def cmd_role(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_access(update, context):
        return
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 Обычный", callback_data="role:default")],
        [InlineKeyboardButton("💻 Программист", callback_data="role:programmer")],
        [InlineKeyboardButton("🌐 Переводчик", callback_data="role:translator")],
        [InlineKeyboardButton("📚 Учитель", callback_data="role:teacher")],
        [InlineKeyboardButton("✂️ Кратко", callback_data="role:concise")],
        [InlineKeyboardButton("🎨 Креатив", callback_data="role:creative")],
    ])
    await update.message.reply_text("Выберите роль бота:", reply_markup=keyboard)

async def cmd_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_access(update, context):
        return
    if not context.args:
        u = get_user(update.effective_user.id)
        current = u.get("custom_prompt") or "(не задан)"
        await update.message.reply_text(
            f"<b>Текущий промпт:</b>\n<code>{html.escape(current[:500])}</code>\n\n"
            f"Чтобы изменить: <code>/prompt Твой промпт...</code>",
            parse_mode=ParseMode.HTML,
        )
        return
    prompt = " ".join(context.args)
    set_custom_prompt(update.effective_user.id, prompt)
    await update.message.reply_text("✅ Промпт сохранён. Он будет использоваться вместо роли.")

async def cmd_inlinemodel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_access(update, context):
        return
    inline_buttons = []
    for row in MODEL_BUTTONS:
        btn = row[0]
        new_data = btn.callback_data.replace("model:", "inlinemodel:")
        inline_buttons.append([InlineKeyboardButton(btn.text, callback_data=new_data)])
    await update.message.reply_text("Выберите модель для инлайн-режима:", reply_markup=InlineKeyboardMarkup(inline_buttons))

async def cmd_inlineprompt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_access(update, context):
        return
    if not context.args:
        u = get_user(update.effective_user.id)
        current = u.get("inline_prompt") or "(не задан)"
        await update.message.reply_text(
            f"<b>Текущий инлайн-промпт:</b>\n<code>{html.escape(current[:500])}</code>\n\n"
            f"Чтобы изменить: <code>/inlineprompt Твой промпт...</code>",
            parse_mode=ParseMode.HTML,
        )
        return
    prompt = " ".join(context.args)
    set_inline_prompt(update.effective_user.id, prompt)
    await update.message.reply_text("✅ Инлайн-промпт сохранён.")

async def cmd_clear(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_access(update, context):
        return
    dialog = get_active_dialog(update.effective_user.id)
    if dialog:
        conn = _conn()
        c = conn.cursor()
        c.execute("DELETE FROM dialog_messages WHERE dialog_id = ?", (dialog["id"],))
        c.execute("UPDATE dialogs SET token_count = 0 WHERE id = ?", (dialog["id"],))
        conn.commit()
        conn.close()
        await update.message.reply_text("🗑 История активного диалога очищена.")
    else:
        await update.message.reply_text("Нет активного диалога. Создайте /new")

async def cmd_export(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_access(update, context):
        return
    user_id = update.effective_user.id
    dialog = get_active_dialog(user_id)
    if not dialog:
        await update.message.reply_text("Нет активного диалога для экспорта.")
        return
    history = await asyncio.to_thread(get_dialog_history, dialog["id"], limit=1000)
    if not history:
        await update.message.reply_text("Диалог пуст.")
        return
    lines = [f"# Диалог #{dialog['id']}: {dialog.get('title', 'Без названия')}\n"]
    for msg in history:
        role = "👤 Пользователь" if msg["role"] == "user" else "🤖 Ассистент"
        lines.append(f"## {role}\n\n{msg['content']}\n")
    md_text = "\n".join(lines)
    bio = io.BytesIO(md_text.encode("utf-8"))
    bio.name = f"dialog_{dialog['id']}.md"
    await update.message.reply_document(document=bio, caption="📄 Экспорт диалога")

async def cmd_voice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_access(update, context):
        return
    if not EDGE_TTS_AVAILABLE:
        await update.message.reply_text("🔊 TTS недоступен: библиотека edge-tts не установлена.")
        return
    current = context.user_data.get(VOICE_ENABLED_KEY, False)
    context.user_data[VOICE_ENABLED_KEY] = not current
    status = "включено ✅" if not current else "выключено ❌"
    await update.message.reply_text(f"🔊 Голосовые ответы: {status}")

async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    lines = ["🏓 <b>Ping провайдеров</b>"]
    client = get_mp_client()
    for provider in ("groq", "google", "openrouter", "web2api"):
        if not provider_config.is_provider_available(provider):
            continue
        try:
            ok, ms = await client.healthcheck(provider)
            icon = "✅" if ok else "❌"
            lines.append(f"• {provider}: <code>{ms} мс</code> {icon}")
        except Exception as e:
            lines.append(f"• {provider}: ❌ <code>{html.escape(str(e)[:100])}</code>")
    if DEEPSEEK_API_KEY:
        try:
            ok, ms = await client.healthcheck("deepseek")
            icon = "✅" if ok else "❌"
            lines.append(f"• deepseek: <code>{ms} мс</code> {icon}")
        except Exception as e:
            lines.append(f"• deepseek: ❌ <code>{html.escape(str(e)[:100])}</code>")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)

async def cmd_providers(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    lines = ["<b>📡 Провайдеры</b>"]
    lines.append(f"• Groq: {'✅' if GROQ_API_KEY else '❌ (добавь GROQ_API_KEY в .env)'}")
    lines.append(f"• Google AI Studio: <code>{len(provider_config.google_keys)}</code> ключей")
    lines.append(f"• OpenRouter: {'✅' if OPENROUTER_API_KEY else '❌'}")
    lines.append(f"• web2api: <code>{WEB2API_URL}</code>")
    lines.append(f"• DeepSeek: {'✅' if DEEPSEEK_API_KEY else '❌ (добавь DEEPSEEK_API_KEY в .env)'}")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)

async def cmd_reloadkeys(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != ADMIN_ID:
        return
    provider_config.reload_google_keys()
    await update.message.reply_text(f"🔄 Перечитано <code>{len(provider_config.google_keys)}</code> Google-ключей.", parse_mode=ParseMode.HTML)

# ─── Callbacks ───────────────────────────────────────────────────────────────

async def callback_model(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    data = query.data
    if data.startswith("model:"):
        slug = data[len("model:"):]
        context.user_data["model"] = slug
        provider, model = _parse_model_slug(slug)
        await query.edit_message_text(
            f"✅ Модель: <code>{html.escape(model)}</code>\nПровайдер: <code>{provider}</code>",
            parse_mode=ParseMode.HTML,
        )

async def callback_inlinemodel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    data = query.data
    if data.startswith("inlinemodel:"):
        slug = data[len("inlinemodel:"):]
        set_inline_model(update.effective_user.id, slug)
        provider, model = _parse_model_slug(slug)
        await query.edit_message_text(
            f"✅ Инлайн-модель: <code>{html.escape(model)}</code>\nПровайдер: <code>{provider}</code>",
            parse_mode=ParseMode.HTML,
        )

async def callback_role(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    data = query.data
    if data.startswith("role:"):
        role = data.split(":", 1)[1]
        set_system_role(update.effective_user.id, role if role != "default" else None)
        set_custom_prompt(update.effective_user.id, None)
        name = {"programmer": "💻 Программист", "translator": "🌐 Переводчик", "teacher": "📚 Учитель", "concise": "✂️ Кратко", "creative": "🎨 Креатив"}.get(role, "🔄 Обычный")
        await query.edit_message_text(f"✅ Роль: {name}", parse_mode=ParseMode.HTML)

async def callback_switch(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    data = query.data
    if data.startswith("switch:"):
        dialog_id = int(data.split(":", 1)[1])
        if switch_dialog(update.effective_user.id, dialog_id):
            await query.edit_message_text(f"✅ Переключено на диалог <code>#{dialog_id}</code>", parse_mode=ParseMode.HTML)
        else:
            await query.answer("Ошибка переключения", show_alert=True)

async def callback_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if update.effective_user.id != ADMIN_ID:
        await query.answer("Недостаточно прав.", show_alert=True)
        return
    await query.answer()
    data = query.data
    if not data or ":" not in data:
        return
    action, user_id_str = data.split(":", 1)
    try:
        user_id = int(user_id_str)
    except ValueError:
        return
    if action == "allow":
        set_allowed(user_id, 1)
        set_blocked(user_id, 0)
        await query.edit_message_text(f"✅ Доступ разрешён для <code>{user_id}</code>", parse_mode=ParseMode.HTML)
        try:
            await safe_send_message(context.bot, user_id, "🎉 Администратор разрешил вам доступ!")
        except Exception:
            pass
    elif action == "ignore":
        add_ignore(user_id)
        await query.edit_message_text(f"🚫 Запрос от <code>{user_id}</code> проигнорирован на 5 минут.", parse_mode=ParseMode.HTML)

async def callback_actions(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    data = query.data
    if data.startswith("rephrase:"):
        prompt = context.user_data.get("last_prompt", "")
        if prompt:
            await _process_text(update, context, f"Перефразируй иначе: {prompt}", edit_msg=query.message)
    elif data.startswith("continue:"):
        await _process_text(update, context, "Продолжай", edit_msg=query.message)
    elif data.startswith("delete:"):
        try:
            await query.message.delete()
        except Exception:
            pass

# ─── Message processing ──────────────────────────────────────────────────────

async def _process_stream_locked(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    prompt: str,
    model_slug: str,
    dialog_id: Optional[int],
) -> None:
    user_id = update.effective_user.id
    provider, model = _parse_model_slug(model_slug)

    messages = []
    u = get_user(user_id)
    final_system = None
    if u and u.get("custom_prompt"):
        final_system = u["custom_prompt"]
    if not final_system and u and u.get("system_role"):
        role_text = {
            "programmer": "Ты senior-разработчик. Пиши чистый, современный код с пояснениями.",
            "translator": "Ты профессиональный переводчик. Переводи точно, сохраняя стиль.",
            "teacher": "Ты терпеливый учитель. Объясняй сложные вещи простым языком, с примерами.",
            "concise": "Отвечай максимально кратко и по делу. Без воды.",
            "creative": "Ты креативный помощник. Предлагай нестандартные идеи.",
        }.get(u["system_role"], "")
        if role_text:
            final_system = role_text
    if final_system:
        messages.append({"role": "system", "content": final_system})

    if dialog_id:
        history = await asyncio.to_thread(get_dialog_history, dialog_id, GEMINI_HISTORY_MESSAGES)
        messages.extend(trim_dialog_history(history))

    messages.append({"role": "user", "content": prompt})

    payload = {"messages": messages, "stream": True}
    if GEMINI_MAX_TOKENS > 0:
        payload["max_tokens"] = GEMINI_MAX_TOKENS

    wait_msg = await update.effective_message.reply_text("⏳ Думаю...")
    buffer = ""
    last_edit = _time_module.monotonic()
    edit_errors = 0

    try:
        await _increment_active()
        async for chunk in get_mp_client().stream_complete(provider, model, payload):
            buffer += chunk
            now = _time_module.monotonic()
            if now - last_edit > 1.2 and buffer:
                preview = buffer[-3500:]
                try:
                    await safe_edit_message_text(
                        wait_msg,
                        html.escape(preview) + " ▌",
                        parse_mode=ParseMode.HTML,
                    )
                    last_edit = now
                    edit_errors = 0
                except Exception as e:
                    edit_errors += 1
                    if edit_errors > 5:
                        raise
                    logger.warning("Stream edit error: %s", e)

        await safe_delete_message(wait_msg)

        if not buffer.strip():
            await update.effective_message.reply_text("(пустой ответ)")
            return

        if dialog_id:
            await asyncio.to_thread(add_dialog_message, dialog_id, "user", prompt)
            await asyncio.to_thread(add_dialog_message, dialog_id, "assistant", buffer)
            pt = max(1, len(json.dumps(messages, ensure_ascii=False)) // 4)
            ct = max(1, len(buffer) // 4)
            await asyncio.to_thread(update_dialog_meta, dialog_id, pt + ct)
            await asyncio.to_thread(update_stats, user_id, pt, ct)

        raw_chunks = split_plain_text(buffer)
        chunks = [format_gemini_text(chunk) for chunk in raw_chunks]
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Перефразируй", callback_data="rephrase:1"),
             InlineKeyboardButton("💡 Продолжай", callback_data="continue:1")],
            [InlineKeyboardButton("❌ Удалить", callback_data="delete:1")],
        ])

        last_msg = None
        for i, chunk in enumerate(chunks):
            try:
                last_msg = await update.effective_message.reply_text(
                    chunk,
                    parse_mode=ParseMode.HTML,
                    reply_markup=keyboard if i == len(chunks) - 1 else None,
                )
            except BadRequest as exc:
                logger.warning("Telegram HTML error: %s", exc)
                last_msg = await update.effective_message.reply_text(
                    raw_chunks[i],
                    parse_mode=None,
                    reply_markup=keyboard if i == len(chunks) - 1 else None,
                )

        if last_msg:
            context.user_data["last_bot_message_id"] = last_msg.message_id
            context.user_data["last_prompt"] = prompt

        if last_msg and len(buffer) <= 3000:
            await _maybe_send_tts(buffer, user_id, context, update.effective_chat.id)

    except ProviderTimeoutError:
        logger.warning("Stream timeout: user=%s model=%s", user_id, model_slug)
        await safe_edit_message_text(
            wait_msg,
            f"⌛ <b>Провайдер не успел ответить.</b>\n\n"
            f"Сервер не прислал ответ за {GEMINI_READ_TIMEOUT:.0f} секунд.\n"
            "Попробуйте ещё раз или выберите другую модель.",
            parse_mode=ParseMode.HTML,
        )
    except httpx.HTTPStatusError as exc:
        logger.error("HTTP error: status=%s body=%r", exc.response.status_code, exc.response.text[:500])
        await safe_edit_message_text(
            wait_msg,
            f"❌ Ошибка API: <code>HTTP {exc.response.status_code}</code>",
            parse_mode=ParseMode.HTML,
        )
    except ProviderError as exc:
        logger.warning("Stream failed: %s", exc)
        await safe_edit_message_text(
            wait_msg,
            f"❌ Провайдер временно недоступен.\n<code>{html.escape(str(exc)[:500])}</code>",
            parse_mode=ParseMode.HTML,
        )
    except Exception:
        logger.exception("Unexpected stream error")
        await safe_edit_message_text(
            wait_msg,
            "❌ Произошла внутренняя ошибка.\nПопробуйте повторить запрос позже.",
            parse_mode=ParseMode.HTML,
        )
    finally:
        await _decrement_active()

async def _process_text_locked(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    prompt: str,
    image_b64: Optional[str] = None,
    doc_text: Optional[str] = None,
    edit_msg: Optional[Any] = None,
) -> None:
    user = update.effective_user
    user_id = user.id
    model_slug = context.user_data.get("model", DEFAULT_MODEL)

    use_stream = (
        STREAMING_ENABLED
        and not image_b64
        and not doc_text
        and not edit_msg
    )

    dialog = get_active_dialog(user_id)
    if not dialog:
        dialog_id = create_dialog(user_id, make_title_from_text(prompt))
    else:
        dialog_id = dialog["id"]
        conn = _conn()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM dialog_messages WHERE dialog_id = ?", (dialog_id,))
        cnt = c.fetchone()[0]
        conn.close()
        if cnt == 0 and dialog.get("title") in (None, "", "Новый диалог"):
            update_dialog_title(dialog_id, make_title_from_text(prompt))

    if use_stream:
        await _process_stream_locked(update, context, prompt, model_slug, dialog_id)
        return

    wait_msg = None
    if edit_msg:
        try:
            await edit_msg.edit_text("⏳ Думаю...")
            wait_msg = edit_msg
        except Exception:
            wait_msg = await update.effective_message.reply_text("⏳ Думаю...")
    else:
        wait_msg = await update.effective_message.reply_text("⏳ Думаю...")

    try:
        await _increment_active()
        result = await ask_ai(user_id, prompt, model_slug, dialog_id=dialog_id, image_b64=image_b64, doc_text=doc_text)
    except ProviderTimeoutError:
        logger.warning("Timeout: user=%s model=%s", user_id, model_slug)
        await wait_msg.edit_text(
            f"⌛ <b>Провайдер не успел ответить.</b>\n\n"
            f"Сервер не прислал ответ за {GEMINI_READ_TIMEOUT:.0f} секунд.\n"
            "Попробуйте ещё раз или выберите другую модель.",
            parse_mode=ParseMode.HTML,
        )
        return
    except httpx.HTTPStatusError as exc:
        logger.error("HTTP error: status=%s body=%r", exc.response.status_code, exc.response.text[:500])
        await wait_msg.edit_text(f"❌ Ошибка API: <code>HTTP {exc.response.status_code}</code>", parse_mode=ParseMode.HTML)
        return
    except ProviderError as exc:
        logger.warning("Request failed: %s", exc)
        await wait_msg.edit_text(f"❌ Провайдер временно недоступен.\n<code>{html.escape(str(exc)[:500])}</code>", parse_mode=ParseMode.HTML)
        return
    except Exception:
        logger.exception("Unexpected error")
        await wait_msg.edit_text("❌ Произошла внутренняя ошибка.\nПопробуйте повторить запрос позже.")
        return
    finally:
        await _decrement_active()

    raw_chunks = split_plain_text(result["text"])
    chunks = [format_gemini_text(chunk) for chunk in raw_chunks]
    context.user_data["last_prompt"] = prompt

    try:
        await wait_msg.delete()
    except Exception:
        pass

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 Перефразируй", callback_data="rephrase:1"),
         InlineKeyboardButton("💡 Продолжай", callback_data="continue:1")],
        [InlineKeyboardButton("❌ Удалить", callback_data="delete:1")],
    ])

    last_msg = None
    for i, chunk in enumerate(chunks):
        try:
            last_msg = await update.effective_message.reply_text(
                chunk, parse_mode=ParseMode.HTML, reply_markup=keyboard if i == len(chunks) - 1 else None
            )
        except BadRequest as exc:
            logger.warning("Telegram HTML error: %s", exc)
            last_msg = await update.effective_message.reply_text(
                raw_chunks[i], parse_mode=None, reply_markup=keyboard if i == len(chunks) - 1 else None
            )
        except Exception as e:
            logger.error(f"Send error: {e}")
            await update.effective_message.reply_text(html.escape(raw_chunks[i]))

    if last_msg:
        context.user_data["last_bot_message_id"] = last_msg.message_id
        if len(result["text"]) <= 3000:
            await _maybe_send_tts(result["text"], user_id, context, update.effective_chat.id)

async def _process_text(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    prompt: str,
    image_b64: Optional[str] = None,
    doc_text: Optional[str] = None,
    edit_msg: Optional[Any] = None,
) -> None:
    user = update.effective_user
    if not user:
        return
    async with get_user_lock(user.id):
        await _process_text_locked(update, context, prompt, image_b64, doc_text, edit_msg)

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_access(update, context):
        return
    prompt = update.message.text
    if not prompt:
        return
    await _process_text(update, context, prompt)

# ─── Photo handler ───────────────────────────────────────────────────────────

async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_access(update, context):
        return
    prompt = update.message.caption or "Опиши эту картинку"
    photo = update.message.photo[-1]
    logger.info("Photo handler: file_id=%s w=%s h=%s", photo.file_id, photo.width, photo.height)
    try:
        file = await photo.get_file()
        bytes_data = await file.download_as_bytearray()
        logger.info("Photo downloaded: %s bytes", len(bytes_data))
        if len(bytes_data) == 0:
            await update.message.reply_text("❌ Не удалось скачать фото (пустой файл).")
            return
        b64 = base64.b64encode(bytes_data).decode()
        if len(b64) > 5_000_000:
            await update.message.reply_text("❌ Изображение слишком большое для обработки (>~3.7 МБ).")
            return
        logger.info("Photo base64 length: %s", len(b64))
        await _process_text(update, context, prompt, image_b64=b64)
    except Exception as e:
        logger.exception("Photo processing failed")
        await update.message.reply_text(f"❌ Ошибка обработки фото: {html.escape(str(e)[:500])}", parse_mode=ParseMode.HTML)

# ─── Document handler ────────────────────────────────────────────────────────

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_access(update, context):
        return
    doc = update.message.document
    if not doc:
        return
    mime = doc.mime_type or ""
    prompt = update.message.caption or "Расскажи о содержании этого документа"
    file = await doc.get_file()
    bytes_data = await file.download_as_bytearray()
    doc_text = ""
    try:
        if mime == "text/plain":
            doc_text = bytes_data.decode("utf-8", errors="replace")[:12000]
        elif mime == "application/pdf":
            try:
                import PyPDF2
                reader = PyPDF2.PdfReader(io.BytesIO(bytes_data))
                parts = [page.extract_text() or "" for page in reader.pages[:10]]
                doc_text = "\n".join(parts)[:12000]
            except Exception as e:
                await update.message.reply_text(f"❌ Ошибка чтения PDF: {html.escape(str(e))}", parse_mode=ParseMode.HTML)
                return
        elif mime in ("application/vnd.openxmlformats-officedocument.wordprocessingml.document",):
            try:
                import docx
                d = docx.Document(io.BytesIO(bytes_data))
                doc_text = "\n".join([p.text for p in d.paragraphs])[:12000]
            except Exception as e:
                await update.message.reply_text(f"❌ Ошибка чтения DOCX: {html.escape(str(e))}", parse_mode=ParseMode.HTML)
                return
        else:
            await update.message.reply_text("📄 Поддерживаются только .txt, .pdf, .docx")
            return
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка обработки файла: {html.escape(str(e))}", parse_mode=ParseMode.HTML)
        return
    if not doc_text.strip():
        await update.message.reply_text("📄 Документ пустой или текст не удалось извлечь.")
        return
    await _process_text(update, context, prompt, doc_text=doc_text)

# ─── Inline mode ─────────────────────────────────────────────────────────────

WAIT_KEYBOARD = InlineKeyboardMarkup([
    [InlineKeyboardButton(text="⏳ Ответ генерируется…", callback_data="ai_wait")]
])

async def inline_query(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    inline = update.inline_query
    user = update.effective_user
    query = (inline.query or "").strip()

    if not user or len(query) < 2:
        await inline.answer([], cache_time=1, is_personal=True)
        return

    ensure_user(user.id, user.username, user.first_name, allowed=1 if user.id == ADMIN_ID else 0)

    if user.id != ADMIN_ID:
        db_user = get_user(user.id)
        if not db_user or not db_user["allowed"] or db_user["blocked"]:
            if not is_ignored(user.id):
                await notify_admin_request(update, context)
            await inline.answer([], cache_time=2, is_personal=True)
            return

    result_id = "gemini:" + hashlib.sha256(f"{user.id}:{query}".encode("utf-8")).hexdigest()[:40]
    title = query[:50] + "..." if len(query) > 50 else query
    results = [
        InlineQueryResultArticle(
            id=result_id,
            title=title,
            input_message_content=InputTextMessageContent(
                f"⏳ <b>Запрос:</b> {html.escape(query[:200])}\n\n<i>Обрабатываю…</i>",
                parse_mode=ParseMode.HTML,
            ),
            reply_markup=WAIT_KEYBOARD,
            description="Нажмите, чтобы отправить запрос",
        )
    ]
    await inline.answer(results, cache_time=0, is_personal=True)

async def callback_ai_wait(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.callback_query.answer("Ответ ещё генерируется…", cache_time=1)

async def _process_inline_chosen(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chosen = update.chosen_inline_result
    if not chosen:
        return
    if not (chosen.result_id == "gemini_inline" or chosen.result_id.startswith("gemini:")):
        return

    query_text = chosen.query.strip()
    user_id = chosen.from_user.id
    inline_message_id = chosen.inline_message_id

    logger.info(f"Inline chosen: user={user_id}, query={query_text[:50]!r}, msg_id={inline_message_id}")

    if user_id != ADMIN_ID:
        u = get_user(user_id)
        if not u or not u["allowed"] or u["blocked"]:
            return

    u = get_user(user_id)
    inline_model = (u.get("inline_model") or INLINE_MODEL) if u else INLINE_MODEL
    inline_prompt = u.get("inline_prompt") if u else None

    try:
        result = await ask_ai_inline(user_id, query_text, inline_model, system_prompt=inline_prompt)
        formatted = format_gemini_text(result["text"])
        if len(formatted) > 4000:
            formatted = formatted[:3990] + "\n\n<i>...обрезано</i>"
        full_text = f"<b>Вопрос:</b> {html.escape(query_text[:200])}\n\n{formatted}"
        usage = result.get("usage", {})
        pt = usage.get("prompt_tokens")
        if pt is None:
            pt = max(1, len(query_text) // 4)
        ct = usage.get("completion_tokens")
        if ct is None:
            ct = max(1, len(result["text"]) // 4)
        await asyncio.to_thread(update_stats, user_id, pt, ct)
    except ProviderTimeoutError:
        logger.warning("Inline timeout: user=%s model=%s", user_id, inline_model)
        full_text = f"<b>Вопрос:</b> {html.escape(query_text[:200])}\n\n⌛ Провайдер не успел ответить. Попробуйте ещё раз или выберите другую модель."
    except httpx.HTTPStatusError as exc:
        logger.error("Inline HTTP error: %s %r", exc.response.status_code, exc.response.text[:500])
        full_text = f"<b>Вопрос:</b> {html.escape(query_text[:200])}\n\n❌ Ошибка API: <code>HTTP {exc.response.status_code}</code>"
    except ProviderError as exc:
        logger.warning("Inline error: %s", exc)
        full_text = f"<b>Вопрос:</b> {html.escape(query_text[:200])}\n\n❌ Провайдер временно недоступен."
    except Exception:
        logger.exception("Unexpected inline error")
        full_text = f"<b>Вопрос:</b> {html.escape(query_text[:200])}\n\n❌ Произошла внутренняя ошибка."

    if inline_message_id:
        try:
            await safe_bot_edit_message_text(
                context.bot,
                text=full_text,
                inline_message_id=inline_message_id,
                parse_mode=ParseMode.HTML,
                reply_markup=None,
            )
            logger.info("Inline: message edited successfully")
        except Exception as e:
            logger.error(f"Inline edit failed: {e}")
    else:
        try:
            await safe_send_message(
                context.bot,
                chat_id=user_id,
                text=f"📨 <b>Ответ на инлайн-запрос:</b>\n\n{full_text}",
                parse_mode=ParseMode.HTML,
            )
        except Exception as e:
            logger.error(f"Inline PM fallback failed: {e}")

async def chosen_inline_result(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    context.application.create_task(_process_inline_chosen(update, context), update=update)

# ─── Admin commands ──────────────────────────────────────────────────────────

async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != ADMIN_ID:
        return
    rows = get_all_stats()
    if not rows:
        await update.message.reply_text("Нет данных.")
        return
    lines = ["<b>📊 Статистика</b>"]
    for r in rows:
        name = r["username"] or r["first_name"] or str(r["user_id"])
        status = "🟢" if r["allowed"] else "🔴"
        if r["blocked"]:
            status = "⛔"
        lines.append(
            f"{status} <code>{r['user_id']}</code> | @{html.escape(name)} | "
            f"квота: {r['quota']} | запросов: {r['requests']} | токенов: {r['tokens_prompt'] + r['tokens_completion']}"
        )
    for chunk in split_html_lines(lines, max_len=3500):
        await update.message.reply_text(chunk, parse_mode=ParseMode.HTML)

async def cmd_users(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != ADMIN_ID:
        return
    rows = get_all_stats()
    lines = ["<b>👥 Пользователи</b>"]
    for r in rows:
        name = r["username"] or r["first_name"] or "—"
        status = "разрешён" if r["allowed"] else "ожидание"
        if r["blocked"]:
            status = "заблокирован"
        lines.append(f"• <code>{r['user_id']}</code> | @{html.escape(name)} | {status} | квота: {r['quota']}")
    for chunk in split_html_lines(lines, max_len=3500):
        await update.message.reply_text(chunk, parse_mode=ParseMode.HTML)

async def cmd_allow(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != ADMIN_ID:
        return
    if not context.args:
        await update.message.reply_text("Использование: /allow &lt;user_id&gt;", parse_mode=ParseMode.HTML)
        return
    try:
        uid = int(context.args[0])
    except ValueError:
        await update.message.reply_text("Неверный ID.")
        return
    set_allowed(uid, 1)
    set_blocked(uid, 0)
    await update.message.reply_text(f"✅ Разрешён доступ для <code>{uid}</code>", parse_mode=ParseMode.HTML)
    try:
        await safe_send_message(context.bot, uid, "🎉 Вам разрешили доступ!")
    except Exception:
        pass

async def cmd_block(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != ADMIN_ID:
        return
    if not context.args:
        await update.message.reply_text("Использование: /block &lt;user_id&gt;", parse_mode=ParseMode.HTML)
        return
    try:
        uid = int(context.args[0])
    except ValueError:
        await update.message.reply_text("Неверный ID.")
        return
    set_blocked(uid, 1)
    set_allowed(uid, 0)
    await update.message.reply_text(f"⛔ Пользователь <code>{uid}</code> заблокирован.", parse_mode=ParseMode.HTML)

async def cmd_setquota(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != ADMIN_ID:
        return
    if len(context.args) < 2:
        await update.message.reply_text("Использование: /setquota &lt;user_id&gt; &lt;n&gt;", parse_mode=ParseMode.HTML)
        return
    try:
        uid = int(context.args[0])
        quota = int(context.args[1])
    except ValueError:
        await update.message.reply_text("Неверные аргументы.")
        return
    set_quota(uid, quota)
    await update.message.reply_text(f"✅ Квота для <code>{uid}</code> установлена: {quota}", parse_mode=ParseMode.HTML)

async def cmd_setrate(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != ADMIN_ID:
        return
    if len(context.args) < 3:
        await update.message.reply_text(
            "Использование: /setrate &lt;сообщений&gt; &lt;окно_сек&gt; &lt;игнор_сек&gt;\n"
            "Пример: <code>/setrate 5 60 600</code> — 5 сообщений в минуту, игнор 10 минут",
            parse_mode=ParseMode.HTML,
        )
        return
    try:
        msgs = int(context.args[0])
        window = int(context.args[1])
        ignore = int(context.args[2])
    except ValueError:
        await update.message.reply_text("Неверные аргументы.")
        return
    set_setting("rate_limit_messages", str(msgs))
    set_setting("rate_limit_window", str(window))
    set_setting("rate_limit_ignore", str(ignore))
    await update.message.reply_text(
        f"✅ Rate limit обновлён:\n"
        f"• {msgs} сообщений за {window} сек\n"
        f"• Игнор при превышении: {ignore} сек",
        parse_mode=ParseMode.HTML,
    )

async def cmd_setglobalinlineprompt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != ADMIN_ID:
        return
    if not context.args:
        current = get_setting("global_inline_prompt", "(не задан)")
        await update.message.reply_text(
            f"<b>Глобальный инлайн-промпт:</b>\n<code>{html.escape(current[:500])}</code>\n\n"
            f"Чтобы изменить: <code>/setglobalinlineprompt Твой промпт...</code>",
            parse_mode=ParseMode.HTML,
        )
        return
    prompt = " ".join(context.args)
    set_setting("global_inline_prompt", prompt)
    await update.message.reply_text("✅ Глобальный инлайн-промпт сохранён.")

# ─── Backup job ──────────────────────────────────────────────────────────────

def create_sqlite_backup(destination: str) -> None:
    source = _conn()
    target = sqlite3.connect(destination)
    try:
        source.backup(target)
    finally:
        target.close()
        source.close()

async def backup_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        os.makedirs("/app/data/backups", exist_ok=True)
        ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        dst = f"/app/data/backups/bot_{ts}.db"
        if os.path.exists(DB_PATH):
            await asyncio.to_thread(create_sqlite_backup, dst)
            files = sorted(
                [f for f in os.listdir("/app/data/backups") if f.startswith("bot_")],
                key=lambda x: os.path.getmtime(os.path.join("/app/data/backups", x)),
            )
            for old in files[:-10]:
                os.remove(os.path.join("/app/data/backups", old))
            logger.info(f"Backup created: {dst}")
    except Exception as e:
        logger.error(f"Backup failed: {e}")

# ─── Healthcheck job ─────────────────────────────────────────────────────────

async def healthcheck_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    client = get_mp_client()
    for provider in ("groq", "google", "openrouter", "web2api"):
        if not provider_config.is_provider_available(provider):
            continue
        try:
            ok, ms = await client.healthcheck(provider)
            if ok:
                logger.debug("Healthcheck %s OK (%d ms)", provider, ms)
            else:
                logger.warning("Healthcheck %s failed (HTTP non-200, %d ms)", provider, ms)
        except Exception as exc:
            logger.warning("Healthcheck %s exception: %s", provider, exc)

# ─── Error handler ───────────────────────────────────────────────────────────

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error(f"Exception while handling an update: {context.error}", exc_info=context.error)
    if update and update.effective_message:
        try:
            await update.effective_message.reply_text(
                f"❌ Произошла внутренняя ошибка: <code>{html.escape(str(context.error))}</code>",
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass

# ─── Main ────────────────────────────────────────────────────────────────────

def main() -> None:
    init_db()
    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .concurrent_updates(TELEGRAM_CONCURRENT_UPDATES)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("help", cmd_help))
    application.add_handler(CommandHandler("new", cmd_new))
    application.add_handler(CommandHandler("dialog", cmd_dialogs))
    application.add_handler(CommandHandler("model", cmd_model))
    application.add_handler(CommandHandler("role", cmd_role))
    application.add_handler(CommandHandler("prompt", cmd_prompt))
    application.add_handler(CommandHandler("inlinemodel", cmd_inlinemodel))
    application.add_handler(CommandHandler("inlineprompt", cmd_inlineprompt))
    application.add_handler(CommandHandler("clear", cmd_clear))
    application.add_handler(CommandHandler("export", cmd_export))
    application.add_handler(CommandHandler("voice", cmd_voice))
    application.add_handler(CommandHandler("ping", cmd_ping))
    application.add_handler(CommandHandler("providers", cmd_providers))
    application.add_handler(CommandHandler("stats", cmd_stats))
    application.add_handler(CommandHandler("users", cmd_users))
    application.add_handler(CommandHandler("allow", cmd_allow))
    application.add_handler(CommandHandler("block", cmd_block))
    application.add_handler(CommandHandler("setquota", cmd_setquota))
    application.add_handler(CommandHandler("setrate", cmd_setrate))
    application.add_handler(CommandHandler("setglobalinlineprompt", cmd_setglobalinlineprompt))
    application.add_handler(CommandHandler("reloadkeys", cmd_reloadkeys))

    application.add_handler(CallbackQueryHandler(callback_model, pattern=r"^model:"))
    application.add_handler(CallbackQueryHandler(callback_inlinemodel, pattern=r"^inlinemodel:"))
    application.add_handler(CallbackQueryHandler(callback_role, pattern=r"^role:"))
    application.add_handler(CallbackQueryHandler(callback_switch, pattern=r"^switch:"))
    application.add_handler(CallbackQueryHandler(callback_admin, pattern=r"^(allow|ignore):"))
    application.add_handler(CallbackQueryHandler(callback_actions, pattern=r"^(rephrase|continue|delete):"))
    application.add_handler(CallbackQueryHandler(callback_ai_wait, pattern=r"^ai_wait$"))

    application.add_handler(InlineQueryHandler(inline_query))
    application.add_handler(ChosenInlineResultHandler(chosen_inline_result))

    application.add_handler(MessageHandler(filters.PHOTO, handle_photo))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    application.add_error_handler(error_handler)
    application.job_queue.run_daily(backup_job, time=dt_time(hour=4, minute=0))
    application.job_queue.run_repeating(healthcheck_job, interval=3600, first=300)  # раз в час!

    logger.info("Бот запущен. Ожидаю сообщения...")
    application.run_polling()

if __name__ == "__main__":
    main()
