
import argparse
import asyncio
import hashlib
import json
import logging
import os
import random
import re
import signal
import sqlite3
import sys
import time
import unicodedata
from pathlib import Path
from statistics import mean
from typing import Any, Iterable

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    CallbackQuery,
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)
from dotenv import load_dotenv


# =========================
# CONFIG
# =========================

load_dotenv()
logging.basicConfig(level=logging.INFO)

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = Path("/app/data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = DATA_DIR / "db.sqlite3"
DISABLE_FLAG = DATA_DIR / "bot_disabled.flag"
CITY_WHITELIST_PATHS = [
    BASE_DIR / "ru_kz_city_whitelist.json",
    DATA_DIR / "ru_kz_city_whitelist.json",
]

DEFAULT_BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_ID = int(os.getenv("ADMIN_ID", "7106262808"))

parser = argparse.ArgumentParser(add_help=False)
parser.add_argument("--child", action="store_true")
parser.add_argument("--token", default="")
parser.add_argument("--instance-key", default="")
CLI_ARGS, _UNKNOWN = parser.parse_known_args()

IS_CHILD_PROCESS = bool(CLI_ARGS.child)
BOT_TOKEN = (CLI_ARGS.token.strip() or DEFAULT_BOT_TOKEN).strip()
INSTANCE_KEY = CLI_ARGS.instance_key.strip()

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN не найден. Проверь .env или аргумент --token")

if not INSTANCE_KEY:
    if IS_CHILD_PROCESS:
        INSTANCE_KEY = hashlib.sha1(BOT_TOKEN.encode("utf-8")).hexdigest()[:12]
    else:
        INSTANCE_KEY = "primary"

BOT = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
DP = Dispatcher()
ROUTER = Router()
DP.include_router(ROUTER)

DB = sqlite3.connect(DB_PATH, check_same_thread=False)
DB.row_factory = sqlite3.Row
DB.execute("PRAGMA journal_mode=WAL")
DB.execute("PRAGMA busy_timeout=5000")

# In-memory state
captcha_answers: dict[int, int] = {}
waiting_for_city: set[int] = set()
waiting_for_city_change: set[int] = set()
waiting_for_promo: set[int] = set()
waiting_for_deposit_amount: dict[int, str] = {}
waiting_for_bot_token: set[int] = set()
admin_state: dict[int, dict[str, Any]] = {}
carts: dict[int, list[dict[str, Any]]] = {}
broadcast_targets: dict[int, dict[str, Any]] = {}
child_processes: dict[str, asyncio.subprocess.Process] = {}

DB_LOCK = asyncio.Lock()


# =========================
# DEFAULT CONTENT
# =========================

SETTINGS_DEFAULTS = {
    "start_text": "<b>💎 Добро пожаловать в лучший шоп России и Казахстана!</b>\n\nСначала пройди капчу.",
    "registration_text": "<b>✅ Капча пройдена</b>\n\nТеперь введи свой город вручную.",
    "account_text": (
        "<b>👤 Аккаунт</b>\n\n"
        "🌍 Город: <code>{city}</code>\n"
        "💰 Баланс: <b>{balance_rub:.2f} ₽</b> (~<b>{balance_usdt:.2f} USDT</b>)\n"
        "📉 Курс: <code>1 USDT ≈ {rate:.2f} ₽</code>\n"
        "🏷 Промокод: <code>{promo}</code>"
    ),
    "deposit_text": "<b>💳 Пополнение баланса</b>\n\nСначала выбери валюту, потом введи сумму в рублях.",
    "support_text": "💬 Поддержка пока не настроена.",
    "catalog_text": "<b>🛍 Каталог</b>\n\nВыбери товар ниже.",
    "empty_cart_text": "<b>🧺 Корзина пустая.</b>",
    "order_success_text": "<b>✅ Заказ оформлен</b>",
    "insufficient_balance_text": "<b>Недостаточно средств</b>",
    "usdt_rate_override": "",
    "kill_switch": "0",
}

MEDIA_TITLES = {
    "registration": "Фото регистрации",
    "account": "Фото аккаунта",
    "deposit": "Фото пополнения",
    "support": "Фото техподдержки",
    "catalog": "Фото каталога",
}

DEFAULT_MEDIA_FILES = {
    "registration": "registration.jpg",
    "account": "account.jpg",
    "deposit": "deposit.jpg",
    "support": "support.jpg",
    "catalog": "catalog.jpg",
}

DEFAULT_WALLETS = {
    "USDT": {"network": "TRC20", "address": "TRC20_ADDRESS_HERE", "active": 1},
    "BTC": {"network": "Bitcoin", "address": "BTC_ADDRESS_HERE", "active": 1},
    "ETH": {"network": "ERC20", "address": "ETH_ADDRESS_HERE", "active": 1},
}

DEFAULT_PRODUCTS = [
    ("Пуэр Шу", "Плотный, тёмный, классический пуэр с глубоким вкусом."),
    ("Шен Пуэр", "Свежий, бодрый шен пуэр с мягкой терпкостью."),
    ("Матча", "Японский зелёный чай для напитков и десертов."),
    ("Сенча", "Лёгкий зелёный чай с травянистым вкусом."),
    ("Да Хун Пао", "Улун с тёплым, орехово-фруктовым профилем."),
    ("Те Гуань Инь", "Ароматный улун с мягким цветочным послевкусием."),
    ("Габа Улун", "Насыщенный улун с мягким сладким оттенком."),
    ("Эрл Грей", "Классический чёрный чай с бергамотом."),
]

DEFAULT_WEIGHTS = {
    1: [("50 г", 390), ("100 г", 720), ("300 г", 1900), ("1 кг", 5600)],
    2: [("50 г", 360), ("100 г", 680), ("250 г", 1550), ("500 г", 2900)],
    3: [("30 г", 450), ("100 г", 1300), ("250 г", 2900)],
    4: [("50 г", 320), ("100 г", 590), ("300 г", 1600)],
    5: [("50 г", 480), ("100 г", 890), ("250 г", 2050)],
    6: [("50 г", 430), ("100 г", 790), ("300 г", 1980)],
    7: [("50 г", 520), ("100 г", 980), ("250 г", 2350)],
    8: [("100 г", 310), ("250 г", 690), ("500 г", 1250)],
}

PROMO_ALLOWED_KINDS = {"discount", "bonus_balance", "gift"}

BYBIT_P2P_URLS = [
    "https://www.bybit.com/en/fiat/trade/otc/sell/USDT/RUB?actionType=0&fiat=RUB",
    "https://www.bybit.com/en/fiat/trade/otc/sell/USDT/RUB?actionType=0&fiat=RUB&paymentMethod=75",
]
DEFAULT_USDT_RUB_RATE = 82.0
RATE_CACHE_TTL = 15 * 60
RATE_CACHE = {"value": None, "updated_at": 0.0}


# =========================
# DB INIT / MIGRATIONS
# =========================

def db_execute(query: str, params: tuple = ()) -> sqlite3.Cursor:
    cur = DB.execute(query, params)
    DB.commit()
    return cur


def db_fetchone(query: str, params: tuple = ()) -> sqlite3.Row | None:
    cur = DB.execute(query, params)
    return cur.fetchone()


def db_fetchall(query: str, params: tuple = ()) -> list[sqlite3.Row]:
    cur = DB.execute(query, params)
    return cur.fetchall()


def init_db() -> None:
    DB.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            full_name TEXT,
            city TEXT,
            balance REAL DEFAULT 0,
            captcha_passed INTEGER DEFAULT 0,
            promo_code TEXT DEFAULT NULL
        )
        """
    )
    DB.execute(
        """
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """
    )
    DB.execute(
        """
        CREATE TABLE IF NOT EXISTS media_assets (
            media_key TEXT PRIMARY KEY,
            file_id TEXT NOT NULL,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    DB.execute(
        """
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            description TEXT NOT NULL,
            active INTEGER NOT NULL DEFAULT 1,
            sort_order INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    DB.execute(
        """
        CREATE TABLE IF NOT EXISTS weights (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER NOT NULL,
            label TEXT NOT NULL,
            price_rub REAL NOT NULL,
            active INTEGER NOT NULL DEFAULT 1,
            sort_order INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    DB.execute(
        """
        CREATE TABLE IF NOT EXISTS wallets (
            currency TEXT PRIMARY KEY,
            network TEXT NOT NULL,
            address TEXT NOT NULL,
            active INTEGER NOT NULL DEFAULT 1
        )
        """
    )
    DB.execute(
        """
        CREATE TABLE IF NOT EXISTS promo_codes (
            code TEXT PRIMARY KEY,
            kind TEXT NOT NULL,
            value REAL NOT NULL,
            active INTEGER NOT NULL DEFAULT 1,
            note TEXT DEFAULT NULL
        )
        """
    )
    DB.execute(
        """
        CREATE TABLE IF NOT EXISTS deposits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            currency TEXT NOT NULL,
            amount_rub REAL NOT NULL,
            amount_usdt REAL NOT NULL,
            address TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            confirmed_at TEXT DEFAULT NULL
        )
        """
    )
    DB.execute(
        """
        CREATE TABLE IF NOT EXISTS relay_links (
            instance_key TEXT NOT NULL,
            chat_id INTEGER NOT NULL,
            message_id INTEGER NOT NULL,
            peer_chat_id INTEGER NOT NULL,
            peer_user_id INTEGER NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (instance_key, chat_id, message_id)
        )
        """
    )
    DB.execute(
        """
        CREATE TABLE IF NOT EXISTS user_bots (
            bot_key TEXT PRIMARY KEY,
            owner_id INTEGER NOT NULL,
            token TEXT NOT NULL,
            label TEXT DEFAULT NULL,
            active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    for key, value in SETTINGS_DEFAULTS.items():
        DB.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (key, value))

    for currency, data in DEFAULT_WALLETS.items():
        DB.execute(
            "INSERT OR IGNORE INTO wallets (currency, network, address, active) VALUES (?, ?, ?, ?)",
            (currency, data["network"], data["address"], data["active"]),
        )

    if db_fetchone("SELECT COUNT(*) AS cnt FROM products")["cnt"] == 0:
        for i, (name, description) in enumerate(DEFAULT_PRODUCTS, start=1):
            cur = DB.execute(
                "INSERT INTO products (name, description, active, sort_order) VALUES (?, ?, 1, ?)",
                (name, description, i),
            )
            pid = cur.lastrowid
            for j, (label, price) in enumerate(DEFAULT_WEIGHTS.get(i, []), start=1):
                DB.execute(
                    "INSERT INTO weights (product_id, label, price_rub, active, sort_order) VALUES (?, ?, ?, 1, ?)",
                    (pid, label, price, j),
                )
    DB.commit()


init_db()


# =========================
# CITY WHITELIST
# =========================

def load_city_whitelist() -> set[str]:
    for path in CITY_WHITELIST_PATHS:
        if path.exists():
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
            except Exception as exc:
                logging.warning("Не удалось прочитать список городов %s: %s", path, exc)
                continue

            cities: set[str] = set()

            def walk(value: Any) -> None:
                if isinstance(value, str):
                    normalized = normalize_city_name(value)
                    if normalized:
                        cities.add(normalized)
                elif isinstance(value, list):
                    for item in value:
                        walk(item)
                elif isinstance(value, dict):
                    for item in value.values():
                        walk(item)

            walk(data)
            if cities:
                logging.info("Загружено городов: %s", len(cities))
                return cities

    logging.warning("Файл ru_kz_city_whitelist.json не найден или пуст. Проверка городов будет мягкой.")
    return set()


def normalize_city_name(value: str) -> str:
    value = unicodedata.normalize("NFKC", value).casefold().replace("ё", "е")
    value = re.sub(r"[\s\u00A0]+", " ", value).strip()
    for prefix in ("г. ", "город ", "г "):
        if value.startswith(prefix):
            value = value[len(prefix):].strip()
    for suffix in (" г.", " город"):
        if value.endswith(suffix):
            value = value[: -len(suffix)].strip()
    return value


VALID_CITIES = load_city_whitelist()


def is_valid_city(value: str) -> bool:
    normalized = normalize_city_name(value)
    if not VALID_CITIES:
        return bool(normalized)
    return normalized in VALID_CITIES


# =========================
# HELPERS
# =========================

class SafeDict(dict):
    def __missing__(self, key):
        return "{" + key + "}"


def render_text(template: str, **kwargs) -> str:
    try:
        return template.format_map(SafeDict(**kwargs))
    except Exception:
        return template


def get_setting(key: str, default: str = "") -> str:
    row = db_fetchone("SELECT value FROM settings WHERE key=?", (key,))
    return row["value"] if row else default


def set_setting(key: str, value: str) -> None:
    db_execute(
        "INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )


def get_user(user_id: int):
    return db_fetchone("SELECT * FROM users WHERE user_id=?", (user_id,))


def create_user_if_needed(message: Message) -> None:
    if get_user(message.from_user.id) is None:
        db_execute(
            "INSERT INTO users (user_id, username, full_name) VALUES (?, ?, ?)",
            (message.from_user.id, message.from_user.username, message.from_user.full_name),
        )


def set_user_city(user_id: int, city: str) -> None:
    db_execute("UPDATE users SET city=? WHERE user_id=?", (city, user_id))


def set_user_captcha_passed(user_id: int, passed: bool = True) -> None:
    db_execute("UPDATE users SET captcha_passed=? WHERE user_id=?", (1 if passed else 0, user_id))


def set_user_promo(user_id: int, promo: str | None) -> None:
    db_execute("UPDATE users SET promo_code=? WHERE user_id=?", (promo, user_id))


def add_balance(user_id: int, amount: float) -> None:
    db_execute("UPDATE users SET balance = balance + ? WHERE user_id=?", (amount, user_id))


def subtract_balance(user_id: int, amount: float) -> None:
    db_execute("UPDATE users SET balance = balance - ? WHERE user_id=?", (amount, user_id))


def get_support_text() -> str:
    return get_setting("support_text", "💬 Поддержка пока не настроена.")


def generate_captcha() -> tuple[str, int]:
    a = random.randint(1, 9)
    b = random.randint(1, 9)
    return f"{a} + {b}", a + b


def get_products(active_only: bool = True):
    if active_only:
        return db_fetchall("SELECT * FROM products WHERE active=1 ORDER BY sort_order, id")
    return db_fetchall("SELECT * FROM products ORDER BY sort_order, id")


def get_product(product_id: int):
    return db_fetchone("SELECT * FROM products WHERE id=?", (product_id,))


def get_weights(product_id: int, active_only: bool = True):
    if active_only:
        return db_fetchall("SELECT * FROM weights WHERE product_id=? AND active=1 ORDER BY sort_order, id", (product_id,))
    return db_fetchall("SELECT * FROM weights WHERE product_id=? ORDER BY sort_order, id", (product_id,))


def get_wallet(currency: str):
    return db_fetchone("SELECT * FROM wallets WHERE currency=?", (currency,))


def get_active_wallets():
    return db_fetchall("SELECT * FROM wallets WHERE active=1 ORDER BY currency")


def get_media_file_id(media_key: str) -> str | None:
    row = db_fetchone("SELECT file_id FROM media_assets WHERE media_key=?", (media_key,))
    return row["file_id"] if row else None


def set_media_file_id(media_key: str, file_id: str) -> None:
    db_execute(
        "INSERT INTO media_assets (media_key, file_id, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP) "
        "ON CONFLICT(media_key) DO UPDATE SET file_id=excluded.file_id, updated_at=CURRENT_TIMESTAMP",
        (media_key, file_id),
    )


def clear_media_file_id(media_key: str) -> None:
    db_execute("DELETE FROM media_assets WHERE media_key=?", (media_key,))


def get_media_source(media_key: str):
    file_id = get_media_file_id(media_key)
    if file_id:
        return file_id
    default_name = DEFAULT_MEDIA_FILES.get(media_key)
    if default_name:
        path = BASE_DIR / "images" / default_name
        if path.exists():
            return FSInputFile(path)
    if media_key.startswith("product_"):
        path = BASE_DIR / "images" / f"{media_key}.jpg"
        if path.exists():
            return FSInputFile(path)
    return None


def get_promo(code: str):
    return db_fetchone("SELECT * FROM promo_codes WHERE code=?", (code.upper().strip(),))


def apply_promo_to_total(total: float, promo_code: str | None) -> float:
    if not promo_code:
        return total
    promo = get_promo(promo_code)
    if not promo or int(promo["active"]) != 1:
        return total
    if promo["kind"] == "discount":
        return round(total * (100 - float(promo["value"])) / 100, 2)
    return total


def main_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🛍 Каталог"), KeyboardButton(text="🧺 Корзина")],
            [KeyboardButton(text="👤 Аккаунт"), KeyboardButton(text="💳 Пополнение")],
            [KeyboardButton(text="💬 Поддержка")],
        ],
        resize_keyboard=True,
    )


def cancel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="action:cancel")]])


def promo_skip_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Пропустить", callback_data="promo:skip")]])


def catalog_keyboard() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text=product["name"], callback_data=f"product:{product['id']}")]
        for product in get_products(True)
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows or [[InlineKeyboardButton(text="Нет товаров", callback_data="noop")]])


def weights_keyboard(product_id: int) -> InlineKeyboardMarkup:
    rows = []
    for weight in get_weights(product_id, True):
        rows.append(
            [
                InlineKeyboardButton(
                    text=f'{weight["label"]} — {float(weight["price_rub"]):.0f} ₽',
                    callback_data=f"buyweight:{product_id}:{weight['id']}",
                )
            ]
        )
    rows.append([InlineKeyboardButton(text="⬅️ Назад к каталогу", callback_data="back:catalog")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def cart_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Купить всё", callback_data="cart:checkout")],
            [InlineKeyboardButton(text="💳 Пополнить", callback_data="cart:topup")],
            [InlineKeyboardButton(text="🧹 Очистить корзину", callback_data="cart:clear")],
        ]
    )


def deposit_keyboard() -> InlineKeyboardMarkup:
    rows = []
    for wallet in get_active_wallets():
        rows.append([InlineKeyboardButton(text=f"{wallet['currency']} ({wallet['network']})", callback_data=f"deposit:{wallet['currency']}")])
    return InlineKeyboardMarkup(inline_keyboard=rows or [[InlineKeyboardButton(text="Нет кошельков", callback_data="noop")]])


def settings_keyboard() -> InlineKeyboardMarkup:
    items = [
        ("start_text", "Текст старта"),
        ("registration_text", "Текст регистрации"),
        ("account_text", "Текст аккаунта"),
        ("deposit_text", "Текст пополнения"),
        ("support_text", "Текст поддержки"),
        ("catalog_text", "Текст каталога"),
        ("empty_cart_text", "Пустая корзина"),
        ("order_success_text", "Успешный заказ"),
        ("insufficient_balance_text", "Недостаточно средств"),
    ]
    rows = [[InlineKeyboardButton(text=label, callback_data=f"setting:{key}")] for key, label in items]
    rows.append([InlineKeyboardButton(text="💱 Курс USDT", callback_data="rate:edit")])
    rows.append([InlineKeyboardButton(text="↩️ Назад", callback_data="admin:back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def media_keyboard() -> InlineKeyboardMarkup:
    keys = ["registration", "account", "deposit", "support", "catalog"]
    rows = [[InlineKeyboardButton(text=MEDIA_TITLES[k], callback_data=f"media:{k}")] for k in keys]
    rows.append([InlineKeyboardButton(text="↩️ Назад", callback_data="admin:back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def wallets_keyboard() -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text="➕ Добавить монету / токен", callback_data="wallet:add")]]
    for wallet in db_fetchall("SELECT * FROM wallets ORDER BY currency"):
        rows.append([InlineKeyboardButton(text=f"{wallet['currency']} — адрес", callback_data=f"wallet:addr:{wallet['currency']}")])
        rows.append([InlineKeyboardButton(text=f"{wallet['currency']} — сеть", callback_data=f"wallet:net:{wallet['currency']}")])
        rows.append([InlineKeyboardButton(text=f"{wallet['currency']} — выкл/вкл", callback_data=f"wallet:toggle:{wallet['currency']}")])
        rows.append([InlineKeyboardButton(text=f"🗑 Удалить {wallet['currency']}", callback_data=f"wallet:delete:{wallet['currency']}")])
    rows.append([InlineKeyboardButton(text="↩️ Назад", callback_data="admin:back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def promo_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="➕ Добавить промокод", callback_data="promo:add")],
            [InlineKeyboardButton(text="📋 Список промокодов", callback_data="promo:list")],
            [InlineKeyboardButton(text="↩️ Назад", callback_data="admin:back")],
        ]
    )


def user_account_keyboard(user_id: int) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="🌍 Сменить город", callback_data="account:city")],
        [InlineKeyboardButton(text="🤖 Создать своего бота", callback_data="account:bot")],
        [InlineKeyboardButton(text="🔄 Обновить", callback_data="account:refresh")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def admin_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⚙️ Настройки", callback_data="admin:settings")],
            [InlineKeyboardButton(text="🖼 Медиа", callback_data="admin:media")],
            [InlineKeyboardButton(text="📦 Товары", callback_data="admin:products")],
            [InlineKeyboardButton(text="👛 Кошельки", callback_data="admin:wallets")],
            [InlineKeyboardButton(text="🏷 Промокоды", callback_data="admin:promos")],
            [InlineKeyboardButton(text="💰 Пополнения", callback_data="admin:deposits")],
            [InlineKeyboardButton(text="👤 Пользователи", callback_data="admin:users")],
            [InlineKeyboardButton(text="📣 Рассылка", callback_data="admin:broadcast")],
            [InlineKeyboardButton(text="💬 Диалоги", callback_data="admin:dialogs")],
            [InlineKeyboardButton(text="⬇️ Выгрузить базу", callback_data="admin:export_db")],
            [InlineKeyboardButton(text="☠️ Уничтожить бота", callback_data="admin:destroy")],
        ]
    )


def product_admin_keyboard(product_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✏️ Название", callback_data=f"prod:name:{product_id}"), InlineKeyboardButton(text="📝 Описание", callback_data=f"prod:desc:{product_id}")],
            [InlineKeyboardButton(text="🖼 Фото", callback_data=f"prod:photo:{product_id}"), InlineKeyboardButton(text="💲 Весы", callback_data=f"prod:weights:{product_id}")],
            [InlineKeyboardButton(text="⚡ Полное обновление", callback_data=f"prod:full:{product_id}")],
            [InlineKeyboardButton(text="🔁 Вкл/выкл", callback_data=f"prod:toggle:{product_id}"), InlineKeyboardButton(text="🗑 Удалить", callback_data=f"prod:delete:{product_id}")],
        ]
    )


def weight_admin_keyboard(product_id: int) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text="➕ Добавить вес", callback_data=f"weight:add:{product_id}")]]
    for weight in get_weights(product_id, False):
        rows.append([
            InlineKeyboardButton(text=f"✏️ {weight['label']}", callback_data=f"weight:label:{weight['id']}"),
            InlineKeyboardButton(text="💵 Цена", callback_data=f"weight:price:{weight['id']}")
        ])
        rows.append([InlineKeyboardButton(text="🔁 Вкл/выкл", callback_data=f"weight:toggle:{weight['id']}"), InlineKeyboardButton(text="🗑 Удалить", callback_data=f"weight:delete:{weight['id']}")])
    rows.append([InlineKeyboardButton(text="↩️ Назад к товарам", callback_data="admin:products")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def template_help_text() -> str:
    return (
        "<b>Шаблон:</b>\n\n"
        "Название: <code>Название товара</code>\n"
        "Описание: <code>Короткое описание</code>\n"
        "Весы:\n"
        "50 г | 390\n"
        "100 г | 720\n"
        "250 г | 1550\n\n"
        "Можно отправить <b>одним сообщением</b> фото товара с этой подписью."
    )


def parse_weights_block(raw: str) -> list[tuple[str, float]]:
    weights: list[tuple[str, float]] = []
    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "|" in line:
            left, right = [part.strip() for part in line.split("|", 1)]
        elif ";" in line:
            left, right = [part.strip() for part in line.split(";", 1)]
        else:
            parts = line.split()
            if len(parts) < 2:
                continue
            left = " ".join(parts[:-1]).strip()
            right = parts[-1].strip()
        try:
            price = float(right.replace(",", "."))
        except ValueError:
            continue
        weights.append((left, price))
    return weights


def parse_product_template(text: str) -> dict[str, Any]:
    name = None
    description = None
    weights_raw = None

    m = re.search(r"(?im)^\s*название\s*:\s*(.+?)\s*$", text)
    if m:
        name = m.group(1).strip()

    m = re.search(r"(?ims)^\s*описание\s*:\s*(.+?)\s*(?:^\s*весы\s*:\s*$|^\s*весы\s*:)", text)
    if m:
        description = m.group(1).strip()

    m = re.search(r"(?ims)^\s*весы\s*:\s*(.+)$", text)
    if m:
        weights_raw = m.group(1).strip()

    if not (name and description and weights_raw):
        raise ValueError("Нужно заполнить Название, Описание и Весы.")

    weights = parse_weights_block(weights_raw)
    if not weights:
        raise ValueError("Не удалось распознать веса. Используй формат '50 г | 390'.")

    return {"name": name, "description": description, "weights": weights}


def rate_from_override_or_market() -> float:
    override = get_setting("usdt_rate_override", "").strip()
    if override:
        try:
            rate = float(override.replace(",", "."))
            if rate > 0:
                return round(rate, 2)
        except ValueError:
            pass
    return get_usdt_rub_rate()


def user_balance_usdt(balance_rub: float) -> tuple[float, float]:
    rate = rate_from_override_or_market()
    return round(balance_rub / rate, 2), rate


def average_price_from_html(html: str) -> float | None:
    prices: list[float] = []
    patterns = [
        r'"price"\s*:\s*"(?P<price>\d+(?:\.\d+)?)"',
        r'"advPrice"\s*:\s*"(?P<price>\d+(?:\.\d+)?)"',
        r'"price"\s*:\s*(?P<price>\d+(?:\.\d+)?)',
        r'"advPrice"\s*:\s*(?P<price>\d+(?:\.\d+)?)',
    ]
    for pattern in patterns:
        for raw in re.findall(pattern, html):
            try:
                price = float(raw)
            except ValueError:
                continue
            if 40 <= price <= 300:
                prices.append(price)
    if not prices:
        return None
    return round(mean(sorted(set(prices))), 2)


def fetch_usdt_rub_rate() -> float:
    from urllib.error import HTTPError, URLError
    from urllib.request import Request, urlopen

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0 Safari/537.36",
        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    }
    last_error: Exception | None = None
    for url in BYBIT_P2P_URLS:
        try:
            request = Request(url, headers=headers)
            with urlopen(request, timeout=12) as response:
                html = response.read().decode("utf-8", errors="ignore")
            rate = average_price_from_html(html)
            if rate:
                return round(rate, 2)
        except (HTTPError, URLError, TimeoutError, ValueError, OSError) as exc:
            last_error = exc
    if last_error:
        logging.warning("Не удалось получить курс Bybit P2P: %s", last_error)
    raise RuntimeError("Не удалось получить курс Bybit P2P")


def get_usdt_rub_rate() -> float:
    now = time.time()
    if RATE_CACHE["value"] is not None and now - RATE_CACHE["updated_at"] < RATE_CACHE_TTL:
        return float(RATE_CACHE["value"])
    try:
        rate = fetch_usdt_rub_rate()
        RATE_CACHE["value"] = rate
        RATE_CACHE["updated_at"] = now
        return rate
    except Exception:
        if RATE_CACHE["value"] is not None:
            return float(RATE_CACHE["value"])
        return DEFAULT_USDT_RUB_RATE


def get_wallet_address(currency: str) -> str:
    wallet = get_wallet(currency)
    if wallet and int(wallet["active"]) == 1 and wallet["address"]:
        return wallet["address"]
    return db_fetchone("SELECT address FROM wallets WHERE currency=?", (currency,))["address"] if get_wallet(currency) else "ADDRESS_HERE"


def tg_user_link(user_id: int, username: str | None = None, full_name: str | None = None) -> str:
    label = username and f"@{username}" or (full_name or f"Пользователь {user_id}")
    return f'<a href="tg://user?id={user_id}">{label}</a>'


async def send_media_message(target, media_key: str, caption: str, reply_markup=None):
    source = get_media_source(media_key)
    if source:
        return await target.answer_photo(photo=source, caption=caption, reply_markup=reply_markup)
    return await target.answer(caption, reply_markup=reply_markup)


async def answer_with_cancel(target: Message | CallbackQuery, text: str, reply_markup=None):
    if isinstance(target, CallbackQuery):
        await target.message.answer(text, reply_markup=reply_markup)
        await target.answer()
    else:
        await target.answer(text, reply_markup=reply_markup)


def extract_bot_key(token: str) -> str:
    return hashlib.sha1(token.encode("utf-8")).hexdigest()[:12]


def current_disabled() -> bool:
    return DISABLE_FLAG.exists()


# =========================
# RELAY / BROADCAST / CHILD BOTS
# =========================

def save_relay_link(chat_id: int, message_id: int, peer_chat_id: int, peer_user_id: int) -> None:
    DB.execute(
        "INSERT OR REPLACE INTO relay_links (instance_key, chat_id, message_id, peer_chat_id, peer_user_id) VALUES (?, ?, ?, ?, ?)",
        (INSTANCE_KEY, chat_id, message_id, peer_chat_id, peer_user_id),
    )
    DB.commit()


def get_relay_link(chat_id: int, message_id: int) -> sqlite3.Row | None:
    return db_fetchone(
        "SELECT * FROM relay_links WHERE instance_key=? AND chat_id=? AND message_id=?",
        (INSTANCE_KEY, chat_id, message_id),
    )


async def relay_reply_message(message: Message) -> bool:
    if not message.reply_to_message:
        return False
    link = get_relay_link(message.chat.id, message.reply_to_message.message_id)
    if not link:
        return False

    try:
        sent = await message.copy_to(link["peer_chat_id"])
        save_relay_link(link["peer_chat_id"], sent.message_id, message.chat.id, message.from_user.id)
        return True
    except Exception as exc:
        logging.warning("Relay failed: %s", exc)
        return False


async def send_open_chat_stub(admin_message: Message, peer_user_id: int, peer_username: str | None = None, peer_name: str | None = None):
    text = (
        f"💬 Диалог с {tg_user_link(peer_user_id, peer_username, peer_name)}\n\n"
        f"Ответь на это сообщение, чтобы написать пользователю.\n"
        f"Любые ответы пользователя на твои сообщения придут сюда же."
    )
    sent = await admin_message.answer(text)
    save_relay_link(admin_message.chat.id, sent.message_id, peer_user_id, peer_user_id)


async def broadcast_message(admin_message: Message) -> tuple[int, int]:
    users = db_fetchall("SELECT user_id FROM users ORDER BY user_id")
    ok = 0
    fail = 0
    for row in users:
        uid = int(row["user_id"])
        if uid == ADMIN_ID:
            continue
        try:
            sent = await admin_message.copy_to(uid)
            save_relay_link(uid, sent.message_id, admin_message.chat.id, admin_message.from_user.id)
            ok += 1
        except Exception:
            fail += 1
    return ok, fail


async def ensure_child_bot(token: str, bot_key: str, label: str | None = None) -> None:
    if bot_key in child_processes and child_processes[bot_key].returncode is None:
        return

    if current_disabled():
        return

    env = os.environ.copy()
    env["BOT_TOKEN"] = token
    env["BOT_INSTANCE_KEY"] = bot_key
    env["BOT_CHILD"] = "1"
    if label:
        env["BOT_LABEL"] = label

    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        str(Path(__file__).resolve()),
        "--child",
        "--token",
        token,
        "--instance-key",
        bot_key,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
        env=env,
    )
    child_processes[bot_key] = proc
    logging.info("Запущен дочерний бот %s (pid=%s)", bot_key, proc.pid)


async def reconcile_child_bots_loop():
    while True:
        try:
            if current_disabled():
                await stop_all_children()
                return

            if not IS_CHILD_PROCESS:
                rows = db_fetchall("SELECT * FROM user_bots WHERE active=1 ORDER BY created_at")
                active_keys = {row["bot_key"] for row in rows}

                for row in rows:
                    token = row["token"]
                    bot_key = row["bot_key"]
                    label = row["label"]
                    proc = child_processes.get(bot_key)
                    if not proc or proc.returncode is not None:
                        await ensure_child_bot(token, bot_key, label)

                for bot_key, proc in list(child_processes.items()):
                    if bot_key not in active_keys:
                        if proc.returncode is None:
                            proc.terminate()
                        child_processes.pop(bot_key, None)
        except Exception as exc:
            logging.exception("Ошибка синхронизации дочерних ботов: %s", exc)
        await asyncio.sleep(5)


async def stop_all_children():
    for bot_key, proc in list(child_processes.items()):
        if proc.returncode is None:
            try:
                proc.terminate()
            except ProcessLookupError:
                pass
    child_processes.clear()


def mark_bot_disabled() -> None:
    DISABLE_FLAG.write_text(f"disabled at {time.time()}\n", encoding="utf-8")


def trigger_process_group_stop():
    try:
        os.killpg(os.getpgrp(), signal.SIGTERM)
    except Exception:
        try:
            os.kill(os.getpid(), signal.SIGTERM)
        except Exception:
            os._exit(0)


# =========================
# ADMIN HANDLERS
# =========================

@ROUTER.message(Command("admin"))
async def admin_panel(message: Message):
    if message.from_user.id != ADMIN_ID:
        await message.answer("❌ Нет доступа")
        return
    await message.answer("<b>⚙️ Админ панель</b>", reply_markup=admin_keyboard())


@ROUTER.callback_query(F.data == "admin:back")
async def admin_back(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    await call.message.answer("<b>⚙️ Админ панель</b>", reply_markup=admin_keyboard())
    await call.answer()


@ROUTER.callback_query(F.data == "admin:settings")
async def admin_settings(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    await call.message.answer("<b>⚙️ Настройки</b>", reply_markup=settings_keyboard())
    await call.answer()


@ROUTER.callback_query(F.data.startswith("setting:"))
async def setting_edit(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    key = call.data.split(":", 1)[1]
    admin_state[call.from_user.id] = {"mode": "set_text", "key": key}
    await call.message.answer(
        "Введи новый текст для <b>{}</b>.\n\nМожно использовать:\n<code>{{city}}</code> <code>{{balance_rub}}</code> <code>{{balance_usdt}}</code> <code>{{rate}}</code> <code>{{promo}}</code>".format(key),
        reply_markup=cancel_keyboard(),
    )
    await call.answer()


@ROUTER.callback_query(F.data == "rate:edit")
async def rate_edit(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    admin_state[call.from_user.id] = {"mode": "set_rate"}
    await call.message.answer(
        "Введи курс в рублях за 1 USDT. Или напиши <code>auto</code> для авто-режима.",
        reply_markup=cancel_keyboard(),
    )
    await call.answer()


@ROUTER.callback_query(F.data == "admin:media")
async def admin_media(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    await call.message.answer(
        "<b>🖼 Медиа-центр</b>\n\nВыбери картинку, затем отправь новую фотографию.",
        reply_markup=media_keyboard(),
    )
    await call.answer()


@ROUTER.callback_query(F.data == "admin:products")
async def admin_products(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return

    products = get_products(False)
    header = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="➕ Добавить товар", callback_data="prod:add")],
            [InlineKeyboardButton(text="⚡ Быстрый шаблон", callback_data="prod:add_template")],
            [InlineKeyboardButton(text="↩️ Назад", callback_data="admin:back")],
        ]
    )
    if not products:
        await call.message.answer("Товаров пока нет.", reply_markup=header)
        await call.answer()
        return

    await call.message.answer("<b>📦 Товары</b>", reply_markup=header)
    for p in products:
        status = "🟢" if int(p["active"]) == 1 else "⚪️"
        text = (
            f"{status} <b>{p['name']}</b>"
            f"\n\n{p['description']}"
            f"\n\nID: <code>{p['id']}</code>"
        )
        source = get_media_source(f"product_{p['id']}")
        if source:
            await call.message.answer_photo(photo=source, caption=text, reply_markup=product_admin_keyboard(p["id"]))
        else:
            await call.message.answer(text, reply_markup=product_admin_keyboard(p["id"]))
    await call.answer()


@ROUTER.callback_query(F.data == "prod:add")
async def product_add(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    admin_state[call.from_user.id] = {"mode": "prod_add_name"}
    await call.message.answer("Введи <b>название нового товара</b>.\n\nИли нажми «⚡ Быстрый шаблон».", reply_markup=cancel_keyboard())
    await call.answer()


@ROUTER.callback_query(F.data == "prod:add_template")
async def product_add_template(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    admin_state[call.from_user.id] = {"mode": "prod_template_add"}
    await call.message.answer(template_help_text(), reply_markup=cancel_keyboard())
    await call.answer()


@ROUTER.callback_query(F.data.startswith("prod:name:"))
async def product_edit_name(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    pid = int(call.data.split(":")[2])
    admin_state[call.from_user.id] = {"mode": "prod_edit_name", "product_id": pid}
    await call.message.answer("Введи <b>новое название</b> товара.", reply_markup=cancel_keyboard())
    await call.answer()


@ROUTER.callback_query(F.data.startswith("prod:desc:"))
async def product_edit_desc(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    pid = int(call.data.split(":")[2])
    admin_state[call.from_user.id] = {"mode": "prod_edit_desc", "product_id": pid}
    await call.message.answer("Введи <b>новое описание</b> товара.", reply_markup=cancel_keyboard())
    await call.answer()


@ROUTER.callback_query(F.data.startswith("prod:photo:"))
async def product_photo(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    pid = int(call.data.split(":")[2])
    admin_state[call.from_user.id] = {"mode": "set_media", "media_key": f"product_{pid}"}
    await call.message.answer("Теперь отправь <b>фото товара</b>.", reply_markup=cancel_keyboard())
    await call.answer()


@ROUTER.callback_query(F.data.startswith("prod:weights:"))
async def product_weights(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    pid = int(call.data.split(":")[2])
    p = get_product(pid)
    if not p:
        await call.answer("Товар не найден", show_alert=True)
        return
    await call.message.answer(f"<b>Весы товара:</b> <b>{p['name']}</b>", reply_markup=weight_admin_keyboard(pid))
    await call.answer()


@ROUTER.callback_query(F.data.startswith("prod:full:"))
async def product_full_edit(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    pid = int(call.data.split(":")[2])
    admin_state[call.from_user.id] = {"mode": "prod_template_edit", "product_id": pid}
    await call.message.answer(
        f"Отправь <b>одно сообщение</b> с фото или без фото и подписью по шаблону для полного обновления товара <code>{pid}</code>:\n\n{template_help_text()}",
        reply_markup=cancel_keyboard(),
    )
    await call.answer()


@ROUTER.callback_query(F.data.startswith("prod:toggle:"))
async def product_toggle(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    pid = int(call.data.split(":")[2])
    p = get_product(pid)
    if not p:
        await call.answer("Товар не найден", show_alert=True)
        return
    new_value = 0 if int(p["active"]) == 1 else 1
    db_execute("UPDATE products SET active=? WHERE id=?", (new_value, pid))
    await call.answer("Состояние изменено")
    await call.message.answer(f"Товар <b>{p['name']}</b> теперь {'включён' if new_value else 'выключен'}.")


@ROUTER.callback_query(F.data.startswith("prod:delete:"))
async def product_delete(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    pid = int(call.data.split(":")[2])
    p = get_product(pid)
    if not p:
        await call.answer("Товар не найден", show_alert=True)
        return
    db_execute("DELETE FROM weights WHERE product_id=?", (pid,))
    db_execute("DELETE FROM products WHERE id=?", (pid,))
    clear_media_file_id(f"product_{pid}")
    await call.answer("Удалено")
    await call.message.answer(f"Товар <b>{p['name']}</b> удалён.")


@ROUTER.callback_query(F.data.startswith("weight:add:"))
async def weight_add(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    pid = int(call.data.split(":")[2])
    admin_state[call.from_user.id] = {"mode": "weight_add_label", "product_id": pid}
    await call.message.answer("Введи <b>название веса</b>, например <code>50 г</code>.", reply_markup=cancel_keyboard())
    await call.answer()


@ROUTER.callback_query(F.data.startswith("weight:label:"))
async def weight_edit_label(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    wid = int(call.data.split(":")[2])
    admin_state[call.from_user.id] = {"mode": "weight_edit_label", "weight_id": wid}
    await call.message.answer("Введи <b>новое название веса</b>.", reply_markup=cancel_keyboard())
    await call.answer()


@ROUTER.callback_query(F.data.startswith("weight:price:"))
async def weight_edit_price(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    wid = int(call.data.split(":")[2])
    admin_state[call.from_user.id] = {"mode": "weight_edit_price", "weight_id": wid}
    await call.message.answer("Введи <b>новую цену</b> в рублях.", reply_markup=cancel_keyboard())
    await call.answer()


@ROUTER.callback_query(F.data.startswith("weight:toggle:"))
async def weight_toggle(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    wid = int(call.data.split(":")[2])
    row = db_fetchone("SELECT * FROM weights WHERE id=?", (wid,))
    if not row:
        await call.answer("Не найдено", show_alert=True)
        return
    new_value = 0 if int(row["active"]) == 1 else 1
    db_execute("UPDATE weights SET active=? WHERE id=?", (new_value, wid))
    await call.answer("Состояние изменено")
    await call.message.answer(f"Вес <b>{row['label']}</b> теперь {'включён' if new_value else 'выключен'}.")


@ROUTER.callback_query(F.data.startswith("weight:delete:"))
async def weight_delete(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    wid = int(call.data.split(":")[2])
    db_execute("DELETE FROM weights WHERE id=?", (wid,))
    await call.answer("Удалено")
    await call.message.answer("Вес удалён.")


@ROUTER.callback_query(F.data == "admin:wallets")
async def admin_wallets(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    await call.message.answer("<b>👛 Кошельки</b>", reply_markup=wallets_keyboard())
    await call.answer()


@ROUTER.callback_query(F.data == "wallet:add")
async def wallet_add(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    admin_state[call.from_user.id] = {"mode": "wallet_add_currency"}
    await call.message.answer(
        "Введи <b>тикер монеты</b> или токена, например <code>SOL</code> или <code>USDT-ARB</code>.",
        reply_markup=cancel_keyboard(),
    )
    await call.answer()


@ROUTER.callback_query(F.data.startswith("wallet:addr:"))
async def wallet_edit_address(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    currency = call.data.split(":")[2]
    admin_state[call.from_user.id] = {"mode": "wallet_address", "currency": currency}
    await call.message.answer(f"Введи новый <b>адрес</b> для <b>{currency}</b>.", reply_markup=cancel_keyboard())
    await call.answer()


@ROUTER.callback_query(F.data.startswith("wallet:net:"))
async def wallet_edit_network(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    currency = call.data.split(":")[2]
    admin_state[call.from_user.id] = {"mode": "wallet_network", "currency": currency}
    await call.message.answer(f"Введи новую <b>сеть</b> для <b>{currency}</b>.", reply_markup=cancel_keyboard())
    await call.answer()


@ROUTER.callback_query(F.data.startswith("wallet:toggle:"))
async def wallet_toggle(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    currency = call.data.split(":")[2]
    row = get_wallet(currency)
    if not row:
        await call.answer("Не найдено", show_alert=True)
        return
    new_value = 0 if int(row["active"]) == 1 else 1
    db_execute("UPDATE wallets SET active=? WHERE currency=?", (new_value, currency))
    await call.answer("Состояние изменено")
    await call.message.answer(f"Кошелёк <b>{currency}</b> теперь {'включён' if new_value else 'выключен'}.")


@ROUTER.callback_query(F.data.startswith("wallet:delete:"))
async def wallet_delete(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    currency = call.data.split(":")[2]
    db_execute("DELETE FROM wallets WHERE currency=?", (currency,))
    await call.answer("Удалено")
    await call.message.answer(f"Кошелёк <b>{currency}</b> удалён.")


@ROUTER.callback_query(F.data == "admin:promos")
async def admin_promos(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    await call.message.answer("<b>🏷 Промокоды</b>", reply_markup=promo_keyboard())
    await call.answer()


@ROUTER.callback_query(F.data == "promo:add")
async def promo_add(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    admin_state[call.from_user.id] = {"mode": "promo_code"}
    await call.message.answer("Введи <b>код промокода</b>, например <code>TEA10</code>.", reply_markup=cancel_keyboard())
    await call.answer()


@ROUTER.callback_query(F.data == "promo:list")
async def promo_list(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    rows = db_fetchall("SELECT * FROM promo_codes ORDER BY code")
    if not rows:
        await call.message.answer("Промокодов пока нет.")
        await call.answer()
        return
    text = ["<b>Промокоды:</b>"]
    for p in rows:
        text.append(f"• <code>{p['code']}</code> | {p['kind']} | {p['value']} | {'on' if int(p['active']) == 1 else 'off'}")
    await call.message.answer("\n".join(text))
    await call.answer()


@ROUTER.callback_query(F.data == "admin:deposits")
async def admin_deposits(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    rows = db_fetchall("SELECT * FROM deposits WHERE status='pending' ORDER BY id DESC")
    if not rows:
        await call.message.answer("Нет заявок на пополнение.")
        await call.answer()
        return
    for d in rows:
        u = get_user(int(d["user_id"]))
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"dep:approve:{d['id']}")],
                [InlineKeyboardButton(text="❌ Отклонить", callback_data=f"dep:decline:{d['id']}")],
                [InlineKeyboardButton(text="💬 Связаться", callback_data=f"chat:open:{d['user_id']}")],
            ]
        )
        await call.message.answer(
            f"<b>Заявка #{d['id']}</b>\n\n"
            f"👤 Пользователь: {tg_user_link(int(d['user_id']), u['username'] if u else None, u['full_name'] if u else None)}\n"
            f"🌍 Город: <b>{u['city'] if u and u['city'] else '—'}</b>\n"
            f"💳 Валюта: <b>{d['currency']}</b>\n"
            f"💰 Сумма: <b>{float(d['amount_rub']):.2f} ₽</b> (~<b>{float(d['amount_usdt']):.2f} USDT</b>)\n"
            f"🏦 Адрес: <code>{d['address']}</code>",
            reply_markup=kb,
        )
    await call.answer()


@ROUTER.callback_query(F.data == "admin:users")
async def admin_users(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    rows = db_fetchall("SELECT * FROM users ORDER BY user_id DESC LIMIT 30")
    if not rows:
        await call.message.answer("Пользователей пока нет.")
        await call.answer()
        return
    lines = ["<b>Пользователи:</b>"]
    for u in rows:
        balance_rub = float(u["balance"] or 0)
        balance_usdt = round(balance_rub / rate_from_override_or_market(), 2)
        lines.append(
            f"• {tg_user_link(int(u['user_id']), u['username'], u['full_name'])} | "
            f"{u['city'] or '—'} | {balance_rub:.2f} ₽ (~{balance_usdt:.2f} USDT)"
        )
    await call.message.answer("\n".join(lines))
    await call.answer()


@ROUTER.callback_query(F.data == "admin:broadcast")
async def admin_broadcast(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    admin_state[call.from_user.id] = {"mode": "broadcast_wait"}
    await call.message.answer(
        "Отправь <b>одно сообщение</b> для рассылки.\n"
        "Поддерживаются текст, фото, подписи и форматирование.",
        reply_markup=cancel_keyboard(),
    )
    await call.answer()


@ROUTER.callback_query(F.data == "admin:export_db")
async def admin_export_db(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    if not DB_PATH.exists():
        await call.message.answer("База данных не найдена.")
        await call.answer()
        return
    await call.message.answer_document(FSInputFile(DB_PATH), caption="⬇️ Выгрузка базы данных")
    await call.answer()


@ROUTER.callback_query(F.data == "admin:destroy")
async def admin_destroy(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    admin_state[call.from_user.id] = {"mode": "destroy_confirm"}
    await call.message.answer(
        "<b>☠️ Уничтожение бота</b>\n\n"
        "Это создаст флаг отключения и остановит все процессы этого проекта.\n"
        "Подтверди действие кнопкой ниже.",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="☠️ Да, уничтожить", callback_data="admin:destroy_confirm")],
                [InlineKeyboardButton(text="↩️ Отмена", callback_data="admin:back")],
            ]
        ),
    )
    await call.answer()


@ROUTER.callback_query(F.data == "admin:destroy_confirm")
async def admin_destroy_confirm(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    mark_bot_disabled()
    try:
        await call.message.answer("Бот отключается.")
    except Exception:
        pass
    await call.answer("Отключение...")
    await stop_all_children()
    trigger_process_group_stop()


@ROUTER.callback_query(F.data.startswith("dep:approve:"))
async def dep_approve(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    dep_id = int(call.data.split(":")[2])
    dep = db_fetchone("SELECT * FROM deposits WHERE id=?", (dep_id,))
    if not dep:
        await call.answer("Не найдено", show_alert=True)
        return
    if dep["status"] != "pending":
        await call.answer("Уже обработано", show_alert=True)
        return
    admin_state[call.from_user.id] = {"mode": "deposit_confirm", "deposit_id": dep_id}
    await call.message.answer(
        f"Подтверди сумму для заявки <b>#{dep_id}</b>.\n"
        f"Сейчас в заявке: <b>{float(dep['amount_rub']):.2f} ₽</b>\n\n"
        f"Можешь отправить новую сумму или нажать «❌ Отмена».",
        reply_markup=cancel_keyboard(),
    )
    await call.answer()


@ROUTER.callback_query(F.data.startswith("dep:decline:"))
async def dep_decline(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    dep_id = int(call.data.split(":")[2])
    dep = db_fetchone("SELECT * FROM deposits WHERE id=?", (dep_id,))
    if not dep:
        await call.answer("Не найдено", show_alert=True)
        return
    if dep["status"] != "pending":
        await call.answer("Уже обработано", show_alert=True)
        return
    db_execute("UPDATE deposits SET status='declined' WHERE id=?", (dep_id,))
    try:
        await BOT.send_message(dep["user_id"], f"❌ Пополнение #{dep_id} отклонено администратором.")
    except Exception:
        pass
    await call.message.answer(f"❌ Пополнение #{dep_id} отклонено.")
    await call.answer()


@ROUTER.callback_query(F.data == "admin:dialogs")
async def admin_dialogs(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    rows = db_fetchall("SELECT * FROM users ORDER BY user_id DESC LIMIT 30")
    if not rows:
        await call.message.answer("Пользователей пока нет.")
        await call.answer()
        return
    keyboard = []
    for u in rows:
        label = u["username"] or u["full_name"] or f"Пользователь {u['user_id']}"
        keyboard.append([InlineKeyboardButton(text=f"💬 {label}", callback_data=f"chat:open:{u['user_id']}")])
    keyboard.append([InlineKeyboardButton(text="↩️ Назад", callback_data="admin:back")])
    await call.message.answer("<b>💬 Диалоги</b>\n\nВыбери пользователя, чтобы открыть чат.", reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard))
    await call.answer()


@ROUTER.callback_query(F.data.startswith("chat:open:"))
async def chat_open(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    uid = int(call.data.split(":")[2])
    user = get_user(uid)
    if not user:
        await call.answer("Пользователь не найден", show_alert=True)
        return
    await send_open_chat_stub(call.message, uid, user["username"], user["full_name"])
    await call.answer("Диалог открыт")


# =========================
# USER CALLBACKS
# =========================

@ROUTER.callback_query(F.data == "noop")
async def noop(call: CallbackQuery):
    await call.answer()


@ROUTER.callback_query(F.data.startswith("buyweight:"))
async def add_to_cart(call: CallbackQuery):
    user = get_user(call.from_user.id)
    if not user:
        await call.answer("Сначала /start", show_alert=True)
        return
    _, pid_str, wid_str = call.data.split(":")
    pid = int(pid_str)
    wid = int(wid_str)
    row = db_fetchone(
        "SELECT p.id AS pid, p.name AS pname, w.label AS wlabel, w.price_rub AS price FROM products p JOIN weights w ON w.product_id=p.id WHERE p.id=? AND w.id=?",
        (pid, wid),
    )
    if not row:
        await call.answer("Не найдено", show_alert=True)
        return
    carts.setdefault(call.from_user.id, []).append(
        {"product_name": row["pname"], "weight_label": row["wlabel"], "price": float(row["price"])}
    )
    await call.answer("Добавлено в корзину", show_alert=True)
    await call.message.answer(
        f"<b>✅ Добавлено в корзину</b>\n\n{row['pname']} — {row['wlabel']} — {float(row['price']):.2f} ₽",
        reply_markup=main_menu(),
    )


@ROUTER.callback_query(F.data.startswith("product:"))
async def open_product(call: CallbackQuery):
    user = get_user(call.from_user.id)
    if not user or not user["city"]:
        await call.answer("Сначала выбери город", show_alert=True)
        return
    pid = int(call.data.split(":", 1)[1])
    product = get_product(pid)
    if not product or int(product["active"]) != 1:
        await call.answer("Товар не найден", show_alert=True)
        return
    caption = f"<b>{product['name']}</b>\n\n{product['description']}\n\nВыбери граммовку:"
    source = get_media_source(f"product_{pid}")
    if source:
        await call.message.answer_photo(photo=source, caption=caption, reply_markup=weights_keyboard(pid))
    else:
        await call.message.answer(caption, reply_markup=weights_keyboard(pid))
    await call.answer()


@ROUTER.callback_query(F.data == "back:catalog")
async def back_catalog(call: CallbackQuery):
    user = get_user(call.from_user.id)
    if not user or not user["city"]:
        await call.answer("Сначала выбери город", show_alert=True)
        return
    await call.message.answer(get_setting("catalog_text"), reply_markup=catalog_keyboard())
    await call.answer()


@ROUTER.callback_query(F.data == "cart:topup")
async def cart_topup(call: CallbackQuery):
    await call.message.answer(get_setting("deposit_text"), reply_markup=deposit_keyboard())
    await call.answer()


@ROUTER.callback_query(F.data == "cart:clear")
async def cart_clear(call: CallbackQuery):
    carts[call.from_user.id] = []
    await call.message.answer("<b>🧹 Корзина очищена</b>")
    await call.answer()


@ROUTER.callback_query(F.data == "cart:checkout")
async def cart_checkout(call: CallbackQuery):
    user = get_user(call.from_user.id)
    items = carts.get(call.from_user.id, [])
    if not items:
        await call.answer("Корзина пустая", show_alert=True)
        return
    total = sum(item["price"] for item in items)
    promo = user["promo_code"] if user else None
    final_total = apply_promo_to_total(total, promo)
    balance = float(user["balance"] or 0)
    if balance < final_total:
        missing_rub = final_total - balance
        missing_usdt = round(missing_rub / rate_from_override_or_market(), 2)
        await call.message.answer(
            f"{get_setting('insufficient_balance_text')}\n\n"
            f"Нужно: <b>{final_total:.2f} ₽</b>\n"
            f"Баланс: <b>{balance:.2f} ₽</b>\n"
            f"Не хватает: <b>{missing_rub:.2f} ₽</b> (~<b>{missing_usdt:.2f} USDT</b>)",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="💳 Пополнить", callback_data="cart:topup")]]),
        )
        await call.answer()
        return
    subtract_balance(call.from_user.id, final_total)
    carts[call.from_user.id] = []
    await call.message.answer(f"{get_setting('order_success_text')}\n\nСписано: <b>{final_total:.2f} ₽</b>", reply_markup=main_menu())
    await call.answer("Заказ оформлен")

    # notify admin
    try:
        items_text = "\n".join(
            [f"• <b>{item['product_name']}</b> — {item['weight_label']} — {item['price']:.2f} ₽" for item in items]
        )
        u = get_user(call.from_user.id)
        admin_text = (
            "<b>🛒 Новая покупка</b>\n\n"
            f"👤 Покупатель: {tg_user_link(call.from_user.id, u['username'] if u else None, u['full_name'] if u else None)}\n"
            f"🌍 Город: <b>{u['city'] if u and u['city'] else '—'}</b>\n"
            f"💰 Сумма: <b>{final_total:.2f} ₽</b>\n"
            f"🧾 Товары:\n{items_text}"
        )
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="💬 Связаться", callback_data=f"chat:open:{call.from_user.id}")],
            ]
        )
        await BOT.send_message(ADMIN_ID, admin_text, reply_markup=kb)
    except Exception as exc:
        logging.warning("Не удалось отправить уведомление о покупке админу: %s", exc)


@ROUTER.callback_query(F.data.startswith("deposit:"))
async def start_deposit(call: CallbackQuery):
    currency = call.data.split(":", 1)[1]
    waiting_for_deposit_amount[call.from_user.id] = currency
    await call.message.answer(
        f"<b>💳 Пополнение через {currency}</b>\n\n"
        "Теперь отправь сумму в рублях одним сообщением.\n"
        "Например: <code>2500</code>\n\n"
        "После этого бот выдаст адрес для пополнения.",
        reply_markup=cancel_keyboard(),
    )
    await call.answer()


@ROUTER.callback_query(F.data == "promo:skip")
async def skip_promo(call: CallbackQuery):
    waiting_for_promo.discard(call.from_user.id)
    await call.answer()
    await call.message.answer("<b>Регистрация завершена ✅</b>\n\nМожешь пользоваться меню.", reply_markup=main_menu())


@ROUTER.callback_query(F.data == "action:cancel")
async def action_cancel(call: CallbackQuery):
    uid = call.from_user.id
    waiting_for_city.discard(uid)
    waiting_for_city_change.discard(uid)
    waiting_for_promo.discard(uid)
    waiting_for_bot_token.discard(uid)
    waiting_for_deposit_amount.pop(uid, None)
    admin_state.pop(uid, None)
    broadcast_targets.pop(uid, None)
    await call.answer("Отменено")
    await call.message.answer("Ок, отменено.", reply_markup=main_menu() if uid != ADMIN_ID else admin_keyboard())


@ROUTER.callback_query(F.data == "account:city")
async def account_city(call: CallbackQuery):
    uid = call.from_user.id
    waiting_for_city_change.add(uid)
    await call.message.answer(
        "Введи новый город одним сообщением.\n\n"
        "Кнопка отмены ниже.",
        reply_markup=cancel_keyboard(),
    )
    await call.answer()


@ROUTER.callback_query(F.data == "account:bot")
async def account_bot(call: CallbackQuery):
    uid = call.from_user.id
    waiting_for_bot_token.add(uid)
    await call.message.answer(
        "Отправь <b>секретный токен Telegram-бота</b>.\n\n"
        "После сохранения он будет запущен как отдельный экземпляр с тем же функционалом.",
        reply_markup=cancel_keyboard(),
    )
    await call.answer()


@ROUTER.callback_query(F.data == "account:refresh")
async def account_refresh(call: CallbackQuery):
    user = get_user(call.from_user.id)
    balance_rub = float(user["balance"] or 0)
    balance_usdt, rate = user_balance_usdt(balance_rub)
    await call.message.answer(
        render_text(
            get_setting("account_text"),
            city=user["city"] or "не выбран",
            balance_rub=balance_rub,
            balance_usdt=balance_usdt,
            rate=rate,
            promo=user["promo_code"] or "нет",
        ),
        reply_markup=user_account_keyboard(call.from_user.id),
    )
    await call.answer()


# =========================
# TEXT / MEDIA ROUTERS
# =========================

@ROUTER.message(CommandStart())
async def cmd_start(message: Message):
    create_user_if_needed(message)
    user = get_user(message.from_user.id)
    if user and int(user["captcha_passed"]) == 1:
        await send_media_message(message, "registration", get_setting("start_text"), reply_markup=main_menu())
        return
    question, answer = generate_captcha()
    captcha_answers[message.from_user.id] = answer
    waiting_for_city.discard(message.from_user.id)
    waiting_for_promo.discard(message.from_user.id)
    await send_media_message(message, "registration", f"{get_setting('start_text')}\n\n<b>{question} = ?</b>")


@ROUTER.message(F.reply_to_message)
async def reply_router(message: Message):
    if await relay_reply_message(message):
        return
    if message.from_user.id == ADMIN_ID and message.from_user.id in admin_state:
        await process_admin_message(message)
        return


@ROUTER.message(F.photo)
async def photo_router(message: Message):
    if await relay_reply_message(message):
        return
    user_id = message.from_user.id
    create_user_if_needed(message)

    # admin photo handlers
    if user_id == ADMIN_ID:
        st = admin_state.get(user_id, {})
        mode = st.get("mode")
        if mode == "set_media":
            media_key = st["media_key"]
            set_media_file_id(media_key, message.photo[-1].file_id)
            admin_state.pop(user_id, None)
            await message.answer(f"✅ Фото для <b>{MEDIA_TITLES.get(media_key, media_key)}</b> сохранено.")
            return

        if mode in {"broadcast_wait", "prod_template_add", "prod_template_edit"}:
            await process_admin_message(message)
            return

    if user_id in waiting_for_bot_token:
        await message.answer("Токен нужно отправить текстом.")
        return

    await message.answer("Используй кнопки меню ниже.", reply_markup=main_menu())


@ROUTER.message()
async def text_router(message: Message):
    if message.content_type != "text":
        return
    user_id = message.from_user.id
    create_user_if_needed(message)
    user = get_user(user_id)
    text = (message.text or "").strip()

    # admin text flow
    if user_id == ADMIN_ID and user_id in admin_state:
        if text.lower() in {"отмена", "cancel"}:
            admin_state.pop(user_id, None)
            await message.answer("Ок, отменено.", reply_markup=admin_keyboard())
            return
        await process_admin_message(message)
        return

    if user_id in waiting_for_deposit_amount:
        if text.lower() in {"отмена", "cancel"}:
            waiting_for_deposit_amount.pop(user_id, None)
            await message.answer("Ок, пополнение отменено.", reply_markup=main_menu())
            return
        try:
            amount_rub = float(text.replace(",", "."))
        except ValueError:
            await message.answer("Введи сумму в рублях числом, например: <code>1500</code>.", reply_markup=cancel_keyboard())
            return
        currency = waiting_for_deposit_amount.pop(user_id)
        rate = rate_from_override_or_market()
        amount_usdt = round(amount_rub / rate, 2)
        address = get_wallet_address(currency)
        cur = DB.execute(
            "INSERT INTO deposits (user_id, currency, amount_rub, amount_usdt, address, status) VALUES (?, ?, ?, ?, ?, 'pending')",
            (user_id, currency, amount_rub, amount_usdt, address),
        )
        dep_id = cur.lastrowid
        DB.commit()
        try:
            u = get_user(user_id)
            await BOT.send_message(
                ADMIN_ID,
                (
                    "<b>💰 Новая заявка на пополнение</b>\n\n"
                    f"Заявка № <b>{dep_id}</b>\n"
                    f"👤 Пользователь: {tg_user_link(user_id, u['username'] if u else None, u['full_name'] if u else None)}\n"
                    f"🌍 Город: <b>{u['city'] if u and u['city'] else '—'}</b>\n"
                    f"💳 Валюта: <b>{currency}</b>\n"
                    f"💰 Сумма: <b>{amount_rub:.2f} ₽</b> (~<b>{amount_usdt:.2f} USDT</b>)\n"
                    f"🏦 Адрес: <code>{address}</code>"
                ),
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"dep:approve:{dep_id}")],
                        [InlineKeyboardButton(text="❌ Отклонить", callback_data=f"dep:decline:{dep_id}")],
                        [InlineKeyboardButton(text="💬 Связаться", callback_data=f"chat:open:{user_id}")],
                    ]
                ),
            )
        except Exception:
            pass
        await send_media_message(
            message,
            "deposit",
            (
                f"<b>✅ Заявка на пополнение создана</b>\n\n"
                f"Валюта: <b>{currency}</b>\n"
                f"Сумма: <b>{amount_rub:.2f} ₽</b> (~<b>{amount_usdt:.2f} USDT</b>)\n"
                f"Адрес: <code>{address}</code>\n\n"
                "Переведи нужную сумму и подожди подтверждение администратора."
            ),
            reply_markup=main_menu(),
        )
        return

    if user_id in waiting_for_city or user_id in waiting_for_city_change:
        is_registration_city = user_id in waiting_for_city
        is_city_change = user_id in waiting_for_city_change
        city = " ".join(text.split())
        if not is_valid_city(city):
            await message.answer(
                "К сожалению, мы не работаем в вашем городе. Попробуй другой вариант.",
                reply_markup=cancel_keyboard(),
            )
            return
        set_user_city(user_id, city)
        waiting_for_city.discard(user_id)
        waiting_for_city_change.discard(user_id)
        if is_registration_city:
            waiting_for_promo.add(user_id)
            await send_media_message(
                message,
                "account",
                f"<b>🌍 Город сохранён:</b> <code>{city}</code>\n\nЕсли есть промокод — введи его сейчас.\nЕсли нет — нажми <b>Пропустить</b>.",
                reply_markup=promo_skip_keyboard(),
            )
        elif is_city_change:
            await message.answer(
                f"<b>🌍 Город обновлён:</b> <code>{city}</code>",
                reply_markup=user_account_keyboard(user_id),
            )
        return

    if user_id in waiting_for_bot_token:
        token = text
        if not re.match(r"^\d+:[A-Za-z0-9_-]{20,}$", token):
            await message.answer("Похоже, это не токен Telegram-бота. Проверь и отправь ещё раз.", reply_markup=cancel_keyboard())
            return
        bot_key = extract_bot_key(token)
        label = f"bot_{bot_key}"
        db_execute(
            "INSERT INTO user_bots (bot_key, owner_id, token, label, active) VALUES (?, ?, ?, ?, 1) "
            "ON CONFLICT(bot_key) DO UPDATE SET owner_id=excluded.owner_id, token=excluded.token, label=excluded.label, active=1",
            (bot_key, user_id, token, label),
        )
        waiting_for_bot_token.discard(user_id)
        await message.answer(
            f"✅ Токен сохранён.\n\nБот <code>{bot_key}</code> будет запущен и получит тот же функционал.",
            reply_markup=user_account_keyboard(user_id),
        )
        if not IS_CHILD_PROCESS:
            await ensure_child_bot(token, bot_key, label)
        return

    # admin state handled above already
    if user and int(user["captcha_passed"]) == 0:
        if user_id not in captcha_answers:
            question, answer = generate_captcha()
            captcha_answers[user_id] = answer
            await message.answer(f"<b>Реши капчу:</b>\n{question} = ?")
            return
        try:
            if int(text) == captcha_answers[user_id]:
                set_user_captcha_passed(user_id, True)
                captcha_answers.pop(user_id, None)
                waiting_for_city.add(user_id)
                await send_media_message(message, "registration", get_setting("registration_text"))
                await message.answer("Введи свой город, например: <code>Москва</code>", reply_markup=cancel_keyboard())
            else:
                question, answer = generate_captcha()
                captcha_answers[user_id] = answer
                await message.answer(f"<b>Неверно.</b>\n{question} = ?")
        except ValueError:
            await message.answer("Введи ответ на капчу числом.")
        return

    if user_id in waiting_for_promo:
        if text.lower() in {"пропустить", "skip", "нет", "no"}:
            waiting_for_promo.discard(user_id)
            await message.answer("<b>Регистрация завершена ✅</b>\n\nВыбирай, что делать дальше.", reply_markup=main_menu())
            return
        promo = text.upper().strip()
        promo_row = get_promo(promo)
        if promo_row and int(promo_row["active"]) == 1:
            set_user_promo(user_id, promo)
            if promo_row["kind"] in {"bonus_balance", "gift"}:
                add_balance(user_id, float(promo_row["value"]))
            waiting_for_promo.discard(user_id)
            await message.answer(f"<b>Промокод принят ✅</b>\n\nКод: <code>{promo}</code>", reply_markup=main_menu())
            return
        await message.answer("<b>Промокод не найден.</b>\nПопробуй ещё раз или нажми «Пропустить».", reply_markup=promo_skip_keyboard())
        return

    if text == "🛍 Каталог":
        if not user or not user["city"]:
            await message.answer("Сначала нужно указать город.")
            return
        await send_media_message(message, "catalog", render_text(get_setting("catalog_text"), city=user["city"]), reply_markup=catalog_keyboard())
        return

    if text == "🧺 Корзина":
        items = carts.get(user_id, [])
        if not items:
            await message.answer(get_setting("empty_cart_text"))
            return
        total = sum(item["price"] for item in items)
        promo = user["promo_code"] if user else None
        discounted_total = apply_promo_to_total(total, promo)
        lines = [f"• <b>{item['product_name']}</b> — {item['weight_label']} — {item['price']:.2f} ₽" for item in items]
        promo_line = f"\nПромокод: <code>{promo}</code>\nСумма со скидкой: <b>{discounted_total:.2f} ₽</b>" if promo else ""
        await message.answer("<b>🧺 Ваша корзина:</b>\n\n" + "\n".join(lines) + f"\n\n<b>Итого:</b> {total:.2f} ₽" + promo_line, reply_markup=cart_keyboard())
        return

    if text == "👤 Аккаунт":
        balance_rub = float(user["balance"] or 0)
        balance_usdt, rate = user_balance_usdt(balance_rub)
        await send_media_message(
            message,
            "account",
            render_text(
                get_setting("account_text"),
                city=user["city"] or "не выбран",
                balance_rub=balance_rub,
                balance_usdt=balance_usdt,
                rate=rate,
                promo=user["promo_code"] or "нет",
            ),
            reply_markup=user_account_keyboard(user_id),
        )
        return

    if text == "💳 Пополнение":
        await send_media_message(message, "deposit", get_setting("deposit_text"), reply_markup=deposit_keyboard())
        return

    if text == "💬 Поддержка":
        await send_media_message(message, "support", f"<b>💬 Поддержка</b>\n\n{get_support_text()}", reply_markup=main_menu())
        return

    await message.answer("Используй кнопки меню ниже.", reply_markup=main_menu())


async def process_admin_message(message: Message):
    user_id = message.from_user.id
    st = admin_state.get(user_id, {})
    mode = st.get("mode")
    text = (message.text or message.caption or "").strip() if message.content_type in {"text", "photo"} else None

    if mode == "set_text" and text is not None:
        set_setting(st["key"], text)
        admin_state.pop(user_id, None)
        await message.answer("✅ Текст сохранён.", reply_markup=admin_keyboard())
        return

    if mode == "set_rate" and text is not None:
        if text.lower() in {"auto", "авто", "сброс"}:
            set_setting("usdt_rate_override", "")
            admin_state.pop(user_id, None)
            await message.answer("✅ Курс переведён в авто-режим.", reply_markup=admin_keyboard())
            return
        try:
            rate = float(text.replace(",", "."))
            if rate <= 0:
                raise ValueError
        except ValueError:
            await message.answer("Введи число, например <code>98.50</code> или напиши <code>auto</code>.", reply_markup=cancel_keyboard())
            return
        set_setting("usdt_rate_override", f"{rate:.2f}")
        admin_state.pop(user_id, None)
        await message.answer(f"✅ Курс сохранён: <b>{rate:.2f} ₽/USDT</b>", reply_markup=admin_keyboard())
        return

    if mode == "wallet_address" and text is not None:
        currency = st["currency"]
        db_execute("UPDATE wallets SET address=? WHERE currency=?", (text, currency))
        admin_state.pop(user_id, None)
        await message.answer(f"✅ Адрес для <b>{currency}</b> сохранён.", reply_markup=admin_keyboard())
        return

    if mode == "wallet_network" and text is not None:
        currency = st["currency"]
        db_execute("UPDATE wallets SET network=? WHERE currency=?", (text, currency))
        admin_state.pop(user_id, None)
        await message.answer(f"✅ Сеть для <b>{currency}</b> сохранена.", reply_markup=admin_keyboard())
        return

    if mode == "wallet_add_currency" and text is not None:
        st["currency"] = text.upper().strip()
        st["mode"] = "wallet_add_network"
        await message.answer("Теперь введи <b>сеть</b>, например <code>TRC20</code>.", reply_markup=cancel_keyboard())
        return

    if mode == "wallet_add_network" and text is not None:
        st["network"] = text.strip()
        st["mode"] = "wallet_add_address"
        await message.answer("Теперь введи <b>адрес</b>.", reply_markup=cancel_keyboard())
        return

    if mode == "wallet_add_address" and text is not None:
        currency = st["currency"]
        network = st["network"]
        db_execute(
            "INSERT INTO wallets (currency, network, address, active) VALUES (?, ?, ?, 1) "
            "ON CONFLICT(currency) DO UPDATE SET network=excluded.network, address=excluded.address, active=1",
            (currency, network, text),
        )
        admin_state.pop(user_id, None)
        await message.answer(f"✅ Кошелёк <b>{currency}</b> добавлен.", reply_markup=admin_keyboard())
        return

    if mode == "prod_add_name" and text is not None:
        st["name"] = text
        st["mode"] = "prod_add_desc"
        await message.answer("Теперь введи <b>описание</b> товара.", reply_markup=cancel_keyboard())
        return

    if mode == "prod_add_desc" and text is not None:
        name = st["name"]
        cur = DB.execute("INSERT INTO products (name, description, active, sort_order) VALUES (?, ?, 1, ?)", (name, text, 9999))
        pid = cur.lastrowid
        DB.commit()
        admin_state.pop(user_id, None)
        await message.answer(f"✅ Товар <b>{name}</b> создан. ID: <code>{pid}</code>.", reply_markup=admin_keyboard())
        return

    if mode == "prod_edit_name" and text is not None:
        pid = st["product_id"]
        db_execute("UPDATE products SET name=? WHERE id=?", (text, pid))
        admin_state.pop(user_id, None)
        await message.answer("✅ Название товара обновлено.", reply_markup=admin_keyboard())
        return

    if mode == "prod_edit_desc" and text is not None:
        pid = st["product_id"]
        db_execute("UPDATE products SET description=? WHERE id=?", (text, pid))
        admin_state.pop(user_id, None)
        await message.answer("✅ Описание товара обновлено.", reply_markup=admin_keyboard())
        return

    if mode == "prod_template_add" and message.content_type in {"text", "photo"}:
        template_text = message.caption if message.content_type == "photo" else (message.text or "")
        try:
            parsed = parse_product_template(template_text)
        except ValueError as exc:
            await message.answer(f"⚠️ {exc}", reply_markup=cancel_keyboard())
            return
        cur = DB.execute("INSERT INTO products (name, description, active, sort_order) VALUES (?, ?, 1, ?)", (parsed["name"], parsed["description"], 9999))
        pid = cur.lastrowid
        DB.commit()
        for index, (label, price) in enumerate(parsed["weights"], start=1):
            DB.execute(
                "INSERT INTO weights (product_id, label, price_rub, active, sort_order) VALUES (?, ?, ?, 1, ?)",
                (pid, label, price, index),
            )
        DB.commit()
        if message.content_type == "photo":
            set_media_file_id(f"product_{pid}", message.photo[-1].file_id)
        admin_state.pop(user_id, None)
        await message.answer(
            f"✅ Товар <b>{parsed['name']}</b> создан одним сообщением.\nID: <code>{pid}</code>",
            reply_markup=admin_keyboard(),
        )
        return

    if mode == "prod_template_edit" and message.content_type in {"text", "photo"}:
        template_text = message.caption if message.content_type == "photo" else (message.text or "")
        try:
            parsed = parse_product_template(template_text)
        except ValueError as exc:
            await message.answer(f"⚠️ {exc}", reply_markup=cancel_keyboard())
            return
        pid = st["product_id"]
        db_execute("UPDATE products SET name=?, description=? WHERE id=?", (parsed["name"], parsed["description"], pid))
        db_execute("DELETE FROM weights WHERE product_id=?", (pid,))
        for index, (label, price) in enumerate(parsed["weights"], start=1):
            DB.execute(
                "INSERT INTO weights (product_id, label, price_rub, active, sort_order) VALUES (?, ?, ?, 1, ?)",
                (pid, label, price, index),
            )
        DB.commit()
        if message.content_type == "photo":
            set_media_file_id(f"product_{pid}", message.photo[-1].file_id)
        admin_state.pop(user_id, None)
        await message.answer(
            f"✅ Товар <b>{parsed['name']}</b> обновлён одним сообщением.",
            reply_markup=admin_keyboard(),
        )
        return

    if mode == "weight_add_label" and text is not None:
        st["label"] = text
        st["mode"] = "weight_add_price"
        await message.answer("Теперь введи <b>цену</b> в рублях.", reply_markup=cancel_keyboard())
        return

    if mode == "weight_add_price" and text is not None:
        try:
            price = float(text.replace(",", "."))
        except ValueError:
            await message.answer("Введи цену числом, например <code>390</code>.", reply_markup=cancel_keyboard())
            return
        pid = st["product_id"]
        label = st["label"]
        cur = DB.execute(
            "INSERT INTO weights (product_id, label, price_rub, active, sort_order) VALUES (?, ?, ?, 1, ?)",
            (pid, label, price, 9999),
        )
        DB.commit()
        admin_state.pop(user_id, None)
        await message.answer(f"✅ Вес <b>{label}</b> добавлен.", reply_markup=admin_keyboard())
        return

    if mode == "weight_edit_label" and text is not None:
        wid = st["weight_id"]
        db_execute("UPDATE weights SET label=? WHERE id=?", (text, wid))
        admin_state.pop(user_id, None)
        await message.answer("✅ Название веса обновлено.", reply_markup=admin_keyboard())
        return

    if mode == "weight_edit_price" and text is not None:
        try:
            price = float(text.replace(",", "."))
        except ValueError:
            await message.answer("Введи цену числом, например <code>720</code>.", reply_markup=cancel_keyboard())
            return
        wid = st["weight_id"]
        db_execute("UPDATE weights SET price_rub=? WHERE id=?", (price, wid))
        admin_state.pop(user_id, None)
        await message.answer("✅ Цена веса обновлена.", reply_markup=admin_keyboard())
        return

    if mode == "promo_code" and text is not None:
        st["code"] = text.upper().strip()
        st["mode"] = "promo_kind"
        await message.answer("Введи тип: <code>discount</code>, <code>bonus_balance</code> или <code>gift</code>.", reply_markup=cancel_keyboard())
        return

    if mode == "promo_kind" and text is not None:
        kind = text.lower().strip()
        if kind not in PROMO_ALLOWED_KINDS:
            await message.answer("Тип должен быть: <code>discount</code>, <code>bonus_balance</code> или <code>gift</code>.", reply_markup=cancel_keyboard())
            return
        st["kind"] = kind
        st["mode"] = "promo_value"
        await message.answer("Введи значение числом. Для скидки — процент, для bonus_balance/gift — сумма в рублях.", reply_markup=cancel_keyboard())
        return

    if mode == "promo_value" and text is not None:
        try:
            value = float(text.replace(",", "."))
        except ValueError:
            await message.answer("Введи число, например <code>10</code> или <code>500</code>.", reply_markup=cancel_keyboard())
            return
        code = st["code"]
        kind = st["kind"]
        db_execute("INSERT INTO promo_codes (code, kind, value, active, note) VALUES (?, ?, ?, 1, ?)", (code, kind, value, None))
        admin_state.pop(user_id, None)
        await message.answer(f"✅ Промокод <code>{code}</code> добавлен.", reply_markup=admin_keyboard())
        return

    if mode == "deposit_confirm" and text is not None:
        dep_id = st["deposit_id"]
        try:
            amount_rub = float(text.replace(",", "."))
        except ValueError:
            await message.answer("Введи сумму числом, например <code>1500</code>.", reply_markup=cancel_keyboard())
            return
        dep = db_fetchone("SELECT * FROM deposits WHERE id=?", (dep_id,))
        if not dep or dep["status"] != "pending":
            admin_state.pop(user_id, None)
            await message.answer("Заявка уже неактуальна.", reply_markup=admin_keyboard())
            return
        amount_usdt = round(amount_rub / rate_from_override_or_market(), 2)
        add_balance(dep["user_id"], amount_rub)
        db_execute(
            "UPDATE deposits SET amount_rub=?, amount_usdt=?, status='confirmed', confirmed_at=CURRENT_TIMESTAMP WHERE id=?",
            (amount_rub, amount_usdt, dep_id),
        )
        admin_state.pop(user_id, None)
        try:
            await BOT.send_message(
                dep["user_id"],
                f"✅ Пополнение #{dep_id} подтверждено.\n\nЗачислено: <b>{amount_rub:.2f} ₽</b> (~<b>{amount_usdt:.2f} USDT</b>)",
            )
        except Exception:
            pass
        await message.answer(f"✅ Пополнение #{dep_id} подтверждено на сумму <b>{amount_rub:.2f} ₽</b>.", reply_markup=admin_keyboard())
        return

    if mode == "broadcast_wait":
        ok, fail = await broadcast_message(message)
        admin_state.pop(user_id, None)
        await message.answer(f"✅ Рассылка завершена.\n\nОтправлено: <b>{ok}</b>\nОшибок: <b>{fail}</b>", reply_markup=admin_keyboard())
        return

    await message.answer("Не понял админское действие.", reply_markup=admin_keyboard())


# generic media types for admin broadcast and relay support
@ROUTER.message(F.document | F.video | F.audio | F.voice | F.sticker | F.video_note)
async def generic_media_router(message: Message):
    if await relay_reply_message(message):
        return
    user_id = message.from_user.id
    if user_id == ADMIN_ID and user_id in admin_state and admin_state[user_id].get("mode") == "broadcast_wait":
        ok, fail = await broadcast_message(message)
        admin_state.pop(user_id, None)
        await message.answer(f"✅ Рассылка завершена.\n\nОтправлено: <b>{ok}</b>\nОшибок: <b>{fail}</b>", reply_markup=admin_keyboard())
        return
    await message.answer("Используй кнопки меню ниже.", reply_markup=main_menu())


# =========================
# STARTUP
# =========================

async def main():
    if current_disabled():
        logging.warning("Бот отключён флагом %s", DISABLE_FLAG)
        return

    logging.info("Бот запускается (%s, instance=%s)...", "child" if IS_CHILD_PROCESS else "primary", INSTANCE_KEY)

    background_tasks = []
    if not IS_CHILD_PROCESS:
        background_tasks.append(asyncio.create_task(reconcile_child_bots_loop()))

    try:
        await DP.start_polling(BOT)
    finally:
        for task in background_tasks:
            task.cancel()
        await stop_all_children()


if __name__ == "__main__":
    asyncio.run(main())
