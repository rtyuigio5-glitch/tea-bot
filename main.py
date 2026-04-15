import asyncio
import io
import json
import logging
import os
import random
import re
import sqlite3
import time
import unicodedata
import zipfile
from pathlib import Path
from statistics import mean
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from aiogram import Bot, Dispatcher, F
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

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = 7106262808 # your Telegram ID

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN не найден. Проверь файл .env")

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "db.sqlite3"
IMAGES_DIR = BASE_DIR / "images"
CITY_CACHE_PATH = BASE_DIR / "ru_kz_city_whitelist.json"

BYBIT_P2P_URLS = [
    "https://www.bybit.com/en/fiat/trade/otc/sell/USDT/RUB?actionType=0&fiat=RUB",
    "https://www.bybit.com/en/fiat/trade/otc/sell/USDT/RUB?actionType=0&fiat=RUB&paymentMethod=75",
]
DEFAULT_USDT_RUB_RATE = 82.0
RATE_CACHE_TTL = 15 * 60
RATE_CACHE = {"value": None, "updated_at": 0.0}

GEONAMES_COUNTRY_URLS = {
    "RU": "https://download.geonames.org/export/dump/RU.zip",
    "KZ": "https://download.geonames.org/export/dump/KZ.zip",
}

bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML),
)
dp = Dispatcher()

# In-memory state
captcha_answers: dict[int, int] = {}
waiting_for_city: set[int] = set()
waiting_for_promo: set[int] = set()
user_waiting_for_deposit_amount: dict[int, str] = {}
admin_state: dict[int, dict] = {}
carts: dict[int, list[dict]] = {}


# =========================
# DEFAULT CONTENT
# =========================

SETTINGS_DEFAULTS = {
    "start_text": "<b>💎 Добро пожаловать в лучший шоп России и Казахстана!</b>\n\nСначала пройди капчу.",
    "registration_text": "<b>✅ Капча пройдена</b>\n\nТеперь введи свой город вручную.",
    "account_text": "<b>👤 Аккаунт</b>\n\n🌍 Город: <code>{city}</code>\n💰 Баланс: <b>{balance_rub:.2f} ₽</b> (~<b>{balance_usdt:.2f} USDT</b>)\n📉 Курс: <code>1 USDT ≈ {rate:.2f} ₽</code>\n🏷 Промокод: <code>{promo}</code>",
    "deposit_text": "<b>💳 Пополнение баланса</b>\n\nСначала выбери валюту, потом введи сумму в рублях.",
    "support_text": "💬 Поддержка пока не настроена.",
    "catalog_text": "<b>🛍 Каталог</b>\n\nВыбери товар ниже.",
    "empty_cart_text": "<b>🧺 Корзина пустая.</b>",
    "order_success_text": "<b>✅ Заказ оформлен</b>",
    "insufficient_balance_text": "<b>Недостаточно средств</b>",
    "usdt_rate_override": "",
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

# Popular fallback whitelist so the bot works immediately even if the online city dump is unavailable.
FALLBACK_CITIES = {
    # Russia
    "москва", "санкт-петербург", "новосибирск", "екатеринбург", "казань", "нижний новгород", "челябинск", "самара",
    "омск", "ростов-на-дону", "уфа", "красноярск", "пермь", "воронеж", "волгоград", "краснодар", "саратов",
    "тюмень", "тольятти", "ижевск", "барнаул", "ульяновск", "иркутск", "хабаровск", "ярославль", "владивосток",
    "махачкала", "томск", "оренбург", "кемерово", "новокузнецк", "рязань", "астрахань", "пенза", "липецк",
    "киров", "чебоксары", "калининград", "брянск", "курск", "иваново", "магнитогорск", "тверь", "ставрополь",
    "сочи", "набережные челны", "благовещенск", "архангельск", "владикавказ", "сургут", "нижневартовск", "мурманск",
    "курган", "томск", "сыктывкар", "орёл", "петрозаводск", "смоленск", "калуга", "орск", "волжский", "воркута",
    # Kazakhstan
    "алматы", "астана", "шымкент", "актобе", "караганда", "тараз", "павлодар", "усть-каменогорск", "семей",
    "костанай", "кызылорда", "уральск", "петропавловск", "актау", "темиртау", "туркестан", "атырау", "талдыкорган",
    "жезказган", "кентау", "балхаш", "кокшетау", "сатпаев", "экибастуз", 
}


# =========================
# DB INIT
# =========================

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

cur.execute(
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

cur.execute(
    """
    CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )
    """
)

cur.execute(
    """
    CREATE TABLE IF NOT EXISTS media_assets (
        media_key TEXT PRIMARY KEY,
        file_id TEXT NOT NULL,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """
)

cur.execute(
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

cur.execute(
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

cur.execute(
    """
    CREATE TABLE IF NOT EXISTS wallets (
        currency TEXT PRIMARY KEY,
        network TEXT NOT NULL,
        address TEXT NOT NULL,
        active INTEGER NOT NULL DEFAULT 1
    )
    """
)

cur.execute(
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

cur.execute(
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

for key, value in SETTINGS_DEFAULTS.items():
    cur.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (key, value))

for currency, data in DEFAULT_WALLETS.items():
    cur.execute(
        "INSERT OR IGNORE INTO wallets (currency, network, address, active) VALUES (?, ?, ?, ?)",
        (currency, data["network"], data["address"], data["active"]),
    )

cur.execute("SELECT COUNT(*) AS cnt FROM products")
if cur.fetchone()["cnt"] == 0:
    for i, (name, description) in enumerate(DEFAULT_PRODUCTS, start=1):
        cur.execute(
            "INSERT INTO products (name, description, active, sort_order) VALUES (?, ?, 1, ?)",
            (name, description, i),
        )
        pid = cur.lastrowid
        for j, (label, price) in enumerate(DEFAULT_WEIGHTS.get(i, []), start=1):
            cur.execute(
                "INSERT INTO weights (product_id, label, price_rub, active, sort_order) VALUES (?, ?, ?, 1, ?)",
                (pid, label, price, j),
            )

conn.commit()



# =========================
# CITY WHITELIST
# =========================


def normalize_city_name(value: str) -> str:
    value = unicodedata.normalize("NFKC", value).casefold().replace("ё", "е")
    value = re.sub(r"[\s\u00A0]+", " ", value).strip()
    # common user variants
    prefixes = ("г. ", "город ", "г ")
    for prefix in prefixes:
        if value.startswith(prefix):
            value = value[len(prefix):].strip()
    suffixes = (" г.", " город")
    for suffix in suffixes:
        if value.endswith(suffix):
            value = value[: -len(suffix)].strip()
    return value


# Local, stable whitelist. This avoids startup failures on Railway and still
# keeps the validation strict enough for the common city names the shop uses.
# Normalize everything once and compare against normalized user input.
VALID_CITIES = {normalize_city_name(city) for city in FALLBACK_CITIES}


def is_valid_city(value: str) -> bool:
    return normalize_city_name(value) in VALID_CITIES


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
    cur.execute("SELECT value FROM settings WHERE key=?", (key,))
    row = cur.fetchone()
    return row["value"] if row else default


def set_setting(key: str, value: str) -> None:
    cur.execute(
        "INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )
    conn.commit()


def get_user(user_id: int):
    cur.execute("SELECT * FROM users WHERE user_id=?", (user_id,))
    return cur.fetchone()


def create_user_if_needed(message: Message) -> None:
    if get_user(message.from_user.id) is None:
        cur.execute(
            "INSERT INTO users (user_id, username, full_name) VALUES (?, ?, ?)",
            (message.from_user.id, message.from_user.username, message.from_user.full_name),
        )
        conn.commit()


def set_user_city(user_id: int, city: str) -> None:
    cur.execute("UPDATE users SET city=? WHERE user_id=?", (city, user_id))
    conn.commit()


def set_user_captcha_passed(user_id: int, passed: bool = True) -> None:
    cur.execute("UPDATE users SET captcha_passed=? WHERE user_id=?", (1 if passed else 0, user_id))
    conn.commit()


def set_user_promo(user_id: int, promo: str | None) -> None:
    cur.execute("UPDATE users SET promo_code=? WHERE user_id=?", (promo, user_id))
    conn.commit()


def add_balance(user_id: int, amount: float) -> None:
    cur.execute("UPDATE users SET balance = balance + ? WHERE user_id=?", (amount, user_id))
    conn.commit()


def subtract_balance(user_id: int, amount: float) -> None:
    cur.execute("UPDATE users SET balance = balance - ? WHERE user_id=?", (amount, user_id))
    conn.commit()


def get_support_text() -> str:
    return get_setting("support_text", "💬 Поддержка пока не настроена.")


def generate_captcha() -> tuple[str, int]:
    a = random.randint(1, 9)
    b = random.randint(1, 9)
    return f"{a} + {b}", a + b


def get_products(active_only: bool = True):
    if active_only:
        cur.execute("SELECT * FROM products WHERE active=1 ORDER BY sort_order, id")
    else:
        cur.execute("SELECT * FROM products ORDER BY sort_order, id")
    return cur.fetchall()


def get_product(product_id: int):
    cur.execute("SELECT * FROM products WHERE id=?", (product_id,))
    return cur.fetchone()


def get_weights(product_id: int, active_only: bool = True):
    if active_only:
        cur.execute("SELECT * FROM weights WHERE product_id=? AND active=1 ORDER BY sort_order, id", (product_id,))
    else:
        cur.execute("SELECT * FROM weights WHERE product_id=? ORDER BY sort_order, id", (product_id,))
    return cur.fetchall()


def get_wallet(currency: str):
    cur.execute("SELECT * FROM wallets WHERE currency=?", (currency,))
    return cur.fetchone()


def get_active_wallets():
    cur.execute("SELECT * FROM wallets WHERE active=1 ORDER BY currency")
    return cur.fetchall()


def get_media_file_id(media_key: str) -> str | None:
    cur.execute("SELECT file_id FROM media_assets WHERE media_key=?", (media_key,))
    row = cur.fetchone()
    return row["file_id"] if row else None


def set_media_file_id(media_key: str, file_id: str) -> None:
    cur.execute(
        "INSERT INTO media_assets (media_key, file_id, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP) "
        "ON CONFLICT(media_key) DO UPDATE SET file_id=excluded.file_id, updated_at=CURRENT_TIMESTAMP",
        (media_key, file_id),
    )
    conn.commit()


def clear_media_file_id(media_key: str) -> None:
    cur.execute("DELETE FROM media_assets WHERE media_key=?", (media_key,))
    conn.commit()


def get_media_source(media_key: str):
    file_id = get_media_file_id(media_key)
    if file_id:
        return file_id
    default_name = DEFAULT_MEDIA_FILES.get(media_key)
    if default_name:
        path = IMAGES_DIR / default_name
        if path.exists():
            return FSInputFile(path)
    if media_key.startswith("product_"):
        path = IMAGES_DIR / f"{media_key}.jpg"
        if path.exists():
            return FSInputFile(path)
    return None


def get_promo(code: str):
    cur.execute("SELECT * FROM promo_codes WHERE code=?", (code.upper().strip(),))
    return cur.fetchone()


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


def promo_skip_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Пропустить", callback_data="promo:skip")]])


def catalog_keyboard() -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text=product["name"], callback_data=f"product:{product['id']}")] for product in get_products(True)]
    return InlineKeyboardMarkup(inline_keyboard=rows or [[InlineKeyboardButton(text="Нет товаров", callback_data="noop")]])


def weights_keyboard(product_id: int) -> InlineKeyboardMarkup:
    rows = []
    for weight in get_weights(product_id, True):
        rows.append([
            InlineKeyboardButton(
                text=f'{weight["label"]} — {float(weight["price_rub"]):.0f} ₽',
                callback_data=f"buyweight:{product_id}:{weight['id']}",
            )
        ])
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
    rows = []
    for wallet in get_active_wallets():
        rows.append([InlineKeyboardButton(text=f"{wallet['currency']} — адрес", callback_data=f"wallet:addr:{wallet['currency']}")])
        rows.append([InlineKeyboardButton(text=f"{wallet['currency']} — сеть", callback_data=f"wallet:net:{wallet['currency']}")])
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
        ]
    )


def product_admin_keyboard(product_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✏️ Название", callback_data=f"prod:name:{product_id}"), InlineKeyboardButton(text="📝 Описание", callback_data=f"prod:desc:{product_id}")],
            [InlineKeyboardButton(text="🖼 Фото", callback_data=f"prod:photo:{product_id}"), InlineKeyboardButton(text="💲 Весы", callback_data=f"prod:weights:{product_id}")],
            [InlineKeyboardButton(text="🔁 Вкл/выкл", callback_data=f"prod:toggle:{product_id}"), InlineKeyboardButton(text="🗑 Удалить", callback_data=f"prod:delete:{product_id}")],
        ]
    )


def weight_admin_keyboard(product_id: int) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text="➕ Добавить вес", callback_data=f"weight:add:{product_id}")]]
    for weight in get_weights(product_id, False):
        rows.append([
            InlineKeyboardButton(text=f"✏️ {weight['label']}", callback_data=f"weight:label:{weight['id']}") ,
            InlineKeyboardButton(text="💵 Цена", callback_data=f"weight:price:{weight['id']}")
        ])
        rows.append([InlineKeyboardButton(text="🗑 Удалить", callback_data=f"weight:delete:{weight['id']}")])
    rows.append([InlineKeyboardButton(text="↩️ Назад к товарам", callback_data="admin:products")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def format_rate() -> float:
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
    rate = format_rate()
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
    return DEFAULT_WALLETS.get(currency, {}).get("address", "ADDRESS_HERE")


async def send_media_message(target, media_key: str, caption: str, reply_markup=None):
    source = get_media_source(media_key)
    if source:
        return await target.answer_photo(photo=source, caption=caption, reply_markup=reply_markup)
    return await target.answer(caption, reply_markup=reply_markup)


# =========================
# ADMIN HANDLERS
# =========================

@dp.message(Command("admin"))
async def admin_panel(message: Message):
    if message.from_user.id != ADMIN_ID:
        await message.answer("❌ Нет доступа")
        return
    await message.answer("<b>⚙️ Админ панель</b>", reply_markup=admin_keyboard())


@dp.callback_query(F.data == "admin:back")
async def admin_back(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    await call.message.answer("<b>⚙️ Админ панель</b>", reply_markup=admin_keyboard())
    await call.answer()


@dp.callback_query(F.data == "admin:settings")
async def admin_settings(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    await call.message.answer("<b>⚙️ Настройки</b>", reply_markup=settings_keyboard())
    await call.answer()


@dp.callback_query(F.data == "admin:media")
async def admin_media(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    await call.message.answer(
        "<b>🖼 Медиа-центр</b>\n\nВыбери картинку, затем отправь новую фотографию.",
        reply_markup=media_keyboard(),
    )
    await call.answer()


@dp.callback_query(F.data == "admin:products")
async def admin_products(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return

    products = get_products(False)
    if not products:
        await call.message.answer(
            "Товаров пока нет.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="➕ Добавить товар", callback_data="prod:add")],
                    [InlineKeyboardButton(text="↩️ Назад", callback_data="admin:back")],
                ]
            ),
        )
        await call.answer()
        return

    await call.message.answer(
        "<b>📦 Товары</b>",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="➕ Добавить товар", callback_data="prod:add")],
                [InlineKeyboardButton(text="↩️ Назад", callback_data="admin:back")],
            ]
        ),
    )
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


@dp.callback_query(F.data == "prod:add")
async def product_add(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    admin_state[call.from_user.id] = {"mode": "prod_add_name"}
    await call.message.answer("Введи <b>название нового товара</b>.")
    await call.answer()


@dp.callback_query(F.data.startswith("prod:name:"))
async def product_edit_name(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    pid = int(call.data.split(":")[2])
    admin_state[call.from_user.id] = {"mode": "prod_edit_name", "product_id": pid}
    await call.message.answer("Введи <b>новое название</b> товара.")
    await call.answer()


@dp.callback_query(F.data.startswith("prod:desc:"))
async def product_edit_desc(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    pid = int(call.data.split(":")[2])
    admin_state[call.from_user.id] = {"mode": "prod_edit_desc", "product_id": pid}
    await call.message.answer("Введи <b>новое описание</b> товара.")
    await call.answer()


@dp.callback_query(F.data.startswith("prod:photo:"))
async def product_photo(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    pid = int(call.data.split(":")[2])
    admin_state[call.from_user.id] = {"mode": "set_media", "media_key": f"product_{pid}"}
    await call.message.answer("Теперь отправь <b>фото товара</b>.")
    await call.answer()


@dp.callback_query(F.data.startswith("prod:weights:"))
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


@dp.callback_query(F.data.startswith("prod:toggle:"))
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
    cur.execute("UPDATE products SET active=? WHERE id=?", (new_value, pid))
    conn.commit()
    await call.answer("Состояние изменено")
    await call.message.answer(f"Товар <b>{p['name']}</b> теперь {'включён' if new_value else 'выключен'}.")


@dp.callback_query(F.data.startswith("prod:delete:"))
async def product_delete(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    pid = int(call.data.split(":")[2])
    p = get_product(pid)
    if not p:
        await call.answer("Товар не найден", show_alert=True)
        return
    cur.execute("DELETE FROM weights WHERE product_id=?", (pid,))
    cur.execute("DELETE FROM products WHERE id=?", (pid,))
    clear_media_file_id(f"product_{pid}")
    conn.commit()
    await call.answer("Удалено")
    await call.message.answer(f"Товар <b>{p['name']}</b> удалён.")


@dp.callback_query(F.data.startswith("weight:add:"))
async def weight_add(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    pid = int(call.data.split(":")[2])
    admin_state[call.from_user.id] = {"mode": "weight_add_label", "product_id": pid}
    await call.message.answer("Введи <b>название веса</b>, например <code>50 г</code>.")
    await call.answer()


@dp.callback_query(F.data.startswith("weight:label:"))
async def weight_edit_label(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    wid = int(call.data.split(":")[2])
    admin_state[call.from_user.id] = {"mode": "weight_edit_label", "weight_id": wid}
    await call.message.answer("Введи <b>новое название веса</b>.")
    await call.answer()


@dp.callback_query(F.data.startswith("weight:price:"))
async def weight_edit_price(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    wid = int(call.data.split(":")[2])
    admin_state[call.from_user.id] = {"mode": "weight_edit_price", "weight_id": wid}
    await call.message.answer("Введи <b>новую цену</b> в рублях.")
    await call.answer()


@dp.callback_query(F.data.startswith("weight:delete:"))
async def weight_delete(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    wid = int(call.data.split(":")[2])
    cur.execute("DELETE FROM weights WHERE id=?", (wid,))
    conn.commit()
    await call.answer("Удалено")
    await call.message.answer("Вес удалён.")


@dp.callback_query(F.data == "admin:wallets")
async def admin_wallets(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    await call.message.answer("<b>👛 Кошельки</b>", reply_markup=wallets_keyboard())
    await call.answer()


@dp.callback_query(F.data.startswith("wallet:addr:"))
async def wallet_edit_address(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    currency = call.data.split(":")[2]
    admin_state[call.from_user.id] = {"mode": "wallet_address", "currency": currency}
    await call.message.answer(f"Введи новый <b>адрес</b> для <b>{currency}</b>.")
    await call.answer()


@dp.callback_query(F.data.startswith("wallet:net:"))
async def wallet_edit_network(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    currency = call.data.split(":")[2]
    admin_state[call.from_user.id] = {"mode": "wallet_network", "currency": currency}
    await call.message.answer(f"Введи новую <b>сеть</b> для <b>{currency}</b>.")
    await call.answer()


@dp.callback_query(F.data == "admin:promos")
async def admin_promos(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    await call.message.answer("<b>🏷 Промокоды</b>", reply_markup=promo_keyboard())
    await call.answer()


@dp.callback_query(F.data == "promo:add")
async def promo_add(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    admin_state[call.from_user.id] = {"mode": "promo_code"}
    await call.message.answer("Введи <b>код промокода</b>, например <code>TEA10</code>.")
    await call.answer()


@dp.callback_query(F.data == "promo:list")
async def promo_list(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    cur.execute("SELECT * FROM promo_codes ORDER BY code")
    rows = cur.fetchall()
    if not rows:
        await call.message.answer("Промокодов пока нет.")
        await call.answer()
        return
    text = ["<b>Промокоды:</b>"]
    for p in rows:
        text.append(f"• <code>{p['code']}</code> | {p['kind']} | {p['value']} | {'on' if int(p['active']) == 1 else 'off'}")
    await call.message.answer("\n".join(text))
    await call.answer()


@dp.callback_query(F.data == "admin:deposits")
async def admin_deposits(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    cur.execute("SELECT * FROM deposits WHERE status='pending' ORDER BY id DESC")
    rows = cur.fetchall()
    if not rows:
        await call.message.answer("Нет заявок на пополнение.")
        await call.answer()
        return
    for d in rows:
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"dep:approve:{d['id']}")],
                [InlineKeyboardButton(text="❌ Отклонить", callback_data=f"dep:decline:{d['id']}")],
            ]
        )
        await call.message.answer(
            f"<b>Заявка #{d['id']}</b>\n\n"
            f"👤 User ID: <code>{d['user_id']}</code>\n"
            f"💳 Валюта: <b>{d['currency']}</b>\n"
            f"💰 Сумма: <b>{float(d['amount_rub']):.2f} ₽</b> (~<b>{float(d['amount_usdt']):.2f} USDT</b>)\n"
            f"🏦 Адрес: <code>{d['address']}</code>",
            reply_markup=kb,
        )
    await call.answer()


@dp.callback_query(F.data == "admin:users")
async def admin_users(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    cur.execute("SELECT * FROM users ORDER BY user_id DESC LIMIT 30")
    rows = cur.fetchall()
    if not rows:
        await call.message.answer("Пользователей пока нет.")
        await call.answer()
        return
    lines = ["<b>Пользователи:</b>"]
    for u in rows:
        balance_rub = float(u["balance"] or 0)
        balance_usdt = round(balance_rub / format_rate(), 2)
        lines.append(f"• <code>{u['user_id']}</code> | {u['city'] or '—'} | {balance_rub:.2f} ₽ (~{balance_usdt:.2f} USDT)")
    await call.message.answer("\n".join(lines))
    await call.answer()


@dp.callback_query(F.data.startswith("dep:approve:"))
async def dep_approve(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    dep_id = int(call.data.split(":")[2])
    cur.execute("SELECT * FROM deposits WHERE id=?", (dep_id,))
    dep = cur.fetchone()
    if not dep:
        await call.answer("Не найдено", show_alert=True)
        return
    if dep["status"] != "pending":
        await call.answer("Уже обработано", show_alert=True)
        return
    admin_state[call.from_user.id] = {"mode": "deposit_confirm", "deposit_id": dep_id}
    await call.message.answer(f"Отправь сумму в рублях для заявки <b>#{dep_id}</b> (или <code>отмена</code>).")
    await call.answer()


@dp.callback_query(F.data.startswith("dep:decline:"))
async def dep_decline(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    dep_id = int(call.data.split(":")[2])
    cur.execute("SELECT * FROM deposits WHERE id=?", (dep_id,))
    dep = cur.fetchone()
    if not dep:
        await call.answer("Не найдено", show_alert=True)
        return
    if dep["status"] != "pending":
        await call.answer("Уже обработано", show_alert=True)
        return
    cur.execute("UPDATE deposits SET status='declined' WHERE id=?", (dep_id,))
    conn.commit()
    try:
        await bot.send_message(dep["user_id"], f"❌ Пополнение #{dep_id} отклонено администратором.")
    except Exception:
        pass
    await call.message.answer(f"❌ Пополнение #{dep_id} отклонено.")
    await call.answer()


# =========================
# USER FLOW
# =========================

@dp.message(CommandStart())
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


@dp.message(F.photo)
async def admin_photo_upload(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    st = admin_state.get(message.from_user.id)
    if not st or st.get("mode") != "set_media":
        return
    media_key = st["media_key"]
    set_media_file_id(media_key, message.photo[-1].file_id)
    admin_state.pop(message.from_user.id, None)
    await message.answer(f"✅ Фото для <b>{MEDIA_TITLES.get(media_key, media_key)}</b> сохранено.")


@dp.message(F.text)
async def text_router(message: Message):
    user_id = message.from_user.id
    create_user_if_needed(message)
    user = get_user(user_id)
    text = (message.text or "").strip()

    # admin text flow
    if user_id == ADMIN_ID and user_id in admin_state:
        st = admin_state[user_id]
        mode = st.get("mode")

        if text.lower() in {"отмена", "cancel"}:
            admin_state.pop(user_id, None)
            await message.answer("Ок, отменено.")
            return

        if mode == "set_text":
            set_setting(st["key"], text)
            admin_state.pop(user_id, None)
            await message.answer("✅ Текст сохранён.")
            return

        if mode == "set_rate":
            if text.lower() in {"auto", "авто", "сброс"}:
                set_setting("usdt_rate_override", "")
                admin_state.pop(user_id, None)
                await message.answer("✅ Курс переведён в авто-режим.")
                return
            try:
                rate = float(text.replace(",", "."))
                if rate <= 0:
                    raise ValueError
            except ValueError:
                await message.answer("Введи число, например <code>98.50</code> или напиши <code>auto</code>.")
                return
            set_setting("usdt_rate_override", f"{rate:.2f}")
            admin_state.pop(user_id, None)
            await message.answer(f"✅ Курс сохранён: <b>{rate:.2f} ₽/USDT</b>")
            return

        if mode == "wallet_address":
            currency = st["currency"]
            cur.execute("UPDATE wallets SET address=? WHERE currency=?", (text, currency))
            conn.commit()
            admin_state.pop(user_id, None)
            await message.answer(f"✅ Адрес для <b>{currency}</b> сохранён.")
            return

        if mode == "wallet_network":
            currency = st["currency"]
            cur.execute("UPDATE wallets SET network=? WHERE currency=?", (text, currency))
            conn.commit()
            admin_state.pop(user_id, None)
            await message.answer(f"✅ Сеть для <b>{currency}</b> сохранена.")
            return

        if mode == "prod_add_name":
            st["name"] = text
            st["mode"] = "prod_add_desc"
            await message.answer("Теперь введи <b>описание</b> товара.")
            return

        if mode == "prod_add_desc":
            name = st["name"]
            cur.execute("INSERT INTO products (name, description, active, sort_order) VALUES (?, ?, 1, ?)", (name, text, 9999))
            pid = cur.lastrowid
            conn.commit()
            admin_state.pop(user_id, None)
            await message.answer(f"✅ Товар <b>{name}</b> создан. ID: <code>{pid}</code>.")
            return

        if mode == "prod_edit_name":
            pid = st["product_id"]
            cur.execute("UPDATE products SET name=? WHERE id=?", (text, pid))
            conn.commit()
            admin_state.pop(user_id, None)
            await message.answer("✅ Название товара обновлено.")
            return

        if mode == "prod_edit_desc":
            pid = st["product_id"]
            cur.execute("UPDATE products SET description=? WHERE id=?", (text, pid))
            conn.commit()
            admin_state.pop(user_id, None)
            await message.answer("✅ Описание товара обновлено.")
            return

        if mode == "weight_add_label":
            st["label"] = text
            st["mode"] = "weight_add_price"
            await message.answer("Теперь введи <b>цену</b> в рублях.")
            return

        if mode == "weight_add_price":
            try:
                price = float(text.replace(",", "."))
            except ValueError:
                await message.answer("Введи цену числом, например <code>390</code>.")
                return
            pid = st["product_id"]
            label = st["label"]
            cur.execute("INSERT INTO weights (product_id, label, price_rub, active, sort_order) VALUES (?, ?, ?, 1, ?)", (pid, label, price, 9999))
            conn.commit()
            admin_state.pop(user_id, None)
            await message.answer(f"✅ Вес <b>{label}</b> добавлен.")
            return

        if mode == "weight_edit_label":
            wid = st["weight_id"]
            cur.execute("UPDATE weights SET label=? WHERE id=?", (text, wid))
            conn.commit()
            admin_state.pop(user_id, None)
            await message.answer("✅ Название веса обновлено.")
            return

        if mode == "weight_edit_price":
            try:
                price = float(text.replace(",", "."))
            except ValueError:
                await message.answer("Введи цену числом, например <code>720</code>.")
                return
            wid = st["weight_id"]
            cur.execute("UPDATE weights SET price_rub=? WHERE id=?", (price, wid))
            conn.commit()
            admin_state.pop(user_id, None)
            await message.answer("✅ Цена веса обновлена.")
            return

        if mode == "promo_code":
            st["code"] = text.upper().strip()
            st["mode"] = "promo_kind"
            await message.answer("Введи тип: <code>discount</code>, <code>bonus_balance</code> или <code>gift</code>.")
            return

        if mode == "promo_kind":
            kind = text.lower().strip()
            if kind not in PROMO_ALLOWED_KINDS:
                await message.answer("Тип должен быть: <code>discount</code>, <code>bonus_balance</code> или <code>gift</code>.")
                return
            st["kind"] = kind
            st["mode"] = "promo_value"
            await message.answer("Введи значение числом. Для скидки — процент, для bonus_balance/gift — сумма в рублях.")
            return

        if mode == "promo_value":
            try:
                value = float(text.replace(",", "."))
            except ValueError:
                await message.answer("Введи число, например <code>10</code> или <code>500</code>.")
                return
            code = st["code"]
            kind = st["kind"]
            cur.execute("INSERT INTO promo_codes (code, kind, value, active, note) VALUES (?, ?, ?, 1, ?)", (code, kind, value, None))
            conn.commit()
            admin_state.pop(user_id, None)
            await message.answer(f"✅ Промокод <code>{code}</code> добавлен.")
            return

        if mode == "deposit_confirm":
            dep_id = st["deposit_id"]
            try:
                amount_rub = float(text.replace(",", "."))
            except ValueError:
                await message.answer("Введи сумму числом, например <code>1500</code>.")
                return
            cur.execute("SELECT * FROM deposits WHERE id=?", (dep_id,))
            dep = cur.fetchone()
            if not dep or dep["status"] != "pending":
                admin_state.pop(user_id, None)
                await message.answer("Заявка уже неактуальна.")
                return
            amount_usdt = round(amount_rub / format_rate(), 2)
            add_balance(dep["user_id"], amount_rub)
            cur.execute("UPDATE deposits SET amount_rub=?, amount_usdt=?, status='confirmed', confirmed_at=CURRENT_TIMESTAMP WHERE id=?", (amount_rub, amount_usdt, dep_id))
            conn.commit()
            admin_state.pop(user_id, None)
            try:
                await bot.send_message(dep["user_id"], f"✅ Пополнение #{dep_id} подтверждено.\n\nЗачислено: <b>{amount_rub:.2f} ₽</b> (~<b>{amount_usdt:.2f} USDT</b>)")
            except Exception:
                pass
            await message.answer(f"✅ Пополнение #{dep_id} подтверждено на сумму <b>{amount_rub:.2f} ₽</b>.")
            return

        await message.answer("Не понял админское действие.")
        return

    # user deposit amount
    if user_id in user_waiting_for_deposit_amount:
        if text.lower() in {"отмена", "cancel"}:
            user_waiting_for_deposit_amount.pop(user_id, None)
            await message.answer("Ок, пополнение отменено.")
            return
        try:
            amount_rub = float(text.replace(",", "."))
        except ValueError:
            await message.answer("Введи сумму в рублях числом, например: <code>1500</code>\nИли напиши <code>отмена</code>.")
            return
        currency = user_waiting_for_deposit_amount.pop(user_id)
        rate = format_rate()
        amount_usdt = round(amount_rub / rate, 2)
        address = get_wallet_address(currency)
        cur.execute("INSERT INTO deposits (user_id, currency, amount_rub, amount_usdt, address, status) VALUES (?, ?, ?, ?, ?, 'pending')", (user_id, currency, amount_rub, amount_usdt, address))
        dep_id = cur.lastrowid
        conn.commit()
        try:
            await bot.send_message(
                ADMIN_ID,
                f"<b>💰 Новая заявка на пополнение</b>\n\n"
                f"Заявка № <b>{dep_id}</b>\n"
                f"👤 User ID: <code>{user_id}</code>\n"
                f"💳 Валюта: <b>{currency}</b>\n"
                f"💰 Сумма: <b>{amount_rub:.2f} ₽</b> (~<b>{amount_usdt:.2f} USDT</b>)\n"
                f"🏦 Адрес: <code>{address}</code>",
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"dep:approve:{dep_id}")],
                        [InlineKeyboardButton(text="❌ Отклонить", callback_data=f"dep:decline:{dep_id}")],
                    ]
                ),
            )
        except Exception:
            pass
        await send_media_message(
            message,
            "deposit",
            f"<b>✅ Заявка на пополнение создана</b>\n\nВалюта: <b>{currency}</b>\nСумма: <b>{amount_rub:.2f} ₽</b> (~<b>{amount_usdt:.2f} USDT</b>)\nАдрес: <code>{address}</code>\n\nПереведи нужную сумму и подожди подтверждение администратора.",
            reply_markup=main_menu(),
        )
        return

    # captcha / registration
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
                await message.answer("Введи свой город, например: <code>Москва</code>")
            else:
                question, answer = generate_captcha()
                captcha_answers[user_id] = answer
                await message.answer(f"<b>Неверно.</b>\n{question} = ?")
        except ValueError:
            await message.answer("Введи ответ на капчу числом.")
        return

    if user_id in waiting_for_city:
        city = " ".join(text.split())
        if not is_valid_city(city):
            await message.answer("К сожалению, мы не работаем в вашем городе. Если хотите, можете изменить его прямо сейчас, написав его повторно.")
            return
        set_user_city(user_id, city)
        waiting_for_city.discard(user_id)
        waiting_for_promo.add(user_id)
        await send_media_message(
            message,
            "account",
            f"<b>🌍 Город сохранён:</b> <code>{city}</code>\n\nЕсли есть промокод — введи его сейчас.\nЕсли нет — отправь <code>пропустить</code>.",
            reply_markup=promo_skip_keyboard(),
        )
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
        await message.answer("<b>Промокод не найден.</b>\nПопробуй ещё раз или нажми «Пропустить».")
        return

    # user menu
    if text == "🛍 Каталог":
        if not user or not user["city"]:
            await message.answer("Сначала нужно указать город через регистрацию.")
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
        lines = [f'• <b>{item["product_name"]}</b> — {item["weight_label"]} — {item["price"]:.2f} ₽' for item in items]
        promo_line = f"\nПромокод: <code>{promo}</code>\nСумма со скидкой: <b>{discounted_total:.2f} ₽</b>" if promo else ""
        await message.answer("<b>🧺 Ваша корзина:</b>\n\n" + "\n".join(lines) + f"\n\n<b>Итого:</b> {total:.2f} ₽" + promo_line, reply_markup=cart_keyboard())
        return

    if text == "👤 Аккаунт":
        balance_rub = float(user["balance"] or 0)
        balance_usdt, rate = user_balance_usdt(balance_rub)
        await send_media_message(
            message,
            "account",
            render_text(get_setting("account_text"), city=user["city"] or "не выбран", balance_rub=balance_rub, balance_usdt=balance_usdt, rate=rate, promo=user["promo_code"] or "нет"),
            reply_markup=main_menu(),
        )
        return

    if text == "💳 Пополнение":
        await send_media_message(message, "deposit", get_setting("deposit_text"), reply_markup=deposit_keyboard())
        return

    if text == "💬 Поддержка":
        await send_media_message(message, "support", f"<b>💬 Поддержка</b>\n\n{get_support_text()}")
        return

    await message.answer("Используй кнопки меню ниже.", reply_markup=main_menu())


# =========================
# USER CALLBACKS
# =========================

@dp.callback_query(F.data == "noop")
async def noop(call: CallbackQuery):
    await call.answer()


@dp.callback_query(F.data.startswith("buyweight:"))
async def add_to_cart(call: CallbackQuery):
    user = get_user(call.from_user.id)
    if not user:
        await call.answer("Сначала /start", show_alert=True)
        return
    _, pid_str, wid_str = call.data.split(":")
    pid = int(pid_str)
    wid = int(wid_str)
    cur.execute(
        "SELECT p.id AS pid, p.name AS pname, w.label AS wlabel, w.price_rub AS price FROM products p JOIN weights w ON w.product_id=p.id WHERE p.id=? AND w.id=?",
        (pid, wid),
    )
    row = cur.fetchone()
    if not row:
        await call.answer("Не найдено", show_alert=True)
        return
    carts.setdefault(call.from_user.id, []).append(
        {"product_name": row["pname"], "weight_label": row["wlabel"], "price": float(row["price"]) }
    )
    await call.answer("Добавлено в корзину", show_alert=True)
    await call.message.answer(f"<b>✅ Добавлено в корзину</b>\n\n{row['pname']} — {row['wlabel']} — {float(row['price']):.2f} ₽", reply_markup=main_menu())


@dp.callback_query(F.data.startswith("product:"))
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


@dp.callback_query(F.data == "back:catalog")
async def back_catalog(call: CallbackQuery):
    user = get_user(call.from_user.id)
    if not user or not user["city"]:
        await call.answer("Сначала выбери город", show_alert=True)
        return
    await call.message.answer(get_setting("catalog_text"), reply_markup=catalog_keyboard())
    await call.answer()


@dp.callback_query(F.data == "cart:topup")
async def cart_topup(call: CallbackQuery):
    await call.message.answer(get_setting("deposit_text"), reply_markup=deposit_keyboard())
    await call.answer()


@dp.callback_query(F.data == "cart:clear")
async def cart_clear(call: CallbackQuery):
    carts[call.from_user.id] = []
    await call.message.answer("<b>🧹 Корзина очищена</b>")
    await call.answer()


@dp.callback_query(F.data == "cart:checkout")
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
        missing_usdt = round(missing_rub / format_rate(), 2)
        await call.message.answer(
            f"{get_setting('insufficient_balance_text')}\n\nНужно: <b>{final_total:.2f} ₽</b>\nБаланс: <b>{balance:.2f} ₽</b>\nНе хватает: <b>{missing_rub:.2f} ₽</b> (~<b>{missing_usdt:.2f} USDT</b>)",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="💳 Пополнить", callback_data="cart:topup")]]),
        )
        await call.answer()
        return
    subtract_balance(call.from_user.id, final_total)
    carts[call.from_user.id] = []
    await call.message.answer(f"{get_setting('order_success_text')}\n\nСписано: <b>{final_total:.2f} ₽</b>", reply_markup=main_menu())
    await call.answer("Заказ оформлен")


@dp.callback_query(F.data.startswith("deposit:"))
async def start_deposit(call: CallbackQuery):
    currency = call.data.split(":", 1)[1]
    user_waiting_for_deposit_amount[call.from_user.id] = currency
    await call.message.answer(
        f"<b>💳 Пополнение через {currency}</b>\n\nТеперь отправь сумму в рублях одним сообщением.\nНапример: <code>2500</code>\n\nПосле этого бот выдаст ваш личный адрес для пополнения.",
        reply_markup=main_menu(),
    )
    await call.answer()


@dp.callback_query(F.data == "promo:skip")
async def skip_promo(call: CallbackQuery):
    waiting_for_promo.discard(call.from_user.id)
    await call.answer()
    await call.message.answer("<b>Регистрация завершена ✅</b>\n\nМожешь пользоваться меню.", reply_markup=main_menu())


# =========================
# ADMIN CALLBACKS
# =========================

@dp.callback_query(F.data.startswith("setting:"))
async def setting_edit(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    key = call.data.split(":", 1)[1]
    admin_state[call.from_user.id] = {"mode": "set_text", "key": key}
    await call.message.answer(
        f"Введи новый текст для <b>{key}</b>.\n\nМожно использовать:\n<code>{{city}}</code> <code>{{balance_rub}}</code> <code>{{balance_usdt}}</code> <code>{{rate}}</code> <code>{{promo}}</code>"
    )
    await call.answer()


@dp.callback_query(F.data == "rate:edit")
async def rate_edit(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    admin_state[call.from_user.id] = {"mode": "set_rate"}
    await call.message.answer("Введи курс в рублях за 1 USDT. Или напиши <code>auto</code> для авто-режима.")
    await call.answer()


@dp.callback_query(F.data.startswith("media:"))
async def fixed_media_edit(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    media_key = call.data.split(":", 1)[1]
    admin_state[call.from_user.id] = {"mode": "set_media", "media_key": media_key}
    await call.message.answer(f"Отправь новое фото для <b>{MEDIA_TITLES.get(media_key, media_key)}</b>.")
    await call.answer()


@dp.callback_query(F.data == "admin:wallets")
async def admin_wallets_cb(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    await call.message.answer("<b>👛 Кошельки</b>", reply_markup=wallets_keyboard())
    await call.answer()


@dp.callback_query(F.data == "admin:promos")
async def admin_promos_cb(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    await call.message.answer("<b>🏷 Промокоды</b>", reply_markup=promo_keyboard())
    await call.answer()


@dp.callback_query(F.data == "admin:deposits")
async def admin_deposits_cb(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    cur.execute("SELECT * FROM deposits WHERE status='pending' ORDER BY id DESC")
    rows = cur.fetchall()
    if not rows:
        await call.message.answer("Нет заявок на пополнение.")
        await call.answer()
        return
    for d in rows:
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"dep:approve:{d['id']}")],
                [InlineKeyboardButton(text="❌ Отклонить", callback_data=f"dep:decline:{d['id']}")],
            ]
        )
        await call.message.answer(
            f"<b>Заявка #{d['id']}</b>\n\n👤 User ID: <code>{d['user_id']}</code>\n💳 Валюта: <b>{d['currency']}</b>\n💰 Сумма: <b>{float(d['amount_rub']):.2f} ₽</b> (~<b>{float(d['amount_usdt']):.2f} USDT</b>)\n🏦 Адрес: <code>{d['address']}</code>",
            reply_markup=kb,
        )
    await call.answer()


@dp.callback_query(F.data == "admin:users")
async def admin_users_cb(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    cur.execute("SELECT * FROM users ORDER BY user_id DESC LIMIT 30")
    rows = cur.fetchall()
    if not rows:
        await call.message.answer("Пользователей пока нет.")
        await call.answer()
        return
    lines = ["<b>Пользователи:</b>"]
    for u in rows:
        balance_rub = float(u["balance"] or 0)
        balance_usdt = round(balance_rub / format_rate(), 2)
        lines.append(f"• <code>{u['user_id']}</code> | {u['city'] or '—'} | {balance_rub:.2f} ₽ (~{balance_usdt:.2f} USDT)")
    await call.message.answer("\n".join(lines))
    await call.answer()


@dp.callback_query(F.data.startswith("dep:approve:"))
async def dep_approve(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    dep_id = int(call.data.split(":")[2])
    cur.execute("SELECT * FROM deposits WHERE id=?", (dep_id,))
    dep = cur.fetchone()
    if not dep:
        await call.answer("Не найдено", show_alert=True)
        return
    if dep["status"] != "pending":
        await call.answer("Уже обработано", show_alert=True)
        return
    admin_state[call.from_user.id] = {"mode": "deposit_confirm", "deposit_id": dep_id}
    await call.message.answer(f"Отправь сумму в рублях для заявки <b>#{dep_id}</b> (или <code>отмена</code>).")
    await call.answer()


@dp.callback_query(F.data.startswith("dep:decline:"))
async def dep_decline(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return
    dep_id = int(call.data.split(":")[2])
    cur.execute("SELECT * FROM deposits WHERE id=?", (dep_id,))
    dep = cur.fetchone()
    if not dep:
        await call.answer("Не найдено", show_alert=True)
        return
    if dep["status"] != "pending":
        await call.answer("Уже обработано", show_alert=True)
        return
    cur.execute("UPDATE deposits SET status='declined' WHERE id=?", (dep_id,))
    conn.commit()
    try:
        await bot.send_message(dep["user_id"], f"❌ Пополнение #{dep_id} отклонено администратором.")
    except Exception:
        pass
    await call.message.answer(f"❌ Пополнение #{dep_id} отклонено.")
    await call.answer()


# =========================
# TOP-LEVEL MEDIA HANDLER
# =========================

@dp.message(F.photo)
async def admin_photo_upload(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    st = admin_state.get(message.from_user.id)
    if not st or st.get("mode") != "set_media":
        return
    media_key = st["media_key"]
    set_media_file_id(media_key, message.photo[-1].file_id)
    admin_state.pop(message.from_user.id, None)
    await message.answer(f"✅ Фото для <b>{MEDIA_TITLES.get(media_key, media_key)}</b> сохранено.")


# =========================
# STARTUP
# =========================

async def main():
    logging.info("Бот запускается...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
