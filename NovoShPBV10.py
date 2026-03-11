import random
import asyncio
import aiosqlite
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.filters.state import StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.enums import ChatType
from aiogram.dispatcher.middlewares.base import BaseMiddleware
from typing import Callable, Awaitable, Dict, Any
from aiogram.types import ReplyKeyboardRemove
from PIL import Image, ImageDraw, ImageFont
import os
from aiogram.types import Message
from datetime import datetime, timedelta
from aiogram.types import InputFile, PhotoSize, Video, Animation, Sticker
import re
from aiogram.utils.keyboard import InlineKeyboardBuilder
from datetime import datetime, timedelta
from aiogram.utils.keyboard import ReplyKeyboardBuilder
import logging
import requests
from datetime import datetime, timedelta
from aiogram.types import CallbackQuery


# Настройка логирования
import logging
import sys


# Настройка базовой конфигурации
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)


# Создаем кастомный фильтр
class ExcludeUpdateFilter(logging.Filter):
    def filter(self, record):
        message = record.getMessage()
        # Исключаем сообщения о необработанных апдейтах
        if "is not handled" in message and "Duration" in message:
            return False
        return True


# Получаем корневой логгер и добавляем фильтр ко всем обработчикам
root_logger = logging.getLogger()
for handler in root_logger.handlers:
    handler.addFilter(ExcludeUpdateFilter())

last_used = {}  # словарь последнего вызова пользователем (см в показ паспорта)


# Функции для работы с БД
async def get_city_sequence_number(city_name: str, db: aiosqlite.Connection) -> int:
    """Получает уникальный порядковый номер нового жителя в городе"""
    # Получаем максимальный номер в городе
    cursor = await db.execute("""
        SELECT MAX(CAST(SUBSTR(account_id, 4, 6) AS INTEGER))
        FROM users 
        WHERE account_id LIKE ? AND LENGTH(account_id) >= 10
    """, (f"{city_name[:3].upper()}%",))

    result = await cursor.fetchone()
    max_number = result[0] if result[0] is not None else 0

    # Проверяем, нет ли коллизий
    while True:
        new_number = max_number + 1
        # Проверяем уникальность полного ГосID
        test_govid = await generate_specific_account_number(city_name, new_number, db)
        if await is_account_unique(test_govid, db):
            return new_number
        max_number += 1


async def generate_short_govid_v2(city_name: str, user_id: int) -> str:
    """Сверхкороткий ГосID (6 символов)"""

    # Первая буква города
    city_char = city_name[:3].upper() if city_name else "X"

    # Последние 5 цифр user_id
    user_part = str(user_id)[-4:]  # Берём последние 5 цифр

    # Формируем ГосID: Буква + 4 цифр
    return f"{city_char}{user_part.zfill(5)}"


async def generate_specific_account_number(city_name: str, sequence_number: int, db: aiosqlite.Connection) -> str:
    """Генерирует конкретный ГосID на основе номера"""
    # Получаем код города используя функцию get_city_code
    city_code = await get_city_code(city_name)

    # Форматируем номер жителя (6 цифр с ведущими нулями)
    resident_number = f"{sequence_number:02d}"

    # Формируем базовый номер
    base_number = f"{city_code}{resident_number}"

    # Добавляем контрольную сумму
    checksum = calculate_checksum(base_number)

    return f"{base_number}{checksum}"


async def get_city_code(city_name: str) -> str:
    """Генерирует уникальный код для города на основе его названия"""
    if not city_name:
        return "XXX"

    # Убираем лишние пробелы
    city_name = city_name.strip()

    if ' ' in city_name:
        # Для городов из нескольких слов - первые буквы каждого слова
        words = [word for word in city_name.split() if word]
        city_code = ''.join([word[0].upper() for word in words])

        # Если букв меньше 3, дополняем до 3 символов
        if len(city_code) < 3:
            city_code = city_code.ljust(3, 'X')
        elif len(city_code) > 3:
            city_code = city_code[:3]  # Берем первые 3 буквы
    else:
        # Для городов из одного слова - первые 3 буквы
        city_code = city_name[:3].upper()

    # Дополняем до 3 символов если нужно (на случай, если город из 1-2 букв)
    if len(city_code) < 3:
        city_code = city_code.ljust(3, 'X')

    return city_code


async def generate_account_number(city_name: str, db: aiosqlite.Connection) -> str:
    """Генерирует уникальный номер счета на основе города и порядкового номера"""
    # Получаем уникальный номер
    sequence_number = await get_city_sequence_number(city_name, db)

    # Генерируем код города
    if ' ' in city_name:
        # Если название города состоит из нескольких слов
        words = [word for word in city_name.split() if word]
        city_code = ''.join([word[0].upper() for word in words])
        if len(city_code) < 3:
            city_code = city_code.ljust(3, 'X')
        elif len(city_code) > 3:
            city_code = city_code[:3]
    else:
        # Если название города состоит из одного слова
        city_code = city_name[:3].upper()

    if len(city_code) < 3:
        city_code = city_code.ljust(3, 'X')

    # Форматируем номер жителя (6 цифр с ведущими нулями)
    resident_number = f"{sequence_number:06d}"

    # Формируем базовый номер
    base_number = f"{city_code}{resident_number}"

    # Добавляем контрольную сумму
    checksum = calculate_checksum(base_number)

    return f"{base_number}{checksum}"


async def get_city_sequence_number(city_name: str, db: aiosqlite.Connection) -> int:
    """Получает уникальный порядковый номер нового жителя в городе"""
    # Получаем максимальный номер в городе
    cursor = await db.execute("""
        SELECT MAX(CAST(SUBSTR(account_id, 4, 6) AS INTEGER))
        FROM users 
        WHERE city = ? AND LENGTH(account_id) >= 10
    """, (city_name,))

    result = await cursor.fetchone()
    max_number = result[0] if result[0] is not None else 0

    # Проверяем, нет ли коллизий
    while True:
        new_number = max_number + 1

        # Генерируем код города для проверки
        if ' ' in city_name:
            words = [word for word in city_name.split() if word]
            city_code = ''.join([word[0].upper() for word in words])
            if len(city_code) < 3:
                city_code = city_code.ljust(3, 'X')
            elif len(city_code) > 3:
                city_code = city_code[:3]
        else:
            city_code = city_name[:3].upper()

        if len(city_code) < 3:
            city_code = city_code.ljust(3, 'X')

        # Форматируем номер жителя
        resident_number = f"{new_number:06d}"

        # Формируем базовый номер
        base_number = f"{city_code}{resident_number}"

        # Добавляем контрольную сумму
        checksum = calculate_checksum(base_number)
        test_govid = f"{base_number}{checksum}"

        # Проверяем уникальность
        if await is_account_unique(test_govid, db):
            return new_number

        max_number += 1


async def generate_unique_govid(city_name: str, user_id: int, db: aiosqlite.Connection) -> str:
    """Генерирует уникальный ГосID на основе user_id"""

    # Получаем код города (2 символа)
    def get_city_code(city):
        if len(city) >= 2:
            return city[:2].upper()
        elif len(city) == 1:
            return city.upper() + "X"
        else:
            return "XX"

    city_code = get_city_code(city_name)

    # Используем последние 4 цифры user_id
    user_suffix = str(user_id)[-4:]

    # Проверяем уникальность
    base_govid = f"{city_code}{user_suffix}"

    # Если не уникален, добавляем префикс
    if not await is_account_unique(base_govid, db):
        # Пробуем разные варианты
        for attempt in range(1, 100):
            new_govid = f"{city_code}{user_suffix[:3]}{attempt}"
            if await is_account_unique(new_govid, db):
                return new_govid

    return base_govid


def calculate_checksum(number: str) -> str:
    """Вычисляет контрольную сумму для номера"""
    # Простой алгоритм контрольной суммы
    total = 0
    for char in number:
        if char.isdigit():
            total += int(char)
        else:
            # Для букв используем их порядковый номер в алфавите
            total += ord(char.upper()) - ord('A') + 1

    checksum = total % 10  # Последняя цифра суммы
    return str(checksum)


async def is_account_unique(account_number: str, db: aiosqlite.Connection) -> bool:
    """Проверяет уникальность номера счета"""
    cursor = await db.execute(
        "SELECT 1 FROM users WHERE account_id = ?",
        (account_number,)
    )
    return await cursor.fetchone() is None


# Настройки
BOT_TOKEN = '7666509245:AAGnAh0ep0XRkD6_jwcMzElRCJRB9YzSBmY'
AdminID = 6313754974  # Замените на ваш ID администратора
ADMIN_ID = [6313754974]
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Глобальные настройки налогов
TAX_ACCOUNT = "915020684862"
LAST_TAX_COLLECTION = None
TAX_THRESHOLD = 250
TAX_RATE = 0.01


# Middleware-фильтр, разрешающий команды только в ЛС (кроме /deportation)
class PrivateCommandMiddleware(BaseMiddleware):
    async def __call__(
            self,
            handler: Callable[[types.Message, Dict[str, Any]], Awaitable[Any]],
            event: types.Message,
            data: Dict[str, Any]
    ) -> Any:
        if event.text and (event.text.startswith("/deportation") or
                           event.text.startswith("/clear_buttons") or
                           event.text.startswith("/purchase_stats") or
                           event.text.startswith("/cities") or
                           event.text.startswith("/city_info") or
                           event.text.startswith("/attractions") or
                           event.text.startswith("/help_shpb") or
                           event.text.startswith("/perevod") or
                           event.text.startswith("/citata") or
                           event.text.startswith("/set_inoagent") or
                           event.text.startswith("/inoagents_list") or
                           event.text.startswith("/check_inoagent") or
                           event.text.startswith("/verified_cities") or
                           event.text.startswith("/add_citata") or
                           event.text.startswith("/say") or
                           event.text.startswith("/id") or
                           event.text.startswith("/mp")):
            return await handler(event, data)
        if event.chat.type != ChatType.PRIVATE and event.text and event.text.startswith("/"):
            return  # игнорируем команду вне ЛС
        return await handler(event, data)


@dp.message(Command("clear_buttons"))
async def clear_buttons(message: types.Message):
    """Убирает все кнопки и показывает сообщение без клавиатуры"""
    await message.answer(
        "✅ Кнопки убраны\n"
        "Для возврата в меню используйте",
        reply_markup=ReplyKeyboardRemove()
    )


# Регистрация middleware
dp.message.middleware(PrivateCommandMiddleware())


# FSM классы
#корпорации
# Состояния для создания корпорации
class CorporationStates(StatesGroup):
    waiting_for_name = State()
    waiting_for_description = State()

# Состояния для вступления в корпорацию
class CorporationJoinStates(StatesGroup):
    waiting_for_corp_id = State()
    waiting_for_join_message = State()

# Состояния для управления заявками
class ApplicationManagementStates(StatesGroup):
    viewing_applications = State()

# Состояния для управления ролями
class RoleManagementStates(StatesGroup):
    choosing_action = State()
    waiting_for_role_name = State()
    waiting_for_user_to_assign = State()
    waiting_for_role_to_assign = State()
    waiting_for_user_to_change_role = State()
    waiting_for_new_role = State()


# Добавьте состояния для FSM
class Marketplace(StatesGroup):
    choose_action = State()
    add_title = State()
    add_description = State()
    add_price = State()
    add_quantity = State()
    add_category = State()
    add_image = State()
    browse_category = State()
    view_item = State()
    my_items = State()
    manage_item = State()
    confirm_delete = State()
    edit_price = State()
    edit_title = State()
    edit_description = State()
    edit_image = State()
    confirm_purchase = State()
    enter_purchase_quantity = State()
    confirm_purchase_final = State()
    confirm_completion_buyer = State()
    wait_buyer_confirmation = State()
    confirm_completion_seller = State()
    choose_category_for_add = State()
    add_first_item = State()
    offer_relocation = State()
    add_property_address = State()
    enter_address_city = State()
    enter_address_street = State()
    enter_address_house = State()
    view_new_items = State()
    choose_new_items_period = State()


# отправка сообщений в чат
class CrossChatMessage(StatesGroup):
    waiting_for_chat_selection = State()
    waiting_for_thread_selection = State()
    waiting_for_message_content = State()
    confirm_message = State()
    waiting_for_chat_input = State()


class CrossChatSettings(StatesGroup):
    managing_saved_chats = State()
    adding_new_chat = State()
    editing_chat_name = State()
    deleting_chat = State()


class MarriageStates(StatesGroup):
    enter_spouse_account = State()
    confirm_marriage_request = State()
    waiting_marriage_response = State()


# Состояния для развода
class DivorceStates(StatesGroup):
    confirm_divorce = State()


class Settings(StatesGroup):
    waiting_for_new_name = State()


class Broadcast(StatesGroup):
    waiting_for_message = State()
    confirm = State()


class SetBalance(StatesGroup):
    waiting_for_user_id = State()
    waiting_for_balance = State()


class Register(StatesGroup):
    name = State()  # Для ввода имени
    gender = State()  # Для выбора пола
    city = State()  # Для выбора города
    new_city = State()  # Для ввода нового города
    waiting_approval = State()  # Для ожидания подтверждения


class MayorManagement(StatesGroup):
    waiting_for_new_mayor = State()
    confirm_city_deletion = State()
    waiting_for_coordinates = State()


class StreetManagement(StatesGroup):
    managing_streets = State()
    add_street_name = State()
    remove_street_select = State()
    confirm_street_deletion = State()


class HouseManagement(StatesGroup):
    managing_houses = State()
    select_street = State()
    add_house_number = State()
    remove_house_select = State()
    confirm_house_deletion = State()
    view_houses = State()


class Transfer(StatesGroup):
    enter_recipient = State()
    enter_amount = State()
    confirm = State()


class Divorce(StatesGroup):
    confirm = State()


class Appointment(StatesGroup):
    choose_doctor = State()
    choose_time = State()


class Statement(StatesGroup):
    enter_text = State()


# состояние для мэрского меню
class MayorMenu(StatesGroup):
    managing_city = State()
    rename_city = State()
    managing_attractions = State()
    add_attraction_name = State()
    add_attraction_description = State()
    add_attraction_type = State()
    confirm_attraction_deletion = State()
    broadcast_to_citizens = State()
    confirm_broadcast = State()


class ChangeAddress(StatesGroup):
    choose_city = State()  # Для выбора города
    choose_street = State()  # Для выбора улицы
    choose_house = State()  # Для выбора дома
    waiting_approval = State()  # Для ожидания подтверждения
    enter_custom_city = State()  # Для ввода нового города
    create_new_street = State()  # Для создания новой улицы


class PropertyManagement(StatesGroup):
    viewing_properties = State()
    view_property_details = State()
    manage_property = State()
    confirm_property_sale = State()
    edit_property_address = State()


# кнопка в главное меню
@dp.message(F.text == "⬅️ В главное меню")
async def back_to_main_from_marketplace(message: types.Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state:
        await state.clear()
    await show_main_menu(message, message.from_user.id)


# Инициализация БД
async def init_db():
    async with aiosqlite.connect("database.db") as db:
        # Включаем поддержку внешних ключей
        await db.execute("PRAGMA foreign_keys = ON")
        tables = [
            "CREATE TABLE IF NOT EXISTS foreign_agents (user_id INTEGER PRIMARY KEY, agent_name TEXT NOT NULL,expires_at TEXT NOT NULL, created_at TEXT DEFAULT CURRENT_TIMESTAMP)",

            "CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, name TEXT NOT NULL, username TEXT, balance INTEGER DEFAULT 0, account_id TEXT UNIQUE, gender TEXT, city TEXT, spouse_id TEXT, street TEXT, house_number TEXT, created_date DATETIME DEFAULT CURRENT_TIMESTAMP, marriage_id TEXT, marriage_date TEXT, otp TEXT, otp_expires DATETIME, last_login DATETIME, is_online BOOLEAN DEFAULT 0)",

            "CREATE TABLE IF NOT EXISTS marketplace_items (id INTEGER PRIMARY KEY AUTOINCREMENT, seller_id INTEGER NOT NULL, title TEXT NOT NULL, description TEXT, price INTEGER NOT NULL, quantity INTEGER DEFAULT 1, category TEXT NOT NULL, status TEXT DEFAULT 'active', created_date DATETIME DEFAULT CURRENT_TIMESTAMP, image_id TEXT, FOREIGN KEY (seller_id) REFERENCES users(user_id))",

            "CREATE TABLE IF NOT EXISTS marketplace_transactions (id INTEGER PRIMARY KEY AUTOINCREMENT, item_id INTEGER NOT NULL, buyer_id INTEGER NOT NULL, seller_id INTEGER NOT NULL, price REAL NOT NULL, quantity INTEGER NOT NULL DEFAULT 1, status TEXT NOT NULL DEFAULT 'pending_confirmation', buyer_confirmed INTEGER NOT NULL DEFAULT 0, seller_confirmed INTEGER NOT NULL DEFAULT 0, confirmation_expires_at DATETIME, confirmed_at DATETIME, cancelled_at DATETIME, cancellation_reason TEXT, created_at DATETIME DEFAULT CURRENT_TIMESTAMP, completed_at DATETIME, FOREIGN KEY (item_id) REFERENCES marketplace_items (id), FOREIGN KEY (buyer_id) REFERENCES users (user_id), FOREIGN KEY (seller_id) REFERENCES users (user_id))",

            "CREATE TABLE IF NOT EXISTS savings_accounts (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, balance INTEGER DEFAULT 0, created_date DATETIME DEFAULT CURRENT_TIMESTAMP, last_interest_date DATETIME DEFAULT CURRENT_TIMESTAMP, FOREIGN KEY (user_id) REFERENCES users(user_id))",

            "CREATE TABLE IF NOT EXISTS tax_collections (id INTEGER PRIMARY KEY AUTOINCREMENT, collection_date DATETIME DEFAULT CURRENT_TIMESTAMP, total_collected INTEGER DEFAULT 0, user_count INTEGER DEFAULT 0, exempt_count INTEGER DEFAULT 0)",

            "CREATE TABLE IF NOT EXISTS temp_registrations (user_id INTEGER PRIMARY KEY, name TEXT NOT NULL, gender TEXT NOT NULL, city TEXT NOT NULL, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)",

            "CREATE TABLE IF NOT EXISTS transfers (id INTEGER PRIMARY KEY AUTOINCREMENT, from_user TEXT NOT NULL, to_user TEXT NOT NULL, amount INTEGER NOT NULL, commission INTEGER DEFAULT 0, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)",

            "CREATE TABLE IF NOT EXISTS statements (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, text TEXT NOT NULL, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)",

            "CREATE TABLE IF NOT EXISTS appointments (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, doctor TEXT NOT NULL, time TEXT NOT NULL, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)",

            "CREATE TABLE IF NOT EXISTS broadcasts (id INTEGER PRIMARY KEY AUTOINCREMENT, admin_id INTEGER NOT NULL, message TEXT NOT NULL, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP, success_count INTEGER DEFAULT 0, fail_count INTEGER DEFAULT 0)",

            "CREATE TABLE IF NOT EXISTS cities (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE NOT NULL, mayor_id INTEGER, population INTEGER DEFAULT 0, coord_x INTEGER, coord_z INTEGER, created_date DATETIME DEFAULT CURRENT_TIMESTAMP, FOREIGN KEY (mayor_id) REFERENCES users(user_id))",

            "CREATE TABLE IF NOT EXISTS attractions (id INTEGER PRIMARY KEY AUTOINCREMENT, city_name TEXT NOT NULL, name TEXT NOT NULL, description TEXT, type TEXT NOT NULL, created_by INTEGER NOT NULL, created_date DATETIME DEFAULT CURRENT_TIMESTAMP, FOREIGN KEY (created_by) REFERENCES users(user_id), FOREIGN KEY (city_name) REFERENCES cities(name) ON DELETE CASCADE, UNIQUE(city_name, name))",

            "CREATE TABLE IF NOT EXISTS streets (id INTEGER PRIMARY KEY AUTOINCREMENT, city_name TEXT NOT NULL, street_name TEXT NOT NULL, created_by INTEGER NOT NULL, created_date DATETIME DEFAULT CURRENT_TIMESTAMP, FOREIGN KEY (created_by) REFERENCES users(user_id))",

            "CREATE TABLE IF NOT EXISTS houses (id INTEGER PRIMARY KEY AUTOINCREMENT, city_name TEXT NOT NULL, street_name TEXT NOT NULL, house_number TEXT NOT NULL, created_by INTEGER NOT NULL, created_date DATETIME DEFAULT CURRENT_TIMESTAMP, FOREIGN KEY (created_by) REFERENCES users(user_id), UNIQUE(city_name, street_name, house_number))",

            "CREATE TABLE IF NOT EXISTS registration_requests (request_id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, city TEXT NOT NULL, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP, status TEXT DEFAULT 'pending', FOREIGN KEY (user_id) REFERENCES users(user_id))",

            "CREATE TABLE IF NOT EXISTS mayor_broadcasts (id INTEGER PRIMARY KEY AUTOINCREMENT, mayor_id INTEGER NOT NULL, city_name TEXT NOT NULL, message_type TEXT NOT NULL, message TEXT, sent_count INTEGER DEFAULT 0, failed_count INTEGER DEFAULT 0, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP, FOREIGN KEY (mayor_id) REFERENCES users(user_id))",
            "CREATE TABLE IF NOT EXISTS city_change_requests (id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,old_city TEXT NOT NULL,new_city TEXT NOT NULL,street TEXT NOT NULL,house_number TEXT NOT NULL,status TEXT DEFAULT 'pending',timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,FOREIGN KEY (user_id) REFERENCES users(user_id))",
            "CREATE TABLE IF NOT EXISTS user_saved_chats (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, chat_id INTEGER NOT NULL, chat_title TEXT NOT NULL, custom_name TEXT, is_group INTEGER DEFAULT 1, created_date DATETIME DEFAULT CURRENT_TIMESTAMP, UNIQUE(user_id, chat_id))",
            "CREATE TABLE IF NOT EXISTS gov_orders (id INTEGER PRIMARY KEY AUTOINCREMENT, admin_id INTEGER NOT NULL, chat_id TEXT NOT NULL, chat_title TEXT, message_type TEXT CHECK(message_type IN ('text', 'photo', 'video', 'document')) DEFAULT 'text', message_text TEXT, media_file_id TEXT, required_reactions INTEGER DEFAULT 0, reward INTEGER NOT NULL, max_executors INTEGER DEFAULT 1, current_executors INTEGER DEFAULT 0, status TEXT CHECK(status IN ('active', 'completed', 'cancelled')) DEFAULT 'active', created_at DATETIME DEFAULT CURRENT_TIMESTAMP, expires_at DATETIME)",
            "CREATE TABLE IF NOT EXISTS gov_order_executions (id INTEGER PRIMARY KEY AUTOINCREMENT, order_id INTEGER NOT NULL, user_id INTEGER NOT NULL, message_id TEXT, proof_photo_id TEXT, status TEXT CHECK(status IN ('pending', 'approved', 'rejected')) DEFAULT 'pending', executed_at DATETIME DEFAULT CURRENT_TIMESTAMP, reviewed_at DATETIME, reviewed_by INTEGER, FOREIGN KEY (order_id) REFERENCES gov_orders(id) ON DELETE CASCADE)",
            "CREATE TABLE IF NOT EXISTS allowed_chats (id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id TEXT UNIQUE NOT NULL, chat_title TEXT, added_by INTEGER, added_at DATETIME DEFAULT CURRENT_TIMESTAMP)",
            "CREATE TABLE IF NOT EXISTS corporations(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT UNIQUE NOT NULL,description TEXT,owner_id INTEGER NOT NULL,created_at DATETIME DEFAULT CURRENT_TIMESTAMP,logo_file_id TEXT, balance INTEGER DEFAULT 0, FOREIGN KEY(owner_id) REFERENCES users(user_id))",
            "CREATE TABLE IF NOT EXISTS corporation_roles(id INTEGER PRIMARY KEY AUTOINCREMENT, corporation_id INTEGER NOT NULL, name TEXT NOT NULL, permissions FSMContext DEFAULT 'basic', created_at DATETIME DEFAULT CURRENT_TIMESTAMP, UNIQUE(corporation_id, name), FOREIGN KEY(corporation_id) REFERENCES corporations(id) ON DELETE CASCADE)",
            "CREATE TABLE IF NOT EXISTS corporation_members(id INTEGER PRIMARY KEY AUTOINCREMENT, corporation_id INTEGER NOT NULL, user_id INTEGER NOT NULL, role_id INTEGER NOT NULL, joined_at DATETIME DEFAULT CURRENT_TIMESTAMP, UNIQUE(corporation_id, user_id), FOREIGN KEY(corporation_id) REFERENCES corporations(id) ON DELETE CASCADE, FOREIGN KEY(user_id) REFERENCES users(user_id) ON DELETE CASCADE, FOREIGN KEY(role_id) REFERENCES corporation_roles(id) ON DELETE CASCADE)",
            "CREATE TABLE IF NOT EXISTS corporation_applications(id INTEGER PRIMARY KEY AUTOINCREMENT, corporation_id INTEGER NOT NULL, user_id INTEGER NOT NULL, message TEXT, status TEXT DEFAULT 'pending', created_at DATETIME DEFAULT CURRENT_TIMESTAMP, reviewed_at DATETIME,reviewed_by INTEGER,UNIQUE(corporation_id, user_id),FOREIGN KEY(corporation_id) REFERENCES corporations(id) ON DELETE CASCADE, FOREIGN KEY(user_id) REFERENCES users(user_id) ON DELETE CASCADE,FOREIGN KEY(reviewed_by) REFERENCES users(user_id))"
                    ]

        # ===== МИГРАЦИЯ: добавляем role_id в corporation_members =====
        # Проверяем наличие столбца role_id
        cursor = await db.execute("PRAGMA table_info(corporation_members)")
        columns = await cursor.fetchall()
        column_names = [col[1] for col in columns]

        if 'role_id' not in column_names:
            print("Обнаружена устаревшая структура таблицы corporation_members. Запуск миграции...")

            # 1. Добавляем столбец role_id (может быть NULL)
            await db.execute("ALTER TABLE corporation_members ADD COLUMN role_id INTEGER")

            # 2. Убеждаемся, что таблица corporation_roles существует
            await db.execute("""
                CREATE TABLE IF NOT EXISTS corporation_roles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    corporation_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    permissions TEXT DEFAULT 'basic',
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(corporation_id, name),
                    FOREIGN KEY (corporation_id) REFERENCES corporations(id) ON DELETE CASCADE
                )
            """)

            # 3. Для каждой корпорации создаём стандартные роли (если их ещё нет)
            cursor = await db.execute("SELECT id, owner_id FROM corporations")
            corps = await cursor.fetchall()
            for corp_id, owner_id in corps:
                # Создаём роли Владелец, Администратор, Участник
                for role_name in ["Владелец", "Администратор", "Участник"]:
                    await db.execute(
                        "INSERT OR IGNORE INTO corporation_roles (corporation_id, name) VALUES (?, ?)",
                        (corp_id, role_name)
                    )

                # Получаем ID роли "Владелец"
                cursor_role = await db.execute(
                    "SELECT id FROM corporation_roles WHERE corporation_id = ? AND name = 'Владелец'",
                    (corp_id,)
                )
                owner_role = await cursor_role.fetchone()
                if owner_role:
                    owner_role_id = owner_role[0]
                    # Назначаем владельцу роль "Владелец"
                    await db.execute("""
                        UPDATE corporation_members
                        SET role_id = ?
                        WHERE corporation_id = ? AND user_id = ?
                    """, (owner_role_id, corp_id, owner_id))

            # 4. Для всех остальных участников (если они есть) назначаем роль "Участник"
            cursor = await db.execute("SELECT id FROM corporations")
            all_corps = await cursor.fetchall()
            for corp_id, in all_corps:
                cursor_role = await db.execute(
                    "SELECT id FROM corporation_roles WHERE corporation_id = ? AND name = 'Участник'",
                    (corp_id,)
                )
                member_role = await cursor_role.fetchone()
                if member_role:
                    member_role_id = member_role[0]
                    await db.execute("""
                        UPDATE corporation_members
                        SET role_id = ?
                        WHERE corporation_id = ? AND role_id IS NULL
                    """, (member_role_id, corp_id))

            await db.commit()
            print("Миграция завершена. Столбец role_id добавлен и заполнен.")

        # Выполняем все запросы создания таблиц
        for table_query in tables:
            try:
                await db.execute(table_query)
            except Exception as e:
                print(f"Ошибка при создании таблицы: {e}")
                # Пробуем создать таблицу без IF NOT EXISTS, если есть проблемы
                if "IF NOT EXISTS" not in table_query:
                    try:
                        new_query = table_query.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS")
                        await db.execute(new_query)
                    except Exception as e2:
                        print(f"Ошибка при создании таблицы с IF NOT EXISTS: {e2}")

        await db.commit()
        print("База данных успешно инициализирована")


# регистрация городов
async def complete_registration(message: types.Message, state: FSMContext):
    """Завершает регистрацию пользователя"""
    user_id = message.from_user.id
    data = await state.get_data()

    async with aiosqlite.connect("database.db") as db:
        try:
            # Генерируем номер счета на основе города
            account_number = await generate_account_number(data['city'], db)

            # Сохраняем пользователя
            await db.execute(
                """
                INSERT INTO users 
                (user_id, account_id, name, gender, city, balance) 
                VALUES (?, ?, ?, ?, ?, 1)
                """,
                (user_id, account_number, data['name'], data['gender'], data['city'])
            )
            await db.commit()

            # Получаем номер в городе
            city_sequence = await get_city_sequence_number(data['city'], db) - 1

            # Формируем сообщение с кодом города
            city_code = await get_city_code(data['city'])

            await message.answer(
                f"✅ Регистрация завершена!\n\n"
                f"🏙 Город: {data['city']}\n"
                f"🎫 Код города: {city_code}\n"
                f"🎫 ГосID: {account_number}\n"
                f"👤 Номер в городе: {city_sequence}\n"
                f"💳 Баланс: 1 шуек",
                reply_markup=await main_menu_kb(user_id)
            )

        except Exception as e:
            await message.answer("❌ Ошибка при регистрации. Попробуйте позже.")
            print(f"Ошибка регистрации: {e}")
        finally:
            await state.clear()


@dp.message(PropertyManagement.viewing_properties, F.text == "⬅️ Назад в настройки")
async def back_to_settings_from_properties(message: types.Message, state: FSMContext):
    """Возврат в меню настроек"""
    await state.clear()
    await message.answer("Возврат в настройки.", reply_markup=await settings_menu_kb(message.from_user.id))


# Клавиатуры

def get_back_to_admin_kb():
    """Клавиатура для возврата в админ-панель"""
    builder = InlineKeyboardBuilder()

    builder.button(text="🔙 Назад", callback_data="admin_back")

    builder.adjust(1)
    return builder.as_markup()


def get_back_to_user_kb():
    """Клавиатура для возврата в пользовательскую панель"""
    builder = InlineKeyboardBuilder()

    builder.button(text="🔙 Назад", callback_data="user_back")

    builder.adjust(1)
    return builder.as_markup()


# 11. Обновим хэндлеры для использования новых клавиатур

# Клавиатура категорий
def categories_kb():
    keyboard = []
    row = []
    for i, category in enumerate(MARKETPLACE_CATEGORIES, 1):
        row.append(KeyboardButton(text=category))
        if i % 2 == 0:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([KeyboardButton(text="❌ Отмена")])
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)


def new_items_period_kb():
    """Клавиатура для выбора периода новых товаров"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🕐 Сегодня"), KeyboardButton(text="📅 За неделю")],
            [KeyboardButton(text="🗓️ За месяц"), KeyboardButton(text="🎯 Все новые")],
            [KeyboardButton(text="⬅️ Назад в маркетплейс")]
        ],
        resize_keyboard=True
    )

#клавиатура корпораций
def corporations_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Создать корпорацию")],
            [KeyboardButton(text="Мои корпорации")],
            [KeyboardButton(text="Вступить в корпорацию")],
            [KeyboardButton(text="Управление заявками")],
            [KeyboardButton(text="Управление ролями")],
            [KeyboardButton(text="⬅️ Назад")]
        ],
        resize_keyboard=True
    )
@dp.message(F.text == "Корпорации")
async def open_corporation(message: types.Message):
    await message.answer(
        "Меню корпораций:",
        reply_markup=corporations_kb()
    )



# клавиатура мэра
async def mayor_menu_kb(city_name: str) -> ReplyKeyboardMarkup:
    buttons = [
        [KeyboardButton(text=f"👥 Жители города {city_name}")],
        [KeyboardButton(text="📢 Сделать рассылку")],
        [KeyboardButton(text=f"🏛️ Управление достопримечательностями")],
        [KeyboardButton(text=f"📍 Указать координаты")],
        [KeyboardButton(text=f"🏘️ Управление улицами")],
        [KeyboardButton(text=f"✏️ Переименовать город")],
        [KeyboardButton(text="👑 Передать город")],
        [KeyboardButton(text="🗑️ Удалить город")],
        [KeyboardButton(text="↩ Назад")]
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


# Клавиатура для мэра города. Улицы
async def streets_menu_kb(city_name: str) -> ReplyKeyboardMarkup:
    buttons = [
        [KeyboardButton(text=f"📋 Список улиц {city_name}")],
        [KeyboardButton(text="➕ Добавить улицу")],
        [KeyboardButton(text="🗑️ Удалить улицу")],
        [KeyboardButton(text="↩ Назад к управлению городом")]
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


async def houses_menu_kb(city_name: str, street_name: str) -> ReplyKeyboardMarkup:
    buttons = [
        [KeyboardButton(text=f"🏠 Дома на {street_name}")],
        [KeyboardButton(text="➕ Добавить дом")],
        [KeyboardButton(text="🗑️ Удалить дом")],
        [KeyboardButton(text="↩ Назад к улицам")]
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


async def streets_with_houses_kb(city_name: str) -> ReplyKeyboardMarkup:
    buttons = [
        [KeyboardButton(text="🏠 Управление домами")],
        [KeyboardButton(text="📋 Список улиц")],
        [KeyboardButton(text="➕ Добавить улицу")],
        [KeyboardButton(text="🗑️ Удалить улицу")],
        [KeyboardButton(text="↩ Назад к управлению городом")]
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


# клавиатура мэра для управления улицами
async def streets_menu_kb(city_name: str) -> ReplyKeyboardMarkup:
    buttons = [
        [KeyboardButton(text="🏠 Управление домами")],
        [KeyboardButton(text="📋 Список улиц")],
        [KeyboardButton(text="➕ Добавить улицу")],
        [KeyboardButton(text="🗑️ Удалить улицу")],
        [KeyboardButton(text="↩ Назад к управлению городом")]
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


# клавиатура мэра для достопримечательностей
async def attractions_menu_kb(city_name: str) -> ReplyKeyboardMarkup:
    buttons = [
        [KeyboardButton(text=f"🏛️ Список достопримечательностей {city_name}")],
        [KeyboardButton(text="➕ Добавить достопримечательность")],
        [KeyboardButton(text="🗑️ Удалить достопримечательность")],
        [KeyboardButton(text="↩ Назад к управлению городом")]
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


# Клавиатура для выбора типа достопримечательности
def attraction_types_kb():
    keyboard = []
    row = []
    for i, attraction_type in enumerate(ATTRACTION_TYPES, 1):
        row.append(KeyboardButton(text=attraction_type))
        if i % 2 == 0:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([KeyboardButton(text="❌ Отмена")])
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)


# Клавиатура для выбора пола
gender_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Мужской"), KeyboardButton(text="Женский")]
    ],
    resize_keyboard=True
)


# главное меню
async def main_menu_kb(user_id: int = None) -> ReplyKeyboardMarkup:
    """Главное меню"""
    buttons = [
        [KeyboardButton(text="💰 Новый перевод"), KeyboardButton(text="Корпорации")],
        [KeyboardButton(text="🛒 Маркетплейс")],
        [KeyboardButton(text="📝 Написать заявление"), KeyboardButton(text="🧑‍⚕ Записаться ко врачу")],
        [KeyboardButton(text="📂 Истории"), KeyboardButton(text="⚙️ Настройки")],
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


async def settings_menu_kb(user_id: int) -> ReplyKeyboardMarkup:
    """Создает клавиатуру меню настроек"""
    async with aiosqlite.connect("database.db") as db:
        # Проверяем семейный статус
        cursor = await db.execute("SELECT spouse_id FROM users WHERE user_id = ?", (user_id,))
        result = await cursor.fetchone()
        has_spouse = result and result[0] is not None

        # Проверяем статус мера
        cursor = await db.execute("SELECT 1 FROM cities WHERE mayor_id = ?", (user_id,))
        is_mayor = await cursor.fetchone() is not None

        # 🔥 ИСПРАВЛЕНИЕ: Проверяем, есть ли у пользователя недвижимость
        cursor = await db.execute("""
            SELECT COUNT(*) FROM marketplace_items 
            WHERE seller_id = ? AND category = '🏠 Недвижимость' AND status = 'active'
        """, (user_id,))
        property_result = await cursor.fetchone()

        # 🔥 ВАЖНО: Проверяем что результат не None и преобразуем в bool
        if property_result and property_result[0] is not None:
            property_count = property_result[0]
            has_property = property_count > 0
        else:
            has_property = False  # 🔥 Значение по умолчанию

    buttons = []

    # Добавляем кнопку мэра если нужно
    if is_mayor:
        buttons.append([KeyboardButton(text="👨‍💼 Мэрское меню")])

    # Основные настройки
    buttons.extend([
        [KeyboardButton(text="🏠 Изменить место жительства")],
        [KeyboardButton(text="✏️ Изменить имя")]
    ])

    # Кнопка недвижимости (🔴 ИСПРАВЛЕНО: используем has_property вместо property_count)
    if has_property:  # 🔥 Теперь переменная определена
        buttons.append([KeyboardButton(text="🏘️ Моя недвижимость")])

    # Кнопки брака/развода
    if has_spouse:
        buttons.append([KeyboardButton(text="💒 Свидетельство о браке")])
        buttons.append([KeyboardButton(text="💔 Развестись")])
    else:
        buttons.append([KeyboardButton(text="💍 Зарегистрировать брак")])

    # Кнопка возврата
    buttons.append([KeyboardButton(text="⬅️ Назад")])

    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


def back_to_main_kb():
    """Клавиатура возврата в главное меню"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="⬅️ Главное меню")],
        ],
        resize_keyboard=True
    )


# Обработчик входа в настройки
@dp.message(F.text == "⚙️ Настройки")
async def open_settings(message: types.Message):
    await message.answer(
        "🔧 Меню настроек профиля:",
        reply_markup=await settings_menu_kb(message.from_user.id)
    )


# Клавиатура меню истории
def history_menu_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📜 История переводов")],
            [KeyboardButton(text="📄 История заявлений")],
            [KeyboardButton(text="📄 История рассылок")],
            [KeyboardButton(text="🧑‍⚕ История записей ко врачу")],
            [KeyboardButton(text="↩ Назад")],
        ],
        resize_keyboard=True
    )


# Клавиатура для открытия счета
async def savings_open_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="✅ Открыть накопительный счет")],
            [KeyboardButton(text="❌ Отмена")]
        ],
        resize_keyboard=True
    )


# Клавиатура для управления счетом
async def savings_menu_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📥 Пополнить накопительный счет")],
            [KeyboardButton(text="📤 Снять с накопительного счета")],
            [KeyboardButton(text="📊 Информация о счете")],
            [KeyboardButton(text="⬅️ Назад")]
        ],
        resize_keyboard=True
    )


# клавиатура городов
async def get_cities_keyboard():
    async with aiosqlite.connect("database.db") as db:
        cursor = await db.execute("SELECT name FROM cities")
        cities = await cursor.fetchall()

    builder = ReplyKeyboardBuilder()
    for city in cities:
        builder.button(text=city[0])
    builder.adjust(2)  # Располагаем кнопки по 2 в ряду
    return builder.as_markup(resize_keyboard=True)


def confirm_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Подтвердить"), KeyboardButton(text="Отмена")],
        ], resize_keyboard=True
    )


def doctors_kb():
    buttons = [
        [KeyboardButton(
            text="(уборщик) Кандидат инцельских наук и инцелофильствоведства Родион Сергеевич Пидорасович")],
        [KeyboardButton(text="(зубной) Иванов Иван Иванович")],
        [KeyboardButton(text="(Венеролог) Лобанов Илья Питрюхонович")],
        [KeyboardButton(text="(Офтальмолог) Вагинов Андрей Михайлович")],
        [KeyboardButton(text="(Уролог) Пистрохуньев Евгений Артёмович")],
        [KeyboardButton(text="(Гинекологичка) Шлюхова Наталья Ивановна")]
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True, one_time_keyboard=True)


def times_kb():
    buttons = [
        [KeyboardButton(text="10:00")],
        [KeyboardButton(text="10:30")],
        [KeyboardButton(text="11:00")],
        [KeyboardButton(text="11:30")],
        [KeyboardButton(text="12:00")],
        [KeyboardButton(text="12:30")],
        [KeyboardButton(text="13:00")],
        [KeyboardButton(text="13:30")],
        [KeyboardButton(text="15:00")],
        [KeyboardButton(text="15:30")],
        [KeyboardButton(text="16:00")],
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True, one_time_keyboard=True)


# Клавиатура для выбора города (динамически заполняется)
async def cities_kb():
    async with aiosqlite.connect("database.db") as db:
        # Получаем все города из БД
        cursor = await db.execute("SELECT name FROM cities ORDER BY name")
        cities = [row[0] for row in await cursor.fetchall()]

    # Создаем кнопки для каждого города
    keyboard = []
    row = []
    for i, city in enumerate(cities, 1):
        row.append(KeyboardButton(text=city))
        if i % 2 == 0:  # Размещаем по 2 города в строке
            keyboard.append(row)
            row = []
    if row:  # Добавляем оставшиеся города
        keyboard.append(row)

    # Добавляем кнопку для нового города
    keyboard.append([KeyboardButton(text="🏙 Зарегистрировать новый город")])

    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)


# Обработчик открытия счета
@dp.message(F.text == "✅ Открыть накопительный счет")
async def open_savings_account(message: types.Message):
    user_id = message.from_user.id

    async with aiosqlite.connect("database.db") as db:
        # Проверяем основной баланс
        cursor = await db.execute(
            "SELECT balance FROM users WHERE user_id = ?",
            (user_id,)
        )
        main_balance = (await cursor.fetchone())[0]

        if main_balance < 5:
            await message.answer(
                "❌ Недостаточно средств для открытия счета!\n"
                f"Нужно: 5 шуек\n"
                f"Ваш баланс: {main_balance} шуек"
            )
            return

        # Проверяем, нет ли уже счета
        cursor = await db.execute(
            "SELECT 1 FROM savings_accounts WHERE user_id = ?",
            (user_id,)
        )
        if await cursor.fetchone():
            await message.answer("❌ У вас уже есть накопительный счет!")
            return

        # Создаем накопительный счет
        await db.execute(
            "INSERT INTO savings_accounts (user_id, balance) VALUES (?, 0)",
            (user_id,)
        )

        # Списываем 5 шуек за открытие
        await db.execute(
            "UPDATE users SET balance = balance - 5 WHERE user_id = ?",
            (user_id,)
        )

        await db.commit()

        await message.answer(
            "✅ Накопительный счет успешно открыт!\n"
            "С вашего основного счета списано 5 шуек.\n\n"
            "📈 Теперь вы будете получать 5% ежемесячных начислений!",
            reply_markup=await savings_menu_kb()
        )


# Обработчик отмены открытия счета (ВНЕ предыдущей функции!)
@dp.message(F.text == "❌ Отмена")
async def cancel_savings_open(message: types.Message):
    await show_main_menu(message, message.from_user.id)


# Обработчик информации о счете
@dp.message(F.text == "📊 Информация о счете")
async def savings_info(message: types.Message):
    user_id = message.from_user.id

    async with aiosqlite.connect("database.db") as db:
        cursor = await db.execute(
            "SELECT balance, created_date, last_interest_date FROM savings_accounts WHERE user_id = ?",
            (user_id,)
        )
        savings_data = await cursor.fetchone()

        if not savings_data:
            await message.answer("❌ У вас нет накопительного счета!")
            return

        balance, created_date, last_interest = savings_data

        # Рассчитываем следующее начисление
        next_interest = datetime.strptime(last_interest, '%Y-%m-%d %H:%M:%S') + timedelta(days=30)
        days_until_interest = (next_interest - datetime.now()).days

        await message.answer(
            f"💳 <b>Информация о накопительном счете</b>\n\n"
            f"💰 Текущий баланс: {balance} шуек\n"
            f"📅 Дата открытия: {created_date[:10]}\n"
            f"📈 Процентная ставка: 5% в месяц\n"
            f"⏰ Следующее начисление: через {days_until_interest} дней\n"
            f"💵 Будущее начисление: {int(balance * 0.05)} шуек"
        )


# обработчик для накопительного счета
@dp.message(F.text == "💳 Накопительный счет")
async def savings_account_menu(message: types.Message):
    user_id = message.from_user.id

    async with aiosqlite.connect("database.db") as db:
        # Проверяем, есть ли у пользователя накопительный счет
        cursor = await db.execute(
            "SELECT balance, created_date FROM savings_accounts WHERE user_id = ?",
            (user_id,)
        )
        savings_data = await cursor.fetchone()

        if savings_data:
            balance, created_date = savings_data
            await message.answer(
                f"💳 Ваш накопительный счёт\n\n"
                f"💰 Баланс: {balance} шуек\n"
                f"📅 Открыт: {created_date[:10]}\n"
                f"📈 Ежемесячные начисления: 5%\n\n"
                f"Выберите действие:",
                reply_markup=await savings_menu_kb()
            )
        else:
            await message.answer(
                f"💳 Накопительный счёт\n\n"
                f"Откройте накопительный счёт всего за 5 шуек!\n"
                f"📈 Получайте 5% ежемесячных начислений\n"
                f"💰 Минимальный баланс: 5 шуек\n\n"
                f"Хотите открыть счёт?",
                reply_markup=await savings_open_kb()
            )


# Функция показа главного меню
async def show_main_menu(message: types.Message, user_id: int):
    """Показывает главное меню с информацией о пользователе"""
    # Получаем информацию о паспорте используя вашу функцию - НЕ ЗАБУДЬТЕ AWAIT!
    passport_info = await my_passport_get(user_id)

    if not passport_info:
        await message.answer("❌ Профиль не найден. Введите /start для регистрации")
        return

    # Отправляем паспорт пользователя
    await message.answer(passport_info, parse_mode="HTML")

    # Показываем главное меню
    await message.answer("Выберите действие:", reply_markup=await main_menu_kb(user_id))


# старт
@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    async with aiosqlite.connect("database.db") as db:
        cursor = await db.execute("SELECT 1 FROM users WHERE user_id = ?", (user_id,))
        if await cursor.fetchone():
            await show_main_menu(message, user_id)
            return

    await message.answer("👋 Добро пожаловать! Для регистрации введите ваше имя:")
    await state.set_state(Register.name)


# имя
@dp.message(Register.name)
async def process_name(message: types.Message, state: FSMContext):
    name = message.text.strip()
    if len(name) < 2 or len(name) > 100:
        await message.answer("Имя должно быть от 2 до 100 символов. Попробуйте еще раз.")
        return

    await state.update_data(name=name)
    await message.answer("Выберите ваш пол:", reply_markup=gender_kb)
    await state.set_state(Register.gender)


# Обработка пола
@dp.message(Register.gender, F.text.in_(["Мужской", "Женский"]))
async def process_gender(message: types.Message, state: FSMContext):
    gender = "м" if message.text == "Мужской" else "ж"
    await state.update_data(gender=gender)
    await message.answer(f"Выберите ваш город:", reply_markup=await cities_kb())
    await state.set_state(Register.city)


# Обработка выбора города
@dp.message(Register.city)
async def process_city(message: types.Message, state: FSMContext):
    selected_city = message.text.strip()
    user_id = message.from_user.id

    # Обработка отмены
    if selected_city.lower() == "отмена":
        await message.answer("Регистрация отменена.")
        await state.clear()
        return

    if selected_city == "🏙 Зарегистрировать новый город":
        await message.answer("Введите название нового города:")
        await state.set_state(Register.new_city)
        return

    async with aiosqlite.connect("database.db") as db:
        # Проверяем существование города и получаем мэра
        cursor = await db.execute(
            "SELECT mayor_id FROM cities WHERE name = ?",
            (selected_city,)
        )
        city_data = await cursor.fetchone()

        if not city_data:
            await message.answer("❌ Этот город не зарегистрирован. Выберите город из списка.")
            return

        mayor_id = city_data[0]
        data = await state.get_data()

        # Если у города нет мэра, регистрируем сразу
        if not mayor_id:
            try:
                # Используем новую функцию генерации ГосID
                account_number = await generate_account_number(selected_city, db)
                await db.execute(
                    "INSERT INTO users (user_id, account_id, name, gender, city, balance) "
                    "VALUES (?, ?, ?, ?, ?, 1)",
                    (user_id, account_number, data['name'], data['gender'], selected_city)
                )
                await db.commit()

                await message.answer(
                    f"✅ Регистрация в городе {selected_city} завершена!",
                    reply_markup=await main_menu_kb(user_id)
                )
                await state.clear()
                return

            except Exception as e:
                await message.answer("❌ Ошибка при регистрации. Попробуйте позже.")
                print(f"Ошибка регистрации: {e}")
                await state.clear()
                return

        # Если есть мэр, отправляем запрос на подтверждение
        try:
            # Сохраняем во временную таблицу
            await db.execute(
                "INSERT OR REPLACE INTO temp_registrations (user_id, name, gender, city) "
                "VALUES (?, ?, ?, ?)",
                (user_id, data['name'], data['gender'], selected_city)
            )
            await db.commit()

            # Создаем клавиатуру для мэра
            builder = InlineKeyboardBuilder()
            builder.button(
                text="✅ Одобрить",
                callback_data=f"approve_reg:{user_id}"
            )
            builder.button(
                text="❌ Отклонить",
                callback_data=f"reject_reg:{user_id}"
            )
            builder.adjust(2)

            kb = builder.as_markup()

            # Отправляем запрос мэру
            await bot.send_message(
                mayor_id,
                f"📝 Новый запрос на регистрацию в городе {selected_city}:\n\n"
                f"👤 Имя: {data['name']}\n"
                f"👫 Пол: {'Мужской' if data['gender'] == 'м' else 'Женский'}\n"
                f"Подтвердить регистрацию?",
                reply_markup=kb
            )

            await message.answer(
                f"📨 Ваша заявка на регистрацию в городе {selected_city} отправлена мэру. "
                f"Ожидайте подтверждения."
            )

        except Exception as e:
            print(f"Ошибка отправки запроса мэру: {e}")
            # Если не удалось отправить мэру, регистрируем сразу
            try:
                account_number = await generate_account_number(selected_city, db)
                await db.execute(
                    "INSERT INTO users (user_id, account_id, name, gender, city, balance) "
                    "VALUES (?, ?, ?, ?, ?, 1)",
                    (user_id, account_number, data['name'], data['gender'], selected_city)
                )
                await db.execute(
                    "DELETE FROM temp_registrations WHERE user_id = ?",
                    (user_id,)
                )
                await db.commit()

                await message.answer(
                    f"✅ Регистрация в городе {selected_city} завершена!",
                    reply_markup=await main_menu_kb(user_id)
                )
            except Exception as e2:
                await message.answer("❌ Ошибка при регистрации. Попробуйте позже.")
                print(f"Ошибка регистрации: {e2}")

    await state.clear()


# Обработка нового города
@dp.message(Register.new_city)
async def process_new_city(message: types.Message, state: FSMContext):
    city_name = message.text.strip()
    user_id = message.from_user.id

    # Проверка длины названия города
    if len(city_name) > 50:
        await message.answer("Название города слишком длинное. Максимум 50 символов.")
        return
    if len(city_name) < 2:
        await message.answer("Название города слишком короткое. Минимум 2 символа.")
        return

    async with aiosqlite.connect("database.db") as db:
        try:
            # Проверяем, не существует ли уже город с таким названием
            cursor = await db.execute(
                "SELECT 1 FROM cities WHERE name = ?",
                (city_name,)
            )
            if await cursor.fetchone():
                await message.answer("❌ Этот город уже существует. Выберите другое название.")
                return

            # Получаем данные из состояния
            data = await state.get_data()
            await state.update_data(city=city_name)

            # СОХРАНЯЕМ ВО ВРЕМЕННУЮ ТАБЛИЦУ ДЛЯ ПОДТВЕРЖДЕНИЯ АДМИНИСТРАТОРОМ
            await db.execute(
                "INSERT OR REPLACE INTO temp_registrations (user_id, name, gender, city) "
                "VALUES (?, ?, ?, ?)",
                (user_id, data['name'], data['gender'], city_name)
            )
            await db.commit()

            # Уведомляем администраторов о новом городе
            admin_message = (
                f"🌆 <b>Запрос на регистрацию нового города</b>\n\n"
                f"👤 Пользователь: {data['name']}\n"
                f"💳 ID: {user_id}\n"
                f"🏙️ Название города: {city_name}\n"
                f"👫 Пол: {'Мужской' if data['gender'] == 'м' else 'Женский'}"
            )

            # Создаем inline-клавиатуру для администратора
            builder = InlineKeyboardBuilder()
            builder.button(
                text="✅ Одобрить город",
                callback_data=f"approve_city:{user_id}:{city_name}"
            )
            builder.button(
                text="❌ Отклонить город",
                callback_data=f"reject_city:{user_id}:{city_name}"
            )
            builder.adjust(2)

            # Отправляем запрос всем администраторам
            for admin_id in ADMIN_ID:
                try:
                    await bot.send_message(
                        admin_id,
                        admin_message,
                        parse_mode="HTML",
                        reply_markup=builder.as_markup()
                    )
                except Exception as e:
                    print(f"Не удалось отправить запрос администратору {admin_id}: {e}")

            await message.answer(
                f"📨 Ваша заявка на регистрацию города '{city_name}' отправлена администраторам "
                f"на проверку. Ожидайте подтверждения."
            )

            await state.clear()

        except Exception as e:
            await message.answer("❌ Произошла ошибка при регистрации города.")
            print(f"Ошибка регистрации города: {e}")
            await state.clear()


# Обработка подтверждения от мера
@dp.callback_query(F.data.startswith("approve_reg:"))
async def approve_registration(callback: types.CallbackQuery):
    user_id = int(callback.data.split(":")[1])

    async with aiosqlite.connect("database.db") as db:
        # Получаем данные из временной таблицы
        cursor = await db.execute("""
            SELECT name, gender, city 
            FROM temp_registrations 
            WHERE user_id = ?
            """, (user_id,))
        reg_data = await cursor.fetchone()

        if not reg_data:
            await callback.answer("Заявка не найдена!")
            return

        name, gender, city = reg_data

        # Завершаем регистрацию
        account_number = await generate_account_number(city, db)
        await db.execute("""
            INSERT INTO users 
            (user_id, account_id, name, gender, city, balance) 
            VALUES (?, ?, ?, ?, ?, 1)
            """, (user_id, account_number, name, gender, city))

        await db.execute("""
            DELETE FROM temp_registrations 
            WHERE user_id = ?
            """, (user_id,))
        await db.commit()

    # Получаем код города для отображения
    city_code = await get_city_code(city)
    city_sequence = await get_city_sequence_number(city, db) - 1

    await callback.message.edit_text(
        f"✅ Регистрация пользователя {name} подтверждена!"
    )

    await bot.send_message(
        user_id,
        f"✅ Ваша регистрация в {city} подтверждена мэром!\n\n"
        f"🏙 Город: {city}\n"
        f"🎫 Код города: {city_code}\n"
        f"🎫 ГосID: {account_number}\n"
        f"👤 Номер в городе: {city_sequence}\n"
        f"💳 Баланс: 1 шуек",
        reply_markup=await main_menu_kb(user_id)
    )


@dp.callback_query(F.data.startswith("reject_reg:"))
async def reject_registration(callback: types.CallbackQuery):
    user_id = int(callback.data.split(":")[1])

    async with aiosqlite.connect("database.db") as db:
        # Получаем данные заявки
        cursor = await db.execute("""
            SELECT city FROM temp_registrations 
            WHERE user_id = ?
            """, (user_id,))
        city = (await cursor.fetchone())[0]

        await db.execute("""
            DELETE FROM temp_registrations 
            WHERE user_id = ?
            """, (user_id,))
        await db.commit()

    await callback.message.edit_text(
        "❌ Регистрация отклонена"
    )
    await bot.send_message(
        user_id,
        f"❌ Мэр города {city} отклонил вашу заявку. "
        "Пожалуйста, выберите другой город.",
        reply_markup=await cities_kb()
    )


"""
               мер настройки города

"""
ATTRACTION_TYPES = [
    "🏛️ гос здания",
    "🏭 автофермы",
    "🌳 классические фермы",
    "🎭 культура",
    "🎪 развлечения",
    "🎨 искусство"
]


@dp.message(F.text == "👨‍💼 Мэрское меню")
async def mayor_menu(message: types.Message, state: FSMContext):
    user_id = message.from_user.id

    async with aiosqlite.connect("database.db") as db:
        # Проверяем, является ли пользователь мером какого-либо города
        cursor = await db.execute(
            "SELECT name FROM cities WHERE mayor_id = ?",  # Здесь была пропущена закрывающая скобка
            (user_id,)  # Добавлена закрывающая скобка
        )
        city_data = await cursor.fetchone()

        if not city_data:
            await message.answer("Вы не являетесь мэром ни одного города.")
            return

        city_name = city_data[0]
        await state.update_data(city_name=city_name)
        await message.answer(
            f"🏙 Вы управляете городом {city_name}",
            reply_markup=await mayor_menu_kb(city_name)
        )
        await state.set_state(MayorMenu.managing_city)


def cancel_broadcast_kb():
    """Клавиатура для отмены рассылки"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="❌ Отмена рассылки")]
        ],
        resize_keyboard=True
    )


def confirm_broadcast_kb():
    """Клавиатура подтверждения рассылки"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="✅ Да, отправить рассылку")],
            [KeyboardButton(text="❌ Отмена")]
        ],
        resize_keyboard=True
    )


# Обработчик кнопки "📢 Сделать рассылку"
@dp.message(MayorMenu.managing_city, F.text == "📢 Сделать рассылку")
async def start_mayor_broadcast(message: types.Message, state: FSMContext):
    """Начало рассылки сообщений жителям города"""
    data = await state.get_data()
    city_name = data.get('city_name')

    async with aiosqlite.connect("database.db") as db:
        # Получаем количество жителей
        cursor = await db.execute(
            "SELECT COUNT(*) FROM users WHERE city = ?",
            (city_name,)
        )
        citizen_count = (await cursor.fetchone())[0]

    await message.answer(
        f"📢 <b>Рассылка сообщений жителям города {city_name}</b>\n\n"
        f"👥 Всего жителей: {citizen_count}\n\n"
        f"Введите сообщение для рассылки (максимум 2000 символов):\n\n"
        f"Вы можете добавить:\n"
        f"• Текст сообщения\n"
        f"• Фото (прикрепите как обычно)\n"
        f"• Видео, GIF или стикеры\n\n"
        f"Или напишите <code>❌ Отмена рассылки</code> для отмены",
        parse_mode="HTML",
        reply_markup=cancel_broadcast_kb()
    )
    await state.set_state(MayorMenu.broadcast_to_citizens)


@dp.message(StateFilter(
    MayorMenu.broadcast_to_citizens,
    MayorMenu.confirm_broadcast
), F.text == "↩ Назад")
async def back_from_broadcast(message: types.Message, state: FSMContext):
    """Возврат из меню рассылки"""
    data = await state.get_data()
    city_name = data.get('city_name')

    await message.answer(
        f"🏙 Возврат к управлению городом {city_name}",
        reply_markup=await mayor_menu_kb(city_name)
    )
    await state.set_state(MayorMenu.managing_city)


# Обработчик ввода сообщения для рассылки
@dp.message(MayorMenu.broadcast_to_citizens)
async def process_mayor_broadcast_message(message: types.Message, state: FSMContext):
    """Обработка сообщения для рассылки"""
    data = await state.get_data()
    city_name = data.get('city_name')

    if message.text == "❌ Отмена рассылки":
        await message.answer(
            "Рассылка отменена.",
            reply_markup=await mayor_menu_kb(city_name)
        )
        await state.set_state(MayorMenu.managing_city)
        return

    # Сохраняем либо текст, либо медиа
    broadcast_data = {}

    if message.text:
        if len(message.text) > 2000:
            await message.answer("❌ Сообщение слишком длинное. Максимум 2000 символов.")
            return
        broadcast_data['message_type'] = 'text'
        broadcast_data['message'] = message.text

    elif message.photo:
        broadcast_data['message_type'] = 'photo'
        broadcast_data['message'] = message.caption or ""
        broadcast_data['media_file_id'] = message.photo[-1].file_id  # Берем самое высокое качество

    elif message.video:
        broadcast_data['message_type'] = 'video'
        broadcast_data['message'] = message.caption or ""
        broadcast_data['media_file_id'] = message.video.file_id

    elif message.animation:  # GIF
        broadcast_data['message_type'] = 'animation'
        broadcast_data['message'] = message.caption or ""
        broadcast_data['media_file_id'] = message.animation.file_id

    elif message.sticker:
        broadcast_data['message_type'] = 'sticker'
        broadcast_data['message'] = ""
        broadcast_data['media_file_id'] = message.sticker.file_id

    else:
        await message.answer(
            "❌ Пожалуйста, отправьте текст или медиа-файл (фото, видео, GIF или стикер).",
            reply_markup=cancel_broadcast_kb()
        )
        return

    # Сохраняем данные рассылки в состоянии
    await state.update_data(broadcast_data=broadcast_data)

    # Получаем статистику по жителям
    async with aiosqlite.connect("database.db") as db:
        cursor = await db.execute(
            "SELECT COUNT(*) FROM users WHERE city = ?",
            (city_name,)
        )
        citizen_count = (await cursor.fetchone())[0]

    # Формируем предварительный просмотр
    preview_text = "📢 <b>Предварительный просмотр рассылки:</b>\n\n"

    if broadcast_data['message_type'] == 'text':
        preview_text += broadcast_data['message']
    else:
        if broadcast_data['message']:
            preview_text += f"Подпись: {broadcast_data['message']}\n"
        preview_text += f"Тип медиа: {broadcast_data['message_type']}"

    preview_text += f"\n\n📊 <b>Статистика:</b>\n"
    preview_text += f"• 👥 Жителей в городе: {citizen_count}\n"
    preview_text += f"• 📅 Дата: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n\n"
    preview_text += f"<b>Вы уверены, что хотите отправить эту рассылку?</b>"

    await message.answer(
        preview_text,
        parse_mode="HTML",
        reply_markup=confirm_broadcast_kb()
    )
    await state.set_state(MayorMenu.confirm_broadcast)


# Обработчик подтверждения рассылки
@dp.message(MayorMenu.confirm_broadcast, F.text == "✅ Да, отправить рассылку")
async def confirm_mayor_broadcast(message: types.Message, state: FSMContext):
    """Подтверждение и отправка рассылки"""
    data = await state.get_data()
    city_name = data.get('city_name')
    broadcast_data = data.get('broadcast_data', {})
    mayor_id = message.from_user.id

    if not broadcast_data:
        await message.answer("❌ Ошибка: данные рассылки не найдены.")
        await state.set_state(MayorMenu.managing_city)
        return

    await message.answer("⏳ Начинаю рассылку сообщения жителям города...")

    try:
        async with aiosqlite.connect("database.db") as db:
            # Получаем всех жителей города
            cursor = await db.execute(
                "SELECT user_id, name FROM users WHERE city = ? AND user_id != ?",
                (city_name, mayor_id)
            )
            citizens = await cursor.fetchall()

        successful = 0
        failed = 0
        failed_users = []

        # Отправляем сообщение каждому жителю
        for user_id, user_name in citizens:
            try:
                if broadcast_data['message_type'] == 'text':
                    await bot.send_message(
                        user_id,
                        f"📢 <b>Сообщение от мэра города {city_name}:</b>\n\n{broadcast_data['message']}",
                        parse_mode="HTML"
                    )

                elif broadcast_data['message_type'] == 'photo':
                    await bot.send_photo(
                        user_id,
                        photo=broadcast_data['media_file_id'],
                        caption=f"📢 <b>Сообщение от мэра города {city_name}:</b>\n\n{broadcast_data['message']}" if
                        broadcast_data['message'] else None,
                        parse_mode="HTML"
                    )

                elif broadcast_data['message_type'] == 'video':
                    await bot.send_video(
                        user_id,
                        video=broadcast_data['media_file_id'],
                        caption=f"📢 <b>Сообщение от мэра города {city_name}:</b>\n\n{broadcast_data['message']}" if
                        broadcast_data['message'] else None,
                        parse_mode="HTML"
                    )

                elif broadcast_data['message_type'] == 'animation':
                    await bot.send_animation(
                        user_id,
                        animation=broadcast_data['media_file_id'],
                        caption=f"📢 <b>Сообщение от мэра города {city_name}:</b>\n\n{broadcast_data['message']}" if
                        broadcast_data['message'] else None,
                        parse_mode="HTML"
                    )

                elif broadcast_data['message_type'] == 'sticker':
                    # Сначала отправляем стикер
                    await bot.send_sticker(user_id, sticker=broadcast_data['media_file_id'])
                    # Затем текстовое сообщение если есть текст
                    if broadcast_data['message']:
                        await bot.send_message(
                            user_id,
                            f"📢 <b>Сообщение от мэра города {city_name}:</b>\n\n{broadcast_data['message']}",
                            parse_mode="HTML"
                        )

                successful += 1

                # Небольшая задержка чтобы не превысить лимиты Telegram
                await asyncio.sleep(0.05)

            except Exception as e:
                failed += 1
                failed_users.append(f"{user_name} (ID: {user_id})")
                print(f"Не удалось отправить сообщение пользователю {user_id}: {e}")

        # Формируем отчет
        total = successful + failed

        report = (
            f"✅ <b>Рассылка завершена!</b>\n\n"
            f"📊 <b>Статистика:</b>\n"
            f"• 📈 Всего жителей: {total}\n"
            f"• ✅ Успешно отправлено: {successful}\n"
            f"• ❌ Не удалось отправить: {failed}\n"
            f"• 📅 Дата отправки: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
        )

        if failed_users:
            report += f"\n<code>Жители, не получившие сообщение:</code>\n"
            for failed_user in failed_users[:10]:  # Показываем только первые 10
                report += f"• {failed_user}\n"
            if len(failed_users) > 10:
                report += f"• ... и ещё {len(failed_users) - 10} жителей\n"

        report += f"\n<i>Рассылка была отправлена от имени мэра города {city_name}.</i>"

        await message.answer(
            report,
            parse_mode="HTML",
            reply_markup=await mayor_menu_kb(city_name)
        )

        # Записываем лог рассылки в базу данных
        try:
            async with aiosqlite.connect("database.db") as db:
                await db.execute("""
                    INSERT INTO mayor_broadcasts 
                    (mayor_id, city_name, message_type, message, sent_count, failed_count, timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """, (
                    mayor_id, city_name, broadcast_data['message_type'],
                    broadcast_data.get('message', '')[:500],  # Обрезаем для базы
                    successful, failed
                ))
                await db.commit()
        except Exception as e:
            print(f"Ошибка записи лога рассылки: {e}")

    except Exception as e:
        await message.answer(
            f"❌ Произошла ошибка при рассылке: {str(e)[:100]}",
            reply_markup=await mayor_menu_kb(city_name)
        )

    await state.set_state(MayorMenu.managing_city)


# Обработчик отмены рассылки
@dp.message(MayorMenu.confirm_broadcast, F.text == "❌ Отмена")
async def cancel_mayor_broadcast(message: types.Message, state: FSMContext):
    """Отмена рассылки"""
    data = await state.get_data()
    city_name = data.get('city_name')

    await message.answer(
        "❌ Рассылка отменена.",
        reply_markup=await mayor_menu_kb(city_name)
    )
    await state.set_state(MayorMenu.managing_city)


# Обработчик отмены при вводе сообщения
@dp.message(MayorMenu.broadcast_to_citizens, F.text == "❌ Отмена рассылки")
async def cancel_mayor_broadcast_input(message: types.Message, state: FSMContext):
    """Отмена рассылки при вводе сообщения"""
    data = await state.get_data()
    city_name = data.get('city_name')

    await message.answer(
        "❌ Рассылка отменена.",
        reply_markup=await mayor_menu_kb(city_name)
    )
    await state.set_state(MayorMenu.managing_city)


# Обработчик кнопки "Жители города"
@dp.message(MayorMenu.managing_city, F.text.startswith("👥 Жители города"))
async def show_citizens(message: types.Message, state: FSMContext):
    data = await state.get_data()
    city_name = data.get('city_name')

    async with aiosqlite.connect("database.db") as db:
        # Получаем всех жителей города
        cursor = await db.execute(
            "SELECT name, account_id, balance FROM users WHERE city = ? ORDER BY name",
            (city_name,)
        )
        citizens = await cursor.fetchall()

        if not citizens:
            await message.answer("В вашем городе пока нет жителей.")
            return

        response = f"👥 Жители города {city_name}:\n\n"
        for idx, (name, account_id, balance) in enumerate(citizens, 1):
            response += f"{idx}. {name} (ID: {account_id})\nБаланс: {balance} шуек\n\n"

        # Разбиваем на части, если сообщение слишком длинное
        for part in [response[i:i + 4000] for i in range(0, len(response), 4000)]:
            await message.answer(part)

        await message.answer(
            f"Всего жителей: {len(citizens)}",
            reply_markup=await mayor_menu_kb(city_name)
        )


# Обработчик кнопки "Переименовать город"
@dp.message(MayorMenu.managing_city, F.text == "✏️ Переименовать город")
async def start_city_rename(message: types.Message, state: FSMContext):
    rename_city = "Введите новое название для города (от 2 до 50 символов или <code>Отмена</code>, если хотите отменить переименовывание):"
    await message.answer(rename_city, parse_mode="HTML", reply_markup=ReplyKeyboardRemove())
    await state.set_state(MayorMenu.rename_city)


# Обработчик нового названия города
@dp.message(MayorMenu.rename_city)
async def process_city_rename(message: types.Message, state: FSMContext):
    new_name = message.text.strip()
    data = await state.get_data()
    old_name = data.get('city_name')
    user_id = message.from_user.id

    if message.text.lower() == "отмена":
        await show_main_menu(message, message.from_user.id)
        await state.clear()
        return

    # Проверка длины названия
    if len(new_name) < 2 or len(new_name) > 50:
        await message.answer("Название города должно быть от 2 до 50 символов.")
        return

    async with aiosqlite.connect("database.db") as db:
        try:
            # Проверяем, не существует ли уже город с таким названием
            cursor = await db.execute(
                "SELECT 1 FROM cities WHERE name = ?",
                (new_name,)
            )
            if await cursor.fetchone():
                await message.answer("Город с таким названием уже существует.")
                return

            # Начинаем транзакцию
            await db.execute("BEGIN TRANSACTION")

            # 1. Переименовываем город в таблице cities
            await db.execute(
                "UPDATE cities SET name = ? WHERE name = ?",
                (new_name, old_name)
            )

            # 2. Обновляем город у всех жителей в таблице users
            await db.execute(
                "UPDATE users SET city = ? WHERE city = ?",
                (new_name, old_name)
            )

            # 3. Обновляем город в таблице streets
            await db.execute(
                "UPDATE streets SET city_name = ? WHERE city_name = ?",
                (new_name, old_name)
            )

            # 4. Обновляем город в таблице houses
            await db.execute(
                "UPDATE houses SET city_name = ? WHERE city_name = ?",
                (new_name, old_name)
            )

            # 5. Обновляем город в таблице attractions
            await db.execute(
                "UPDATE attractions SET city_name = ? WHERE city_name = ?",
                (new_name, old_name)
            )

            # 6. Обновляем город в таблице city_change_requests
            await db.execute(
                "UPDATE city_change_requests SET new_city = ? WHERE new_city = ?",
                (new_name, old_name)
            )
            await db.execute(
                "UPDATE city_change_requests SET old_city = ? WHERE old_city = ?",
                (new_name, old_name)
            )

            # 7. Обновляем город в таблице mayor_broadcasts
            await db.execute(
                "UPDATE mayor_broadcasts SET city_name = ? WHERE city_name = ?",
                (new_name, old_name)
            )

            # 8. Обновляем город в таблице registration_requests
            await db.execute(
                "UPDATE registration_requests SET city = ? WHERE city = ?",
                (new_name, old_name)
            )

            await db.commit()

            await message.answer(
                f"✅ Город успешно переименован!\n"
                f"Старое название: {old_name}\n"
                f"Новое название: {new_name}\n\n"
                f"Все связанные данные (улицы, дома, жители) обновлены автоматически.",
                reply_markup=await mayor_menu_kb(new_name)
            )

            # Обновляем состояние
            await state.update_data(city_name=new_name)
            await state.set_state(MayorMenu.managing_city)

        except Exception as e:
            await db.rollback()
            await message.answer(f"❌ Произошла ошибка при переименовании города: {str(e)[:100]}")
            print(f"Ошибка переименования города: {e}")

#показ полного адресса
async def get_user_full_address(user_id: int) -> str:
    """Получает полный адрес пользователя"""
    async with aiosqlite.connect("database.db") as db:
        cursor = await db.execute("""
            SELECT u.city, u.street, u.house_number 
            FROM users u 
            WHERE u.user_id = ?
        """, (user_id,))
        user_data = await cursor.fetchone()

        if not user_data:
            return "Адрес не указан"

        city, street, house = user_data

        address_parts = []
        if city:
            address_parts.append(f"🏙️ {city}")
        if street:
            address_parts.append(f"🏘️ {street}")
        if house:
            address_parts.append(f"🏠 {house}")

        return " | ".join(address_parts) if address_parts else "Адрес не указан"

# Обработчик кнопки "Назад" в мэрском меню
@dp.message(MayorMenu.managing_city, F.text == "↩ Назад")
async def back_from_mayor_menu(message: types.Message, state: FSMContext):
    await state.clear()
    await show_main_menu(message, message.from_user.id)


# Обработчик передачи города
@dp.message(F.text == "👑 Передать город")
async def transfer_city_start(message: types.Message, state: FSMContext):
    user_id = message.from_user.id

    async with aiosqlite.connect("database.db") as db:
        # Получаем город пользователя
        cursor = await db.execute(
            "SELECT name FROM cities WHERE mayor_id = ?",
            (user_id,)
        )
        city_data = await cursor.fetchone()

        if not city_data:
            await message.answer("❌ Вы не являетесь мером ни одного города.")
            return

        city_name = city_data[0]

        # Получаем жителей города (кроме себя)
        cursor = await db.execute(
            "SELECT user_id, name FROM users WHERE city = ? AND user_id != ?",
            (city_name, user_id)
        )
        citizens = await cursor.fetchall()

        if not citizens:
            await message.answer("❌ В вашем городе нет других жителей для передачи.")
            return

        # Создаем inline-клавиатуру
        builder = InlineKeyboardBuilder()
        for citizen_id, citizen_name in citizens:
            builder.button(
                text=f"{citizen_name} (ID: {citizen_id})",
                callback_data=f"transfer_city:{citizen_id}"
            )
        builder.adjust(1)  # По одной кнопке в строке

        await message.answer(
            "👑 Выберите нового мера города:",
            reply_markup=builder.as_markup()
        )
        await state.update_data(city_name=city_name)


# Обработчик выбора нового мера
@dp.callback_query(F.data.startswith("transfer_city:"))
async def process_city_transfer(callback: types.CallbackQuery, state: FSMContext):
    new_mayor_id = int(callback.data.split(":")[1])
    data = await state.get_data()
    city_name = data["city_name"]
    old_mayor_id = callback.from_user.id

    async with aiosqlite.connect("database.db") as db:
        try:
            await db.execute("BEGIN TRANSACTION")

            # Передаем город
            await db.execute(
                "UPDATE cities SET mayor_id = ? WHERE name = ?",
                (new_mayor_id, city_name)
            )

            # Получаем имя нового мера
            cursor = await db.execute(
                "SELECT name FROM users WHERE user_id = ?",
                (new_mayor_id,)
            )
            new_mayor_name = (await cursor.fetchone())[0]

            await db.commit()

            # Уведомления
            await callback.message.edit_text(
                f"✅ Вы передали город {city_name} пользователю {new_mayor_name}"
            )
            try:
                await bot.send_message(
                    new_mayor_id,
                    f"🎉 Теперь вы мер города {city_name}!\n"
                    "Доступно меню управления городом."
                )
            except Exception as e:
                print(f"Не удалось уведомить нового мера: {e}")

        except Exception as e:
            await db.rollback()
            await callback.message.edit_text("❌ Ошибка при передаче города")
            print(f"Ошибка передачи города: {e}")

    await state.clear()
    await show_main_menu(message, message.from_user.id)


# Обработчик удаления города
@dp.message(F.text == "🗑️ Удалить город")
async def delete_city_start(message: types.Message, state: FSMContext):
    user_id = message.from_user.id

    async with aiosqlite.connect("database.db") as db:
        # Получаем город, которым управляет пользователь
        cursor = await db.execute(
            "SELECT name FROM cities WHERE mayor_id = ?",
            (user_id,)
        )
        city_data = await cursor.fetchone()

        if not city_data:
            await message.answer("Вы не являетесь мером ни одного города.")
            return

        city_name = city_data[0]

        # Проверяем количество жителей
        cursor = await db.execute(
            "SELECT COUNT(*) FROM users WHERE city = ?",
            (city_name,)
        )
        citizens_count = (await cursor.fetchone())[0]

        if citizens_count > 1:
            await message.answer(
                "❌ Нельзя удалить город, в котором есть другие жители.\n"
                f"В городе {city_name} проживает {citizens_count - 1} других жителей.\n"
                "Сначала передайте город другому жителю или дождитесь, пока они переедут."
            )
            return

        await message.answer(
            f"⚠️ Вы уверены, что хотите удалить город {city_name}?\n"
            "Это действие невозможно отменить!",
            reply_markup=confirm_kb()
        )
        await state.set_data({"city_name": city_name})
        await state.set_state(MayorManagement.confirm_city_deletion)


# Подтверждение удаления города
@dp.message(MayorManagement.confirm_city_deletion, F.text == "Подтвердить")
async def confirm_city_deletion(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    data = await state.get_data()
    city_name = data["city_name"]

    async with aiosqlite.connect("database.db") as db:
        try:
            # Начинаем транзакцию
            await db.execute("BEGIN TRANSACTION")

            # Удаляем город
            await db.execute(
                "DELETE FROM cities WHERE name = ? AND mayor_id = ?",
                (city_name, user_id)
            )

            # Перемещаем бывшего мера в "Улицу"
            await db.execute(
                "UPDATE users SET city = 'Улица' WHERE user_id = ?",
                (user_id,)
            )

            await db.commit()

            await message.answer(
                f"🗑️ Город {city_name} успешно удален.\n"
                "Ваше текущее место жительства изменено на 'Улица'.",
                reply_markup=await main_menu_kb(user_id)
            )

        except Exception as e:
            await db.rollback()
            await message.answer("❌ Произошла ошибка при удалении города.")
            print(f"Ошибка удаления города: {e}")

    await state.clear()
    await show_main_menu(message, message.from_user.id)


@dp.message(MayorManagement.confirm_city_deletion, F.text == "Отмена")
async def cancel_city_deletion(message: types.Message, state: FSMContext):
    await message.answer("Удаление города отменено.")
    await state.clear()
    await show_main_menu(message, message.from_user.id)


@dp.message(MayorMenu.managing_city, F.text == "🏘️ Управление улицами")
async def manage_streets(message: types.Message, state: FSMContext):
    data = await state.get_data()
    city_name = data.get('city_name')

    await message.answer(
        f"🏘️ Управление улицами города {city_name}",
        reply_markup=await streets_menu_kb(city_name)
    )
    await state.set_state(StreetManagement.managing_streets)


# Обработчик показа списка улиц
@dp.message(StreetManagement.managing_streets, F.text.startswith("📋 Список улиц"))
async def show_streets_list(message: types.Message, state: FSMContext):
    data = await state.get_data()
    city_name = data.get('city_name')

    async with aiosqlite.connect("database.db") as db:
        cursor = await db.execute("""
            SELECT s.street_name, u.name 
            FROM streets s 
            LEFT JOIN users u ON s.created_by = u.user_id 
            WHERE s.city_name = ? 
            ORDER BY s.street_name
        """, (city_name,))
        streets = await cursor.fetchall()

    if not streets:
        await message.answer("В вашем городе пока нет зарегистрированных улиц.")
        return

    response = f"🏘️ Список улиц города {city_name}:\n\n"
    for idx, (street_name, creator_name) in enumerate(streets, 1):
        response += f"{idx}. {street_name}"
        if creator_name:
            response += f" (добавлена: {creator_name})"
        response += "\n"

    response += f"\nВсего улиц: {len(streets)}"

    await message.answer(response)


# Обработчик добавления улицы
@dp.message(StreetManagement.managing_streets, F.text == "➕ Добавить улицу")
async def add_street_start(message: types.Message, state: FSMContext):
    await message.answer(
        "Введите название новой улицы (от 2 до 50 символов):\n"
        "Или напишите 'Отмена' для отмены",
        reply_markup=ReplyKeyboardRemove()
    )
    await state.set_state(StreetManagement.add_street_name)


# Обработчик ввода названия улицы
@dp.message(StreetManagement.add_street_name)
async def process_street_name(message: types.Message, state: FSMContext):
    street_name = message.text.strip()

    if message.text == "Отмена":
        data = await state.get_data()
        city_name = data.get('city_name')
        await message.answer(
            "Добавление улицы отменено.",
            reply_markup=await streets_menu_kb(city_name)
        )
        await state.set_state(StreetManagement.managing_streets)
        return

    # Валидация названия улицы
    if len(street_name) < 2 or len(street_name) > 85:
        await message.answer("Название улицы должно быть от 2 до 85 символов. Попробуйте еще раз:")
        return

    if not re.match(r"^[a-zA-Zа-яА-Я0-9\s\-\.]+$", street_name):
        await message.answer("Название улицы может содержать только буквы, цифры, пробелы, дефисы и точки.")
        return

    data = await state.get_data()
    city_name = data.get('city_name')
    user_id = message.from_user.id

    async with aiosqlite.connect("database.db") as db:
        try:
            # Проверяем, существует ли уже такая улица в городе
            cursor = await db.execute(
                "SELECT 1 FROM streets WHERE city_name = ? AND street_name = ?",
                (city_name, street_name)
            )
            if await cursor.fetchone():
                await message.answer("Улица с таким названием уже существует в вашем городе.")
                return

            # Добавляем новую улицу
            await db.execute(
                "INSERT INTO streets (city_name, street_name, created_by) VALUES (?, ?, ?)",
                (city_name, street_name, user_id)
            )
            await db.commit()

            await message.answer(
                f"✅ Улица '{street_name}' успешно добавлена в город {city_name}!",
                reply_markup=await streets_menu_kb(city_name)
            )

        except Exception as e:
            await message.answer("❌ Произошла ошибка при добавлении улицы. Попробуйте позже.")
            print(f"Ошибка добавления улицы: {e}")

    await state.set_state(StreetManagement.managing_streets)


# Обработчик удаления улицы
@dp.message(StreetManagement.managing_streets, F.text == "🗑️ Удалить улицу")
async def remove_street_start(message: types.Message, state: FSMContext):
    data = await state.get_data()
    city_name = data.get('city_name')

    async with aiosqlite.connect("database.db") as db:
        cursor = await db.execute(
            "SELECT street_name FROM streets WHERE city_name = ? ORDER BY street_name",
            (city_name,)
        )
        streets = await cursor.fetchall()

    if not streets:
        await message.answer("В вашем городе нет улиц для удаления.")
        return

    # Создаем клавиатуру с улицами для удаления
    builder = ReplyKeyboardBuilder()
    for street in streets:
        builder.button(text=street[0])
    builder.button(text="↩ Назад")
    builder.adjust(2)

    await message.answer(
        "Выберите улицу для удаления:",
        reply_markup=builder.as_markup(resize_keyboard=True)
    )
    await state.set_state(StreetManagement.remove_street_select)


# Обработчик выбора улицы для удаления
@dp.message(ChangeAddress.choose_street)
async def process_street_selection(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    selected_street = message.text.strip()

    if selected_street == "❌ Отмена":
        await message.answer("Смена адреса отменена.")
        await state.clear()
        await show_main_menu(message, user_id)
        return

    if selected_street == "➕ Создать новую улицу":
        await message.answer(
            "Введите название новой улицы (от 2 до 85 символов):\n"
            "Или напишите 'Отмена' для отмены",
            reply_markup=ReplyKeyboardRemove()
        )
        await state.set_state(ChangeAddress.create_new_street)
        return

    data = await state.get_data()
    city_name = data.get('selected_city')

    if not city_name:
        await message.answer("❌ Ошибка: город не выбран. Начните заново.")
        await state.clear()
        return

    # Проверяем существование улицы
    async with aiosqlite.connect("database.db") as db:
        cursor = await db.execute(
            "SELECT 1 FROM streets WHERE city_name = ? AND street_name = ?",
            (city_name, selected_street)
        )
        if not await cursor.fetchone():
            # Попробуем найти похожие улицы
            cursor = await db.execute(
                "SELECT street_name FROM streets WHERE city_name = ? AND street_name LIKE ?",
                (city_name, f"%{selected_street}%")
            )
            similar_streets = await cursor.fetchall()

            if similar_streets:
                response = f"❌ Улица '{selected_street}' не найдена в городе {city_name}.\n\n"
                response += "Возможно вы имели в виду:\n"
                for similar_street in similar_streets[:5]:
                    response += f"• {similar_street[0]}\n"
                await message.answer(response)
            else:
                await message.answer(f"❌ Улица '{selected_street}' не найдена в городе {city_name}.")
            return

    # Сохраняем выбранную улицу
    await state.update_data(selected_street=selected_street)

    # Переходим к выбору дома
    await show_houses_for_street(message, state, city_name, selected_street)


# Обработчик подтверждения удаления улицы
@dp.message(StreetManagement.confirm_street_deletion, F.text == "Подтвердить")
async def confirm_street_deletion(message: types.Message, state: FSMContext):
    data = await state.get_data()
    city_name = data.get('city_name')
    street_name = data.get('selected_street')

    async with aiosqlite.connect("database.db") as db:
        try:
            await db.execute(
                "DELETE FROM streets WHERE city_name = ? AND street_name = ?",
                (city_name, street_name)
            )
            await db.commit()

            await message.answer(
                f"✅ Улица '{street_name}' успешно удалена!",
                reply_markup=await streets_menu_kb(city_name)
            )

        except Exception as e:
            await message.answer("❌ Произошла ошибка при удалении улицы.")
            print(f"Ошибка удаления улицы: {e}")

    await state.set_state(StreetManagement.managing_streets)


# Обработчик отмены удаления улицы
@dp.message(StreetManagement.confirm_street_deletion, F.text == "Отмена")
async def cancel_street_deletion(message: types.Message, state: FSMContext):
    data = await state.get_data()
    city_name = data.get('city_name')

    await message.answer(
        "Удаление улицы отменено.",
        reply_markup=await streets_menu_kb(city_name)
    )
    await state.set_state(StreetManagement.managing_streets)


# Обработчик возврата из управления улицами
@dp.message(StreetManagement.managing_streets, F.text == "↩ Назад к управлению городом")
async def back_from_streets_management(message: types.Message, state: FSMContext):
    data = await state.get_data()
    city_name = data.get('city_name')

    await message.answer(
        f"🏙 Возврат к управлению городом {city_name}",
        reply_markup=await mayor_menu_kb(city_name)
    )
    await state.set_state(MayorMenu.managing_city)


# Также добавьте обработчики отмены для всех состояний управления улицами
@dp.message(StreetManagement.add_street_name, F.text == "Отмена")
async def cancel_street_addition(message: types.Message, state: FSMContext):
    data = await state.get_data()
    city_name = data.get('city_name')

    await message.answer(
        "Добавление улицы отменено.",
        reply_markup=await streets_menu_kb(city_name)
    )
    await state.set_state(StreetManagement.managing_streets)


# Обработчик входа в управление домами
@dp.message(StreetManagement.managing_streets, F.text == "🏠 Управление домами")
async def manage_houses_start(message: types.Message, state: FSMContext):
    data = await state.get_data()
    city_name = data.get('city_name')

    async with aiosqlite.connect("database.db") as db:
        cursor = await db.execute(
            "SELECT street_name FROM streets WHERE city_name = ? ORDER BY street_name",
            (city_name,)
        )
        streets = await cursor.fetchall()

    if not streets:
        await message.answer("В вашем городе пока нет улиц. Сначала добавьте улицы.")
        return

    # Создаем клавиатуру с улицами для выбора
    builder = ReplyKeyboardBuilder()
    for street in streets:
        builder.button(text=street[0])
    builder.button(text="↩ Назад")
    builder.adjust(2)

    await message.answer(
        "Выберите улицу для управления домами:",
        reply_markup=builder.as_markup(resize_keyboard=True)
    )
    await state.set_state(HouseManagement.select_street)


# Обработчик выбора улицы для управления домами
@dp.message(HouseManagement.select_street)
async def process_street_for_houses(message: types.Message, state: FSMContext):
    if message.text == "↩ Назад":
        data = await state.get_data()
        city_name = data.get('city_name')
        await message.answer(
            "Возврат в меню управления улицами.",
            reply_markup=await streets_menu_kb(city_name)
        )
        await state.set_state(StreetManagement.managing_streets)
        return

    street_name = message.text.strip()
    data = await state.get_data()
    city_name = data.get('city_name')

    # Проверяем существование улицы
    async with aiosqlite.connect("database.db") as db:
        cursor = await db.execute(
            "SELECT 1 FROM streets WHERE city_name = ? AND street_name = ?",
            (city_name, street_name)
        )
        if not await cursor.fetchone():
            await message.answer("Улица не найдена. Пожалуйста, выберите из списка.")
            return

    await state.update_data(selected_street=street_name)

    await message.answer(
        f"🏘️ Управление домами на улице {street_name}",
        reply_markup=await houses_menu_kb(city_name, street_name)
    )
    await state.set_state(HouseManagement.managing_houses)


# Обработчик показа домов на улице
@dp.message(HouseManagement.managing_houses, F.text.startswith("🏠 Дома на"))
async def show_houses_on_street(message: types.Message, state: FSMContext):
    data = await state.get_data()
    city_name = data.get('city_name')
    street_name = data.get('selected_street')

    async with aiosqlite.connect("database.db") as db:
        cursor = await db.execute("""
            SELECT h.house_number, u.name, h.created_date 
            FROM houses h 
            LEFT JOIN users u ON h.created_by = u.user_id 
            WHERE h.city_name = ? AND h.street_name = ? 
            ORDER BY 
                CAST(h.house_number AS INTEGER),
                h.house_number
        """, (city_name, street_name))
        houses = await cursor.fetchall()

    if not houses:
        await message.answer(f"На улице {street_name} пока нет домов.")
        return

    response = f"🏠 Дома на улице {street_name}:\n\n"
    for idx, (house_number, creator_name, created_date) in enumerate(houses, 1):
        response += f"{idx}. Дом {house_number}"
        if creator_name:
            response += f" (добавлен: {creator_name})"
        response += f"\n   📅 {created_date[:10]}\n"

    response += f"\nВсего домов: {len(houses)}"

    await message.answer(response)


# Обработчик добавления дома
@dp.message(HouseManagement.managing_houses, F.text == "➕ Добавить дом")
async def add_house_start(message: types.Message, state: FSMContext):
    await message.answer(
        "Введите номер дома (можно использовать цифры, буквы, дроби):\n"
        "Примеры: 1, 2А, 3/1, 4Б, 5-7\n"
        "Или напишите 'Отмена' для отмены",
        reply_markup=ReplyKeyboardRemove()
    )
    await state.set_state(HouseManagement.add_house_number)


# Обработчик ввода номера дома
@dp.message(HouseManagement.add_house_number)
async def process_house_number(message: types.Message, state: FSMContext):
    house_number = message.text.strip()

    if message.text == "Отмена":
        data = await state.get_data()
        city_name = data.get('city_name')
        street_name = data.get('selected_street')
        await message.answer(
            "Добавление дома отменено.",
            reply_markup=await houses_menu_kb(city_name, street_name)
        )
        await state.set_state(HouseManagement.managing_houses)
        return

    # Валидация номера дома
    if len(house_number) < 1 or len(house_number) > 10:
        await message.answer("Номер дома должен быть от 1 до 10 символов. Попробуйте еще раз:")
        return

    if not re.match(r"^[a-zA-Zа-яА-Я0-9\/\-\.\s]+$", house_number):
        await message.answer("Номер дома может содержать только буквы, цифры, пробелы, дроби (/), дефисы и точки.")
        return

    data = await state.get_data()
    city_name = data.get('city_name')
    street_name = data.get('selected_street')
    user_id = message.from_user.id

    async with aiosqlite.connect("database.db") as db:
        try:
            # Проверяем, существует ли уже такой дом на этой улице
            cursor = await db.execute(
                "SELECT 1 FROM houses WHERE city_name = ? AND street_name = ? AND house_number = ?",
                (city_name, street_name, house_number)
            )
            if await cursor.fetchone():
                await message.answer("Дом с таким номером уже существует на этой улице.")
                return

            # Добавляем новый дом
            await db.execute(
                "INSERT INTO houses (city_name, street_name, house_number, created_by) VALUES (?, ?, ?, ?)",
                (city_name, street_name, house_number, user_id)
            )
            await db.commit()

            await message.answer(
                f"✅ Дом №{house_number} успешно добавлен на улицу {street_name}!",
                reply_markup=await houses_menu_kb(city_name, street_name)
            )

        except Exception as e:
            await message.answer("❌ Произошла ошибка при добавлении дома. Попробуйте позже.")
            print(f"Ошибка добавления дома: {e}")

    await state.set_state(HouseManagement.managing_houses)


# Обработчик удаления дома
@dp.message(HouseManagement.managing_houses, F.text == "🗑️ Удалить дом")
async def remove_house_start(message: types.Message, state: FSMContext):
    data = await state.get_data()
    city_name = data.get('city_name')
    street_name = data.get('selected_street')

    async with aiosqlite.connect("database.db") as db:
        cursor = await db.execute(
            "SELECT house_number FROM houses WHERE city_name = ? AND street_name = ? ORDER BY house_number",
            (city_name, street_name)
        )
        houses = await cursor.fetchall()

    if not houses:
        await message.answer("На этой улице нет домов для удаления.")
        return

    # Создаем клавиатуру с домами для удаления
    builder = ReplyKeyboardBuilder()
    for house in houses:
        builder.button(text=house[0])
    builder.button(text="↩ Назад")
    builder.adjust(2)

    await message.answer(
        f"Выберите дом для удаления с улицы {street_name}:",
        reply_markup=builder.as_markup(resize_keyboard=True)
    )
    await state.set_state(HouseManagement.remove_house_select)


# Обработчик выбора дома для удаления
@dp.message(HouseManagement.remove_house_select)
async def process_house_selection(message: types.Message, state: FSMContext):
    if message.text == "↩ Назад":
        data = await state.get_data()
        city_name = data.get('city_name')
        street_name = data.get('selected_street')
        await message.answer(
            "Возврат в меню управления домами.",
            reply_markup=await houses_menu_kb(city_name, street_name)
        )
        await state.set_state(HouseManagement.managing_houses)
        return

    house_number = message.text.strip()
    data = await state.get_data()
    city_name = data.get('city_name')
    street_name = data.get('selected_street')

    # Проверяем существование дома
    async with aiosqlite.connect("database.db") as db:
        cursor = await db.execute(
            "SELECT 1 FROM houses WHERE city_name = ? AND street_name = ? AND house_number = ?",
            (city_name, street_name, house_number)
        )
        if not await cursor.fetchone():
            await message.answer("Дом не найден. Пожалуйста, выберите из списка.")
            return

    await state.update_data(selected_house=house_number)

    await message.answer(
        f"⚠️ Вы уверены, что хотите удалить дом №{house_number} с улицы {street_name}?\n"
        "Это действие нельзя отменить!",
        reply_markup=confirm_kb()
    )
    await state.set_state(HouseManagement.confirm_house_deletion)


# Обработчик подтверждения удаления дома
@dp.message(HouseManagement.confirm_house_deletion, F.text == "Подтвердить")
async def confirm_house_deletion(message: types.Message, state: FSMContext):
    data = await state.get_data()
    city_name = data.get('city_name')
    street_name = data.get('selected_street')
    house_number = data.get('selected_house')

    async with aiosqlite.connect("database.db") as db:
        try:
            await db.execute(
                "DELETE FROM houses WHERE city_name = ? AND street_name = ? AND house_number = ?",
                (city_name, street_name, house_number)
            )
            await db.commit()

            await message.answer(
                f"✅ Дом №{house_number} успешно удален с улицы {street_name}!",
                reply_markup=await houses_menu_kb(city_name, street_name)
            )

        except Exception as e:
            await message.answer("❌ Произошла ошибка при удалении дома.")
            print(f"Ошибка удаления дома: {e}")

    await state.set_state(HouseManagement.managing_houses)


# Обработчик отмены удаления дома
@dp.message(HouseManagement.confirm_house_deletion, F.text == "Отмена")
async def cancel_house_deletion(message: types.Message, state: FSMContext):
    data = await state.get_data()
    city_name = data.get('city_name')
    street_name = data.get('selected_street')

    await message.answer(
        "Удаление дома отменено.",
        reply_markup=await houses_menu_kb(city_name, street_name)
    )
    await state.set_state(HouseManagement.managing_houses)


# Обработчик возврата из управления домами
@dp.message(HouseManagement.managing_houses, F.text == "↩ Назад к улицам")
async def back_from_houses_management(message: types.Message, state: FSMContext):
    data = await state.get_data()
    city_name = data.get('city_name')

    await message.answer(
        f"🏘️ Возврат к управлению улицами города {city_name}",
        reply_markup=await streets_menu_kb(city_name)
    )
    await state.set_state(StreetManagement.managing_streets)


# Также добавьте обработчики отмены для всех состояний управления домами
@dp.message(HouseManagement.add_house_number, F.text == "Отмена")
async def cancel_house_addition(message: types.Message, state: FSMContext):
    data = await state.get_data()
    city_name = data.get('city_name')
    street_name = data.get('selected_street')

    await message.answer(
        "Добавление дома отменено.",
        reply_markup=await houses_menu_kb(city_name, street_name)
    )
    await state.set_state(HouseManagement.managing_houses)


# Обновите функцию показа улиц, чтобы отображать количество домов
@dp.message(StreetManagement.managing_streets, F.text == "📋 Список улиц")
async def show_streets_list_with_houses(message: types.Message, state: FSMContext):
    data = await state.get_data()
    city_name = data.get('city_name')

    async with aiosqlite.connect("database.db") as db:
        cursor = await db.execute("""
            SELECT s.street_name, u.name, COUNT(h.id) as house_count
            FROM streets s 
            LEFT JOIN users u ON s.created_by = u.user_id 
            LEFT JOIN houses h ON s.city_name = h.city_name AND s.street_name = h.street_name
            WHERE s.city_name = ? 
            GROUP BY s.street_name
            ORDER BY s.street_name
        """, (city_name,))
        streets = await cursor.fetchall()

    if not streets:
        await message.answer("В вашем городе пока нет зарегистрированных улиц.")
        return

    response = f"🏘️ Список улиц города {city_name}:\n\n"
    for idx, (street_name, creator_name, house_count) in enumerate(streets, 1):
        response += f"{idx}. {street_name}"
        if creator_name:
            response += f" (добавлена: {creator_name})"
        response += f" - 🏠 {house_count} домов\n"

    response += f"\nВсего улиц: {len(streets)}"

    await message.answer(response)


# Обработчик возврата из управления достопримечательностями
@dp.message(MayorMenu.managing_attractions, F.text == "↩ Назад к управлению городом")
async def back_from_attractions_management(message: types.Message, state: FSMContext):
    data = await state.get_data()
    city_name = data.get('city_name')

    await message.answer(
        f"🏙 Возврат к управлению городом {city_name}",
        reply_markup=await mayor_menu_kb(city_name)
    )
    await state.set_state(MayorMenu.managing_city)


# Обработчик входа в управление достопримечательностями
@dp.message(MayorMenu.managing_city, F.text.startswith("🏛️ Управление достопримечательностями"))
async def manage_attractions_start(message: types.Message, state: FSMContext):
    data = await state.get_data()
    city_name = data.get('city_name')

    await message.answer(
        f"🏛️ Управление достопримечательностями города {city_name}",
        reply_markup=await attractions_menu_kb(city_name)
    )
    await state.set_state(MayorMenu.managing_attractions)


# Обработчик показа списка достопримечательностей
# Улучшенная версия с пагинацией
@dp.message(MayorMenu.managing_attractions, F.text.startswith("🏛️ Список достопримечательностей"))
async def show_attractions_list(message: types.Message, state: FSMContext):
    data = await state.get_data()
    city_name = data.get('city_name')

    # Получаем номер страницы из данных состояния (если есть)
    page = data.get('attractions_page', 1)
    page_size = 5  # Количество достопримечательностей на странице

    async with aiosqlite.connect("database.db") as db:
        # Получаем общее количество
        cursor = await db.execute("SELECT COUNT(*) FROM attractions WHERE city_name = ?", (city_name,))
        total_count = (await cursor.fetchone())[0]

        if total_count == 0:
            await message.answer(
                f"🏛️ В городе {city_name} пока нет достопримечательностей.\n\n"
                f"Добавьте первую достопримечательность, чтобы привлечь туристов!"
            )
            return

        # Рассчитываем смещение для пагинации
        offset = (page - 1) * page_size
        total_pages = (total_count + page_size - 1) // page_size  # Округление вверх

        # Получаем достопримечательности для текущей страницы
        cursor = await db.execute("""
            SELECT a.name, a.description, a.type, u.name as creator_name, 
                   a.created_date
            FROM attractions a 
            LEFT JOIN users u ON a.created_by = u.user_id 
            WHERE a.city_name = ? 
            ORDER BY a.created_date DESC
            LIMIT ? OFFSET ?
        """, (city_name, page_size, offset))
        attractions = await cursor.fetchall()

        # Получаем статистику по типам
        cursor = await db.execute("""
            SELECT type, COUNT(*) as count
            FROM attractions 
            WHERE city_name = ?
            GROUP BY type
            ORDER BY count DESC
        """, (city_name,))
        type_stats = await cursor.fetchall()

    response = f"🏛️ <b>Достопримечательности города {city_name}</b>\n"
    response += f"📄 Страница {page} из {total_pages}\n\n"

    if not attractions:
        await message.answer("Нет достопримечательностей для отображения на этой странице.")
        return

    for idx, (name, description, attraction_type, creator_name, created_date) in enumerate(attractions, 1):
        item_num = offset + idx
        response += f"<b>{item_num}. {name}</b>\n"
        response += f"   📍 Тип: {attraction_type}\n"
        if description:
            short_desc = description[:120] + "..." if len(description) > 80 else description
            response += f"   📝 Описание: {short_desc}\n"

    # Добавляем статистику
    response += f"\n📊 <b>Статистика:</b>\n"
    response += f"   🏛️ Всего: {total_count} достопримечательностей\n"

    if type_stats:
        response += f"   📈 По типам:\n"
        for attraction_type, count in type_stats:
            response += f"      • {attraction_type}: {count}\n"

    # Создаем клавиатуру для навигации
    keyboard_buttons = []

    # Кнопки пагинации
    if total_pages > 1:
        row = []
        if page > 1:
            row.append(KeyboardButton(text="⬅️ Предыдущая страница"))
        if page < total_pages:
            row.append(KeyboardButton(text="➡️ Следующая страница"))
        if row:
            keyboard_buttons.append(row)

    keyboard_buttons.append([KeyboardButton(text="↩ Назад к управлению достопримечательностями")])

    # Сохраняем текущую страницу в состоянии
    await state.update_data(
        attractions_page=page,
        total_attractions_pages=total_pages,
        total_attractions_count=total_count
    )

    await message.answer(
        response,
        parse_mode="HTML",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=keyboard_buttons,
            resize_keyboard=True
        )
    )


# Обработчики для пагинации
@dp.message(MayorMenu.managing_attractions, F.text == "⬅️ Предыдущая страница")
async def prev_attractions_page(message: types.Message, state: FSMContext):
    data = await state.get_data()
    current_page = data.get('attractions_page', 1)

    if current_page > 1:
        await state.update_data(attractions_page=current_page - 1)
        await show_attractions_list(message, state)
    else:
        await message.answer("Вы уже на первой странице.")


@dp.message(MayorMenu.managing_attractions, F.text == "➡️ Следующая страница")
async def next_attractions_page(message: types.Message, state: FSMContext):
    data = await state.get_data()
    current_page = data.get('attractions_page', 1)
    total_pages = data.get('total_attractions_pages', 1)

    if current_page < total_pages:
        await state.update_data(attractions_page=current_page + 1)
        await show_attractions_list(message, state)
    else:
        await message.answer("Вы уже на последней странице.")


@dp.message(MayorMenu.managing_attractions, F.text == "↩ Назад к управлению достопримечательностями")
async def back_to_attractions_menu(message: types.Message, state: FSMContext):
    data = await state.get_data()
    city_name = data.get('city_name')

    # Очищаем данные пагинации
    await state.update_data(
        attractions_page=None,
        total_attractions_pages=None,
        total_attractions_count=None
    )

    await message.answer(
        f"🏛️ Возврат к управлению достопримечательностями {city_name}",
        reply_markup=await attractions_menu_kb(city_name)
    )


# Обработчик добавления достопримечательности
@dp.message(MayorMenu.managing_attractions, F.text == "➕ Добавить достопримечательность")
async def add_attraction_start(message: types.Message, state: FSMContext):
    await message.answer(
        "Введите название достопримечательности (от 2 до 100 символов):\n"
        "Примеры: 'Шуйский собор', 'Памятник Петру I', 'Центральный парк'\n"
        "Или напишите 'Отмена' для отмены",
        reply_markup=ReplyKeyboardRemove()
    )
    await state.set_state(MayorMenu.add_attraction_name)


# Обработчик ввода названия достопримечательности
@dp.message(MayorMenu.add_attraction_name)
async def process_attraction_name(message: types.Message, state: FSMContext):
    attraction_name = message.text.strip()

    if attraction_name.lower() == "отмена":
        data = await state.get_data()
        city_name = data.get('city_name')
        await message.answer(
            "Добавление достопримечательности отменено.",
            reply_markup=await attractions_menu_kb(city_name)
        )
        await state.set_state(MayorMenu.managing_attractions)
        return

    # Валидация названия
    if len(attraction_name) < 2 or len(attraction_name) > 100:
        await message.answer("Название должно быть от 2 до 100 символов. Попробуйте еще раз:")
        return

    if not re.match(r"^[a-zA-Zа-яА-Я0-9\s\-\.\',!]+$", attraction_name):
        await message.answer("Название может содержать только буквы, цифры, пробелы, дефисы, точки и запятые.")
        return

    data = await state.get_data()
    city_name = data.get('city_name')

    # Проверяем уникальность названия в городе
    async with aiosqlite.connect("database.db") as db:
        cursor = await db.execute(
            "SELECT 1 FROM attractions WHERE city_name = ? AND name = ?",
            (city_name, attraction_name)
        )
        if await cursor.fetchone():
            await message.answer("Достопримечательность с таким названием уже существует в вашем городе.")
            return

    await state.update_data(attraction_name=attraction_name)

    await message.answer(
        f"Название: {attraction_name}\n\n"
        "Введите описание достопримечательности (до 500 символов):\n"
        "Или напишите 'пропустить' если не хотите добавлять описание"
    )
    await state.set_state(MayorMenu.add_attraction_description)


# Обработчик ввода описания достопримечательности
@dp.message(MayorMenu.add_attraction_description)
async def process_attraction_description(message: types.Message, state: FSMContext):
    description = message.text.strip()

    if description.lower() == "пропустить":
        description = None
    elif len(description) > 500:
        await message.answer("Описание слишком длинное. Максимум 500 символов.")
        return

    await state.update_data(attraction_description=description)

    await message.answer(
        "Выберите тип достопримечательности:",
        reply_markup=attraction_types_kb()
    )
    await state.set_state(MayorMenu.add_attraction_type)


# Обработчик выбора типа достопримечательности
@dp.message(MayorMenu.add_attraction_type)
async def process_attraction_type(message: types.Message, state: FSMContext):
    attraction_type = message.text

    if attraction_type == "❌ Отмена":
        data = await state.get_data()
        city_name = data.get('city_name')
        await message.answer(
            "Добавление достопримечательности отменено.",
            reply_markup=await attractions_menu_kb(city_name)
        )
        await state.set_state(MayorMenu.managing_attractions)
        return

    if attraction_type not in ATTRACTION_TYPES:
        await message.answer("Пожалуйста, выберите тип из списка:", reply_markup=attraction_types_kb())
        return

    data = await state.get_data()
    attraction_name = data.get('attraction_name')
    description = data.get('attraction_description')
    city_name = data.get('city_name')
    user_id = message.from_user.id

    # Сохраняем достопримечательность в базу
    async with aiosqlite.connect("database.db") as db:
        try:
            await db.execute(
                "INSERT INTO attractions (city_name, name, description, type, created_by) VALUES (?, ?, ?, ?, ?)",
                (city_name, attraction_name, description, attraction_type, user_id)
            )
            await db.commit()

            response = f"✅ Достопримечательность успешно добавлена!\n\n"
            response += f"🏛️ <b>{attraction_name}</b>\n"
            response += f"📍 Тип: {attraction_type}\n"
            if description:
                response += f"📝 Описание: {description}\n"
            response += f"🏙️ Город: {city_name}\n"

            await message.answer(response, parse_mode="HTML", reply_markup=await attractions_menu_kb(city_name))

            # Уведомляем жителей города о новой достопримечательности
            await notify_citizens_about_new_attraction(city_name, attraction_name, attraction_type, user_id)

        except Exception as e:
            await message.answer("❌ Произошла ошибка при добавлении достопримечательности.")
            print(f"Ошибка добавления достопримечательности: {e}")

    await state.set_state(MayorMenu.managing_attractions)


# Функция уведомления жителей о новой достопримечательности
async def notify_citizens_about_new_attraction(city_name: str, attraction_name: str, attraction_type: str,
                                               mayor_id: int):
    """Уведомляет жителей города о новой достопримечательности"""
    try:
        async with aiosqlite.connect("database.db") as db:
            cursor = await db.execute(
                "SELECT user_id FROM users WHERE city = ? AND user_id != ?",
                (city_name, mayor_id)
            )
            citizens = await cursor.fetchall()

        notification_count = 0
        for (user_id,) in citizens:
            try:
                await bot.send_message(
                    user_id,
                    f"🎉 <b>Новая достопримечательность в {city_name}!</b>\n\n"
                    f"🏛️ {attraction_name}\n"
                    f"📍 Тип: {attraction_type}\n\n"
                    f"Ваш город становится еще интереснее!",
                    parse_mode="HTML"
                )
                notification_count += 1
                await asyncio.sleep(0.1)  # Чтобы не превысить лимиты Telegram
            except Exception as e:
                # Игнорируем ошибки (пользователь заблокировал бота и т.д.)
                pass

        print(f"Уведомлено {notification_count} жителей о новой достопримечательности в {city_name}")

    except Exception as e:
        print(f"Ошибка уведомления жителей: {e}")


# Обработчик удаления достопримечательности
@dp.message(MayorMenu.managing_attractions, F.text == "🗑️ Удалить достопримечательность")
async def remove_attraction_start(message: types.Message, state: FSMContext):
    data = await state.get_data()
    city_name = data.get('city_name')

    async with aiosqlite.connect("database.db") as db:
        cursor = await db.execute(
            "SELECT id, name, type FROM attractions WHERE city_name = ? ORDER BY name",
            (city_name,)
        )
        attractions = await cursor.fetchall()

    if not attractions:
        await message.answer("В вашем городе нет достопримечательностей для удаления.")
        return

    # Создаем клавиатуру с достопримечательностями для удаления
    builder = ReplyKeyboardBuilder()
    for attraction_id, attraction_name, attraction_type in attractions:
        builder.button(text=f"{attraction_name} ({attraction_type})")
    builder.button(text="↩ Назад")
    builder.adjust(1)  # По одной кнопке в строке

    await message.answer(
        "Выберите достопримечательность для удаления:",
        reply_markup=builder.as_markup(resize_keyboard=True)
    )

    # Сохраняем список достопримечательностей во временные данные
    await state.update_data(attractions_list=[(aid, aname, atype) for aid, aname, atype in attractions])
    # Состояние останется MayorMenu.managing_attractions, но будем проверять текст


# Обработчик выбора достопримечательности для удаления
@dp.message(MayorMenu.managing_attractions)
async def process_attraction_selection_for_deletion(message: types.Message, state: FSMContext):
    if message.text == "↩ Назад":
        data = await state.get_data()
        city_name = data.get('city_name')
        await message.answer(
            "Возврат в меню управления достопримечательностями.",
            reply_markup=await attractions_menu_kb(city_name)
        )
        return

    data = await state.get_data()
    attractions_list = data.get('attractions_list', [])
    city_name = data.get('city_name')

    # Находим выбранную достопримечательность
    selected_attraction = None
    for attraction_id, attraction_name, attraction_type in attractions_list:
        if message.text == f"{attraction_name} ({attraction_type})":
            selected_attraction = (attraction_id, attraction_name, attraction_type)
            break

    if not selected_attraction:
        await message.answer("Достопримечательность не найдена. Пожалуйста, выберите из списка.")
        return

    attraction_id, attraction_name, attraction_type = selected_attraction

    await state.update_data(
        selected_attraction_id=attraction_id,
        selected_attraction_name=attraction_name,
        selected_attraction_type=attraction_type
    )

    await message.answer(
        f"⚠️ Вы уверены, что хотите удалить достопримечательность?\n\n"
        f"🏛️ <b>{attraction_name}</b>\n"
        f"📍 Тип: {attraction_type}\n\n"
        f"Это действие нельзя отменить!",
        parse_mode="HTML",
        reply_markup=confirm_kb()
    )
    await state.set_state(MayorMenu.confirm_attraction_deletion)


# Обработчик подтверждения удаления достопримечательности
@dp.message(MayorMenu.confirm_attraction_deletion, F.text == "Подтвердить")
async def confirm_attraction_deletion(message: types.Message, state: FSMContext):
    data = await state.get_data()
    attraction_id = data.get('selected_attraction_id')
    attraction_name = data.get('selected_attraction_name')
    city_name = data.get('city_name')
    user_id = message.from_user.id

    async with aiosqlite.connect("database.db") as db:
        try:
            # Проверяем, что пользователь является мэром этого города
            cursor = await db.execute(
                "SELECT 1 FROM cities WHERE name = ? AND mayor_id = ?",
                (city_name, user_id)
            )
            if not await cursor.fetchone():
                await message.answer("❌ Вы не можете удалить эту достопримечательность.")
                await state.clear()
                return

            # Удаляем достопримечательность
            await db.execute(
                "DELETE FROM attractions WHERE id = ? AND city_name = ?",
                (attraction_id, city_name)
            )
            await db.commit()

            await message.answer(
                f"✅ Достопримечательность '{attraction_name}' успешно удалена!",
                reply_markup=await attractions_menu_kb(city_name)
            )

        except Exception as e:
            await message.answer("❌ Произошла ошибка при удалении достопримечательности.")
            print(f"Ошибка удаления достопримечательности: {e}")

    await state.set_state(MayorMenu.managing_attractions)


# Обработчик отмены удаления достопримечательности
@dp.message(MayorMenu.confirm_attraction_deletion, F.text == "Отмена")
async def cancel_attraction_deletion(message: types.Message, state: FSMContext):
    data = await state.get_data()
    city_name = data.get('city_name')

    await message.answer(
        "Удаление достопримечательности отменено.",
        reply_markup=await attractions_menu_kb(city_name)
    )
    await state.set_state(MayorMenu.managing_attractions)


# Команда для просмотра всех достопримечательностей города
@dp.message(Command("attractions"))
async def show_all_attractions(message: types.Message):
    """Показывает все достопримечательности города"""
    args = message.text.split()

    if len(args) < 2:
        # Если город не указан, показываем достопримечательности города пользователя
        user_id = message.from_user.id
        async with aiosqlite.connect("database.db") as db:
            cursor = await db.execute(
                "SELECT city FROM users WHERE user_id = ?",
                (user_id,)
            )
            user_city = await cursor.fetchone()

        if not user_city or not user_city[0]:
            await message.answer(
                "Использование: /attractions <название города>\n"
                "Или добавьте город в свой профиль для просмотра достопримечательностей своего города."
            )
            return

        city_name = user_city[0]
    else:
        city_name = " ".join(args[1:])

    async with aiosqlite.connect("database.db") as db:
        # Проверяем существование города
        cursor = await db.execute(
            "SELECT 1 FROM cities WHERE name = ?",
            (city_name,)
        )
        if not await cursor.fetchone():
            await message.answer(f"❌ Город '{city_name}' не найден.")
            return

        # Получаем все достопримечательности
        cursor = await db.execute("""
            SELECT a.name, a.description, a.type, u.name as creator_name, 
                   a.created_date
            FROM attractions a 
            LEFT JOIN users u ON a.created_by = u.user_id 
            WHERE a.city_name = ? 
            ORDER BY 
                CASE a.type 
                    WHEN '🌳 классические фермы' THEN 1
                    WHEN '🎨 Искусство' THEN 2
                    WHEN '🌳 Природа' THEN 3
                    WHEN '🎭 Культура' THEN 4
                    WHEN '🎪 Развлечения' THEN 5
                    WHEN '🏭 автофермы' THEN 6
                    WHEN '🏛️ гос здания' THEN 7
                END,
                a.name
        """, (city_name,))

        attractions = await cursor.fetchall()

        # Получаем статистику
        cursor = await db.execute("""
            SELECT COUNT(*) as total_count,
                   COUNT(DISTINCT type) as types_count
            FROM attractions 
            WHERE city_name = ?
        """, (city_name,))

        stats = await cursor.fetchone()

    if not attractions:
        await message.answer(f"🏛️ В городе {city_name} пока нет достопримечательностей.")
        return

    total_count, types_count = stats if stats else (0, 0)

    response = f"🏛️ <b>Достопримечательности города {city_name}</b>\n\n"
    response += f"📊 Всего: {total_count} достопримечательностей\n"
    response += f"📍 Разнообразие: {types_count} различных типов\n\n"

    current_type = None
    for name, description, attraction_type, creator_name, created_date in attractions:
        if attraction_type != current_type:
            response += f"\n<b>{attraction_type}:</b>\n"
            current_type = attraction_type

        response += f"• <b>{name}</b>\n"
        if description:
            short_desc = description[:80] + "..." if len(description) > 80 else description
            response += f"  {short_desc}\n"
        if creator_name:
            response += f"  👤 Добавил: {creator_name}\n"
        response += f"  📅 {created_date[:10]}\n"

    response += f"\n✨ Город {city_name} богат культурными и историческими объектами!"

    # Разбиваем сообщение если оно слишком длинное
    if len(response) > 4000:
        parts = []
        current_part = ""
        lines = response.split('\n')

        for line in lines:
            if len(current_part + line + '\n') < 4000:
                current_part += line + '\n'
            else:
                parts.append(current_part)
                current_part = line + '\n'

        if current_part:
            parts.append(current_part)

        for part in parts:
            await message.answer(part, parse_mode="HTML")
    else:
        await message.answer(response, parse_mode="HTML")


# Добавьте обработчик отмены для состояний добавления достопримечательности
@dp.message(MayorMenu.add_attraction_name, F.text.lower() == "отмена")
@dp.message(MayorMenu.add_attraction_description, F.text.lower() == "отмена")
@dp.message(MayorMenu.add_attraction_type, F.text.lower() == "отмена")
async def cancel_attraction_addition(message: types.Message, state: FSMContext):
    data = await state.get_data()
    city_name = data.get('city_name')

    await message.answer(
        "Добавление достопримечательности отменено.",
        reply_markup=await attractions_menu_kb(city_name)
    )
    await state.set_state(MayorMenu.managing_attractions)


# Добавьте функцию для получения полного адреса пользователя
async def get_user_address(user_id: int) -> str:
    """Получает полный адрес пользователя (город, улица, дом)"""
    async with aiosqlite.connect("database.db") as db:
        cursor = await db.execute("""
            SELECT u.city, u.street, u.house_number 
            FROM users u 
            WHERE u.user_id = ?
        """, (user_id,))
        user_data = await cursor.fetchone()

        if not user_data:
            return "Адрес не указан"

        city, street, house = user_data

        if not street or not house:
            return f"🏙️ {city} (адрес не полный)"

        return f"🏙️ {city}, 🏘️ {street}, 🏠 {house}"


# Обработчик выбора города
@dp.message(ChangeAddress.choose_city)
async def process_city_selection(message: types.Message, state: FSMContext):
    selected_city = message.text.strip()
    user_id = message.from_user.id

    if selected_city.lower() == "отмена":
        await message.answer("Смена адреса отменена.")
        await state.clear()
        await show_main_menu(message, user_id)
        return

    if selected_city == "🏙 Зарегистрировать новый город":
        await message.answer(
            "Введите название нового города (от 2 до 50 символов):\n"
            "Или напишите 'Отмена' для отмены",
            reply_markup=ReplyKeyboardRemove()
        )
        await state.set_state(ChangeAddress.enter_custom_city)
        return

    async with aiosqlite.connect("database.db") as db:
        # Проверяем существование города
        cursor = await db.execute(
            "SELECT mayor_id FROM cities WHERE name = ?",
            (selected_city,)
        )
        city_data = await cursor.fetchone()

        if not city_data:
            await message.answer("❌ Этот город не зарегистрирован. Выберите город из списка.")
            return

    # Получаем данные из состояния
    data = await state.get_data()
    change_type = data.get('change_type', 'full')

    # Сохраняем выбранный город в состоянии
    await state.update_data(selected_city=selected_city)

    if change_type == 'city_only':
        # Если меняем только город, получаем старые улицу и дом
        street = data.get('current_street')
        house = data.get('current_house')

        if street and house:
            # Есть полный адрес - отправляем на подтверждение мэру
            await request_mayor_approval(message, state, selected_city, street, house)
        else:
            # Нет полного адреса - просто меняем город
            async with aiosqlite.connect("database.db") as db:
                await db.execute(
                    "UPDATE users SET city = ? WHERE user_id = ?",
                    (selected_city, user_id)
                )
                await db.commit()

            await message.answer(
                f"✅ Город успешно изменён на {selected_city}!",
                reply_markup=await main_menu_kb(user_id)
            )
            await state.clear()
    else:
        # Меняем весь адрес - показываем улицы
        await show_streets_for_city(message, state, selected_city)

# Обработчик создания нового города
@dp.message(ChangeAddress.enter_custom_city)
async def process_new_city_input(message: types.Message, state: FSMContext):
    new_city_name = message.text.strip()
    user_id = message.from_user.id

    if new_city_name.lower() == "отмена":
        await message.answer("Смена адреса отменена.")
        await state.clear()
        await show_main_menu(message, user_id)
        return

    # Валидация названия города
    if len(new_city_name) < 2 or len(new_city_name) > 50:
        await message.answer("Название города должно быть от 2 до 50 символов. Попробуйте еще раз:")
        return

    if not re.match(r"^[a-zA-Zа-яА-Я0-9\s\-]+$", new_city_name):
        await message.answer("Название города может содержать только буквы, цифры, пробелы и дефисы.")
        return

    async with aiosqlite.connect("database.db") as db:
        try:
            # Проверяем, существует ли уже такой город
            cursor = await db.execute(
                "SELECT 1 FROM cities WHERE name = ?",
                (new_city_name,)
            )
            if await cursor.fetchone():
                await message.answer(
                    "❌ Город с таким названием уже существует. Выберите другое название."
                )
                return

            # Регистрируем новый город с пользователем как мэром
            await db.execute(
                "INSERT INTO cities (name, mayor_id, created_date) VALUES (?, ?, CURRENT_TIMESTAMP)",
                (new_city_name, user_id)
            )

            # Получаем данные из состояния
            data = await state.get_data()

            if 'current_street' in data and 'current_house' in data:
                # Меняем только город
                street = data['current_street']
                house = data['current_house']
                await db.execute(
                    "UPDATE users SET city = ?, street = ?, house_number = ? WHERE user_id = ?",
                    (new_city_name, street, house, user_id)
                )
            else:
                # Меняем весь адрес
                await db.execute(
                    "UPDATE users SET city = ? WHERE user_id = ?",
                    (new_city_name, user_id)
                )

            await db.commit()

            # Получаем дату создания для отображения
            cursor = await db.execute(
                "SELECT created_date FROM cities WHERE name = ?",
                (new_city_name,)
            )
            creation_date_result = await cursor.fetchone()
            creation_date = creation_date_result[0] if creation_date_result else "сегодня"

            await message.answer(
                f"✅ Город {new_city_name} успешно зарегистрирован!\n"
                f"📅 Дата основания: {creation_date[:10]}\n"
                f"👑 Вы стали первым мэром этого города.\n"
                f"🏙️ Ваш город изменён на {new_city_name}",
                reply_markup=await main_menu_kb(user_id)
            )

        except aiosqlite.IntegrityError:
            await message.answer("❌ Этот город уже существует. Выберите другое название.")
        except Exception as e:
            await message.answer("❌ Произошла ошибка при создании города.")
            print(f"Ошибка создания города: {e}")

    await state.clear()


# Функция показа улиц города
async def show_streets_for_city(message: types.Message, state: FSMContext, city_name: str):
    """Показывает улицы для выбранного города"""

    # Очищаем состояние для правильной работы
    await state.update_data(selected_city=city_name)

    async with aiosqlite.connect("database.db") as db:
        cursor = await db.execute(
            "SELECT street_name FROM streets WHERE city_name = ? ORDER BY street_name",
            (city_name,)
        )
        streets = await cursor.fetchall()

    if not streets:
        # Если улиц нет, предлагаем создать новую
        builder = ReplyKeyboardBuilder()
        builder.button(text="➕ Создать новую улицу")
        builder.button(text="❌ Отмена")
        builder.adjust(1)

        await message.answer(
            f"В городе {city_name} пока нет улиц.\n"
            "Хотите создать новую улицу?",
            reply_markup=builder.as_markup(resize_keyboard=True)
        )
        await state.set_state(ChangeAddress.create_new_street)
        return

    # Создаем клавиатуру с улицами
    builder = ReplyKeyboardBuilder()
    for street in streets:
        builder.button(text=street[0])
    builder.button(text="➕ Создать новую улицу")
    builder.button(text="❌ Отмена")
    builder.adjust(2)

    await message.answer(
        f"🏘️ Выберите улицу в городе {city_name}:",
        reply_markup=builder.as_markup(resize_keyboard=True)
    )
    await state.set_state(ChangeAddress.choose_street)


# Обработчик создания новой улицы
@dp.message(ChangeAddress.create_new_street)
async def process_create_new_street(message: types.Message, state: FSMContext):
    user_id = message.from_user.id

    if message.text == "❌ Отмена":
        await message.answer("Смена адреса отменена.")
        await state.clear()
        await show_main_menu(message, user_id)
        return

    if message.text == "➕ Создать новую улицу":
        await message.answer(
            "Введите название новой улицы (от 2 до 50 символов):\n"
            "Или напишите 'Отмена' для отмены",
            reply_markup=ReplyKeyboardRemove()
        )
        return

    # Обработка ввода названия улицы
    street_name = message.text.strip()

    if street_name.lower() == "отмена":
        await message.answer("Смена адреса отменена.")
        await state.clear()
        await show_main_menu(message, user_id)
        return

    # Валидация названия улицы
    if len(street_name) < 2 or len(street_name) > 50:
        await message.answer("Название улицы должно быть от 2 до 50 символов. Попробуйте еще раз:")
        return

    if not re.match(r"^[a-zA-Zа-яА-Я0-9\s\-\.]+$", street_name):
        await message.answer("Название улицы может содержать только буквы, цифры, пробелы, дефисы и точки.")
        return

    data = await state.get_data()
    city_name = data.get('selected_city')

    async with aiosqlite.connect("database.db") as db:
        try:
            # Проверяем, существует ли уже такая улица в городе
            cursor = await db.execute(
                "SELECT 1 FROM streets WHERE city_name = ? AND street_name = ?",
                (city_name, street_name)
            )
            if await cursor.fetchone():
                await message.answer("Улица с таким названием уже существует в этом городе.")
                return

            # Добавляем новую улицу
            await db.execute(
                "INSERT INTO streets (city_name, street_name, created_by) VALUES (?, ?, ?)",
                (city_name, street_name, user_id)
            )
            await db.commit()

            await message.answer(f"✅ Улица '{street_name}' успешно создана!")
            await state.update_data(selected_street=street_name)

            # Переходим к выбору дома
            await show_houses_for_street(message, state, city_name, street_name)

        except Exception as e:
            await message.answer("❌ Произошла ошибка при создании улицы. Попробуйте позже.")
            print(f"Ошибка создания улицы: {e}")

@dp.message(Marketplace.add_property_address)
async def process_property_address_choice(message: types.Message, state: FSMContext):
    """Обработчик выбора указания адреса для недвижимости"""
    if message.text == "❌ Нет, пропустить":
        # Пропускаем адрес, переходим к названию
        await message.answer(
            "Введите название недвижимости (максимум 100 символов):\n"
            "Или напишите 'отмена' для отмены",
            reply_markup=ReplyKeyboardRemove()
        )
        await state.set_state(Marketplace.add_title)
        return

    elif message.text == "↩ Отмена":
        await state.clear()
        await message.answer("Добавление товара отменено.", reply_markup=await marketplace_kb())
        return

    elif message.text == "✅ Да, указать адрес":
        await message.answer(
            "Введите город, где находится недвижимость:\n"
            "Или напишите 'отмена' для отмены",
            reply_markup=ReplyKeyboardRemove()
        )
        await state.set_state(Marketplace.enter_address_city)


@dp.message(Marketplace.enter_address_city)
async def process_address_city(message: types.Message, state: FSMContext):
    """Обработчик ввода города для адреса недвижимости"""
    if message.text.lower() == "отмена":
        await state.clear()
        await message.answer("Добавление товара отменено.", reply_markup=await marketplace_kb())
        return

    city = message.text.strip()
    if len(city) < 2 or len(city) > 50:
        await message.answer("Название города должно быть от 2 до 50 символов. Попробуйте еще раз:")
        return

    await state.update_data(property_city=city)
    await message.answer("Введите название улицы:\nИли напишите 'отмена' для отмены")
    await state.set_state(Marketplace.enter_address_street)


# Функция показа домов на улице
async def show_houses_for_street(message: types.Message, state: FSMContext, city_name: str, street_name: str):
    """Показывает дома на выбранной улице"""

    async with aiosqlite.connect("database.db") as db:
        cursor = await db.execute(
            "SELECT house_number FROM houses WHERE city_name = ? AND street_name = ? ORDER BY house_number",
            (city_name, street_name)
        )
        houses = await cursor.fetchall()

    if not houses:
        # Если домов нет, предлагаем создать новый
        builder = ReplyKeyboardBuilder()
        builder.button(text="➕ Создать новый дом")
        builder.button(text="❌ Отмена")
        builder.adjust(1)

        await message.answer(
            f"На улице {street_name} пока нет домов.\n"
            "Хотите создать новый дом?",
            reply_markup=builder.as_markup(resize_keyboard=True)
        )
        await state.set_state(ChangeAddress.choose_house)
        return

    # Создаем клавиатуру с домами
    builder = ReplyKeyboardBuilder()
    for house in houses:
        builder.button(text=house[0])
    builder.button(text="➕ Создать новый дом")
    builder.button(text="❌ Отмена")
    builder.adjust(2)

    await message.answer(
        f"🏠 Выберите дом на улице {street_name}:",
        reply_markup=builder.as_markup(resize_keyboard=True)
    )
    await state.set_state(ChangeAddress.choose_house)


# Обработчик выбора дома
@dp.message(ChangeAddress.choose_house)
async def process_house_selection(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    house_input = message.text.strip()

    if house_input == "❌ Отмена":
        await message.answer("Смена адреса отменена.")
        await state.clear()
        await show_main_menu(message, user_id)
        return

    if house_input == "➕ Создать новый дом":
        await message.answer(
            "Введите номер дома (можно использовать цифры, буквы, дроби):\n"
            "Примеры: 1, 2А, 3/1, 4Б, 5-7\n"
            "Или напишите 'Отмена' для отмены",
            reply_markup=ReplyKeyboardRemove()
        )
        return

    # Обработка ввода номера дома
    house_number = house_input

    # Валидация номера дома
    if len(house_number) < 1 or len(house_number) > 10:
        await message.answer("Номер дома должен быть от 1 до 10 символов. Попробуйте еще раз:")
        return

    if not re.match(r"^[a-zA-Zа-яА-Я0-9\/\-\.\s]+$", house_number):
        await message.answer("Номер дома может содержать только буквы, цифры, пробелы, дроби (/), дефисы и точки.")
        return

    data = await state.get_data()
    city_name = data.get('selected_city')
    street_name = data.get('selected_street')

    async with aiosqlite.connect("database.db") as db:
        try:
            # Проверяем, существует ли уже такой дом на этой улице
            cursor = await db.execute(
                "SELECT 1 FROM houses WHERE city_name = ? AND street_name = ? AND house_number = ?",
                (city_name, street_name, house_number)
            )

            if not await cursor.fetchone():
                # Создаем новый дом
                await db.execute(
                    "INSERT INTO houses (city_name, street_name, house_number, created_by) VALUES (?, ?, ?, ?)",
                    (city_name, street_name, house_number, user_id)
                )
                await db.commit()
                await message.answer(f"✅ Дом №{house_number} успешно создан!")

            await state.update_data(selected_house=house_number)

            # Переходим к подтверждению от мэра
            await request_mayor_approval(message, state, city_name, street_name, house_number)

        except Exception as e:
            await message.answer("❌ Произошла ошибка при выборе дома. Попробуйте позже.")
            print(f"Ошибка выбора дома: {e}")


# Функция запроса подтверждения от мэра
async def request_mayor_approval(message: types.Message, state: FSMContext, city_name: str, street_name: str, house_number: str):
    user_id = message.from_user.id

    try:
        async with aiosqlite.connect("database.db") as db:
            # Получаем ID мэра города
            cursor = await db.execute(
                "SELECT mayor_id FROM cities WHERE name = ?",
                (city_name,)
            )
            city_data = await cursor.fetchone()

            if not city_data or not city_data[0]:
                await complete_address_change(message, state, city_name, street_name, house_number)
                return

            mayor_id = city_data[0]

            if user_id == mayor_id:
                await message.answer(
                    f"👑 Вы являетесь мэром города {city_name}, "
                    f"поэтому переезд подтверждается автоматически."
                )
                await complete_address_change(message, state, city_name, street_name, house_number)
                return

            # Получаем информацию о пользователе
            cursor = await db.execute(
                "SELECT name, account_id FROM users WHERE user_id = ?",
                (user_id,)
            )
            user_data = await cursor.fetchone()
            user_name = user_data[0] if user_data else "Неизвестный пользователь"
            user_account = user_data[1] if user_data else "Неизвестный счет"

            old_city = await get_user_city(user_id)

            # Сохраняем запрос и получаем его ID
            cursor = await db.execute("""
                INSERT INTO city_change_requests 
                (user_id, old_city, new_city, street, house_number, status) 
                VALUES (?, ?, ?, ?, ?, 'pending')
            """, (user_id, old_city, city_name, street_name, house_number))

            await db.commit()

            # Получаем ID созданной записи
            request_id = cursor.lastrowid

        # Передаем request_id в callback_data
        builder = InlineKeyboardBuilder()
        builder.button(
            text="✅ Одобрить",
            callback_data=f"approve_address:{request_id}"
        )
        builder.button(
            text="❌ Отклонить",
            callback_data=f"reject_address:{request_id}"
        )
        builder.adjust(2)

        kb = builder.as_markup()

        await bot.send_message(
            mayor_id,
            f"🏠 Новый запрос на смену адреса в городе {city_name}:\n\n"
            f"👤 Пользователь: {user_name}\n"
            f"💳 Счет: {user_account}\n"
            f"📍 Старый город: {old_city}\n"
            f"📍 Новый адрес: {city_name}, {street_name}, {house_number}\n\n"
            f"Подтвердить смену адреса?",
            reply_markup=kb
        )

        await message.answer(
            f"📨 Ваш запрос на смену адреса отправлен мэру города {city_name}.\n"
            f"📍 Новый адрес: {city_name}, {street_name}, {house_number}\n\n"
            f"⏳ Ожидайте подтверждения..."
        )
        await state.set_state(ChangeAddress.waiting_approval)

    except Exception as e:
        print(f"Ошибка отправки запроса мэру: {e}")
        await complete_address_change(message, state, city_name, street_name, house_number)

# Функция получения текущего города пользователя
async def get_user_city(user_id: int) -> str:
    async with aiosqlite.connect("database.db") as db:
        cursor = await db.execute(
            "SELECT city FROM users WHERE user_id = ?",
            (user_id,)
        )
        result = await cursor.fetchone()
        return result[0] if result else "Не указан"


# Обработчик подтверждения от мэра
@dp.callback_query(F.data.startswith("approve_address:"))
async def approve_address_change(callback: types.CallbackQuery):
    request_id = int(callback.data.split(":")[1])

    async with aiosqlite.connect("database.db") as db:
        # Получаем полные данные запроса по request_id
        cursor = await db.execute("""
            SELECT user_id, old_city, new_city, street, house_number, status 
            FROM city_change_requests 
            WHERE id = ?
        """, (request_id,))

        request_data = await cursor.fetchone()

        if not request_data:
            await callback.answer("Запрос не найден!")
            return

        user_id, old_city, city_name, street_name, house_number, status = request_data

        if status != 'pending':
            await callback.answer("Этот запрос уже обработан!")
            return

        # Обновляем статус запроса
        await db.execute(
            "UPDATE city_change_requests SET status = 'approved' WHERE id = ?",
            (request_id,)
        )

        # Обновляем адрес пользователя
        await db.execute(
            "UPDATE users SET city = ?, street = ?, house_number = ? WHERE user_id = ?",
            (city_name, street_name, house_number, user_id)
        )

        # Обновляем статистику населения
        if old_city and old_city != city_name:
            await db.execute(
                "UPDATE cities SET population = population - 1 WHERE name = ?",
                (old_city,)
            )
            await db.execute(
                "UPDATE cities SET population = population + 1 WHERE name = ?",
                (city_name,)
            )

        await db.commit()

    await callback.message.edit_text(
        f"✅ Запрос на смену адреса одобрен!\n"
        f"Пользователь теперь проживает по адресу: {city_name}, {street_name}, {house_number}"
    )

    # Обязательно отвечаем на callback
    await callback.answer("Заявка одобрена!")

    # Уведомляем пользователя
    try:
        await bot.send_message(
            user_id,
            f"✅ Мэр города {city_name} одобрил вашу заявку на смену адреса!\n\n"
            f"📍 Ваш новый адрес:\n"
            f"🏙️ {city_name}\n"
            f"🏘️ {street_name}\n"
            f"🏠 {house_number}"
        )
    except Exception as e:
        print(f"Не удалось уведомить пользователя: {e}")



@dp.callback_query(F.data.startswith("reject_address:"))
async def reject_address_change(callback: types.CallbackQuery):
    request_id = int(callback.data.split(":")[1])

    async with aiosqlite.connect("database.db") as db:
        cursor = await db.execute("""
            SELECT user_id, new_city, status 
            FROM city_change_requests 
            WHERE id = ?
        """, (request_id,))

        request_data = await cursor.fetchone()

        if not request_data:
            await callback.answer("Запрос не найден!")
            return

        user_id, city_name, status = request_data

        if status != 'pending':
            await callback.answer("Этот запрос уже обработан!")
            return

        # Обновляем статус запроса
        await db.execute(
            "UPDATE city_change_requests SET status = 'rejected' WHERE id = ?",
            (request_id,)
        )
        await db.commit()

    await callback.message.edit_text(
        f"❌ Запрос на смену адреса отклонен."
    )

    # Обязательно отвечаем на callback
    await callback.answer("Заявка отклонена!")

    # Уведомляем пользователя
    try:
        await bot.send_message(
            user_id,
            f"❌ Мэр города {city_name} отклонил вашу заявку на смену адреса.\n"
            f"Пожалуйста, выберите другой город или обратитесь к мэру для уточнения причин."
        )
    except Exception as e:
        print(f"Не удалось уведомить пользователя: {e}")


# Обработчик отмены для всех состояний смены адреса
@dp.message(ChangeAddress.choose_street, F.text == "❌ Отмена")
@dp.message(ChangeAddress.choose_house, F.text == "❌ Отмена")
@dp.message(ChangeAddress.create_new_street, F.text == "❌ Отмена")
@dp.message(ChangeAddress.waiting_approval, F.text == "❌ Отмена")
async def cancel_address_change(message: types.Message, state: FSMContext):
    await message.answer("Смена адреса отменена.")
    await state.clear()
    await show_main_menu(message, message.from_user.id)


@dp.message(MayorMenu.managing_city, F.text == "📍 Указать координаты")
async def set_city_coordinates_start(message: types.Message, state: FSMContext):
    data = await state.get_data()
    city_name = data.get('city_name')

    async with aiosqlite.connect("database.db") as db:
        try:
            # Проверяем структуру таблицы
            cursor = await db.execute("PRAGMA table_info(cities)")
            columns = await cursor.fetchall()
            column_names = [column[1] for column in columns]

            # Если колонок нет, добавляем их БЕЗ CHECK constraints
            # Проверки будут в Python-коде
            if 'coord_x' not in column_names:
                await db.execute("ALTER TABLE cities ADD COLUMN coord_x INTEGER")
                print("✅ Добавлена колонка coord_x")

                # Устанавливаем начальное значение для существующих записей
                await db.execute("UPDATE cities SET coord_x = 0 WHERE coord_x IS NULL")

            if 'coord_z' not in column_names:
                await db.execute("ALTER TABLE cities ADD COLUMN coord_z INTEGER")
                print("✅ Добавлена колонка coord_z")

                # Устанавливаем начальное значение для существующих записей
                await db.execute("UPDATE cities SET coord_z = 0 WHERE coord_z IS NULL")

            await db.commit()

            # Получаем текущие координаты если они есть
            cursor = await db.execute(
                "SELECT coord_x, coord_z FROM cities WHERE name = ?",
                (city_name,)
            )
            coords = await cursor.fetchone()

            current_text = ""
            if coords and coords[0] is not None and coords[1] is not None:
                current_text = f"Текущие координаты: X={coords[0]}, Z={coords[1]}\n\n"

        except Exception as e:
            await message.answer(f"❌ Ошибка доступа к базе данных: {e}")
            return

    await message.answer(
        f"{current_text}"
        "Введите координаты города в формате:\n"
        "<b>X Z</b> (два целых числа через пробел)\n\n"
        "Примеры:\n"
        "• <code>500 750</code> (положительные)\n"
        "• <code>-500 750</code> (отрицательный X)\n"
        "• <code>500 -750</code> (отрицательный Z)\n"
        "• <code>-500 -750</code> (оба отрицательные)\n\n"
        "<b>Диапазон координат:</b>\n"
        "• X: от -2000 до 2000\n"
        "• Z: от -2000 до 2000\n\n"
        "Или напишите <code>Отмена</code> для отмены",
        parse_mode="HTML",
        reply_markup=ReplyKeyboardRemove()
    )
    await state.set_state(MayorManagement.waiting_for_coordinates)


# Обработчик ввода координат - УПРОЩЕННАЯ ВЕРСИЯ
@dp.message(MayorManagement.waiting_for_coordinates)
async def process_city_coordinates(message: types.Message, state: FSMContext):
    if message.text.lower() == "отмена":
        data = await state.get_data()
        city_name = data.get('city_name')
        await message.answer(
            "Установка координат отменена.",
            reply_markup=await mayor_menu_kb(city_name)
        )
        await state.set_state(MayorMenu.managing_city)
        return

    coordinates = message.text.strip()

    # Упрощенная проверка - только пробуем преобразовать в int
    try:
        # Разделяем по пробелу
        coords_parts = coordinates.split()

        if len(coords_parts) != 2:
            raise ValueError

        coord_x = int(coords_parts[0])
        coord_z = int(coords_parts[1])

    except ValueError:
        await message.answer(
            "❌ Неверный формат координат!\n\n"
            "Пожалуйста, введите координаты в формате:\n"
            "<b>X Z</b> (два целых числа через пробел)\n\n"
            "Пример: <code>500 -750</code>",
            parse_mode="HTML"
        )
        return

    # Проверяем диапазоны координат
    if coord_x < -2000 or coord_x > 2000:
        await message.answer(
            f"❌ Координата X должна быть в диапазоне от -2000 до 2000.\n"
            f"Вы ввели: {coord_x}"
        )
        return

    if coord_z < -2000 or coord_z > 2000:
        await message.answer(
            f"❌ Координата Z должна быть в диапазоне от -2000 до 2000.\n"
            f"Вы ввели: {coord_z}"
        )
        return

    data = await state.get_data()
    city_name = data.get('city_name')
    user_id = message.from_user.id

    async with aiosqlite.connect("database.db") as db:
        try:
            # Обновляем координаты города
            await db.execute(
                "UPDATE cities SET coord_x = ?, coord_z = ? WHERE name = ? AND mayor_id = ?",
                (coord_x, coord_z, city_name, user_id)
            )
            await db.commit()

            await message.answer(
                f"✅ Координаты города {city_name} успешно обновлены!\n\n"
                f"📍 Координаты:\n"
                f"• X: {coord_x}\n"
                f"• Z: {coord_z}\n\n"
                f"🗺️ <b>Ближайшие города:</b>\n"
                f"{await find_nearby_cities(city_name, coord_x, coord_z)}",
                parse_mode="HTML",
                reply_markup=await mayor_menu_kb(city_name)
            )

        except Exception as e:
            await message.answer(
                "❌ Произошла ошибка при сохранении координат.",
                reply_markup=await mayor_menu_kb(city_name)
            )
            print(f"Ошибка сохранения координат: {e}")

    await state.set_state(MayorMenu.managing_city)


# Обработчик ввода координат
@dp.message(MayorManagement.waiting_for_coordinates)
async def process_city_coordinates(message: types.Message, state: FSMContext):
    if message.text.lower() == "отмена":
        data = await state.get_data()
        city_name = data.get('city_name')
        await message.answer(
            "Установка координат отменена.",
            reply_markup=await mayor_menu_kb(city_name)
        )
        await state.set_state(MayorMenu.managing_city)
        return

    coordinates = message.text.strip()

    # Проверяем формат координат
    try:
        # Разделяем по пробелу
        coords_parts = coordinates.split()

        if len(coords_parts) != 2:
            raise ValueError

        coord_x = int(coords_parts[0])
        coord_z = int(coords_parts[1])

        # Проверяем диапазоны координат (разрешаем отрицательные значения от -2000 до 2000)
        if not (-2000 <= coord_x <= 2000) or not (-2000 <= coord_z <= 2000):
            raise ValueError

    except ValueError:
        await message.answer(
            "❌ Неверный формат координат!\n\n"
            "Пожалуйста, введите координаты в формате:\n"
            "<b>X Y</b> (два целых числа через пробел)\n\n"
            "Примеры:\n"
            "• <code>500 750</code> (положительные)\n"
            "• <code>-500 750</code> (отрицательный X)\n"
            "• <code>500 -750</code> (отрицательный Y)\n"
            "• <code>-500 -750</code> (оба отрицательные)\n\n"
            "<b>Диапазон координат:</b>\n"
            "• X: от -2000 до 2000\n"
            "• Y: от -2000 до 2000",
            parse_mode="HTML"
        )
        return

    data = await state.get_data()
    city_name = data.get('city_name')
    user_id = message.from_user.id

    async with aiosqlite.connect("database.db") as db:
        try:
            # Обновляем координаты города
            await db.execute(
                "UPDATE cities SET coord_x = ?, coord_z = ? WHERE name = ? AND mayor_id = ?",
                (coord_x, coord_z, city_name, user_id)
            )
            await db.commit()

            # Генерируем карту с обновленной функцией для отрицательных координат
            await message.answer(
                f"✅ Координаты города {city_name} успешно обновлены!\n\n"
                f"📍 Координаты:\n"
                f"• X: {coord_x}\n"
                f"• Y: {coord_z}\n\n"
                f"🗺️ <b>Ближайшие города:</b>\n"
                f"{await find_nearby_cities(city_name, coord_x, coord_z)}",
                parse_mode="HTML",
                reply_markup=await mayor_menu_kb(city_name)
            )

        except Exception as e:
            await message.answer(
                "❌ Произошла ошибка при сохранении координат.",
                reply_markup=await mayor_menu_kb(city_name)
            )
            print(f"Ошибка сохранения координат: {e}")

    await state.set_state(MayorMenu.managing_city)


# Функция для поиска ближайших городов
async def find_nearby_cities(current_city: str, x: int, y: int, limit: int = 5) -> str:
    """Находит ближайшие города"""
    async with aiosqlite.connect("database.db") as db:
        cursor = await db.execute("""
            SELECT name, coord_x, coord_z 
            FROM cities 
            WHERE name != ? AND coord_x IS NOT NULL AND coord_z IS NOT NULL
            ORDER BY ABS(coord_x - ?) + ABS(coord_z - ?)
            LIMIT ?
        """, (current_city, x, y, limit))

        nearby = await cursor.fetchall()

        if not nearby:
            return "Поблизости нет других городов с координатами."

        result = ""
        for i, (name, other_x, other_y) in enumerate(nearby, 1):
            # Рассчитываем расстояние (манхэттенское расстояние)
            distance = abs(other_x - x) + abs(other_y - y)
            result += f"{i}. {name} - {distance}ед. (X:{other_x}, Y:{other_y})\n"

        return result


# Написать заявление
@dp.message(F.text == "📝 Написать заявление")
async def write_statement(message: types.Message, state: FSMContext):
    await state.set_state(Statement.enter_text)
    await message.answer("Пожалуйста, напишите текст заявления:\n   или напиши Отмена для прекращения процесса")


@dp.message(Statement.enter_text)
async def save_statement(message: types.Message, state: FSMContext):
    text = message.text.strip()

    if message.text == "Отмена":
        await show_main_menu(message, message.from_user.id)
        await state.clear()
        return

    if not text:
        await message.answer("Текст заявления не может быть пустым. Пожалуйста, напишите его снова.")
        return

    user_id = message.from_user.id

    async with aiosqlite.connect("database.db") as db:
        await db.execute(
            "INSERT INTO statements (user_id, text) VALUES (?, ?)",
            (user_id, text)
        )
        await db.commit()

    admin_message = f"Новое заявление от пользователя {user_id}:\n\n{text}"

    # Если ADMIN_ID — список, отправляем каждому админу отдельно
    if isinstance(ADMIN_ID, list):  # Проверяем, что это список
        for admin_id in ADMIN_ID:
            try:
                await bot.send_message(chat_id=int(admin_id), text=admin_message)
            except Exception as e:
                print(f"Не удалось отправить сообщение админу {admin_id}: {e}")
    else:  # Если ADMIN_ID одиночный (число или строка)
        await bot.send_message(chat_id=int(ADMIN_ID), text=admin_message)

    await message.answer("Ваше заявление сохранено. Спасибо!")
    await show_main_menu(message, user_id)
    await state.clear()

    # Отправляем сообщение админу с заявлением и данными пользователя
    admin_message = (
        f"Новое заявление от пользователя {user_id}:\n\n{text}"
    )
    await bot.send_message(ADMIN_ID, admin_message)

    await message.answer("Ваше заявление сохранено. Спасибо!")
    await show_main_menu(message, user_id)
    await state.clear()


# Записаться ко врачу
@dp.message(F.text == "🧑‍⚕ Записаться ко врачу")
async def start_appointment(message: types.Message, state: FSMContext):
    await message.answer("Выберите врача:", reply_markup=doctors_kb())
    await state.set_state(Appointment.choose_doctor)


@dp.message(Appointment.choose_doctor)
async def choose_time(message: types.Message, state: FSMContext):
    if message.text == "Отмена":
        await show_main_menu(message, message.from_user.id)
        await state.clear()
        return
    doctor = message.text.strip()
    await state.update_data(doctor=doctor)
    await message.answer("Выберите время:", reply_markup=times_kb())
    await state.set_state(Appointment.choose_time)


@dp.message(Appointment.choose_time)
async def confirm_appointment(message: types.Message, state: FSMContext):
    if message.text == "Отмена":
        await show_main_menu(message, message.from_user.id)
        await state.clear()
        return
    time = message.text.strip()
    data = await state.get_data()
    doctor = data.get("doctor")
    user_id = message.from_user.id
    async with aiosqlite.connect("database.db") as db:
        await db.execute("INSERT INTO appointments (user_id, doctor, time) VALUES (?, ?, ?)",
                         (user_id, doctor, time))
        await db.commit()
    await message.answer(f"Вы успешно записаны к {doctor} на {time}.")
    await show_main_menu(message, user_id)
    await state.clear()


# Регистрация брака
@dp.message(F.text == "💍 Зарегистрировать брак")
async def start_marriage(message: types.Message, state: FSMContext):
    """Начало регистрации брака"""
    user_id = message.from_user.id

    try:
        # Проверяем, не состоит ли пользователь уже в браке
        async with aiosqlite.connect("database.db") as db:
            cursor = await db.execute("SELECT spouse_id FROM users WHERE user_id = ?", (user_id,))
            result = await cursor.fetchone()

            if result and result[0]:
                await message.answer(
                    "❌ Вы уже состоите в браке!",
                    reply_markup=await settings_menu_kb(user_id)
                )
                return

        await state.set_state(MarriageStates.enter_spouse_account)
        await message.answer(
            "💍 <b>Регистрация брака</b>\n\n"
            "Введите ГосID пользователя, с которым хотите вступить в брак:\n\n"
            "<i>ГосID может содержать буквы, цифры и символы любой длины\n"
            "Или напишите 'Отмена' для отмены</i>",
            parse_mode="HTML",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="❌ Отмена")]],
                resize_keyboard=True
            )
        )
    except Exception as e:
        print(f"Ошибка в start_marriage: {e}")
        await message.answer("❌ Произошла ошибка. Попробуйте позже.")


@dp.message(MarriageStates.enter_spouse_account)
async def process_spouse_account(message: types.Message, state: FSMContext):
    """Обработка ввода ГосID супруга"""
    gosid = message.text.strip()

    if message.text == "Отмена":
        await state.clear()
        await show_main_menu(message, message.from_user.id)
        return

    # Новая проверка формата ГосID - минимальная длина 3 символа
    if len(gosid) < 3:
        await message.answer(
            "❌ ГосID должен содержать минимум 3 символа\n"
            "Пожалуйста, введите корректный ГосID:",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="Отмена")]],
                resize_keyboard=True
            )
        )
        return

    # Можно добавить дополнительные проверки, если нужно
    # Например, запретить определенные символы:
    forbidden_chars = ['<', '>', '&', '"', "'", '`', '\\']
    for char in forbidden_chars:
        if char in gosid:
            await message.answer(
                f"❌ ГосID содержит запрещенные символы\n"
                f"Пожалуйста, используйте только буквы, цифры и стандартные символы",
                reply_markup=ReplyKeyboardMarkup(
                    keyboard=[[KeyboardButton(text="Отмена")]],
                    resize_keyboard=True
                )
            )
            return

    user_id = message.from_user.id

    try:
        async with aiosqlite.connect("database.db") as db:
            # Получаем данные текущего пользователя
            cursor = await db.execute(
                "SELECT account_id, name FROM users WHERE user_id = ?",
                (user_id,)
            )
            sender_data = await cursor.fetchone()

            if not sender_data:
                await message.answer("❌ Ваш профиль не найден. Введите /start")
                await state.clear()
                return

            sender_account, sender_name = sender_data

            # Проверяем, не пытается ли пользователь жениться на себе
            if gosid.upper() == sender_account.upper():
                await message.answer("❌ Вы не можете вступить в брак с самим собой")
                await state.clear()
                await show_main_menu(message, user_id)
                return

            # Проверяем существование и статус получателя
            # Ищем пользователя по account_id (ГосID)
            cursor = await db.execute("""
                SELECT user_id, name, spouse_id, account_id 
                FROM users 
                WHERE UPPER(account_id) = UPPER(?)
            """, (gosid,))
            spouse_data = await cursor.fetchone()

            if not spouse_data:
                await message.answer("❌ Пользователь с таким ГосID не найден")
                await state.clear()
                await show_main_menu(message, user_id)
                return

            spouse_user_id, spouse_name, spouse_spouse_id, spouse_account_id = spouse_data

            # ДОПОЛНИТЕЛЬНАЯ ПРОВЕРКА: существует ли пользователь в Telegram
            try:
                # Пытаемся получить информацию о пользователе
                user_info = await bot.get_chat(spouse_user_id)
                if not user_info:
                    await message.answer("❌ Пользователь не найден в системе Telegram")
                    await state.clear()
                    await show_main_menu(message, user_id)
                    return
            except Exception as e:
                print(f"Ошибка получения информации о пользователе: {e}")
                await message.answer("❌ Не удалось найти пользователя. Возможно, он заблокировал бота")
                await state.clear()
                await show_main_menu(message, user_id)
                return

            if spouse_spouse_id is not None:
                await message.answer("❌ Этот пользователь уже состоит в браке")
                await state.clear()
                await show_main_menu(message, user_id)
                return

        # Сохраняем данные в state
        await state.update_data(
            spouse_gosid=gosid,
            spouse_account=spouse_account_id,  # сохраняем account_id супруга
            spouse_user_id=spouse_user_id,
            spouse_name=spouse_name,
            sender_account=sender_account,
            sender_name=sender_name,
            sender_user_id=user_id
        )

        # Запрашиваем подтверждение
        await message.answer(
            f"💍 <b>Подтверждение запроса на брак</b>\n\n"
            f"Вы хотите отправить запрос на брак:\n"
            f"👤 Имя: {spouse_name}\n"
            f"🆔 ГосID: {gosid}\n"
            f"💳 Счет: {spouse_account_id}\n\n"
            f"Подтвердите отправку запроса:",
            parse_mode="HTML",
            reply_markup=confirm_kb()
        )
        await state.set_state(MarriageStates.confirm_marriage_request)

    except Exception as e:
        print(f"Ошибка в process_spouse_account: {e}")
        await message.answer("❌ Произошла ошибка. Попробуйте позже.")
        await state.clear()


@dp.message(MarriageStates.confirm_marriage_request, F.text == "Подтвердить")
async def send_marriage_request(message: types.Message, state: FSMContext):
    """Отправка запроса на брак"""
    data = await state.get_data()

    if not data:
        await message.answer("❌ Данные устарели. Начните заново.")
        await state.clear()
        return

    try:
        # Создаем inline клавиатуру для ответа
        builder = InlineKeyboardBuilder()
        builder.button(
            text="💍 Принять",
            callback_data=f"marriage_accept:{data['sender_account']}:{data['sender_user_id']}"
        )
        builder.button(
            text="❌ Отклонить",
            callback_data=f"marriage_decline:{data['sender_account']}:{data['sender_user_id']}"
        )
        builder.adjust(2)
        kb = builder.as_markup()

        # Логируем попытку отправки
        print(f"Отправка запроса пользователю {data['spouse_user_id']} от {data['sender_user_id']}")

        # Отправляем запрос второму пользователю
        await bot.send_message(
            data['spouse_user_id'],
            f"💍 <b>Предложение о браке!</b>\n\n"
            f"Пользователь {data['sender_name']} "
            f"(счет: {data['sender_account']}) "
            f"хочет вступить с вами в брак.\n\n"
            f"Принять предложение?",
            parse_mode="HTML",
            reply_markup=kb
        )

        await message.answer(
            f"✅ Запрос на брак отправлен пользователю {data['spouse_name']}!\n"
            f"Ожидайте подтверждения...",
            reply_markup=back_to_main_kb()
        )

    except ChatNotFound:
        await message.answer(
            "❌ Не удалось отправить запрос. Пользователь не найден.",
            reply_markup=back_to_main_kb()
        )
    except BotBlocked:
        await message.answer(
            "❌ Не удалось отправить запрос. Пользователь заблокировал бота.",
            reply_markup=back_to_main_kb()
        )
    except Exception as e:
        await message.answer(
            "❌ Не удалось отправить запрос. Попробуйте позже.",
            reply_markup=back_to_main_kb()
        )
        print(f"Ошибка отправки запроса на брак: {e}")

    await state.clear()


@dp.message(MarriageStates.confirm_marriage_request, F.text == "❌ Отмена")
async def cancel_marriage_request(message: types.Message, state: FSMContext):
    """Отмена запроса на брак"""
    await message.answer(
        "❌ Регистрация брака отменена.",
        reply_markup=back_to_main_kb()
    )
    await state.clear()


# Обработчики callback-ов для брака
@dp.callback_query(F.data.startswith("marriage_accept:"))
async def accept_marriage(callback: types.CallbackQuery):
    """Обработка принятия предложения о браке"""
    try:
        data_parts = callback.data.split(":")
        if len(data_parts) != 3:
            await callback.answer("❌ Неверный формат данных")
            return

        sender_account = data_parts[1]
        sender_user_id = int(data_parts[2])
        acceptor_id = callback.from_user.id

        # Генерируем ID брака и дату
        marriage_id = str(random.randint(100000, 999999))
        marriage_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        async with aiosqlite.connect("database.db") as db:
            # Проверяем, что колонки существуют, если нет - создаем их
            try:
                # Сначала проверим структуру таблицы
                cursor = await db.execute("PRAGMA table_info(users)")
                columns = await cursor.fetchall()
                column_names = [column[1] for column in columns]

                # Если колонок нет, добавляем их
                if 'marriage_date' not in column_names:
                    await db.execute("ALTER TABLE users ADD COLUMN marriage_date TEXT")
                if 'marriage_id' not in column_names:
                    await db.execute("ALTER TABLE users ADD COLUMN marriage_id TEXT")
                if 'spouse_id' not in column_names:
                    await db.execute("ALTER TABLE users ADD COLUMN spouse_id TEXT")

                await db.commit()
            except Exception as e:
                print(f"Ошибка при проверке структуры БД: {e}")

            # Получаем данные обоих пользователей
            cursor = await db.execute("""
                SELECT u1.name as sender_name, u1.account_id as sender_account,
                       u2.name as acceptor_name, u2.account_id as acceptor_account
                FROM users u1, users u2
                WHERE u1.account_id = ? AND u2.user_id = ?
            """, (sender_account, acceptor_id))

            data = await cursor.fetchone()

            if not data:
                await callback.answer("❌ Ошибка: пользователь не найден")
                return

            sender_name, sender_acc, acceptor_name, acceptor_acc = data

            # Регистрируем брак
            await db.execute("BEGIN TRANSACTION")

            # Обновляем данные о браке для обоих пользователей
            await db.execute(
                "UPDATE users SET spouse_id = ?, marriage_date = ?, marriage_id = ? WHERE account_id = ?",
                (acceptor_acc, marriage_date, marriage_id, sender_account)
            )
            await db.execute(
                "UPDATE users SET spouse_id = ?, marriage_date = ?, marriage_id = ? WHERE user_id = ?",
                (sender_account, marriage_date, marriage_id, acceptor_id)
            )

            await db.commit()

            # Сообщение об успешной регистрации
            wedding_message = (
                f"💕 <b>Поздравляем с регистрацией брака!</b>\n\n"
                f"👰 {sender_name} + 🤵 {acceptor_name}\n"
                f"📅 Дата: {marriage_date[:10]}\n"
                f"💍 ID брака: {marriage_id}\n\n"
                f"Желаем счастья и любви! 💖"
            )

            # Отправляем уведомления обоим пользователям
            try:
                await bot.send_message(sender_user_id, wedding_message, parse_mode="HTML")
            except Exception as e:
                print(f"Не удалось отправить сообщение отправителю: {e}")

            try:
                await bot.send_message(acceptor_id, wedding_message, parse_mode="HTML")
            except Exception as e:
                print(f"Не удалось отправить сообщение получателю: {e}")

            # Редактируем сообщение с запросом
            await callback.message.edit_text(
                "💍 Вы приняли предложение о браке! Брак успешно зарегистрирован.",
                reply_markup=None
            )

            await callback.answer("✅ Брак зарегистрирован!")

    except Exception as e:
        # Упрощенная обработка ошибок без rollback
        print(f"Ошибка регистрации брака: {e}")
        await callback.answer("❌ Произошла ошибка при регистрации брака")


@dp.message(MarriageStates.confirm_marriage_request, F.text == "Подтвердить")
async def send_marriage_request(message: types.Message, state: FSMContext):
    """Отправка запроса на брак"""
    data = await state.get_data()

    if not data:
        await message.answer("❌ Данные устарели. Начните заново.")
        await state.clear()
        return

    try:
        # Создаем inline клавиатуру для ответа
        builder = InlineKeyboardBuilder()
        builder.button(
            text="💍 Принять",
            callback_data=f"marriage_accept:{data['sender_account']}:{data['sender_user_id']}"
        )
        builder.button(
            text="❌ Отклонить",
            callback_data=f"marriage_decline:{data['sender_account']}:{data['sender_user_id']}"
        )
        builder.adjust(2)
        kb = builder.as_markup()

        # Логируем попытку отправки
        print(f"Отправка запроса пользователю {data['spouse_user_id']} от {data['sender_user_id']}")

        # Отправляем запрос второму пользователю
        await bot.send_message(
            data['spouse_user_id'],
            f"💍 <b>Предложение о браке!</b>\n\n"
            f"Пользователь {data['sender_name']} "
            f"(счет: {data['sender_account']}) "
            f"хочет вступить с вами в брак.\n\n"
            f"Принять предложение?",
            parse_mode="HTML",
            reply_markup=kb
        )

        await message.answer(
            f"✅ Запрос на брак отправлен пользователю {data['spouse_name']}!\n"
            f"Ожидайте подтверждения...",
            reply_markup=back_to_main_kb()
        )

    except ChatNotFound:
        await message.answer(
            "❌ Не удалось отправить запрос. Пользователь не найден.",
            reply_markup=back_to_main_kb()
        )
    except BotBlocked:
        await message.answer(
            "❌ Не удалось отправить запрос. Пользователь заблокировал бота.",
            reply_markup=back_to_main_kb()
        )
    except Exception as e:
        await message.answer(
            "❌ Не удалось отправить запрос. Попробуйте позже.",
            reply_markup=back_to_main_kb()
        )
        print(f"Ошибка отправки запроса на брак: {e}")

    await state.clear()


@dp.message(DivorceStates.confirm_divorce, F.text == "Отмена")
async def cancel_divorce(message: types.Message, state: FSMContext):
    """Отмена развода"""
    await message.answer(
        "✅ Развод отменен.",
        reply_markup=back_to_main_kb()
    )
    await state.clear()


# Свидетельство о браке
@dp.message(F.text == "💒 Свидетельство о браке")
async def show_marriage_certificate(message: types.Message):
    """Показать свидетельство о браке"""
    user_id = message.from_user.id

    async with aiosqlite.connect("database.db") as db:
        cursor = await db.execute("""
            SELECT u.name, u.spouse_id, s.name as spouse_name, 
                   u.marriage_date, u.marriage_id, s.account_id as spouse_account,
                   u.account_id as user_account
            FROM users u
            LEFT JOIN users s ON u.spouse_id = s.account_id
            WHERE u.user_id = ?
        """, (user_id,))

        marriage_data = await cursor.fetchone()

    if not marriage_data or not marriage_data[1]:  # spouse_id
        await message.answer("❌ Вы не состоите в браке")
        return

    name, spouse_id, spouse_name, marriage_date, marriage_id, spouse_account, user_account = marriage_data

    certificate = (
        f"💒 <b>СВИДЕТЕЛЬСТВО О БРАКЕ</b>\n\n"
        f"💍 <b>ID брака:</b> {marriage_id or 'Не указан'}\n"
        f"📅 <b>Дата регистрации:</b> {marriage_date[:10] if marriage_date else 'Неизвестно'}\n\n"
        f"<b>Супруги:</b>\n"
        f"👰 <b>Муж:</b> {name if name else 'Неизвестно'}\n"
        f"🤵 <b>Жена:</b> {spouse_name if spouse_name else 'Неизвестно'}\n\n"
        f"<b>Идентификаторы:</b>\n"
        f"🔹 {name}: {user_account}\n"
        f"🔹 {spouse_name}: {spouse_account}\n\n")

    await message.answer(certificate, parse_mode="HTML")



async def get_marriage_info(user_id: int):
    """Получить информацию о браке пользователя"""
    async with aiosqlite.connect("database.db") as db:
        cursor = await db.execute("""
            SELECT spouse_id, marriage_date, marriage_id, 
                   (SELECT name FROM users WHERE account_id = users.spouse_id) as spouse_name
            FROM users 
            WHERE user_id = ?
        """, (user_id,))
        data = await cursor.fetchone()

    if data and data[0]:  # spouse_id не пустой
        spouse_id, marriage_date, marriage_id, spouse_name = data
        return {
            "spouse_id": spouse_id,
            "marriage_date": marriage_date,
            "marriage_id": marriage_id,
            "spouse_name": spouse_name or "Неизвестно"
        }
    return None

@dp.message(F.text == "💔 Развестись")
async def start_divorce(message: types.Message, state: FSMContext):
    """Начало процесса развода"""
    user_id = message.from_user.id

    try:
        # Получаем информацию о браке
        marriage_info = await get_marriage_info(user_id)

        if not marriage_info:
            await message.answer(
                "❌ Вы не состоите в браке!",
                reply_markup=back_to_main_kb()
            )
            return

        # Сохраняем информацию в state
        await state.update_data(
            spouse_id=marriage_info["spouse_id"],
            spouse_name=marriage_info["spouse_name"],
            marriage_id=marriage_info["marriage_id"],
            marriage_date=marriage_info["marriage_date"]
        )

        # Запрашиваем подтверждение
        await message.answer(
            f"💔 <b>Заявление на развод</b>\n\n"
            f"Вы действительно хотите расторгнуть брак?\n\n"
            f"👤 Супруг(а): {marriage_info['spouse_name']}\n"
            f"💍 ID брака: {marriage_info['marriage_id']}\n"
            f"📅 Дата заключения: {marriage_info['marriage_date']}\n\n"
            f"<i>Развод приведет к аннулированию всех связанных данных о браке</i>",
            parse_mode="HTML",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="✅ Да, развестись")],
                    [KeyboardButton(text="❌ Нет, отмена")]
                ],
                resize_keyboard=True
            )
        )
        await state.set_state(DivorceStates.confirm_divorce)

    except Exception as e:
        print(f"Ошибка в start_divorce: {e}")
        await message.answer(
            "❌ Произошла ошибка. Попробуйте позже.",
            reply_markup=back_to_main_kb()
        )


@dp.message(DivorceStates.confirm_divorce, F.text == "✅ Да, развестись")
async def confirm_divorce(message: types.Message, state: FSMContext):
    """Подтверждение развода"""
    user_id = message.from_user.id
    data = await state.get_data()

    if not data:
        await message.answer(
            "❌ Данные устарели. Начните заново.",
            reply_markup=back_to_main_kb()
        )
        await state.clear()
        return

    try:
        spouse_id = data['spouse_id']
        spouse_name = data['spouse_name']
        marriage_id = data['marriage_id']

        async with aiosqlite.connect("database.db") as db:
            # Начинаем транзакцию
            await db.execute("BEGIN TRANSACTION")

            try:
                # 1. Получаем данные обоих супругов перед разводом
                cursor = await db.execute(
                    "SELECT user_id, name, account_id FROM users WHERE account_id = ?",
                    (spouse_id,)
                )
                spouse_data = await cursor.fetchone()

                if not spouse_data:
                    await message.answer(
                        "❌ Супруг(а) не найден(а) в системе",
                        reply_markup=back_to_main_kb()
                    )
                    await state.clear()
                    return

                spouse_user_id, spouse_full_name, spouse_account = spouse_data

                # 2. Удаляем данные о браке у обоих супругов
                await db.execute(
                    "UPDATE users SET spouse_id = NULL, marriage_date = NULL, marriage_id = NULL WHERE user_id = ?",
                    (user_id,)
                )
                await db.execute(
                    "UPDATE users SET spouse_id = NULL, marriage_date = NULL, marriage_id = NULL WHERE user_id = ?",
                    (spouse_user_id,)
                )

                await db.commit()

                # 3. Уведомляем обоих пользователей
                divorce_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                divorce_message = (
                    f"💔 <b>Брак расторгнут</b>\n\n"
                    f"Брачный союз официально расторгнут.\n"
                    f"📅 Дата развода: {divorce_date}\n"
                    f"💍 ID брака: {marriage_id}\n\n"
                    f"Вы снова свободны. Будьте счастливы! ✨"
                )

                # Отправляем уведомление инициатору развода
                await message.answer(
                    divorce_message,
                    parse_mode="HTML",
                    reply_markup=back_to_main_kb()
                )

                # Отправляем уведомление второму супругу
                try:
                    await bot.send_message(
                        spouse_user_id,
                        f"💔 <b>Ваш брак расторгнут</b>\n\n"
                        f"Ваш(а) супруг(а) {message.from_user.full_name} "
                        f"подал(а) заявление на развод.\n\n"
                        f"📅 Дата развода: {divorce_date}\n"
                        f"💍 ID брака: {marriage_id}\n\n"
                        f"Брак официально расторгнут.",
                        parse_mode="HTML"
                    )
                except Exception as e:
                    print(f"Не удалось отправить уведомление супругу: {e}")

                # Логируем развод
                print(f"Развод: {user_id} развелся с {spouse_user_id}, брак {marriage_id}")

            except Exception as e:
                await db.rollback()
                print(f"Ошибка при разводе: {e}")
                await message.answer(
                    "❌ Произошла ошибка при обработке развода",
                    reply_markup=back_to_main_kb()
                )
                return

        await state.clear()

    except Exception as e:
        print(f"Ошибка в confirm_divorce: {e}")
        await message.answer(
            "❌ Произошла ошибка. Попробуйте позже.",
            reply_markup=back_to_main_kb()
        )
        await state.clear()


@dp.message(DivorceStates.confirm_divorce, F.text == "❌ Нет, отмена")
async def cancel_divorce(message: types.Message, state: FSMContext):
    """Отмена процесса развода"""
    await message.answer(
        "✅ Развод отменен. Брак сохранен.",
        reply_markup=back_to_main_kb()
    )
    await state.clear()


# Вспомогательные функции


# Обработчики возврата в главное меню
@dp.message(F.text == "⬅️ Назад")
async def back_to_settings(message: types.Message):
    """Возврат из настроек в главное меню"""
    await show_main_menu(message, message.from_user.id)


@dp.message(F.text == "⬅️ Главное меню")
async def back_to_main_menu(message: types.Message, state: FSMContext):
    """Возврат в главное меню с очисткой состояния"""
    if await state.get_state():
        await state.clear()
    await show_main_menu(message, message.from_user.id)


# Обработчик настроек
@dp.message(F.text == "⚙️ Настройки")
async def open_settings(message: types.Message):
    """Открыть меню настроек"""
    await message.answer(
        "🔧 Меню настроек профиля:",
        reply_markup=await settings_menu_kb(message.from_user.id)
    )


# Изменить город
@dp.message(F.text == "🏠 Изменить место жительства")
async def change_address_start(message: types.Message, state: FSMContext):
    user_id = message.from_user.id

    try:
        # Получаем текущий полный адрес
        current_address = await get_user_full_address(user_id)

        # Проверяем, является ли пользователь мэром какого-либо города
        async with aiosqlite.connect("database.db") as db:
            cursor = await db.execute(
                "SELECT name FROM cities WHERE mayor_id = ?",
                (user_id,)
            )
            mayor_cities = await cursor.fetchall()

        # Формируем информацию об адресе
        address_info = f"📍 <b>Ваш текущий адрес:</b>\n{current_address}\n\n"

        # Показываем города, где пользователь мэр
        if mayor_cities:
            address_info += "👑 <b>Вы являетесь мэром городов:</b>\n"
            for mayor_city in mayor_cities:
                address_info += f"• {mayor_city[0]}\n"
            address_info += "\n<i>Переезд в ваш город подтвердится автоматически.</i>\n\n"

        keyboard = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="🏘️ смена улицы и дома")],
                [KeyboardButton(text="🏙️ смена города")],
                [KeyboardButton(text="❌ Отмена")]
            ],
            resize_keyboard=True
        )

        await message.answer(
            f"{address_info}"
            "Что вы хотите изменить?",
            parse_mode="HTML",
            reply_markup=keyboard
        )

        # Сохраняем информацию о городах, где пользователь мэр
        await state.update_data(
            mayor_cities=[city[0] for city in mayor_cities] if mayor_cities else []
        )

    except Exception as e:
        print(f"Ошибка при получении адреса: {e}")
        await message.answer(
            "❌ Ошибка при загрузке данных. Попробуйте позже.",
            reply_markup=await settings_menu_kb(user_id)
        )

@dp.message(F.text == "🏘️ смена улицы и дома")
async def change_street_house_only(message: types.Message, state: FSMContext):
    """Начинает изменение улицы и дома"""
    data = await state.get_data()
    current_city = data.get('current_city')

    if not current_city:
        await message.answer("❌ У вас не указан город. Сначала выберите город.")
        return

    await state.update_data(selected_city=current_city)

    # Показываем улицы текущего города
    await show_streets_for_city(message, state, current_city)
    await state.set_state(ChangeAddress.choose_street)


@dp.message(F.text == "🏙️ смена города")
async def change_full_address(message: types.Message, state: FSMContext):
    """Начинает изменение полного адреса"""
    keyboard = await cities_kb()
    await message.answer(
        "🏙️ Выберите новый город для полной смены адреса:\n"
        "Или напишите 'Отмена' для отмены",
        reply_markup=keyboard
    )
    await state.set_state(ChangeAddress.choose_city)
    await state.update_data(change_type='full')


@dp.message(F.text == "↩ Назад")
async def back_to_settings(message: types.Message, state: FSMContext):
    """Возврат из меню изменения адреса в настройки"""
    await state.clear()
    await message.answer(
        "Возврат в настройки",
        reply_markup=await settings_menu_kb(message.from_user.id)
    )

@dp.message(StateFilter(ChangeAddress.choose_city,ChangeAddress.choose_street,ChangeAddress.choose_house,ChangeAddress.create_new_street,ChangeAddress.waiting_approval),F.text == "❌ Отмена")
async def cancel_address_change_all(message: types.Message, state: FSMContext):
    """Отмена смены адреса из любого состояния"""
    await message.answer(
        "Смена адреса отменена.",
        reply_markup=await settings_menu_kb(message.from_user.id)
    )
    await state.clear()

# Остальные обработчики остаются без изменений, но убедитесь, что они используют ChangeAddress
@dp.message(ChangeAddress.create_new_street)
async def process_create_new_street(message: types.Message, state: FSMContext):
    """Обработчик создания новой улицы"""
    user_id = message.from_user.id
    new_street = message.text.strip()

    if new_street.lower() == "отмена":
        await show_main_menu(message, user_id)
        await state.clear()
        return

    # Валидация названия улицы
    if len(new_street) < 2 or len(new_street) > 50:
        await message.answer("❌ Название улицы должно быть от 2 до 50 символов. Попробуйте еще раз:")
        return

    data = await state.get_data()
    city_name = data.get('selected_city') or data.get('new_city')

    try:
        # Сохраняем новую улицу в базу
        async with aiosqlite.connect("database.db") as db:
            await db.execute(
                "INSERT OR IGNORE INTO streets (city_name, street_name) VALUES (?, ?)",
                (city_name, new_street)
            )
            await db.commit()

        await state.update_data(street=new_street)
        await message.answer(
            f"✅ Новая улица '{new_street}' добавлена!\n"
            f"🏠 Теперь введите номер дома:",
            reply_markup=ReplyKeyboardRemove()
        )
        await state.set_state(ChangeAddress.choose_house)

    except Exception as e:
        logging.error(f"❌ Ошибка при создании улицы: {e}")
        await message.answer(
            "❌ Произошла ошибка при создании улицы. Попробуйте позже.",
            reply_markup=ReplyKeyboardRemove()
        )
        await show_main_menu(message, user_id)
        await state.clear()


# смена имени
@dp.message(F.text == "✏️ Изменить имя")
async def start_name_change(message: types.Message, state: FSMContext):
    await message.answer(
        "Введите новое имя (от 2 до 50 символов):",
        reply_markup=ReplyKeyboardRemove()
    )
    await state.set_state(Settings.waiting_for_new_name)


# Обработчик ввода нового имени
@dp.message(Settings.waiting_for_new_name)
async def process_new_name(message: types.Message, state: FSMContext):
    new_name = message.text.strip()

    # Валидация имени
    if new_name == "Отмена":
        await show_main_menu(message, message.from_user.id)
        await state.clear()
        return
    else:
        if len(new_name) < 2 or len(new_name) > 50:
            await message.answer("Имя должно содержать от 2 до 50 символов. Попробуйте еще раз.")
            return
        user_id = message.from_user.id

    async with aiosqlite.connect("database.db") as db:
        # Обновляем имя в базе данных
        await db.execute(
            "UPDATE users SET name = ? WHERE user_id = ?",
            (new_name, user_id)
        )
        await db.commit()

        # Получаем обновленные данные для отображения
        cursor = await db.execute(
            "SELECT name, account_id FROM users WHERE user_id = ?",
            (user_id,)
        )
        updated_user = await cursor.fetchone()

    if updated_user:
        await message.answer(
            f"✅ Ваше имя успешно изменено на: {updated_user[0]}\n"
            f"Номер счета: {updated_user[1]}"
        )
    else:
        await message.answer("❌ Произошла ошибка при изменении имени")

    await state.clear()
    await show_main_menu(message, user_id)


# Переводы
@dp.message(F.text == "💰 Новый перевод")
async def start_transfer(message: types.Message, state: FSMContext):
    await message.answer(
        f'Введите ГосID получателя: \n или напишите <code>Отмена</code> для отмены перевода',
        parse_mode="HTML"
    )
    await state.set_state(Transfer.enter_recipient)


@dp.message(Transfer.enter_recipient)
async def transfer_enter_amount(message: types.Message, state: FSMContext):
    recipient_account = message.text.strip()
    if message.text == "Отмена":
        await show_main_menu(message, message.from_user.id)
        await state.clear()
        return

    user_id = message.from_user.id

    async with aiosqlite.connect("database.db") as db:
        # Проверяем существование получателя
        cursor = await db.execute(
            "SELECT 1 FROM users WHERE account_id = ?",
            (recipient_account,)
        )
        recipient_exists = await cursor.fetchone()

        if not recipient_exists:
            await message.answer("Пользователь с таким номером счета не найден.")
            return

        # Проверяем что не переводим себе
        cursor = await db.execute(
            "SELECT account_id FROM users WHERE user_id = ?",
            (user_id,)
        )
        sender_account = (await cursor.fetchone())[0]

        if sender_account == recipient_account:
            await message.answer("Вы не можете перевести деньги себе.")
            return

    await state.update_data(recipient_account=recipient_account)
    await message.answer("Введите сумму перевода (целое число): \n или 0, если передумали отправлять деньги")
    await state.set_state(Transfer.enter_amount)


@dp.message(Transfer.enter_amount)
async def transfer_confirm(message: types.Message, state: FSMContext):
    try:
        amount = int(message.text.strip())
        if amount == 0:
            await show_main_menu(message, message.from_user.id)
            await state.clear()
            return
        if amount <= 0:
            raise ValueError
    except ValueError:
        await message.answer("Некорректная сумма. Введите положительное целое число.")
        return

    data = await state.get_data()
    recipient_account = data.get("recipient_account")
    user_id = message.from_user.id

    async with aiosqlite.connect("database.db") as db:
        # Получаем все необходимые данные за один запрос
        cursor = await db.execute(
            """SELECT u1.balance, u1.city, u2.city 
               FROM users u1, users u2 
               WHERE u1.user_id = ? AND u2.account_id = ?""",
            (user_id, recipient_account)
        )
        sender_balance, sender_city, recipient_city = await cursor.fetchone()

        # Проверяем достаточность средств
        commission = 0 if sender_city.startswith("ШИ") and recipient_city.startswith("ШИ") else 1
        total_amount = amount + commission

        if sender_balance < total_amount:
            await message.answer(f"Недостаточно средств:(\n На вашем счету: {sender_balance} шуек.")
            await state.clear()
            return

        text = (f"Вы собираетесь перевести {amount} шуек на счет {recipient_account}\n"
                f"Комиссия: {commission} шуйка\n"
                f"Итого будет списано: {total_amount} шуек\n"
                f"Остаток после перевода: {sender_balance - total_amount} шуек")

        await state.update_data(
            amount=amount,
            commission=commission,
            total_amount=total_amount,
            recipient_account=recipient_account
        )
        await message.answer(text, reply_markup=confirm_kb())
        await state.set_state(Transfer.confirm)


@dp.message(Transfer.confirm, F.text == "Подтвердить")
async def execute_transfer(message: types.Message, state: FSMContext):
    user_id = message.from_user.id  # Получаем user_id
    data = await state.get_data()
    user_id = message.from_user.id
    amount = data['amount']
    commission = data['commission']
    total_amount = data['total_amount']
    recipient_account = data['recipient_account']

    async with aiosqlite.connect("database.db") as db:
        try:
            # Начинаем транзакцию
            await db.execute("BEGIN TRANSACTION")

            # Списание у отправителя
            await db.execute(
                "UPDATE users SET balance = balance - ? WHERE user_id = ? AND balance >= ?",
                (total_amount, user_id, total_amount)
            )

            # Зачисление получателю (без комиссии)
            await db.execute(
                "UPDATE users SET balance = balance + ? WHERE account_id = ?",
                (amount, recipient_account)
            )

            # Запись в историю переводов
            await db.execute(
                """INSERT INTO transfers (from_user, to_user, amount, commission) 
                   VALUES ((SELECT account_id FROM users WHERE user_id = ?), ?, ?, ?)""",
                (user_id, recipient_account, amount, commission)
            )

            await db.commit()

            await message.answer(
                f"Перевод успешно выполнен!\n"
                f"Переведено: {amount} шуек\n"
                f"Комиссия: {commission} шуйка"
            )

        except Exception as e:
            await db.rollback()
            await message.answer("Произошла ошибка при переводе. Попробуйте позже.")
            print(f"Transfer error: {e}")
        finally:
            await state.clear()
            await show_main_menu(message, user_id)


@dp.message(Transfer.confirm, F.text == "Отмена")
async def cancel_transfer(message: types.Message, state: FSMContext):
    await message.answer("Перевод отменён.")
    await state.clear()
    await show_main_menu(message, message.from_user.id)


# истории
# Обработчик кнопки Истории
@dp.message(F.text == "📂 Истории")
async def open_history_menu(message: types.Message):
    await message.answer("Выберите тип истории:", reply_markup=history_menu_kb())


# История переводов
@dp.message(F.text == "📜 История переводов")
async def transfer_history(message: types.Message):
    user_id = message.from_user.id

    async with aiosqlite.connect("database.db") as db:
        # Получаем account_id текущего пользователя
        cursor = await db.execute(
            "SELECT account_id FROM users WHERE user_id = ?",
            (user_id,)
        )
        user_account = await cursor.fetchone()

        if not user_account:
            await message.answer("❌ Ошибка: ваш аккаунт не найден")
            return

        account_id = user_account[0]

        # Получаем полную историю переводов с именами получателей (без LIMIT)
        cursor = await db.execute("""
            SELECT t.amount, t.timestamp, t.to_user, u.name 
            FROM transfers t
            LEFT JOIN users u ON t.to_user = u.account_id
            WHERE t.from_user = ?
            ORDER BY t.timestamp DESC
        """, (account_id,))

        transfers = await cursor.fetchall()

    if not transfers:
        await message.answer("📭 История переводов пуста")
        return

    # Разбиваем историю на части, если она слишком большая
    chunk_size = 30  # Количество переводов в одном сообщении
    chunks = [transfers[i:i + chunk_size] for i in range(0, len(transfers), chunk_size)]

    for chunk_num, chunk in enumerate(chunks, 1):
        response = f"📜 История переводов (часть {chunk_num}):\n\n"
        for idx, (amount, timestamp, to_account, to_name) in enumerate(chunk, 1):
            recipient = to_name if to_name else f"Аккаунт {to_account[:4]}...{to_account[-4:]}"
            response += (
                f"{idx}. {timestamp[:16]}\n"
                f"→ {recipient}\n"
                f"Сумма: {amount} шуек\n"
                f"───────────────────\n"
            )

        # Если это последний кусок и он почти полный - не добавляем "Продолжение следует"
        if chunk_num < len(chunks) or len(chunk) >= chunk_size - 2:
            response += "\n⬇️ Продолжение следует..."

        await message.answer(response)


# История заявлений
@dp.message(F.text == "📄 История заявлений")
async def statement_history(message: types.Message):
    async with aiosqlite.connect("database.db") as db:
        cursor = await db.execute("SELECT text, timestamp FROM statements WHERE user_id = ?", (message.from_user.id,))
        rows = await cursor.fetchall()
    if not rows:
        await message.answer("История заявлений пуста.")
    else:
        text = "📄 История заявлений:\n"
        for content, timestamp in rows:
            text += f"• {timestamp}: {content}\n"
        await message.answer(text)


@dp.message(MayorMenu.managing_city, F.text == "📄 история рассылок")
async def broadcast_history(message: types.Message):
    """Показывает историю рассылок мэра"""
    user_id = message.from_user.id

    async with aiosqlite.connect("database.db") as db:
        # Проверяем, является ли пользователь мэром
        cursor = await db.execute(
            "SELECT name FROM cities WHERE mayor_id = ?",
            (user_id,)
        )
        city_data = await cursor.fetchone()

        if not city_data:
            await message.answer("❌ Вы не являетесь мэром ни одного города.")
            return

        city_name = city_data[0]

        # Получаем историю рассылок
        cursor = await db.execute("""
            SELECT 
                message_type, 
                message, 
                sent_count, 
                failed_count, 
                timestamp,
                strftime('%d.%m.%Y %H:%M', timestamp) as formatted_date
            FROM mayor_broadcasts 
            WHERE mayor_id = ? AND city_name = ?
            ORDER BY timestamp DESC
            LIMIT 10
        """, (user_id, city_name))

        broadcasts = await cursor.fetchall()

        if not broadcasts:
            await message.answer(f"📭 В городе {city_name} еще не было рассылок.")
            return

        # Общая статистика
        cursor = await db.execute("""
            SELECT 
                COUNT(*) as total_broadcasts,
                SUM(sent_count) as total_sent,
                SUM(failed_count) as total_failed
            FROM mayor_broadcasts 
            WHERE mayor_id = ? AND city_name = ?
        """, (user_id, city_name))

        stats = await cursor.fetchone()
        total_broadcasts, total_sent, total_failed = stats if stats else (0, 0, 0)

        response = f"📊 <b>История рассылок города {city_name}</b>\n\n"
        response += f"📅 Всего рассылок: {total_broadcasts}\n"
        response += f"✅ Успешно отправлено: {total_sent} сообщений\n"
        response += f"❌ Не доставлено: {total_failed} сообщений\n\n"
        response += "<b>Последние 10 рассылок:</b>\n\n"

        for idx, (msg_type, msg, sent, failed, timestamp, formatted_date) in enumerate(broadcasts, 1):
            # Короткое описание сообщения
            short_msg = msg[:50] + "..." if msg and len(msg) > 50 else msg or "[медиа]"

            response += f"<b>{idx}. {formatted_date}</b>\n"
            response += f"   📝 Тип: {msg_type}\n"
            response += f"   💬 Сообщение: {short_msg}\n"
            response += f"   📊 Статистика: {sent}✅/{failed}❌\n"
            response += "─" * 30 + "\n"

        response += f"\n<i>Чтобы сделать новую рассылку, используйте меню мэра.</i>"

        await message.answer(response, parse_mode="HTML")


# История записей ко врачу
@dp.message(F.text == "🧑‍⚕ История записей ко врачу")
async def appointment_history(message: types.Message):
    async with aiosqlite.connect("database.db") as db:
        cursor = await db.execute("SELECT doctor, time, timestamp FROM appointments WHERE user_id = ?",
                                  (message.from_user.id,))
        rows = await cursor.fetchall()
    if not rows:
        await message.answer("История записей пуста.")
    else:
        text = "🧑‍⚕ История записей ко врачу:\n"
        for doctor, time, timestamp in rows:
            text += f"• {doctor} в {time} | {timestamp}\n"
        await message.answer(text)


# Обработчик кнопки Назад из меню истории
@dp.message(F.text == "↩ Назад")
async def back_to_main_from_history(message: types.Message):
    await show_main_menu(message, message.from_user.id)


"""
Накопительные счета
"""


async def tax_checker():
    while True:
        await asyncio.sleep(86400)  # Проверяем раз в день
        await calculate_savings_interest()  # Начисляем проценты


# Обработчик пополнения счета
@dp.message(F.text == "📥 Пополнить накопительный счет")
async def add_to_savings(message: types.Message, state: FSMContext):
    user_id = message.from_user.id

    async with aiosqlite.connect("database.db") as db:
        cursor = await db.execute(
            "SELECT balance FROM users WHERE user_id = ?",
            (user_id,)
        )
        main_balance = (await cursor.fetchone())[0]

        if main_balance < 1:
            await message.answer("❌ На основном счете нет средств для пополнения!")
            return

    await message.answer(
        f"💰 Ваш основной баланс: {main_balance} шуек\n"
        "Введите сумму для пополнения накопительного счета:",
        reply_markup=ReplyKeyboardRemove()
    )
    await state.set_state("waiting_savings_deposit")


# Обработчик ввода суммы пополнения
@dp.message(F.text, StateFilter("waiting_savings_deposit"))
async def process_savings_deposit(message: types.Message, state: FSMContext):
    try:
        amount = int(message.text.strip())
        if amount <= 0:
            await message.answer("❌ Сумма должна быть положительной!")
            return
    except ValueError:
        await message.answer("❌ Введите корректное число!")
        return

    user_id = message.from_user.id

    async with aiosqlite.connect("database.db") as db:
        # Проверяем основной баланс
        cursor = await db.execute(
            "SELECT balance FROM users WHERE user_id = ?",
            (user_id,)
        )
        main_balance = (await cursor.fetchone())[0]

        if amount > main_balance:
            await message.answer(
                f"❌ Недостаточно средств на основном счете!\n"
                f"Нужно: {amount} шуек\n"
                f"Доступно: {main_balance} шуек"
            )
            await state.clear()
            return

        # Проверяем наличие накопительного счета
        cursor = await db.execute(
            "SELECT 1 FROM savings_accounts WHERE user_id = ?",
            (user_id,)
        )
        if not await cursor.fetchone():
            await message.answer("❌ У вас нет накопительного счета!")
            await state.clear()
            return

        # Переводим средства
        await db.execute(
            "UPDATE users SET balance = balance - ? WHERE user_id = ?",
            (amount, user_id)
        )
        await db.execute(
            "UPDATE savings_accounts SET balance = balance + ? WHERE user_id = ?",
            (amount, user_id)
        )
        await db.commit()

        await message.answer(
            f"✅ Успешно пополнено на {amount} шуек!\n"
            f"💰 Новый баланс накопительного счета: {await get_savings_balance(user_id)} шуек",
            reply_markup=await savings_menu_kb()
        )

    await state.clear()


# Функция для получения баланса накопительного счета
async def get_savings_balance(user_id: int):
    async with aiosqlite.connect("database.db") as db:
        cursor = await db.execute(
            "SELECT balance FROM savings_accounts WHERE user_id = ?",
            (user_id,)
        )
        result = await cursor.fetchone()
        return result[0] if result else 0


# Обработчик снятия средств
@dp.message(F.text == "📤 Снять с накопительного счета")
async def withdraw_from_savings(message: types.Message, state: FSMContext):
    user_id = message.from_user.id

    async with aiosqlite.connect("database.db") as db:
        cursor = await db.execute(
            "SELECT balance FROM savings_accounts WHERE user_id = ?",
            (user_id,)
        )
        result = await cursor.fetchone()
        savings_balance = result[0] if result else 0

        if savings_balance < 1:
            await message.answer("❌ На накопительном счете нет средств для снятия!")
            return

    await message.answer(
        f"💰 Баланс накопительного счета: {savings_balance} шуек\n"
        "Введите сумму для снятия:",
        reply_markup=ReplyKeyboardRemove()
    )
    await state.set_state("waiting_savings_withdraw")


# Обработчик ввода суммы снятия
@dp.message(F.text, StateFilter("waiting_savings_withdraw"))
async def process_savings_withdraw(message: types.Message, state: FSMContext):
    try:
        amount = int(message.text.strip())
        if amount <= 0:
            await message.answer("❌ Сумма должна быть положительной!")
            return
    except ValueError:
        await message.answer("❌ Введите корректное число!")
        return

    user_id = message.from_user.id

    async with aiosqlite.connect("database.db") as db:
        # Проверяем баланс накопительного счета
        cursor = await db.execute(
            "SELECT balance FROM savings_accounts WHERE user_id = ?",
            (user_id,)
        )
        savings_balance = (await cursor.fetchone())[0]

        if amount > savings_balance:
            await message.answer(
                f"❌ Недостаточно средств на накопительном счете!\n"
                f"Нужно: {amount} шуек\n"
                f"Доступно: {savings_balance} шуек"
            )
            await state.clear()
            return

        # Переводим средства
        await db.execute(
            "UPDATE savings_accounts SET balance = balance - ? WHERE user_id = ?",
            (amount, user_id)
        )
        await db.execute(
            "UPDATE users SET balance = balance + ? WHERE user_id = ?",
            (amount, user_id)
        )
        await db.commit()

        await message.answer(
            f"✅ Успешно снято {amount} шуек!\n"
            f"💰 Новый баланс накопительного счета: {savings_balance - amount} шуек",
            reply_markup=await savings_menu_kb()
        )

    await state.clear()


# Функция для начисления процентов
async def calculate_savings_interest():
    async with aiosqlite.connect("database.db") as db:
        # Получаем все счета, которым пора начислить проценты
        cursor = await db.execute("""
            SELECT user_id, balance, last_interest_date 
            FROM savings_accounts 
            WHERE balance > 0 
            AND date(last_interest_date) <= date('now', '-30 days')
        """)
        accounts = await cursor.fetchall()

        for user_id, balance, last_interest in accounts:
            interest = int(balance * 0.05)  # 5% от баланса

            if interest > 0:
                # Начисляем проценты
                await db.execute(
                    "UPDATE savings_accounts SET balance = balance + ?, last_interest_date = CURRENT_TIMESTAMP WHERE user_id = ?",
                    (interest, user_id)
                )

                # Уведомляем пользователя
                try:
                    await bot.send_message(
                        user_id,
                        f"🎉 <b>Начисление процентов</b>\n\n"
                        f"На ваш накопительный счет начислено {interest} шуек (5%)\n"
                        f"💰 Новый баланс: {balance + interest} шуек\n"
                        f"📈 Следующее начисление: через 30 дней"
                    )
                except Exception as e:
                    print(f"Не удалось уведомить пользователя {user_id}: {e}")

        await db.commit()


"""
АДМИИИИИИИН
"""


@dp.message(Command("admin_m"))
async def cmd_help(message: types.Message):
    help_text = """
🆘 <b>Список доступных команд:</b> 🆘

<code>/verify_city</code> 
<code>/unverify_city</code> 
<code>/verified_cities</code> 
<code>/delete_city</code> 
<code>/full_reset_cities</code>
<code>/reset_cities</code> 
<code>/clear_cities</code> 
<code>/set_balance</code> 
<code>/users</code> 
<code>/broadcast</code> 
<code>/update_all_govid</code> 
<code>/notify_govid_update</code>     
"""
    await message.answer(help_text, parse_mode="HTML")


@dp.message(Command("verify_city"))
async def verify_city_command(message: types.Message):
    """Добавить галочку городу"""
    if message.from_user.id not in ADMIN_ID:
        await message.answer("❌ У вас нет прав администратора.")
        return

    # Получаем все аргументы после команды
    args = message.text.split()
    if len(args) < 2:
        await message.answer("❌ Использование: /verify_city <название города>")
        return

    # Объединяем все аргументы кроме первого (команды) в название города
    city_name = " ".join(args[1:]).strip()

    # Убираем возможные лишние кавычки
    city_name = city_name.strip('"').strip("'")

    if not city_name:
        await message.answer("❌ Название города не может быть пустым.")
        return

    await message.answer(f"🔄 Ищу город: '{city_name}'...")

    try:
        async with aiosqlite.connect("database.db") as db:
            # Сначала проверяем наличие колонки is_verified
            cursor = await db.execute("PRAGMA table_info(cities)")
            columns = await cursor.fetchall()
            column_names = [col[1] for col in columns]

            if 'is_verified' not in column_names:
                await message.answer(
                    "❌ Колонка 'is_verified' не найдена в таблице cities.\n\n"
                    "Запустите команду для исправления:\n"
                    "<code>/fix_cities_table</code>"
                )
                return

            # Проверяем существование города (чувствительно к регистру)
            cursor = await db.execute(
                "SELECT name FROM cities WHERE name = ? COLLATE NOCASE",
                (city_name,)
            )
            city = await cursor.fetchone()

            if not city:
                # Попробуем найти похожие города
                cursor = await db.execute(
                    "SELECT name FROM cities WHERE name LIKE ? COLLATE NOCASE",
                    (f"%{city_name}%",)
                )
                similar_cities = await cursor.fetchall()

                if similar_cities:
                    response = f"❌ Город '{city_name}' не найден. Возможно вы имели в виду:\n\n"
                    for similar_city in similar_cities[:5]:
                        response += f"• {similar_city[0]}\n"
                    response += f"\nИспользуйте точное название города."
                else:
                    response = f"❌ Город '{city_name}' не найден в базе данных.\n"
                    response += "Проверьте правильность написания."

                await message.answer(response)
                return

            actual_city_name = city[0]  # Получаем точное название из БД

            # Проверяем текущее состояние галочки
            cursor = await db.execute(
                "SELECT is_verified FROM cities WHERE name = ?",
                (actual_city_name,)
            )
            result = await cursor.fetchone()

            if result and result[0]:
                await message.answer(f"✅ Город '{actual_city_name}' уже имеет галочку.")
                return

            # Устанавливаем галочку
            await db.execute(
                "UPDATE cities SET is_verified = 1 WHERE name = ?",
                (actual_city_name,)
            )
            await db.commit()

            # Получаем информацию о городе
            cursor = await db.execute("""
                SELECT mayor_id, population, created_date 
                FROM cities WHERE name = ?
            """, (actual_city_name,))
            city_info = await cursor.fetchone()

            response = f"✅ Городу '{actual_city_name}' успешно добавлена галочка! ✅\n\n"

            if city_info:
                mayor_id, population, created_date = city_info
                if created_date:
                    response += f"📅 Основан: {created_date[:10]}\n"
                if population:
                    response += f"👥 Жителей: {population}\n"

            await message.answer(response)

            # Получаем мэра города для уведомления
            if city_info and city_info[0]:
                mayor_id = city_info[0]
                try:
                    await bot.send_message(
                        mayor_id,
                        f"🎉 Ваш город '{actual_city_name}' получил официальную галочку от администрации! ✅\n\n"
                        f"Теперь ваш город отмечен как проверенный и будет выделяться в списках."
                    )
                except Exception as notify_error:
                    print(f"Не удалось уведомить мэра {mayor_id}: {notify_error}")

    except Exception as e:
        error_msg = str(e)
        await message.answer(f"❌ Произошла ошибка:\n{error_msg[:200]}")
        print(f"Ошибка verify_city_command: {e}")


@dp.message(Command("unverify_city"))
async def unverify_city_command(message: types.Message):
    """Убрать галочку у города"""
    if message.from_user.id not in ADMIN_ID:
        await message.answer("❌ У вас нет прав администратора.")
        return

    # Получаем полный текст сообщения
    full_text = message.text or ""

    # Разделяем команду и аргументы
    parts = full_text.split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("❌ Использование: /unverify_city <название города>")
        return

    city_name = parts[1].strip()

    async with aiosqlite.connect("database.db") as db:
        # Проверяем существование города
        cursor = await db.execute(
            "SELECT name, is_verified FROM cities WHERE name = ?",
            (city_name,)
        )
        city = await cursor.fetchone()

        if not city:
            await message.answer(f"❌ Город '{city_name}' не найден.")
            return

        current_name, current_verified = city

        if not current_verified:
            await message.answer(f"❌ У города '{city_name}' нет галочки.")
            return

        # Убираем галочку
        await db.execute(
            "UPDATE cities SET is_verified = 0 WHERE name = ?",
            (city_name,)
        )
        await db.commit()

        await message.answer(f"✅ Галочка убрана у города '{city_name}'.")


@dp.message(Command("verified_cities"))
async def list_verified_cities(message: types.Message):
    """Список городов с зелёными галочками"""
    async with aiosqlite.connect("database.db") as db:
        # Проверяем наличие колонки
        cursor = await db.execute("PRAGMA table_info(cities)")
        columns = await cursor.fetchall()
        column_names = [col[1] for col in columns]

        if 'is_verified' not in column_names:
            await message.answer(
                "❌ Колонка 'is_verified' не найдена в таблице cities.\n\n"
                "Запустите команду для исправления:\n"
                "<code>/force_update_db</code>"
            )
            return

        cursor = await db.execute("""
            SELECT 
                c.name, 
                u.name as mayor_name,
                c.population,
                c.created_date,
                (SELECT COUNT(*) FROM users WHERE city = c.name) as citizens_count
            FROM cities c
            LEFT JOIN users u ON c.mayor_id = u.user_id
            WHERE c.is_verified = 1
            ORDER BY c.name
        """)
        verified_cities = await cursor.fetchall()

    if not verified_cities:
        await message.answer("🏙️ Городов с зелёными галочками пока нет.")
        return

    text = "✅ <b>Города с официальными зелёными галочками:</b>\n\n"

    for idx, (city_name, mayor_name, population, created_date, citizens_count) in enumerate(verified_cities, 1):
        # ЗЕЛЁНАЯ ГАЛОЧКА в начале каждой строки
        text += f"✅ <b>{city_name}</b>\n"
        text += f"   👑 Мэр: {mayor_name or 'Нет мэра'}\n"
        text += f"   👥 Жителей: {citizens_count or 0}\n"
        if created_date:
            text += f"   📅 Основан: {created_date[:10]}\n"
        text += "\n"

    text += f"📊 Всего городов с зелёными галочками: {len(verified_cities)}"

    await message.answer(text, parse_mode="HTML")


# Обработчик подтверждения города администратором
@dp.callback_query(F.data.startswith("approve_city:"))
async def approve_city(callback: types.CallbackQuery):
    data_parts = callback.data.split(":")
    if len(data_parts) != 3:
        await callback.answer("❌ Неверный формат данных")
        return

    user_id = int(data_parts[1])
    city_name = data_parts[2]
    admin_id = callback.from_user.id

    if admin_id not in ADMIN_ID:
        await callback.answer("❌ У вас нет прав администратора")
        return

    async with aiosqlite.connect("database.db") as db:
        try:
            # Получаем данные из временной таблицы
            cursor = await db.execute(
                "SELECT name, gender FROM temp_registrations WHERE user_id = ?",
                (user_id,)
            )
            reg_data = await cursor.fetchone()

            if not reg_data:
                await callback.answer("Заявка не найдена!")
                return

            name, gender = reg_data

            # Регистрируем город
            await db.execute(
                "INSERT INTO cities (name, mayor_id, created_date) VALUES (?, ?, CURRENT_TIMESTAMP)",
                (city_name, user_id)
            )

            # Генерируем номер счета и регистрируем пользователя
            account_number = await generate_account_number(city_name, db)  # Используем новую функцию
            await db.execute(
                "INSERT INTO users (user_id, account_id, name, gender, city, balance) "
                "VALUES (?, ?, ?, ?, ?, 1)",
                (user_id, account_number, name, gender, city_name)
            )

            # Удаляем из временной таблицы
            await db.execute(
                "DELETE FROM temp_registrations WHERE user_id = ?",
                (user_id,)
            )
            await db.commit()

            # Получаем дату создания для отображения
            cursor = await db.execute(
                "SELECT created_date FROM cities WHERE name = ?",
                (city_name,)
            )
            creation_date_result = await cursor.fetchone()
            creation_date = creation_date_result[0] if creation_date_result else "сегодня"

            # Уведомляем пользователя
            await bot.send_message(
                user_id,
                f"✅ Ваша заявка на регистрацию города '{city_name}' одобрена администратором!\n\n"
                f"🏙️ Город {city_name} успешно зарегистрирован!\n"
                f"📅 Дата основания: {creation_date[:10]}\n"
                f"👑 Вы стали первым мэром этого города.\n"
                f"💳 Ваш номер счета: {account_number}",
                reply_markup=await main_menu_kb(user_id)
            )

            await callback.message.edit_text(
                f"✅ Город '{city_name}' одобрен!\n"
                f"Пользователь {name} теперь мэр этого города."
            )

            await callback.answer("Город одобрен!")

        except Exception as e:
            await callback.answer("❌ Ошибка при обработке заявки")
            print(f"Ошибка подтверждения города: {e}")


# Обработчик отклонения города администратором
@dp.callback_query(F.data.startswith("reject_city:"))
async def reject_city(callback: types.CallbackQuery):
    data_parts = callback.data.split(":")
    if len(data_parts) != 3:
        await callback.answer("❌ Неверный формат данных")
        return

    user_id = int(data_parts[1])
    city_name = data_parts[2]
    admin_id = callback.from_user.id

    if admin_id not in ADMIN_ID:
        await callback.answer("❌ У вас нет прав администратора")
        return

    async with aiosqlite.connect("database.db") as db:
        try:
            # Удаляем из временной таблицы
            await db.execute(
                "DELETE FROM temp_registrations WHERE user_id = ?",
                (user_id,)
            )
            await db.commit()

            # Уведомляем пользователя
            await bot.send_message(
                user_id,
                f"❌ Ваша заявка на регистрацию города '{city_name}' отклонена администратором.\n\n"
                f"Пожалуйста, выберите другой город или обратитесь к администрации для уточнения причин.",
                reply_markup=await cities_kb()
            )

            await callback.message.edit_text(f"❌ Город '{city_name}' отклонен")

            await callback.answer("Город отклонен")

        except Exception as e:
            await callback.answer("❌ Ошибка при обработке заявки")
            print(f"Ошибка отклонения города: {e}")


async def update_existing_cities():
    """Добавляет created_date к существующим городам и заполняет его"""
    async with aiosqlite.connect("database.db") as db:
        try:
            # 1. Проверяем, есть ли колонка created_date
            cursor = await db.execute("PRAGMA table_info(cities)")
            columns = await cursor.fetchall()
            column_names = [col[1] for col in columns]

            # 2. Если колонки нет - добавляем
            if 'created_date' not in column_names:
                print("🔄 Добавляю колонку created_date в таблицу cities...")
                await db.execute("ALTER TABLE cities ADD COLUMN created_date DATETIME")
                await db.commit()

            # 3. Проверяем, у каких городов нет даты создания
            cursor = await db.execute("""
                SELECT name FROM cities WHERE created_date IS NULL OR created_date = ''
            """)
            cities_without_date = await cursor.fetchall()

            if cities_without_date:
                print(f"🔄 Обновляю даты создания для {len(cities_without_date)} городов...")

                # 4. Устанавливаем текущую дату для городов без даты
                # Можно использовать дату из таблицы users как приблизительную
                for city_name in cities_without_date:
                    city_name = city_name[0]

                    # Пытаемся найти самую раннюю дату регистрации пользователя в этом городе
                    cursor = await db.execute("""
                        SELECT MIN(created_date) FROM users 
                        WHERE city = ? AND created_date IS NOT NULL
                    """, (city_name,))

                    earliest_user_date = (await cursor.fetchone())[0]

                    if earliest_user_date:
                        # Используем дату первого пользователя
                        await db.execute(
                            "UPDATE cities SET created_date = ? WHERE name = ?",
                            (earliest_user_date, city_name)
                        )
                    else:
                        # Или текущую дату как fallback
                        await db.execute(
                            "UPDATE cities SET created_date = datetime('now') WHERE name = ?",
                            (city_name,)
                        )

                await db.commit()
                print(f"✅ Обновлено {len(cities_without_date)} городов")

        except Exception as e:
            print(f"❌ Ошибка при обновлении городов: {e}")


# установка времени регистрации города
@dp.message(Command("delete_city"))
async def delete_city(message: types.Message):
    if message.from_user.id not in ADMIN_ID:
        await message.answer("У вас нет прав для этой команды.")
        return

    command_args = message.text.split(maxsplit=1)
    if len(command_args) < 2:
        await message.answer("Укажите название города: /delete_city <название>")
        return

    city = command_args[1].strip()

    try:
        async with aiosqlite.connect("database.db") as db:
            cursor = await db.execute("SELECT 1 FROM cities WHERE name = ?", (city,))
            if not await cursor.fetchone():
                await message.answer(f"Город {city} не найден в базе данных.")
                return

            await db.execute("DELETE FROM cities WHERE name = ?", (city,))
            await db.commit()

            # Обновляем клавиатуру
            keyboard = await get_cities_keyboard()
            await message.answer(
                f"Город {city} успешно удалён",
            )
    except Exception as e:
        await message.answer(f"Произошла ошибка при удалении города: {e}")


@dp.message(Command("full_reset_cities"))
async def full_reset_cities(message: types.Message):
    if message.from_user.id not in ADMIN_ID:
        await message.answer("У вас нет прав для этой команды.")
        return

    try:
        async with aiosqlite.connect("database.db") as db:
            await db.execute("DELETE FROM cities")
            await db.execute("UPDATE users SET city = 'Не указан'")
            await db.commit()

            # Обновляем клавиатуру (останется пустая или с дефолтными кнопками)
            keyboard = await get_cities_keyboard()
            await message.answer(
                "Полный сброс городов выполнен успешно.",
                reply_markup=keyboard
            )
    except Exception as e:
        await message.answer(f"Ошибка при полном сбросе городов: {e}")
    finally:
        await show_main_menu(message, message.from_user.id)


@dp.message(Command("reset_cities"))
async def reset_cities(message: types.Message):
    if message.from_user.id not in ADMIN_ID:
        await message.answer("У вас нет прав для этой команды.")
        return

    try:
        async with aiosqlite.connect("database.db") as db:
            await db.execute("DELETE FROM cities")
            await db.commit()

            # Обновляем клавиатуру
            keyboard = await get_cities_keyboard()
            await message.answer(
                "Таблица городов полностью очищена.",
                reply_markup=keyboard
            )
    except Exception as e:
        await message.answer(f"Ошибка при очистке таблицы городов: {e}")
    finally:
        await show_main_menu(message, message.from_user.id)


@dp.message(Command("clear_cities"))
async def clear_cities(message: types.Message):
    if message.from_user.id not in ADMIN_ID:
        await message.answer("У вас нет прав для этой команды.")
        return

    try:
        async with aiosqlite.connect("database.db") as db:
            cursor = await db.execute("SELECT COUNT(*) FROM cities WHERE mayor_id IS NULL")
            count = (await cursor.fetchone())[0]

            if count == 0:
                await message.answer("Нет незанятых городов для удаления.")
                return

            await db.execute("DELETE FROM cities WHERE mayor_id IS NULL")
            await db.commit()

            # Обновляем клавиатуру
            keyboard = await get_cities_keyboard()
            await message.answer(
                f"Успешно удалено {count} незанятых городов.",
                reply_markup=keyboard
            )
    except Exception as e:
        await message.answer(f"Ошибка при очистке незанятых городов: {e}")
    finally:
        await show_main_menu(message, message.from_user.id)


@dp.message(Command("set_balance"))
async def handle_set_balance_command(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_ID:
        await message.answer("У вас нет прав для выполнения этой команды.")
        await show_main_menu(message, message.from_user.id)

        return
    await message.answer("Пожалуйста, отправьте ID пользователя, чей баланс нужно изменить:")
    await state.set_state(SetBalance.waiting_for_user_id)


@dp.message(StateFilter(SetBalance.waiting_for_user_id))
async def handle_user_id(message: types.Message, state: FSMContext):
    try:
        user_id = int(message.text.strip())
        await state.update_data(user_id=user_id)
        await message.answer("Введите новый баланс:")
        await state.set_state(SetBalance.waiting_for_balance)
    except ValueError:
        await message.answer("Пожалуйста, введите корректный числовой ID пользователя.")


@dp.message(StateFilter(SetBalance.waiting_for_balance))
async def handle_new_balance(message: types.Message, state: FSMContext):
    try:
        new_balance = int(message.text.strip())
        data = await state.get_data()
        user_id = data['user_id']
        async with aiosqlite.connect("database.db") as db:
            await db.execute("UPDATE users SET balance = ? WHERE user_id = ?", (new_balance, user_id))
            await db.commit()
        await message.answer(f"Баланс пользователя {user_id} успешно обновлён на {new_balance}.")
        await state.clear()
        await show_main_menu(message, message.from_user.id)

    except ValueError:
        await message.answer("Пожалуйста, введите корректное число для баланса.")


@dp.message(Command("users"))
async def handle_list_users(message: types.Message):
    if message.from_user.id not in ADMIN_ID:
        await message.answer("У вас нет прав для этой команды.")
        return
    async with aiosqlite.connect("database.db") as db:
        cursor = await db.execute("SELECT user_id, account_id, name, balance, city FROM users")
        users = await cursor.fetchall()
    if not users:
        await message.answer("Пользователи не найдены.")
        return
    text = "Список пользователей:\n"
    for user_id, account_id, name, balance, city in users:
        text += f"тг ID: {user_id} госID {account_id}\nИмя: {name}\n-----Баланс: {balance}\n-----Город: {city}\n\n"
    await message.answer(text)
    await show_main_menu(message, message.from_user.id)


# рассылка
@dp.message(Command("broadcast"))
async def cmd_broadcast(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_ID:
        await message.answer("У вас нет прав для выполнения этой команды.")
        return

    await message.answer(
        "Введите сообщение или отправьте медиа (фото, видео, GIF, стикер) для рассылки всем пользователям:",
        reply_markup=ReplyKeyboardRemove()
    )
    await state.set_state(Broadcast.waiting_for_message)


@dp.message(Broadcast.waiting_for_message)
async def process_broadcast_message(message: types.Message, state: FSMContext):
    # Сохраняем либо текст, либо медиа
    if message.text:
        await state.update_data(broadcast_message=message.text, broadcast_media_type="text")
    elif message.photo:
        await state.update_data(
            broadcast_message=message.caption,
            broadcast_media_type="photo",
            broadcast_media=message.photo[-1].file_id  # Берем самое высокое качество
        )
    elif message.video:
        await state.update_data(
            broadcast_message=message.caption,
            broadcast_media_type="video",
            broadcast_media=message.video.file_id
        )
    elif message.animation:  # GIF
        await state.update_data(
            broadcast_message=message.caption,
            broadcast_media_type="animation",
            broadcast_media=message.animation.file_id
        )
    elif message.sticker:
        await state.update_data(
            broadcast_message=None,
            broadcast_media_type="sticker",
            broadcast_media=message.sticker.file_id
        )
    else:
        await message.answer("Пожалуйста, отправьте текст или медиа-файл (фото, видео, GIF или стикер).")
        return
    # Формируем сообщение для подтверждения
    confirm_text = "Подтвердите рассылку следующего сообщения:\n\n"
    data = await state.get_data()
    if data.get('broadcast_media_type') == "text":
        confirm_text += data.get("broadcast_message", "Текст отсутствует")
    else:
        if data.get("broadcast_message"):
            confirm_text += f"Подпись: {data.get('broadcast_message')}\n"
        confirm_text += f"Тип медиа: {data.get('broadcast_media_type')}"
    await message.answer(
        confirm_text,
        reply_markup=confirm_kb()  # Используем уже существующую клавиатуру подтверждения
    )
    await state.set_state(Broadcast.confirm)


# Обработчик подтверждения рассылки
@dp.message(Broadcast.confirm, F.text == "Подтвердить")
async def confirm_broadcast(message: types.Message, state: FSMContext):
    data = await state.get_data()
    broadcast_message = data.get("broadcast_message")
    media_type = data.get("broadcast_media_type")
    media_file_id = data.get("broadcast_media")
    if not broadcast_message and not media_file_id:
        await message.answer("Ошибка: сообщение не найдено.")
        await state.clear()
        await show_main_menu(message, message.from_user.id)
        return

    await message.answer("⏳ Начинаю рассылку сообщения...", reply_markup=ReplyKeyboardRemove())

    successful = 0
    failed = 0

    async with aiosqlite.connect("database.db") as db:
        cursor = await db.execute("SELECT user_id FROM users")
        users = await cursor.fetchall()

        for (user_id,) in users:
            try:
                if media_type == "text":
                    await bot.send_message(
                        user_id,
                        f"📢 Важное сообщение от администрации:\n\n{broadcast_message}"
                    )
                elif media_type == "photo":
                    await bot.send_photo(
                        user_id,
                        photo=media_file_id,
                        caption=f"📢 Важное сообщение от администрации:\n\n{broadcast_message}" if broadcast_message else None
                    )
                elif media_type == "video":
                    await bot.send_video(
                        user_id,
                        video=media_file_id,
                        caption=f"📢 Важное сообщение от администрации:\n\n{broadcast_message}" if broadcast_message else None
                    )
                elif media_type == "animation":
                    await bot.send_animation(
                        user_id,
                        animation=media_file_id,
                        caption=f"📢 Важное сообщение от администрации:\n\n{broadcast_message}" if broadcast_message else None
                    )
                elif media_type == "sticker":
                    await bot.send_sticker(
                        user_id,
                        sticker=media_file_id
                    )

                successful += 1
            except Exception as e:
                print(f"Не удалось отправить сообщение пользователю {user_id}: {e}")
                failed += 1
            await asyncio.sleep(0.1)
    await message.answer(
        f"✅ Рассылка завершена:\n"
        f"Успешно: {successful}\n"
        f"Не удалось: {failed}"
    )
    await state.clear()
    await show_main_menu(message, message.from_user.id)


@dp.message(Broadcast.confirm, F.text == "Отмена")
async def cancel_broadcast(message: types.Message, state: FSMContext):
    await message.answer("Рассылка отменена.", reply_markup=ReplyKeyboardRemove())
    await state.clear()
    await show_main_menu(message, message.from_user.id)


# отправка соо командой прям в чате
@dp.message(Command("say"))
async def say_in_chat(message: types.Message):
    """Отправляет сообщение от имени бота в текущий чат"""
    if message.from_user.id not in ADMIN_ID:
        await message.answer("❌ У вас нет прав для этой команды.")
        return

    # Получаем текст после команды /say
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.answer("❌ Использование: /say <текст сообщения>")
        return

    text = args[1]

    try:

        # Отправляем сообщение от имени бота
        await message.answer(
            text,
            parse_mode="HTML"
        )

    except Exception as e:
        print(f"❌ Ошибка: {str(e)[:100]}")


#
async def save_chat_for_user(user_id: int, chat_id: int, chat_title: str, custom_name: str = None):
    """Сохраняет чат для пользователя"""
    async with aiosqlite.connect("database.db") as db:
        # Создаем таблицу если ее нет
        await db.execute("""
            CREATE TABLE IF NOT EXISTS user_saved_chats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                chat_title TEXT NOT NULL,
                custom_name TEXT,
                is_group INTEGER DEFAULT 1,
                created_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, chat_id)
            )
        """)

        await db.execute("""
            INSERT OR REPLACE INTO user_saved_chats 
            (user_id, chat_id, chat_title, custom_name, is_group)
            VALUES (?, ?, ?, ?, ?)
        """, (user_id, chat_id, chat_title, custom_name, 1 if chat_id < 0 else 0))

        await db.commit()


async def get_user_saved_chats(user_id: int):
    """Получает список сохраненных чатов пользователя"""
    async with aiosqlite.connect("database.db") as db:
        # Сначала создаем таблицу если ее нет
        await db.execute("""
            CREATE TABLE IF NOT EXISTS user_saved_chats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                chat_title TEXT NOT NULL,
                custom_name TEXT,
                is_group INTEGER DEFAULT 1,
                created_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, chat_id)
            )
        """)

        cursor = await db.execute("""
            SELECT id, chat_id, chat_title, custom_name, is_group 
            FROM user_saved_chats 
            WHERE user_id = ?
            ORDER BY custom_name, chat_title
        """, (user_id,))

        return await cursor.fetchall()


async def delete_saved_chat(user_id: int, chat_db_id: int):
    """Удаляет сохраненный чат"""
    async with aiosqlite.connect("database.db") as db:
        await db.execute("""
            DELETE FROM user_saved_chats 
            WHERE id = ? AND user_id = ?
        """, (chat_db_id, user_id))

        await db.commit()


async def update_chat_custom_name(user_id: int, chat_db_id: int, new_name: str):
    """Обновляет пользовательское название чата"""
    async with aiosqlite.connect("database.db") as db:
        await db.execute("""
            UPDATE user_saved_chats 
            SET custom_name = ? 
            WHERE id = ? AND user_id = ?
        """, (new_name, chat_db_id, user_id))

        await db.commit()


@dp.message(Command("send"))
async def start_cross_chat_message(message: types.Message, state: FSMContext):
    """Начинает процесс отправки сообщения в другой чат"""
    user_id = message.from_user.id

    if user_id not in ADMIN_ID:
        await message.answer("❌ Эта функция доступна только администраторам.")
        return

    try:
        # Получаем сохраненные чаты пользователя
        saved_chats = await get_user_saved_chats(user_id)

        if saved_chats:
            # Создаем клавиатуру с сохраненными чатами
            keyboard_buttons = []

            for chat_db_id, chat_id, chat_title, custom_name, is_group in saved_chats:
                display_name = custom_name or chat_title
                emoji = "👥" if is_group else "👤"
                keyboard_buttons.append([
                    KeyboardButton(text=f"{emoji} {display_name}")
                ])

            # Добавляем кнопки для других действий
            keyboard_buttons.append([
                KeyboardButton(text="➕ Добавить новый чат")
            ])
            keyboard_buttons.append([
                KeyboardButton(text="📋 Управление сохраненными чатами")
            ])
            keyboard_buttons.append([
                KeyboardButton(text="❌ Отмена")
            ])

            await message.answer(
                "💬 <b>Отправка сообщения от имени бота</b>\n\n"
                "Выберите чат для отправки сообщения:",
                parse_mode="HTML",
                reply_markup=ReplyKeyboardMarkup(
                    keyboard=keyboard_buttons,
                    resize_keyboard=True
                )
            )
        else:
            await message.answer(
                "💬 <b>Отправка сообщения от имени бота</b>\n\n"
                "У вас нет сохраненных чатов.\n"
                "Добавьте чат для начала работы:",
                parse_mode="HTML",
                reply_markup=ReplyKeyboardMarkup(
                    keyboard=[
                        [KeyboardButton(text="➕ Добавить новый чат")],
                        [KeyboardButton(text="❌ Отмена")]
                    ],
                    resize_keyboard=True
                )
            )

        await state.set_state(CrossChatMessage.waiting_for_chat_selection)

    except Exception as e:
        await message.answer(f"❌ Ошибка при загрузке чатов: {str(e)[:200]}")
        await state.clear()


@dp.message(CrossChatMessage.waiting_for_chat_selection, F.text == "❌ Отмена")
async def cancel_chat_selection(message: types.Message, state: FSMContext):
    """Отмена выбора чата"""
    await message.answer("❌ Отправка отменена.", reply_markup=ReplyKeyboardRemove())
    await state.clear()


@dp.message(CrossChatMessage.waiting_for_chat_selection)
async def process_chat_selection(message: types.Message, state: FSMContext):
    """Обработчик выбора чата"""
    user_id = message.from_user.id

    if message.text == "➕ Добавить новый чат":
        await message.answer(
            "📝 <b>Добавление нового чата</b>\n\n"
            "Отправьте ID чата (можно получить через @username_to_id_bot):\n\n"
            "<i>Или перешлите любое сообщение из нужного чата</i>",
            parse_mode="HTML",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="❌ Отмена")]],
                resize_keyboard=True
            )
        )
        await state.set_state(CrossChatMessage.waiting_for_chat_input)
        return

    elif message.text == "📋 Управление сохраненными чатами":
        await manage_saved_chats(message, state)
        return

    # Ищем выбранный чат в сохраненных
    saved_chats = await get_user_saved_chats(user_id)
    selected_chat = None

    for chat_db_id, chat_id, chat_title, custom_name, is_group in saved_chats:
        display_name = custom_name or chat_title
        if message.text.endswith(display_name):
            selected_chat = (chat_db_id, chat_id, chat_title, custom_name, is_group)
            break

    if not selected_chat:
        await message.answer("❌ Чат не найден. Выберите из списка.")
        return

    chat_db_id, chat_id, chat_title, custom_name, is_group = selected_chat

    # Сохраняем выбранный чат в состоянии
    await state.update_data(
        selected_chat_id=chat_id,
        selected_chat_title=chat_title,
        selected_chat_custom_name=custom_name,
        selected_chat_db_id=chat_db_id,
        selected_chat_is_group=is_group
    )

    # Проверяем, поддерживает ли чат ветки (только для супергрупп)
    try:
        chat_info = await bot.get_chat(chat_id)

        if chat_info.is_forum and chat_info.message_thread_id:
            # Чат поддерживает ветки, показываем выбор
            await ask_for_thread(message, state, chat_id, chat_info)
        else:
            # Переходим к вводу сообщения
            await ask_for_message_content(message, state)

    except Exception as e:
        await message.answer(
            f"❌ Не удалось получить информацию о чате: {str(e)[:100]}\n"
            f"Переходим к вводу сообщения...",
            parse_mode="HTML"
        )
        await ask_for_message_content(message, state)


async def ask_for_thread(message: types.Message, state: FSMContext, chat_id: int, chat_info):
    """Спрашивает выбор ветки для отправки"""
    data = await state.get_data()
    chat_title = data.get('selected_chat_title', 'Чат')

    try:
        # Получаем активные ветки (темы)
        # В реальном API нет прямого метода, поэтому делаем кнопку для ручного ввода

        await message.answer(
            f"💬 <b>Выбран чат:</b> {chat_title}\n\n"
            f"Этот чат поддерживает темы (ветки).\n"
            f"Хотите отправить в конкретную ветку?\n\n"
            f"<i>Если да - введите ID ветки (число)\n"
            f"Если нет - нажмите 'Без ветки'</i>",
            parse_mode="HTML",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="🚫 Без ветки")],
                    [KeyboardButton(text="❌ Отмена")]
                ],
                resize_keyboard=True
            )
        )

        await state.set_state(CrossChatMessage.waiting_for_thread_selection)

    except Exception as e:
        # Если не удалось получить ветки, просто идем дальше
        await message.answer(
            f"❌ Не удалось получить список веток: {str(e)[:100]}\n"
            f"Отправляем без выбора ветки...",
            parse_mode="HTML"
        )
        await ask_for_message_content(message, state)


@dp.message(CrossChatMessage.waiting_for_thread_selection)
async def process_thread_selection(message: types.Message, state: FSMContext):
    """Обработчик выбора ветки"""
    if message.text == "❌ Отмена":
        await message.answer("❌ Отправка отменена.", reply_markup=ReplyKeyboardRemove())
        await state.clear()
        return

    if message.text == "🚫 Без ветки":
        await state.update_data(selected_thread_id=None)
        await ask_for_message_content(message, state)
        return

    try:
        thread_id = int(message.text)
        await state.update_data(selected_thread_id=thread_id)
        await ask_for_message_content(message, state)
    except ValueError:
        await message.answer("❌ Введите корректный ID ветки (число)")


@dp.message(CrossChatMessage.waiting_for_chat_input)
async def process_chat_id_input(message: types.Message, state: FSMContext):
    """Обработчик ввода ID чата"""
    if message.text == "❌ Отмена":
        await message.answer("❌ Добавление отменено.", reply_markup=ReplyKeyboardRemove())
        await state.clear()
        return

    # Проверяем, переслано ли сообщение
    if message.forward_from_chat:
        # Пользователь переслал сообщение из чата
        chat_id = message.forward_from_chat.id
        chat_title = message.forward_from_chat.title or "Без названия"
        is_group = message.forward_from_chat.type in ['group', 'supergroup', 'channel']

    else:
        # Пользователь ввел ID вручную
        try:
            chat_id = int(message.text)
            chat_info = await bot.get_chat(chat_id)
            chat_title = chat_info.title or "Без названия"
            is_group = chat_info.type in ['group', 'supergroup', 'channel']

        except ValueError:
            await message.answer("❌ Введите корректный ID чата (число)")
            return
        except Exception as e:
            await message.answer(f"❌ Не удалось получить чат: {str(e)[:100]}")
            return

    # Сохраняем чат в состоянии для дальнейшего использования
    await state.update_data(
        new_chat_id=chat_id,
        new_chat_title=chat_title,
        new_chat_is_group=is_group
    )

    # Запрашиваем пользовательское название
    await message.answer(
        f"✅ Чат получен: <b>{chat_title}</b>\n\n"
        f"Введите пользовательское название для этого чата\n"
        f"(или нажмите '🏷️ Оставить оригинальное'):",
        parse_mode="HTML",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="🏷️ Оставить оригинальное")],
                [KeyboardButton(text="❌ Отмена")]
            ],
            resize_keyboard=True
        )
    )
    # Устанавливаем новое состояние для обработки названия
    await state.set_state("waiting_for_custom_name")


@dp.message(F.text == "🏷️ Оставить оригинальное")
async def keep_original_name(message: types.Message, state: FSMContext):
    """Оставляет оригинальное название чата"""
    data = await state.get_data()
    chat_id = data.get('new_chat_id')
    chat_title = data.get('new_chat_title')
    if not chat_id:
        await message.answer("❌ Ошибка: данные чата не найдены.")
        await state.clear()
        return

    # Сохраняем чат в базу
    await save_chat_for_user(message.from_user.id, chat_id, chat_title, None)

    await message.answer(
        f"✅ Чат <b>{chat_title}</b> успешно сохранен!\n\n"
        f"Теперь вы можете отправлять в него сообщения.",
        parse_mode="HTML",
        reply_markup=ReplyKeyboardRemove()
    )

    # Возвращаем к началу отправки
    await start_cross_chat_message(message, state)


@dp.message(StateFilter("waiting_for_custom_name"))
async def process_custom_name(message: types.Message, state: FSMContext):
    """Обработчик ввода пользовательского названия чата"""
    if message.text == "❌ Отмена":
        await message.answer("❌ Добавление отменено.", reply_markup=ReplyKeyboardRemove())
        await state.clear()
        return

    data = await state.get_data()
    chat_id = data.get('new_chat_id')
    chat_title = data.get('new_chat_title')

    if not chat_id:
        await message.answer("❌ Ошибка: данные чата не найдены.")
        await state.clear()
        return

    # Определяем пользовательское название
    if message.text == "🏷️ Оставить оригинальное":
        custom_name = chat_title  # Используем оригинальное название
    else:
        custom_name = message.text.strip()
        if len(custom_name) > 50:
            custom_name = custom_name[:50]

    # Сохраняем чат
    await save_chat_for_user(message.from_user.id, chat_id, chat_title, custom_name)

    await message.answer(
        f"✅ Чат сохранен как: <b>{custom_name}</b>\n"
        f"Оригинальное название: {chat_title}\n\n"
        f"Теперь вы можете отправлять в него сообщения.",
        parse_mode="HTML",
        reply_markup=ReplyKeyboardRemove()
    )

    # Возвращаем к началу отправки
    await start_cross_chat_message(message, state)


async def ask_for_message_content(message: types.Message, state: FSMContext):
    """Запрашивает содержание сообщения"""
    data = await state.get_data()
    chat_title = data.get('selected_chat_custom_name') or data.get('selected_chat_title', 'Чат')
    thread_id = data.get('selected_thread_id')

    thread_info = ""
    if thread_id:
        thread_info = f" в ветку ID: {thread_id}"

    await message.answer(
        f"💬 <b>Отправка в:</b> {chat_title}{thread_info}\n\n"
        f"📝 <b>Введите текст сообщения:</b>\n\n"
        f"<i>Можно отправить:\n"
        f"• Текст\n"
        f"• Фото с подписью\n"
        f"• Видео с подписью\n"
        f"• Документ с подписью\n"
        f"• Стикер</i>",
        parse_mode="HTML",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="❌ Отмена")]],
            resize_keyboard=True
        )
    )

    await state.set_state(CrossChatMessage.waiting_for_message_content)


@dp.message(CrossChatMessage.waiting_for_message_content)
async def process_message_content(message: types.Message, state: FSMContext):
    """Обработчик содержания сообщения"""
    if message.text == "❌ Отмена":
        await message.answer("❌ Отправка отменена.", reply_markup=ReplyKeyboardRemove())
        await state.clear()
        return

    # Сохраняем сообщение в состоянии
    message_data = {}

    if message.text:
        message_data = {
            'type': 'text',
            'content': message.text
        }
    elif message.photo:
        message_data = {
            'type': 'photo',
            'file_id': message.photo[-1].file_id,
            'caption': message.caption or ''
        }
    elif message.video:
        message_data = {
            'type': 'video',
            'file_id': message.video.file_id,
            'caption': message.caption or ''
        }
    elif message.document:
        message_data = {
            'type': 'document',
            'file_id': message.document.file_id,
            'caption': message.caption or ''
        }
    elif message.sticker:
        message_data = {
            'type': 'sticker',
            'file_id': message.sticker.file_id
        }
    elif message.animation:  # GIF
        message_data = {
            'type': 'animation',
            'file_id': message.animation.file_id,
            'caption': message.caption or ''
        }
    else:
        await message.answer("❌ Неподдерживаемый тип сообщения.")
        return

    await state.update_data(message_data=message_data)

    # Переходим к подтверждению
    await confirm_message_sending(message, state)


async def confirm_message_sending(message: types.Message, state: FSMContext):
    """Показывает предпросмотр и запрашивает подтверждение"""
    data = await state.get_data()
    chat_title = data.get('selected_chat_custom_name') or data.get('selected_chat_title', 'Чат')
    thread_id = data.get('selected_thread_id')
    message_data = data.get('message_data', {})

    thread_info = ""
    if thread_id:
        thread_info = f"\n🧵 <b>Ветка:</b> ID {thread_id}"

    preview_text = (
        f"📤 <b>Подтверждение отправки</b>\n\n"
        f"💬 <b>Чат:</b> {chat_title}{thread_info}\n\n"
    )

    if message_data['type'] == 'text':
        text_preview = message_data['content'][:200] + "..." if len(message_data['content']) > 200 else message_data[
            'content']
        preview_text += f"📝 <b>Текст:</b>\n{text_preview}\n\n"
    elif message_data['type'] == 'photo':
        preview_text += f"🖼 <b>Фото</b>"
        if message_data['caption']:
            caption_preview = message_data['caption'][:100] + "..." if len(message_data['caption']) > 100 else \
                message_data['caption']
            preview_text += f"\n📝 Подпись: {caption_preview}\n\n"
        else:
            preview_text += " (без подписи)\n\n"
    elif message_data['type'] == 'video':
        preview_text += f"🎥 <b>Видео</b>"
        if message_data['caption']:
            caption_preview = message_data['caption'][:100] + "..." if len(message_data['caption']) > 100 else \
                message_data['caption']
            preview_text += f"\n📝 Подпись: {caption_preview}\n\n"
        else:
            preview_text += " (без подписи)\n\n"
    elif message_data['type'] == 'document':
        preview_text += f"📎 <b>Документ</b>"
        if message_data['caption']:
            caption_preview = message_data['caption'][:100] + "..." if len(message_data['caption']) > 100 else \
                message_data['caption']
            preview_text += f"\n📝 Подпись: {caption_preview}\n\n"
        else:
            preview_text += " (без подписи)\n\n"
    elif message_data['type'] == 'sticker':
        preview_text += f"😊 <b>Стикер</b>\n\n"

    preview_text += "✅ Отправить это сообщение?"

    await message.answer(
        preview_text,
        parse_mode="HTML",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="✅ Да, отправить")],
                [KeyboardButton(text="🔄 Изменить текст")],
                [KeyboardButton(text="❌ Отмена")]
            ],
            resize_keyboard=True
        )
    )

    await state.set_state(CrossChatMessage.confirm_message)


@dp.message(CrossChatMessage.confirm_message, F.text == "✅ Да, отправить")
async def send_confirmed_message(message: types.Message, state: FSMContext):
    """Отправляет подтвержденное сообщение"""
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    thread_id = data.get('selected_thread_id')
    message_data = data.get('message_data', {})
    chat_title = data.get('selected_chat_custom_name') or data.get('selected_chat_title', 'Чат')

    try:
        # Отправляем сообщение в указанный чат
        if message_data['type'] == 'text':
            if thread_id:
                sent_msg = await bot.send_message(
                    chat_id=chat_id,
                    message_thread_id=thread_id,
                    text=message_data['content'],
                    parse_mode="HTML"
                )
            else:
                sent_msg = await bot.send_message(
                    chat_id=chat_id,
                    text=message_data['content'],
                    parse_mode="HTML"
                )

        elif message_data['type'] == 'photo':
            if thread_id:
                sent_msg = await bot.send_photo(
                    chat_id=chat_id,
                    message_thread_id=thread_id,
                    photo=message_data['file_id'],
                    caption=message_data.get('caption'),
                    parse_mode="HTML"
                )
            else:
                sent_msg = await bot.send_photo(
                    chat_id=chat_id,
                    photo=message_data['file_id'],
                    caption=message_data.get('caption'),
                    parse_mode="HTML"
                )

        elif message_data['type'] == 'video':
            if thread_id:
                sent_msg = await bot.send_video(
                    chat_id=chat_id,
                    message_thread_id=thread_id,
                    video=message_data['file_id'],
                    caption=message_data.get('caption'),
                    parse_mode="HTML"
                )
            else:
                sent_msg = await bot.send_video(
                    chat_id=chat_id,
                    video=message_data['file_id'],
                    caption=message_data.get('caption'),
                    parse_mode="HTML"
                )

        elif message_data['type'] == 'document':
            if thread_id:
                sent_msg = await bot.send_document(
                    chat_id=chat_id,
                    message_thread_id=thread_id,
                    document=message_data['file_id'],
                    caption=message_data.get('caption'),
                    parse_mode="HTML"
                )
            else:
                sent_msg = await bot.send_document(
                    chat_id=chat_id,
                    document=message_data['file_id'],
                    caption=message_data.get('caption'),
                    parse_mode="HTML"
                )

        elif message_data['type'] == 'sticker':
            if thread_id:
                sent_msg = await bot.send_sticker(
                    chat_id=chat_id,
                    message_thread_id=thread_id,
                    sticker=message_data['file_id']
                )
            else:
                sent_msg = await bot.send_sticker(
                    chat_id=chat_id,
                    sticker=message_data['file_id']
                )

        thread_info = f" в ветку ID: {thread_id}" if thread_id else ""

        await message.answer(
            f"✅ Сообщение успешно отправлено в <b>{chat_title}</b>{thread_info}!\n"
            f"🆔 ID сообщения: <code>{sent_msg.message_id}</code>",
            parse_mode="HTML",
            reply_markup=ReplyKeyboardRemove()
        )

        # Предлагаем отправить еще одно сообщение
        await ask_for_another_message(message, state)

    except Exception as e:
        error_msg = str(e)
        await message.answer(
            f"❌ Ошибка при отправке: {error_msg[:200]}\n\n"
            f"Проверьте:\n"
            f"1. Бот добавлен в чат?\n"
            f"2. У бота есть права на отправку сообщений?\n"
            f"3. Ветка существует?",
            parse_mode="HTML",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="🔄 Попробовать еще раз")],
                    [KeyboardButton(text="❌ Отмена")]
                ],
                resize_keyboard=True
            )
        )


async def ask_for_another_message(message: types.Message, state: FSMContext):
    """Спрашивает, отправить ли еще одно сообщение"""
    data = await state.get_data()
    chat_title = data.get('selected_chat_custom_name') or data.get('selected_chat_title', 'Чат')

    await message.answer(
        f"📤 Хотите отправить еще одно сообщение в <b>{chat_title}</b>?",
        parse_mode="HTML",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="✅ Да, отправить еще")],
                [KeyboardButton(text="🔄 Выбрать другой чат")],
                [KeyboardButton(text="❌ Завершить")]
            ],
            resize_keyboard=True
        )
    )


@dp.message(F.text == "✅ Да, отправить еще")
async def send_another_message(message: types.Message, state: FSMContext):
    """Начинает отправку еще одного сообщения в тот же чат"""
    await ask_for_message_content(message, state)


@dp.message(F.text == "🔄 Выбрать другой чат")
async def choose_another_chat(message: types.Message, state: FSMContext):
    """Возвращает к выбору чата"""
    await start_cross_chat_message(message, state)


@dp.message(F.text == "❌ Завершить")
async def finish_sending(message: types.Message, state: FSMContext):
    """Завершает процесс отправки"""
    await message.answer("✅ Процесс отправки завершен.", reply_markup=ReplyKeyboardRemove())
    await state.clear()


@dp.message(CrossChatMessage.confirm_message, F.text == "🔄 Изменить текст")
async def edit_message_text(message: types.Message, state: FSMContext):
    """Возвращает к редактированию текста"""
    await ask_for_message_content(message, state)


@dp.message(CrossChatMessage.confirm_message, F.text == "❌ Отмена")
async def cancel_sending(message: types.Message, state: FSMContext):
    """Отменяет отправку"""
    await message.answer("❌ Отправка отменена.", reply_markup=ReplyKeyboardRemove())
    await state.clear()


async def manage_saved_chats(message: types.Message, state: FSMContext):
    """Показывает меню управления сохраненными чатами"""
    user_id = message.from_user.id
    saved_chats = await get_user_saved_chats(user_id)

    if not saved_chats:
        await message.answer(
            "📋 У вас нет сохраненных чатов.",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="⬅️ Назад")],
                    [KeyboardButton(text="❌ Отмена")]
                ],
                resize_keyboard=True
            )
        )
        return

    response = "📋 <b>Управление сохраненными чатами</b>\n\n"

    for idx, (chat_db_id, chat_id, chat_title, custom_name, is_group) in enumerate(saved_chats, 1):
        display_name = custom_name or chat_title
        emoji = "👥" if is_group else "👤"
        response += f"{idx}. {emoji} <b>{display_name}</b>\n"
        if custom_name:
            response += f"   Оригинал: {chat_title}\n"
        response += f"   ID: <code>{chat_id}</code>\n\n"

    response += "Выберите действие:"

    keyboard_buttons = []

    # Кнопки для каждого чата (первые 5)
    for idx, (chat_db_id, chat_id, chat_title, custom_name, is_group) in enumerate(saved_chats[:5], 1):
        display_name = custom_name or chat_title
        short_name = display_name[:15] + "..." if len(display_name) > 15 else display_name
        keyboard_buttons.append([
            KeyboardButton(text=f"✏️ {idx}. {short_name}")
        ])

    keyboard_buttons.append([
        KeyboardButton(text="➕ Добавить новый чат"),
        KeyboardButton(text="🗑️ Удалить чат")
    ])
    keyboard_buttons.append([
        KeyboardButton(text="⬅️ Назад"),
        KeyboardButton(text="❌ Отмена")
    ])

    await message.answer(
        response,
        parse_mode="HTML",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=keyboard_buttons,
            resize_keyboard=True
        )
    )

    await state.set_state(CrossChatSettings.managing_saved_chats)


@dp.message(CrossChatSettings.managing_saved_chats)
async def process_chat_management(message: types.Message, state: FSMContext):
    """Обработчик управления чатами"""
    if message.text == "⬅️ Назад":
        await start_cross_chat_message(message, state)
        return

    elif message.text == "❌ Отмена":
        await message.answer("❌ Операция отменена.", reply_markup=ReplyKeyboardRemove())
        await state.clear()
        return

    elif message.text == "➕ Добавить новый чат":
        await message.answer(
            "📝 <b>Добавление нового чата</b>\n\n"
            "Отправьте ID чата (можно получить через @username_to_id_bot):\n\n"
            "<i>Или перешлите любое сообщение из нужного чата</i>",
            parse_mode="HTML",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="❌ Отмена")]],
                resize_keyboard=True
            )
        )
        await state.set_state(CrossChatMessage.waiting_for_chat_input)
        return

    elif message.text == "🗑️ Удалить чат":
        await start_chat_deletion(message, state)
        return

    # Проверяем, выбрал ли пользователь чат для редактирования
    elif message.text.startswith("✏️"):
        await start_chat_editing(message, state)
        return


async def start_chat_deletion(message: types.Message, state: FSMContext):
    """Начинает процесс удаления чата"""
    user_id = message.from_user.id
    saved_chats = await get_user_saved_chats(user_id)

    if not saved_chats:
        await message.answer("❌ У вас нет сохраненных чатов.")
        return

    response = "🗑️ <b>Удаление чата</b>\n\n"
    response += "Выберите чат для удаления:\n\n"

    keyboard_buttons = []

    for idx, (chat_db_id, chat_id, chat_title, custom_name, is_group) in enumerate(saved_chats, 1):
        display_name = custom_name or chat_title
        short_name = display_name[:20] + "..." if len(display_name) > 20 else display_name
        response += f"{idx}. {short_name}\n"

        keyboard_buttons.append([
            KeyboardButton(text=f"🗑️ {idx}. {short_name}")
        ])

    keyboard_buttons.append([
        KeyboardButton(text="⬅️ Назад"),
        KeyboardButton(text="❌ Отмена")
    ])

    await message.answer(
        response,
        reply_markup=ReplyKeyboardMarkup(
            keyboard=keyboard_buttons,
            resize_keyboard=True
        )
    )

    await state.set_state(CrossChatSettings.deleting_chat)


@dp.message(CrossChatSettings.deleting_chat)
async def process_chat_deletion(message: types.Message, state: FSMContext):
    """Обработчик удаления чата"""
    if message.text == "⬅️ Назад":
        await manage_saved_chats(message, state)
        return

    elif message.text == "❌ Отмена":
        await message.answer("❌ Операция отменена.", reply_markup=ReplyKeyboardRemove())
        await state.clear()
        return

    # Проверяем, выбрал ли пользователь чат для удаления
    if message.text.startswith("🗑️"):
        try:
            # Извлекаем номер из текста
            chat_number = int(message.text.split(".")[0].replace("🗑️", "").strip())
            user_id = message.from_user.id
            saved_chats = await get_user_saved_chats(user_id)

            if 1 <= chat_number <= len(saved_chats):
                chat_db_id, chat_id, chat_title, custom_name, is_group = saved_chats[chat_number - 1]
                display_name = custom_name or chat_title

                # Удаляем чат
                await delete_saved_chat(user_id, chat_db_id)

                await message.answer(
                    f"✅ Чат <b>{display_name}</b> успешно удален!",
                    parse_mode="HTML",
                    reply_markup=ReplyKeyboardRemove()
                )

                # Возвращаем к управлению чатами
                await manage_saved_chats(message, state)
            else:
                await message.answer("❌ Неверный номер чата.")

        except (ValueError, IndexError):
            await message.answer("❌ Ошибка при обработке команды.")





"""
ОБНОВЛЕНИЕ ГосID
"""


class UpdateGovIDStates(StatesGroup):
    confirming = State()
    in_progress = State()


# Альтернативный метод генерации с суффиксом
async def generate_account_number_with_suffix(city_name: str, db: aiosqlite.Connection, user_id: int) -> str:
    """Генерирует ГосID с суффиксом на основе user_id"""
    # Базовый код города
    city_code = city_name[:3].upper().replace(' ', 'X')
    if len(city_code) < 3:
        city_code = city_code.ljust(3, 'X')

    # Используем последние 4 цифры user_id как суффикс
    user_suffix = str(user_id)[-4:] if user_id > 9999 else str(user_id).zfill(4)

    # Формируем номер
    base_number = f"{city_code}{user_suffix}"
    checksum = calculate_checksum(base_number)

    return f"{base_number}{checksum}"


# Команда для админа
@dp.message(Command("update_all_govid"))
async def update_all_govid_start(message: types.Message):
    """Обновляет все ГосID пользователей по новому формату"""
    if message.from_user.id not in ADMIN_ID:
        await message.answer("❌ У вас нет прав администратора.")
        return

    await message.answer(
        "🔄 Начинаю процесс обновления всех ГосID...\n"
        "Это может занять несколько минут.",
        reply_markup=ReplyKeyboardRemove()
    )

    try:
        async with aiosqlite.connect("database.db") as db:
            # Получаем всех пользователей
            cursor = await db.execute("""
                SELECT user_id, city, account_id, name 
                FROM users 
                ORDER BY user_id
            """)
            all_users = await cursor.fetchall()

            total_users = len(all_users)
            processed = 0
            updated = 0
            failed = 0

            progress_message = await message.answer(f"📊 Найдено {total_users} пользователей\n🔄 Начинаю обновление...")

            # Для каждого города будем вести свой счетчик
            city_sequence = {}

            for user_id, city, old_govid, name in all_users:
                try:
                    processed += 1

                    # Получаем текущий номер пользователя в городе (из старого ГосID)
                    old_city_code = old_govid[:3] if old_govid else ""

                    # Инициализируем счетчик для города
                    if city not in city_sequence:
                        # Пытаемся определить текущий максимальный номер в городе
                        cursor = await db.execute("""
                            SELECT MAX(CAST(SUBSTR(account_id, 4, 6) AS INTEGER))
                            FROM users 
                            WHERE city = ? AND LENGTH(account_id) >= 10
                        """, (city,))

                        result = await cursor.fetchone()
                        max_number = result[0] if result[0] is not None else 0
                        city_sequence[city] = max_number

                    # Генерируем новый ГосID для пользователя
                    city_sequence[city] += 1

                    # Генерируем код города по правилам
                    if ' ' in city:
                        # Если название города состоит из нескольких слов
                        words = [word for word in city.split() if word]
                        city_code = ''.join([word[0].upper() for word in words])
                        if len(city_code) < 3:
                            city_code = city_code.ljust(3, 'X')
                        elif len(city_code) > 3:
                            city_code = city_code[:3]
                    else:
                        # Если название города состоит из одного слова
                        city_code = city[:3].upper()

                    if len(city_code) < 3:
                        city_code = city_code.ljust(3, 'X')

                    # Форматируем номер жителя
                    resident_number = f"{city_sequence[city]:06d}"

                    # Формируем базовый номер
                    base_number = f"{city_code}{resident_number}"

                    # Добавляем контрольную сумму
                    checksum = calculate_checksum(base_number)
                    new_govid = f"{base_number}{checksum}"

                    # Проверяем уникальность
                    cursor = await db.execute(
                        "SELECT 1 FROM users WHERE account_id = ? AND user_id != ?",
                        (new_govid, user_id)
                    )

                    if await cursor.fetchone():
                        # ГосID не уникален, пытаемся найти уникальный вариант
                        for attempt in range(1, 1000):
                            attempt_number = city_sequence[city] + attempt
                            resident_number = f"{attempt_number:06d}"
                            base_number = f"{city_code}{resident_number}"
                            checksum = calculate_checksum(base_number)
                            new_govid = f"{base_number}{checksum}"

                            cursor = await db.execute(
                                "SELECT 1 FROM users WHERE account_id = ?",
                                (new_govid,)
                            )

                            if not await cursor.fetchone():
                                break

                    # Обновляем ГосID пользователя
                    await db.execute(
                        "UPDATE users SET account_id = ? WHERE user_id = ?",
                        (new_govid, user_id)
                    )

                    updated += 1

                    # Обновляем прогресс каждые 50 пользователей
                    if processed % 50 == 0:
                        await progress_message.edit_text(
                            f"🔄 Обновление ГосID...\n"
                            f"📈 Прогресс: {processed}/{total_users}\n"
                            f"✅ Обновлено: {updated}\n"
                            f"❌ Ошибок: {failed}"
                        )

                except Exception as e:
                    failed += 1
                    print(f"Ошибка обновления ГосID для пользователя {user_id} ({name}): {e}")
                    continue

            await db.commit()

            # Формируем отчет
            report = (
                f"✅ Обновление ГосID завершено!\n\n"
                f"📊 Статистика:\n"
                f"👥 Всего пользователей: {total_users}\n"
                f"✅ Успешно обновлено: {updated}\n"
                f"❌ Ошибок: {failed}\n\n"
            )

            # Показываем примеры изменений
            cursor = await db.execute("""
                SELECT u.name, u.account_id, u.city 
                FROM users u 
                ORDER BY RANDOM() 
                LIMIT 5
            """)
            examples = await cursor.fetchall()

            if examples:
                report += f"📋 Примеры новых ГосID:\n"
                for name, new_govid, city in examples:
                    report += f"• {name} ({city}):\n  <code>{new_govid}</code>\n"

            report += f"\n🔄 Используйте /notify_govid_update для уведомления пользователей."

            await message.answer(report, parse_mode="HTML")

    except Exception as e:
        await message.answer(f"❌ Критическая ошибка при обновлении ГосID: {str(e)[:200]}")
        print(f"Ошибка обновления ГосID: {e}")
        import traceback
        traceback.print_exc()


@dp.message(UpdateGovIDStates.confirming, F.text == "Подтвердить")
async def confirm_govid_update(message: types.Message, state: FSMContext):
    """Подтверждение запуска обновления"""
    await message.answer(
        "🔄 Начинаю процесс обновления ГосID...\n"
        "Это может занять несколько минут.",
        reply_markup=ReplyKeyboardRemove()
    )

    # Создаем индикатор прогресса
    progress_message = await message.answer("⏳ Подготовка к обновлению...")

    # Запускаем процесс обновления
    await update_govid_process(message, progress_message, state)


async def update_govid_process(message: types.Message, progress_message: types.Message, state: FSMContext):
    """Основной процесс обновления ГосID - ИСПРАВЛЕННАЯ ВЕРСИЯ"""
    try:
        async with aiosqlite.connect("database.db") as db:
            # Получаем всех пользователей по городам
            cursor = await db.execute("""
                SELECT user_id, city, account_id, name 
                FROM users 
                ORDER BY city, user_id
            """)
            all_users = await cursor.fetchall()

            total_users = len(all_users)
            processed = 0
            updated = 0
            failed = 0
            city_stats = {}

            await progress_message.edit_text(
                f"📊 Найдено {total_users} пользователей\n"
                f"🔄 Начинаю обновление..."
            )

            await state.set_state(UpdateGovIDStates.in_progress)

            # Создаем временную таблицу для новых ГосID
            await db.execute("""
                CREATE TEMP TABLE IF NOT EXISTS temp_new_govid (
                    user_id INTEGER PRIMARY KEY,
                    old_govid TEXT,
                    new_govid TEXT,
                    city TEXT,
                    status TEXT,
                    attempts INTEGER DEFAULT 0
                )
            """)

            # Очищаем временную таблицу
            await db.execute("DELETE FROM temp_new_govid")
            await db.commit()

            # Шаг 1: Генерация новых ГосID
            city_sequence = {}

            for user_id, city, old_govid, name in all_users:
                try:
                    processed += 1

                    # Инициализируем счетчик для города
                    if city not in city_sequence:
                        city_sequence[city] = 0

                    # Генерируем уникальный ГосID с несколькими попытками
                    attempts = 0
                    max_attempts = 100
                    new_govid = None

                    while attempts < max_attempts:
                        try:
                            city_sequence[city] += 1
                            new_govid = await generate_specific_account_number(city, city_sequence[city], db)

                            # Проверяем уникальность в базе
                            cursor = await db.execute(
                                "SELECT 1 FROM users WHERE account_id = ? AND user_id != ?",
                                (new_govid, user_id)
                            )
                            if not await cursor.fetchone():
                                # Проверяем уникальность во временной таблице
                                cursor = await db.execute(
                                    "SELECT 1 FROM temp_new_govid WHERE new_govid = ?",
                                    (new_govid,)
                                )
                                if not await cursor.fetchone():
                                    break  # Уникальный номер найден

                            attempts += 1
                        except Exception as e:
                            print(f"Попытка {attempts} для пользователя {user_id}: {e}")
                            attempts += 1

                    if new_govid and attempts < max_attempts:
                        # Сохраняем во временную таблицу
                        await db.execute("""
                            INSERT INTO temp_new_govid (user_id, old_govid, new_govid, city, status, attempts)
                            VALUES (?, ?, ?, ?, 'pending', ?)
                        """, (user_id, old_govid, new_govid, city, attempts))

                        # Обновляем статистику по городам
                        if city not in city_stats:
                            city_stats[city] = 0
                        city_stats[city] += 1

                        # Обновляем прогресс каждые 10 пользователей
                        if processed % 10 == 0:
                            await progress_message.edit_text(
                                f"🔄 Генерация ГосID...\n"
                                f"📈 Прогресс: {processed}/{total_users}\n"
                                f"✅ Сгенерировано: {processed - failed}\n"
                                f"❌ Ошибок: {failed}"
                            )

                    else:
                        failed += 1
                        await db.execute("""
                            INSERT INTO temp_new_govid (user_id, old_govid, new_govid, city, status, attempts)
                            VALUES (?, ?, ?, ?, 'failed', ?)
                        """, (user_id, old_govid, f"ERROR_{user_id}", city, attempts))
                        print(f"Не удалось сгенерировать ГосID для пользователя {user_id}")

                    await asyncio.sleep(0.01)  # Небольшая задержка

                except Exception as e:
                    failed += 1
                    print(f"Ошибка генерации ГосID для пользователя {user_id}: {e}")

                    # Сохраняем ошибку
                    await db.execute("""
                        INSERT INTO temp_new_govid (user_id, old_govid, new_govid, city, status)
                        VALUES (?, ?, ?, ?, 'error')
                    """, (user_id, old_govid, f"ERROR_{user_id}", city))

            await db.commit()

            # Проверяем уникальность всех сгенерированных ГосID
            cursor = await db.execute("""
                SELECT new_govid, COUNT(*) as count 
                FROM temp_new_govid 
                WHERE status = 'pending'
                GROUP BY new_govid 
                HAVING count > 1
            """)
            duplicates = await cursor.fetchall()

            if duplicates:
                await progress_message.edit_text(
                    f"⚠️ Найдено {len(duplicates)} дубликатов ГосID!\n"
                    f"Исправляю..."
                )

                # Исправляем дубликаты
                for govid, count in duplicates:
                    cursor = await db.execute("""
                        SELECT user_id, city 
                        FROM temp_new_govid 
                        WHERE new_govid = ? AND status = 'pending'
                    """, (govid,))

                    duplicate_users = await cursor.fetchall()

                    for idx, (dup_user_id, dup_city) in enumerate(duplicate_users[1:], 1):
                        # Генерируем новый уникальный ГосID
                        attempts = 0
                        max_attempts = 50
                        new_unique_govid = None

                        while attempts < max_attempts:
                            try:
                                # Используем суффикс для уникальности
                                suffix = f"{idx:02d}"
                                city_code = dup_city[:3].upper().replace(' ', 'X')
                                if len(city_code) < 3:
                                    city_code = city_code.ljust(3, 'X')

                                # Изменяем номер жителя
                                base_govid = govid[:-1]  # Без контрольной суммы
                                new_base = f"{city_code}{str(int(base_govid[3:9]) + idx + 100):06d}"
                                new_checksum = calculate_checksum(new_base)
                                new_unique_govid = f"{new_base}{new_checksum}"

                                # Проверяем уникальность
                                cursor_test = await db.execute("""
                                    SELECT 1 FROM temp_new_govid WHERE new_govid = ?
                                """, (new_unique_govid,))

                                if not await cursor_test.fetchone():
                                    break

                                attempts += 1
                            except Exception as e:
                                attempts += 1

                        if new_unique_govid:
                            await db.execute("""
                                UPDATE temp_new_govid 
                                SET new_govid = ?, attempts = attempts + 1
                                WHERE user_id = ? AND new_govid = ?
                            """, (new_unique_govid, dup_user_id, govid))

            await progress_message.edit_text(
                f"✅ Генерация завершена!\n"
                f"📊 Статистика:\n"
                f"• Всего: {processed}\n"
                f"• Успешно: {processed - failed}\n"
                f"• Ошибок: {failed}\n\n"
                f"🔄 Начинаю обновление в базе данных..."
            )

            # Шаг 2: Обновление в основной таблице
            # Сначала отключаем ограничения для безопасного обновления
            await db.execute("PRAGMA foreign_keys = OFF")

            # Создаем резервную таблицу на случай отката
            await db.execute("""
                CREATE TABLE IF NOT EXISTS backup_users_govid AS 
                SELECT user_id, account_id, city FROM users
            """)

            cursor = await db.execute("""
                SELECT user_id, old_govid, new_govid, city 
                FROM temp_new_govid 
                WHERE status = 'pending'
                ORDER BY user_id
            """)
            pending_updates = await cursor.fetchall()

            update_count = 0
            batch_size = 10

            for user_id, old_govid, new_govid, city in pending_updates:
                try:
                    # Проверяем, не существует ли уже такой ГосID
                    cursor = await db.execute("""
                        SELECT 1 FROM users WHERE account_id = ? AND user_id != ?
                    """, (new_govid, user_id))

                    if await cursor.fetchone():
                        # ГосID уже существует, генерируем новый
                        for attempt in range(1, 11):
                            suffix_govid = f"{new_govid[:-1]}{attempt}{new_govid[-1]}"

                            cursor = await db.execute("""
                                SELECT 1 FROM users WHERE account_id = ? AND user_id != ?
                            """, (suffix_govid, user_id))

                            if not await cursor.fetchone():
                                new_govid = suffix_govid
                                break

                    # Обновляем ГосID пользователя
                    await db.execute("""
                        UPDATE users 
                        SET account_id = ? 
                        WHERE user_id = ?
                    """, (new_govid, user_id))

                    # Обновляем spouse_id в браках
                    await db.execute("""
                        UPDATE users 
                        SET spouse_id = ? 
                        WHERE spouse_id = ?
                    """, (new_govid, old_govid))

                    # Обновляем статус
                    await db.execute("""
                        UPDATE temp_new_govid 
                        SET status = 'completed' 
                        WHERE user_id = ?
                    """, (user_id,))

                    update_count += 1
                    updated += 1

                    # Коммитим каждые batch_size обновлений
                    if update_count % batch_size == 0:
                        await db.commit()
                        await progress_message.edit_text(
                            f"🔄 Обновление базы...\n"
                            f"📈 Прогресс: {update_count}/{len(pending_updates)}\n"
                            f"✅ Обновлено: {update_count}"
                        )

                    await asyncio.sleep(0.02)  # Увеличенная задержка

                except Exception as e:
                    print(f"Ошибка обновления пользователя {user_id}: {e}")
                    await db.execute("""
                        UPDATE temp_new_govid 
                        SET status = 'update_error' 
                        WHERE user_id = ?
                    """, (user_id,))
                    failed += 1

            # Финальный коммит
            await db.commit()

            # Включаем обратно ограничения
            await db.execute("PRAGMA foreign_keys = ON")

            # Шаг 3: Проверяем целостность данных
            await progress_message.edit_text(
                f"✅ Обновление завершено!\n"
                f"🔍 Проверяю целостность данных..."
            )

            # Проверяем дубликаты
            cursor = await db.execute("""
                SELECT account_id, COUNT(*) as count 
                FROM users 
                GROUP BY account_id 
                HAVING count > 1
            """)
            final_duplicates = await cursor.fetchall()

            # Формируем отчет
            report_text = "📊 <b>ОТЧЕТ ОБ ОБНОВЛЕНИИ ГосID</b>\n\n"

            report_text += f"<b>Общая статистика:</b>\n"
            report_text += f"👥 Всего пользователей: {total_users}\n"
            report_text += f"✅ Успешно обновлено: {updated}\n"
            report_text += f"❌ Ошибок: {failed}\n\n"

            if final_duplicates:
                report_text += f"⚠️ <b>ВНИМАНИЕ: Найдено дубликатов ГосID:</b> {len(final_duplicates)}\n"
                for govid, count in final_duplicates[:5]:
                    report_text += f"• ГосID {govid}: {count} раз\n"
                report_text += "\n"

            # Топ городов
            if city_stats:
                report_text += f"<b>🏙️ Топ городов по обновлениям:</b>\n"
                sorted_cities = sorted(city_stats.items(), key=lambda x: x[1], reverse=True)[:5]
                for city, count in sorted_cities:
                    report_text += f"• {city}: {count} пользователей\n"
                report_text += "\n"

            # Примеры изменений
            cursor = await db.execute("""
                SELECT u.name, t.old_govid, t.new_govid, t.city 
                FROM temp_new_govid t
                JOIN users u ON t.user_id = u.user_id
                WHERE t.status = 'completed'
                ORDER BY RANDOM()
                LIMIT 3
            """)
            examples = await cursor.fetchall()

            if examples:
                report_text += f"<b>📋 Примеры изменений:</b>\n"
                for name, old_id, new_id, city in examples:
                    report_text += f"• {name} ({city}):\n"
                    report_text += f"  Было: <code>{old_id}</code>\n"
                    report_text += f"  Стало: <code>{new_id}</code>\n"
                report_text += "\n"

            # Инструкции
            report_text += (
                f"<b>📝 Рекомендации:</b>\n"
                f"1. Проверьте несколько пользователей вручную\n"
                f"2. Запустите тестовый перевод между пользователями\n"
                f"3. Используйте /notify_govid_update для уведомления\n"
                f"4. Если есть проблемы: /restore_govid_backup\n\n"
                f"<b>🔧 Команды для проверки:</b>\n"
                f"• <code>/id</code> - проверка ГосID пользователей\n"
                f"• <code>/users</code> - список всех пользователей\n"
            )

            await message.answer(report_text, parse_mode="HTML")

            # Очищаем временную таблицу
            await db.execute("DROP TABLE IF EXISTS temp_new_govid")

    except Exception as e:
        await message.answer(f"❌ Критическая ошибка при обновлении:\n{str(e)[:200]}")
        print(f"Ошибка в процессе обновления: {e}")
        import traceback
        traceback.print_exc()

    finally:
        await state.clear()
        try:
            await progress_message.delete()
        except:
            pass


@dp.message(UpdateGovIDStates.confirming, F.text == "Отмена")
async def cancel_govid_update(message: types.Message, state: FSMContext):
    """Отмена обновления"""
    await message.answer("❌ Обновление ГосID отменено.")
    await state.clear()


# Добавим команду для уведомления пользователей
@dp.message(Command("notify_govid_update"))
@dp.message(Command("notify_govid_update"))
async def notify_govid_update_command(message: types.Message):
    """Уведомляет всех пользователей об обновлении ГосID"""
    if message.from_user.id not in ADMIN_ID:
        await message.answer("❌ У вас нет прав администратора.")
        return

    await message.answer(
        "📢 Начинаю отправку уведомлений пользователям об их новых ГосID...",
        reply_markup=ReplyKeyboardRemove()
    )

    try:
        async with aiosqlite.connect("database.db") as db:
            cursor = await db.execute("SELECT COUNT(*) FROM users")
            total_users = (await cursor.fetchone())[0]

            cursor = await db.execute("SELECT user_id, account_id, name, city FROM users")
            users = await cursor.fetchall()

        total = len(users)
        successful = 0
        failed = 0
        failed_list = []

        progress_message = await message.answer(f"📤 Отправка уведомлений... 0/{total}")

        for idx, (user_id, govid, name, city) in enumerate(users, 1):
            try:
                # Формируем персональное сообщение
                notification = (
                    f"🎫 <b>Обновление ГосID</b>\n\n"
                    f"Здравствуйте, {name}!\n\n"
                    f"Ваш ГосID был автоматически обновлен.\n"
                    f"📊 Город: {city}\n"
                    f"🎫 Ваш новый ГосID: <code>{govid}</code>\n\n"
                    f"Теперь формат ГосID:\n"
                    f"• Для городов из одного слова: первые 3 буквы города\n"
                    f"• Для городов из нескольких слов: первые буквы каждого слова\n"
                    f"• Затем 6 цифр порядкового номера\n"
                    f"• И контрольная цифра\n\n"
                    f"Используйте команду /mp чтобы посмотреть свой обновленный паспорт."
                )

                await bot.send_message(user_id, notification, parse_mode="HTML")
                successful += 1

                # Обновляем прогресс каждые 20 уведомлений
                if idx % 20 == 0:
                    await progress_message.edit_text(
                        f"📤 Отправка уведомлений... {idx}/{total}\n"
                        f"✅ Успешно: {successful}\n"
                        f"❌ Ошибок: {failed}"
                    )

                # Задержка чтобы не спамить
                await asyncio.sleep(0.1)

            except Exception as e:
                failed += 1
                failed_list.append(f"{name} (ID: {user_id})")
                print(f"Не удалось отправить уведомление пользователю {user_id}: {e}")

        # Формируем отчет
        report = (
            f"✅ Уведомления отправлены!\n\n"
            f"📊 Статистика:\n"
            f"• Всего пользователей: {total}\n"
            f"• Успешно: {successful}\n"
            f"• Не удалось: {failed}\n\n"
        )

        if failed_list:
            report += f"<b>Не получили уведомление:</b>\n"
            for failed_user in failed_list[:10]:
                report += f"• {failed_user}\n"
            if len(failed_list) > 10:
                report += f"• ... и ещё {len(failed_list) - 10} пользователей\n"

        await message.answer(report, parse_mode="HTML")

    except Exception as e:
        await message.answer(f"❌ Ошибка при отправке уведомлений: {str(e)[:200]}")
        print(f"Ошибка notify_govid_update: {e}")


@dp.message(F.text == "✅ Да, уведомить всех")
async def send_govid_notifications(message: types.Message):
    """Отправка уведомлений всем пользователям"""
    await message.answer("🔄 Начинаю отправку уведомлений...", reply_markup=ReplyKeyboardRemove())

    async with aiosqlite.connect("database.db") as db:
        cursor = await db.execute("SELECT user_id, account_id, name, city FROM users")
        users = await cursor.fetchall()

    total = len(users)
    successful = 0
    failed = 0
    failed_list = []

    progress_message = await message.answer(f"📤 Отправка уведомлений... 0/{total}")

    for idx, (user_id, govid, name, city) in enumerate(users, 1):
        try:
            # Формируем персональное сообщение
            notification = (
                f"📢 <b>ВНИМАНИЕ</b>\n\n"
                f"Здравствуйте, {name}, в данный момент проходит обновление структуры ГосID\n\n Ваш актуальный ГосID теперь: <code>{govid}</code>\n\n Спасибо, что пользуйтесь НовоШПБ"

            )

            await bot.send_message(user_id, notification, parse_mode="HTML")
            successful += 1

            # Обновляем прогресс каждые 10 уведомлений
            if idx % 10 == 0:
                await progress_message.edit_text(
                    f"📤 Отправка уведомлений... {idx}/{total}\n"
                    f"✅ Успешно: {successful}\n"
                    f"❌ Ошибок: {failed}"
                )

            # Задержка чтобы не спамить
            await asyncio.sleep(0.1)

        except Exception as e:
            failed += 1
            failed_list.append(f"{name} (ID: {user_id})")
            print(f"Не удалось отправить уведомление пользователю {user_id}: {e}")

    # Формируем отчет
    report = (
        f"✅ Уведомления отправлены!\n\n"
        f"📊 Статистика:\n"
        f"• Всего пользователей: {total}\n"
        f"• Успешно: {successful}\n"
        f"• Не удалось: {failed}\n\n"
    )

    if failed_list:
        report += f"<b>Не получили уведомление:</b>\n"
        for failed_user in failed_list[:10]:  # Показываем первые 10
            report += f"• {failed_user}\n"
        if len(failed_list) > 10:
            report += f"• ... и ещё {len(failed_list) - 10} пользователей\n"

    await message.answer(report, parse_mode="HTML")


# лист городов
@dp.message(Command("cities"))
async def list_cities(message: types.Message):
    async with aiosqlite.connect("database.db") as db:
        # Сначала проверяем наличие колонки is_verified
        cursor = await db.execute("PRAGMA table_info(cities)")
        columns = await cursor.fetchall()
        column_names = [col[1] for col in columns]

        has_verified_column = 'is_verified' in column_names

        if has_verified_column:
            # Получаем список городов с информацией о мэре, количестве жителей и галочке
            cursor = await db.execute("""
                SELECT 
                    c.name, 
                    u.name as mayor_name,
                    (SELECT COUNT(*) FROM users WHERE city = c.name) as population,
                    c.is_verified
                FROM cities c
                LEFT JOIN users u ON c.mayor_id = u.user_id
                ORDER BY 
                    CASE WHEN c.is_verified = 1 THEN 0 ELSE 1 END,  -- Сначала города с галочками
                    c.name
            """)
        else:
            # Если колонки нет, используем старый запрос
            cursor = await db.execute("""
                SELECT 
                    c.name, 
                    u.name as mayor_name,
                    (SELECT COUNT(*) FROM users WHERE city = c.name) as population,
                    0 as is_verified  -- Все города без галочек
                FROM cities c
                LEFT JOIN users u ON c.mayor_id = u.user_id
                ORDER BY c.name
            """)

        cities = await cursor.fetchall()

    if not cities:
        await message.answer("В базе пока нет городов.")
        return

    text = "🏙️ <b>Список зарегистрированных городов:</b>\n\n"

    # Статистика по галочкам (только если колонка существует)
    total_cities = len(cities)
    if has_verified_column:
        verified_count = sum(1 for city in cities if city[3])  # is_verified
        unverified_count = total_cities - verified_count

        text += f"📊 <b>Статистика:</b> Всего городов: {total_cities}\n"
        if verified_count > 0:
            text += f"✅ С галочками: {verified_count}\n"
        text += f"❌ Без галочек: {unverified_count}\n\n"

    # Отдельно показываем города с галочками (зелёной галочкой)
    if has_verified_column and any(city[3] for city in cities):
        text += "<b>✅ оффициальные города:</b>\n"
        for city_name, mayor_name, population, is_verified in cities:
            if is_verified:
                # ЗЕЛЁНАЯ ГАЛОЧКА с эмодзи ✅
                text += f"✅ <b>{city_name}</b>\n"
                text += f"   👑 Мэр: {mayor_name or 'Нет мэра'}\n"
                text += f"   👥 Жителей: {population or 0}\n\n"

        text += "\n<b>🏙️ остальные::</b>\n"
    else:
        text += "<b>🏙️ остальные:</b>\n"

    # Показываем все города (теперь с зелёными галочками для верифицированных)
    for city_name, mayor_name, population, is_verified in cities:
        if not (has_verified_column and is_verified):  # Города без галочек показываем отдельно
            # Для НЕ верифицированных городов - без галочки
            text += f"🏙️ <b>{city_name}</b>\n"
            text += f"   👑 Мэр: {mayor_name or 'Нет мэра'}\n"
            text += f"   👥 Жителей: {population or 0}\n\n"
        else:
            pass

    await message.answer(text, parse_mode="HTML")


@dp.message(MayorMenu.managing_city, F.text.startswith("👥 Жители города"))
async def show_citizens(message: types.Message, state: FSMContext):
    data = await state.get_data()
    city_name = data.get('city_name')

    async with aiosqlite.connect("database.db") as db:
        # Получаем всех жителей города с полной информацией
        cursor = await db.execute("""
            SELECT name, account_id, balance, street, house_number, created_date 
            FROM users 
            WHERE city = ? 
            ORDER BY name
        """, (city_name,))
        citizens = await cursor.fetchall()

        if not citizens:
            await message.answer("В вашем городе пока нет жителей.")
            return

        response = f"👥 <b>Жители города {city_name}:</b>\n\n"

        for idx, (name, account_id, balance, street, house_number, created_date) in enumerate(citizens, 1):
            response += f"<b>{idx}. {name}</b>\n"
            response += f"   💳 ID: {account_id}\n"
            response += f"   💰 Баланс: {balance} шуек\n"

            # Добавляем адрес, если указан
            if street and house_number:
                response += f"   📍 Адрес: {street}, {house_number}\n"
            elif street:
                response += f"   📍 Улица: {street}\n"
            elif house_number:
                response += f"   📍 Дом: {house_number}\n"

            response += f"   📅 Зарегистрирован: {created_date[:10]}\n\n"

        # Добавляем статистику
        total_balance = sum(citizen[2] for citizen in citizens)
        response += f"<b>📊 Статистика:</b>\n"
        response += f"   👤 Всего жителей: {len(citizens)}\n"
        response += f"   💵 Общий баланс: {total_balance} шуек\n"
        response += f"   💰 Средний баланс: {total_balance // len(citizens) if citizens else 0} шуек"

        # Разбиваем на части, если сообщение слишком длинное
        if len(response) > 4000:
            parts = []
            current_part = ""
            lines = response.split('\n')

            for line in lines:
                if len(current_part + line + '\n') < 4000:
                    current_part += line + '\n'
                else:
                    parts.append(current_part)
                    current_part = line + '\n'

            if current_part:
                parts.append(current_part)

            for part in parts:
                await message.answer(part, parse_mode="HTML")
        else:
            await message.answer(response, parse_mode="HTML")

        await message.answer(
            f"Управление городом {city_name}:",
            reply_markup=await mayor_menu_kb(city_name)
        )


# информация о человеке
@dp.message(Command("id"))
async def show_user_id(message: types.Message):
    """Показывает ГосID и имя пользователя, на чьё сообщение ответили"""

    # Проверяем, что команда является ответом на сообщение
    if not message.reply_to_message:
        await message.answer(
            "❌ Эта команда должна быть ответом на сообщение пользователя.\n\n"
        )
        return

    # Получаем информацию о целевом пользователе
    target_user = message.reply_to_message.from_user

    if not target_user:
        await message.answer("❌ Не удалось определить пользователя.")
        return

    target_user_id = target_user.id
    target_username = target_user.username
    target_full_name = target_user.full_name

    try:
        async with aiosqlite.connect("database.db") as db:
            # Ищем пользователя в базе данных
            cursor = await db.execute("""
                SELECT account_id, name, city, balance, gender
                FROM users 
                WHERE user_id = ?
            """, (target_user_id,))

            user_data = await cursor.fetchone()

            if user_data:
                # Пользователь найден в базе
                gov_id, name, city, balance, gender = user_data

                # Получаем номер в городе
                cursor = await db.execute("""
                    SELECT COUNT(*) 
                    FROM users 
                    WHERE city = ? AND user_id <= ?
                """, (city, target_user_id))

                city_number_result = await cursor.fetchone()
                city_number = city_number_result[0] if city_number_result else "?"

                # Получаем семейный статус
                cursor = await db.execute("""
                    SELECT spouse_id, marriage_date 
                    FROM users 
                    WHERE user_id = ?
                """, (target_user_id,))

                marriage_data = await cursor.fetchone()
                spouse_id = marriage_data[0] if marriage_data else None
                marriage_date = marriage_data[1] if marriage_data else None

                # Если есть супруг, получаем его имя
                spouse_name = None
                if spouse_id:
                    cursor = await db.execute("""
                        SELECT name FROM users WHERE account_id = ?
                    """, (spouse_id,))
                    spouse_result = await cursor.fetchone()
                    spouse_name = spouse_result[0] if spouse_result else None

                # Формируем расширенную информацию
                response = f"👤 <b>Информация о пользователе</b>\n\n"

                # Основная информация
                response += f"<b>📝 Имя в системе:</b> <code>{name}</code>\n"
                response += f"<b>👤 Телеграм имя:</b> <code>{target_full_name}</code>\n"

                if target_username:
                    response += f"<b>📱 Юзернейм:</b> <code>@{target_username}</code>\n"
                response += f"<b>🎫 ГосID:</b> <code>{gov_id}</code>\n"
                response += f"<b>🏙️ Город:</b> <code>{city}</code>\n"
                response += f"<b>👫 Пол:</b> {'Мужской' if gender == 'м' else 'Женский'}\n"

                # Информация о браке
                if spouse_name:
                    response += f"<b>💍 В браке с:</b> <code>{spouse_name}</code>\n"
                else:
                    response += f"<b>💍 Семейный статус:</b> Не женат/Не замужем\n"

            else:
                # Пользователь не найден в базе
                response = f"👤 <b>Информация о пользователе</b>\n\n"
                response += f"<b>📝 Имя в Телеграм:</b> <code>{target_full_name}</code>\n"
                if target_username:
                    response += f"<b>📱 Юзернейм:</b> <code>@{target_username}</code>\n"
                    response += "❌ <b>Пользователь не зарегистрирован в системе!</b>\n\n"

        # Отправляем информацию
        if len(response) > 4000:
            # Если сообщение слишком длинное, разбиваем его
            parts = []
            while len(response) > 4000:
                part = response[:4000]
                last_newline = part.rfind('\n')
                if last_newline > 0:
                    parts.append(part[:last_newline])
                    response = response[last_newline + 1:]
                else:
                    parts.append(part)
                    response = response[4000:]
            if response:
                parts.append(response)

            for part in parts:
                await message.answer(part, parse_mode="HTML")
        else:
            await message.answer(response, parse_mode="HTML")

    except Exception as e:
        await message.answer(f"❌ Произошла ошибка при получении информации: {str(e)[:100]}")
        print(f"Ошибка в команде /id: {e}")


# конкретный город
@dp.message(Command("city_info"))
async def city_info(message: types.Message):
    """Показывает подробную информацию о городе с зелёной галочкой"""
    args = message.text.split()
    if len(args) < 2:
        await message.answer("Использование: /city_info <название города>")
        return

    city_name = " ".join(args[1:])

    async with aiosqlite.connect("database.db") as db:
        try:
            # Получаем основную информацию о городе
            cursor = await db.execute("""
                SELECT 
                    c.name, 
                    u.name as mayor_name, 
                    u.account_id as mayor_account,
                    u.user_id as mayor_user_id,
                    c.coord_x,
                    c.coord_z,
                    (SELECT COUNT(*) FROM users WHERE city = c.name) as population,
                    (SELECT COUNT(*) FROM streets WHERE city_name = c.name) as street_count,
                    (SELECT COUNT(*) FROM houses WHERE city_name = c.name) as house_count,
                    (SELECT SUM(balance) FROM users WHERE city = c.name) as total_wealth,
                    c.created_date,
                    c.is_verified
                FROM cities c
                LEFT JOIN users u ON c.mayor_id = u.user_id
                WHERE c.name = ? COLLATE NOCASE
            """, (city_name,))

            city_data = await cursor.fetchone()

            if not city_data:
                # Попробуем найти похожие города
                cursor = await db.execute("""
                    SELECT name FROM cities WHERE name LIKE ? COLLATE NOCASE LIMIT 5
                """, (f"%{city_name}%",))
                similar = await cursor.fetchall()

                response = f"❌ Город '{city_name}' не найден."
                if similar:
                    response += "\n\nВозможно вы имели в виду:\n"
                    for sim_city in similar:
                        response += f"• {sim_city[0]}\n"

                await message.answer(response)
                return

            (city_name, mayor_name, mayor_account, mayor_user_id, coord_x, coord_z,
             population, street_count, house_count, total_wealth, created_date, is_verified) = city_data

            # Формируем ответ с зелёной галочкой если есть
            if is_verified:
                # ГОРОД С ЗЕЛЁНОЙ ГАЛОЧКОЙ ✅
                response = f"✅ <b>Информация о городе {city_name}</b>\n\n"
                response += "📋 <b>Статус:</b> <code>✅ Официально проверен</code>\n\n"
            else:
                response = f"🏙️ <b>Информация о городе {city_name}</b>\n\n"
                response += "📋 <b>Статус:</b> <code>Не проверен</code>\n\n"

            # Основная информация
            response += "📋 <b>Основная информация:</b>\n"
            response += f"👑 <b>Мэр:</b> {mayor_name or 'Нет мэра'}\n"
            if mayor_account:
                response += f"   💳 ID мэра: {mayor_account}\n"

            if created_date:
                response += f"📅 <b>Дата основания:</b> {created_date[:10]}\n"

            if coord_x is not None and coord_z is not None:
                response += f"📍 <b>Координаты:</b> X:{coord_x} Z:{coord_z}\n"

            response += f"👥 <b>Население:</b> {population} жителей\n\n"

            # Инфраструктура
            response += "🏘️ <b>Инфраструктура:</b>\n"
            response += f"   🛣️ Улиц: {street_count}\n"
            response += f"   🏠 Домов: {house_count}\n\n"

            # Экономика
            if total_wealth and total_wealth > 0 and population > 0:
                avg_wealth = total_wealth // population
                response += f"💰 <b>Экономика:</b>\n"
                response += f"   💵 Общее богатство: {total_wealth} шуек\n"
                response += f"   📊 Средний баланс: {avg_wealth} шуек\n\n"

            # Добавляем информацию о ближайших городах если есть координаты
            if coord_x is not None and coord_z is not None:
                cursor = await db.execute("""
                    SELECT name, coord_x, coord_z, is_verified 
                    FROM cities 
                    WHERE name != ? AND coord_x IS NOT NULL AND coord_z IS NOT NULL
                    ORDER BY (ABS(coord_x - ?) + ABS(coord_z - ?))
                    LIMIT 3
                """, (city_name, coord_x, coord_z))

                nearby_cities = await cursor.fetchall()

                if nearby_cities:
                    response += "🗺️ <b>Ближайшие города:</b>\n"
                    for i, (name, other_x, other_y, is_nearby_verified) in enumerate(nearby_cities, 1):
                        distance = abs(other_x - coord_x) + abs(other_y - coord_z)
                        verified_mark = " ✅" if is_nearby_verified else ""
                        response += f"{i}. {name}{verified_mark} - {distance}ед. (X:{other_x}, Z:{other_y})\n"
                    response += "\n"

            # Информация для администраторов
            if message.from_user.id in ADMIN_ID:
                response += "🛠️ <b>Админ-панель:</b>\n"
                if is_verified:
                    response += f"<code>/unverify_city \"{city_name}\"</code> - убрать зелёную галочку\n"
                else:
                    response += f"<code>/verify_city \"{city_name}\"</code> - добавить зелёную галочку ✅\n"

            await message.answer(response, parse_mode="HTML")

        except Exception as e:
            await message.answer(f"❌ Произошла ошибка при получении информации о городе: {str(e)[:100]}")
            print(f"Ошибка в city_info: {e}")


# помощь
@dp.message(Command("help_shpb"))
async def cmd_help(message: types.Message):
    help_text = """
🆘 <b>Список доступных команд:</b> 🆘

<code>/start</code> - Регистрация и показ главного меню (работает только в личных сообщениях)
<code>/cities</code> - Показывает список городов
<code>/verified_cities</code> - показывает список только оффицальных городов
<code>/attractions</code> (название города) - показывает достопримечательности города проживания (без указания города будет писать о том, в котором вы прописаны)
<code>/city_info</code> (название города) - показывает информацию о городе
<code>/help_shpb</code> - Показывает это сообщение
<code>/mp</code> - показать свой паспорт 
<code>/deportation</code> - депортирование пользователя, на чьё сообщение вы ответили 
<code>/set_inoagent</code> - внести в список иноагентов пользователя, на чьё сообщение вы ответили на 3 дня
<code>/check_inoagent</code> - проверить пользователя на иноагенство
<code>/inoagents_list</code> - список иноагентов


📌 Если что-то не работает, пишите заявление в боте (кнопка: "📝 Написать заявление")
    """
    await message.answer(help_text, parse_mode="HTML")


"""
ДЕПОРТРАТОР
"""


@dp.message(Command("deportation"))
async def deport_user(message: types.Message):
    # Проверка на иноагента
    user_id = message.from_user.id

    async with aiosqlite.connect("database.db") as db:
        cursor = await db.execute(
            "SELECT 1 FROM foreign_agents WHERE user_id = ? AND expires_at > datetime('now')",
            (user_id,)
        )
        is_foreign_agent = await cursor.fetchone()

        if is_foreign_agent:
            await message.answer("❌ Иноагентам запрещено использовать команду /deportation!")
            return

    # Остальной код команды
    if not message.reply_to_message:
        await message.answer("Эта команда должна быть ответом на сообщение пользователя.")
        return

    target_user_id = message.reply_to_message.from_user.id
    deportation_places = [
        "Психо-Неврологический Диспанцер имени Михаила Бабушкина",
        "Шуйско-Болванский мост",
        "Болвания",
        "Улица",
        "Шахта"
    ]
    new_place = random.choice(deportation_places)

    await message.answer(
        f"Пользователь {message.reply_to_message.from_user.full_name} депортирован. \n"
        f"Новое место жительства {message.reply_to_message.from_user.full_name} теперь: {new_place}."
    )


"""
ИНОАГЕНСТВО
"""


@dp.message(Command("set_inoagent"))
async def make_foreign_agent(message: types.Message):
    logging.info(f"Command set_inoagent received from user {message.from_user.id}")
    # Проверка отправителя на статус иноагента
    sender_id = message.from_user.id

    async with aiosqlite.connect("database.db") as db:
        cursor = await db.execute(
            "SELECT 1 FROM foreign_agents WHERE user_id = ? AND expires_at > datetime('now')",
            (sender_id,)
        )
        is_sender_agent = await cursor.fetchone()

        if is_sender_agent:
            await message.answer("❌ Иноагентам запрещено использовать команду /set_inoagent!")
            return

    try:
        # Проверка что команда является ответом на сообщение
        if not message.reply_to_message:
            await message.answer("Эта команда должна быть ответом на сообщение пользователя.")
            return

        # Проверка на наличие пользователя в пересланном сообщении
        if not message.reply_to_message.from_user:
            await message.answer("Не удалось определить пользователя.")
            return

        target_user_id = message.reply_to_message.from_user.id
        target_user_name = message.reply_to_message.from_user.full_name

        # Проверяем, что бот не пытается назначить сам себя иноагентом
        if target_user_id == message.bot.id:
            await message.answer("иди нахуй, пж")
            return

        # Проверяем, что пользователь не назначает сам себя
        if target_user_id == message.from_user.id:
            await message.answer("Спасибо что внесли сами себя в список!")

        agent_number = random.randint(1, 999)
        agent_name = f"Агент №{agent_number:03d}"

        # Устанавливаем срок на 3 дня
        expiration_date = datetime.now() + timedelta(days=3)

        async with aiosqlite.connect("database.db") as db:
            # Создаем таблицу если она не существует
            await db.execute('''
                CREATE TABLE IF NOT EXISTS foreign_agents (
                    user_id INTEGER PRIMARY KEY,
                    agent_name TEXT NOT NULL,
                    expires_at TEXT NOT NULL
                )
            ''')

            # Добавляем пользователя в список иноагентов
            await db.execute(
                "INSERT OR REPLACE INTO foreign_agents (user_id, agent_name, expires_at) VALUES (?, ?, ?)",
                (target_user_id, agent_name, expiration_date.isoformat())
            )
            await db.commit()

        await message.answer(
            f"🕵️ Пользователь {target_user_name} признан иноагентом!\n"
            f"Кодовое имя: {agent_name}\n"
            f"Срок действия статуса: 3 дня (до {expiration_date.strftime('%d.%m.%Y %H:%M')})"
        )

    except Exception as e:
        logging.error(f"Database error in set_inoagent: {e}")
        await message.answer("Произошла ошибка при добавлении в базу данных.")


@dp.message(Command("inoagents_list"))
async def show_foreign_agents(message: types.Message):
    logging.info(f"Command inoagents received from user {message.from_user.id}")

    try:
        async with aiosqlite.connect("database.db") as db:
            # Получаем текущих иноагентов (у которых не истек срок)
            cursor = await db.execute('''
                SELECT user_id, agent_name, expires_at 
                FROM foreign_agents 
                WHERE expires_at > datetime('now')
                ORDER BY agent_name
            ''')
            active_agents = await cursor.fetchall()

            if not active_agents:
                await message.answer("📋 Список иноагентов пуст.\nВ настоящее время нет активных иноагентов.")
                return

            # Формируем сообщение со списком
            agents_list = "🕵️ **Список иноагентов:**\n\n"

            for i, (user_id, agent_name, expires_at) in enumerate(active_agents, 1):
                expires_date = datetime.fromisoformat(expires_at)
                time_remaining = expires_date - datetime.now()
                days_remaining = time_remaining.days
                hours_remaining = time_remaining.seconds // 3600

                agents_list += (
                    f"{i}. {agent_name}\n"
                    f"   ID: {user_id}\n"
                    f"   Осталось: {days_remaining}д {hours_remaining}ч\n"
                    f"   Истекает: {expires_date.strftime('%d.%m.%Y в %H:%M')}\n\n"
                )

            agents_list += f"Всего активных иноагентов: {len(active_agents)}"

            await message.answer(agents_list)

    except Exception as e:
        logging.error(f"Database error in inoagents command: {e}")
        await message.answer("Произошла ошибка при получении списка иноагентов.")


@dp.message(Command("check_inoagent"))
async def check_agent_status(message: types.Message):
    logging.info(f"Command check_agent received from user {message.from_user.id}")

    # Если команда отправлена ответом на сообщение
    if message.reply_to_message and message.reply_to_message.from_user:
        target_user_id = message.reply_to_message.from_user.id
        target_user_name = message.reply_to_message.from_user.full_name
    else:
        # Получаем аргументы из текста сообщения (исправленная версия)
        args = message.text.split()[1:] if len(message.text.split()) > 1 else []
        if args and args[0].isdigit():
            target_user_id = int(args[0])
            target_user_name = f"пользователь с ID {target_user_id}"
        else:
            await message.answer(
                "Использование:\n"
                "- Ответьте командой на сообщение пользователя\n"
                "- Или укажите ID пользователя: /check_agent <user_id>"
            )
            return

    try:
        async with aiosqlite.connect("database.db") as db:
            cursor = await db.execute(
                "SELECT agent_name, expires_at FROM foreign_agents WHERE user_id = ?",
                (target_user_id,)
            )
            agent_data = await cursor.fetchone()

            if not agent_data:
                await message.answer(f"✅ {target_user_name} не является иноагентом.")
                return

            agent_name, expires_at = agent_data
            expires_date = datetime.fromisoformat(expires_at)

            if expires_date > datetime.now():
                time_remaining = expires_date - datetime.now()
                days_remaining = time_remaining.days
                hours_remaining = time_remaining.seconds // 3600

                await message.answer(
                    f"🕵️ {target_user_name} является иноагентом!\n"
                    f"Кодовое имя: {agent_name}\n"
                    f"Статус истекает через: {days_remaining}д {hours_remaining}ч\n"
                    f"Дата истечения: {expires_date.strftime('%d.%m.%Y в %H:%M')}"
                )
            else:
                await message.answer(
                    f"✅ {target_user_name} ранее был иноагентом.\n"
                    f"Бывшее кодовое имя: {agent_name}\n"
                    f"Статус истёк: {expires_date.strftime('%d.%m.%Y в %H:%M')}"
                )

    except Exception as e:
        logging.error(f"Database error in check_agent command: {e}")
        await message.answer("Произошла ошибка при проверке статуса пользователя.")


"""
показ паспорта
"""


async def my_passport_get(user_id):
    try:
        async with aiosqlite.connect("database.db") as db:
            cursor = await db.execute("""
                SELECT 
                    u.name, u.gender, u.city, u.street, u.house_number, 
                    u.balance, u.account_id,
                    u.spouse_id, s.name as spouse_name, u.marriage_date, u.marriage_id
                FROM users u
                LEFT JOIN users s ON u.spouse_id = s.account_id
                WHERE u.user_id = ?
                """, (user_id,))

            user = await cursor.fetchone()

            if not user:
                return None

            name, gender, city, street, house_number, balance, account_id, spouse_id, spouse_name, marriage_date, marriage_id = user

            # Формируем полный адрес
            address_parts = []
            if city:
                address_parts.append(f"город🏙️ {city}")
            if street:
                address_parts.append(f"улица🏘️ {street}")
            if house_number:
                address_parts.append(f"дом🏠 {house_number}")

            full_address = ", ".join(address_parts) if address_parts else "Не указан"

            # Формируем информацию о браке
            marriage_info = ""
            if spouse_name and marriage_date:
                marriage_info = f"❤️├ 💍 <b>Брак:</b> с {spouse_name}\n"
            elif spouse_name:
                marriage_info = f"❤️├ 💍 <b>Супруг(а):</b> <code>{spouse_name}</code>\n"

            profile_info = (
                f"📕 <b>Паспорт пользователя {name}</b>\n\n"
                f"🤍├ 🆔 <b>Гос ID:</b> <code>{account_id}</code>\n"
                f"🤍├ 👤 <b>Имя:</b> <code>{name}</code>\n"
                f"❤️├ 👫 <b>Пол:</b> <code>{'Мужской' if gender == 'м' else 'Женский'}</code>\n"
                f"️❤️├{marriage_info if marriage_info else '❤️├ 💍 <b>Семейное положение:</b> <code>Не женат/Не замужем</code>'}\n"
                f"🖤├ 📍 <b>Адрес:</b> <code>{full_address}</code>\n"
                f"🖤└ 💳 <b>Баланс:</b> <code>{balance}</code> шуек\n"
            )

            return profile_info
    except Exception as e:
        print(f"Ошибка при получении паспорта: {e}")
        return None


# мой паспорт
@dp.message(Command("mp"))
async def send_passport(message: types.Message):
    user_id = message.from_user.id
    current_time = datetime.now()

    # Проверяем, когда пользователь последний раз использовал команду
    if user_id in last_used:
        last_time = last_used[user_id]
        if current_time - last_time < timedelta(seconds=10):
            remaining = 10 - (current_time - last_time).seconds
            await message.answer(f"⏳ Подождите {remaining} секунд перед повторным запросом!")
            return

    # Обновляем время и выполняем команду
    last_used[user_id] = current_time
    passport_info = await my_passport_get(user_id)

    if passport_info:
        await message.answer(passport_info, parse_mode="HTML")
    else:
        await message.answer("❌ Данные паспорта не найдены.")


"""
МАГАЗИН
"""
# Категории товаров
MARKETPLACE_CATEGORIES = [
    "🧱 блоки",
    "👕 Одежда",
    "📚 Книги",
    "🏠 Недвижимость",
    "🏁 Флаги",
    "🍷 Зелья",
    "🛠 Инструменты",
    "📦 Разное"
]


# Клавиатура для маркетплейса
async def marketplace_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🛒 Посмотреть товары"), KeyboardButton(text="➕ Добавить товар")],
            [KeyboardButton(text="📦 Мои товары"), KeyboardButton(text="💰 Мои покупки")],
            [KeyboardButton(text="⬅️ В главное меню"), KeyboardButton(text="🆕 Новые товары")]
        ],
        resize_keyboard=True
    )


# Клавиатура категорий
def categories_kb():
    keyboard = []
    row = []
    for i, category in enumerate(MARKETPLACE_CATEGORIES, 1):
        row.append(KeyboardButton(text=category))
        if i % 2 == 0:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([KeyboardButton(text="❌ Отмена")])
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)


# Клавиатура подтверждения для покупки
def purchase_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="✅ Купить"), KeyboardButton(text="❌ Отмена")]
        ],
        resize_keyboard=True
    )


# Обработчик входа в маркетплейс
@dp.message(F.text == "🛒 Маркетплейс")
async def open_marketplace(message: types.Message):
    await message.answer(
        "🏪 Добро пожаловать на шуйскую торговую площадку!\n\n"
        "Здесь вы можете покупать и продавать товары другим пользователям.",
        reply_markup=await marketplace_kb()
    )


# Обработчик кнопки "Посмотреть товары"
@dp.message(F.text == "🛒 Посмотреть товары")
async def browse_items(message: types.Message, state: FSMContext):
    await message.answer(
        "Выберите категорию товаров:",
        reply_markup=categories_kb()
    )
    await state.set_state(Marketplace.browse_category)


# Обработчик выбора категории
@dp.message(Marketplace.browse_category)
async def show_category_items(message: types.Message, state: FSMContext):
    category = message.text

    if category == "❌ Отмена":
        await state.clear()
        await message.answer("Отменено.", reply_markup=await marketplace_kb())
        return

    if category not in MARKETPLACE_CATEGORIES:
        await message.answer("Пожалуйста, выберите категорию из списка.", reply_markup=categories_kb())
        return

    # Сохраняем выбранную категорию для возможного возврата
    await state.update_data(selected_category=category)

    async with aiosqlite.connect("database.db") as db:
        # Проверяем наличие колонки quantity
        cursor = await db.execute("PRAGMA table_info(marketplace_items)")
        columns = await cursor.fetchall()
        column_names = [column[1] for column in columns]

        if 'quantity' not in column_names:
            cursor = await db.execute("""
                SELECT mi.id, mi.title, mi.price, u.name 
                FROM marketplace_items mi
                JOIN users u ON mi.seller_id = u.user_id
                WHERE mi.category = ? AND mi.status = 'active'
                ORDER BY mi.created_date DESC
                LIMIT 20
            """, (category,))
        else:
            cursor = await db.execute("""
                SELECT mi.id, mi.title, mi.price, mi.quantity, u.name 
                FROM marketplace_items mi
                JOIN users u ON mi.seller_id = u.user_id
                WHERE mi.category = ? AND mi.status = 'active' AND mi.quantity > 0
                ORDER BY mi.created_date DESC
                LIMIT 20
            """, (category,))

        items = await cursor.fetchall()  # Переменная items, а не attractions!

    if not items:  # Проверяем items, а не attractions
        # Если товаров нет, предлагаем добавить первый товар
        add_first_button = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="➕ Добавить первый товар в эту категорию")],
                [KeyboardButton(text="🔄 Выбрать другую категорию")],
                [KeyboardButton(text="⬅️ В меню маркетплейса")]
            ],
            resize_keyboard=True
        )

        await message.answer(
            f"😔 В категории '{category}' пока нет товаров.\n\n"
            f"Хотите добавить первый товар в эту категорию?",
            reply_markup=add_first_button
        )
        # Переходим в специальное состояние для добавления первого товара
        await state.set_state(Marketplace.add_first_item)
        return

    # Если товары есть, показываем их
    if 'quantity' in column_names:
        response = f"📦 Товары в категории '{category}':\n\n"
        for item_id, title, price, quantity, seller_name in items:
            response += f"🆔 {item_id}: {title}\n"
            response += f"💰 Цена: {price} шуек\n"
            response += f"📊 В наличии: {quantity} шт.\n"
            response += f"👤 Продавец: {seller_name}\n"
            response += "─" * 30 + "\n"
    else:
        response = f"📦 Товары в категории '{category}':\n\n"
        for item_id, title, price, seller_name in items:
            response += f"🆔 {item_id}: {title}\n"
            response += f"💰 Цена: {price} шуек\n"
            response += f"👤 Продавец: {seller_name}\n"
            response += "─" * 30 + "\n"

    # Кнопки навигации
    nav_keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="➕ Добавить товар в эту категорию")],
            [KeyboardButton(text="⬅️ Выбрать другую категорию")],
            [KeyboardButton(text="🏠 В меню маркетплейса")]
        ],
        resize_keyboard=True
    )

    response += "\nВведите ID товара для просмотра подробностей.\n"
    response += "Или используйте кнопки для навигации:"

    await message.answer(response, reply_markup=nav_keyboard)
    await state.set_state(Marketplace.view_item)


@dp.message(Marketplace.add_first_item, F.text == "➕ Добавить первый товар в эту категорию")
async def add_first_item_start(message: types.Message, state: FSMContext):
    """Начинает процесс добавления товара в пустую категорию"""
    data = await state.get_data()
    category = data.get('selected_category')

    if not category:
        await message.answer("❌ Ошибка: категория не выбрана.")
        await state.clear()
        return

    # Сохраняем категорию
    await state.update_data(category=category)

    # Если это недвижимость - спрашиваем адрес
    if category == "🏠 Недвижимость":
        await message.answer(
            "🏠 Вы добавляете недвижимость. Для удобства покупателей "
            "рекомендуется указать адрес.\n\n"
            "Хотите указать адрес недвижимости?",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="✅ Да, указать адрес")],
                    [KeyboardButton(text="❌ Нет, пропустить")],
                    [KeyboardButton(text="↩ Отмена")]
                ],
                resize_keyboard=True
            )
        )
        await state.set_state(Marketplace.add_property_address)
    else:
        # Для других категорий сразу к названию
        await message.answer(
            f"Выбранная категория: {category}\n\n"
            "Введите название товара (максимум 100 символов):\n"
            "Или напишите 'отмена' для отмены",
            reply_markup=ReplyKeyboardRemove()
        )
        await state.set_state(Marketplace.add_title)


# Обработчик выбора категории для добавления товара
@dp.message(Marketplace.choose_category_for_add)
async def choose_category_for_add(message: types.Message, state: FSMContext):
    category = message.text

    if category == "❌ Отмена":
        await state.clear()
        await message.answer("Добавление товара отменено.", reply_markup=await marketplace_kb())
        return

    if category not in MARKETPLACE_CATEGORIES:
        await message.answer("Пожалуйста, выберите категорию из списка.", reply_markup=categories_kb())
        return

    # Сохраняем категорию
    await state.update_data(category=category)

    # Если это недвижимость, запрашиваем адрес
    if category == "🏠 Недвижимость":
        await message.answer(
            "🏠 Вы добавляете недвижимость. Для удобства покупателей "
            "рекомендуется указать адрес.\n\n"
            "Хотите указать адрес недвижимости?",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="✅ Да, указать адрес")],
                    [KeyboardButton(text="❌ Нет, пропустить")],
                    [KeyboardButton(text="↩ Отмена")]
                ],
                resize_keyboard=True
            )
        )
        await state.set_state(Marketplace.add_property_address)
    else:
        # Для других категорий сразу переходим к названию
        await message.answer(
            f"Выбранная категория: {category}\n\n"
            "Введите название товара (максимум 100 символов):\n"
            "Или напишите 'отмена' для отмены",
            reply_markup=ReplyKeyboardRemove()
        )
        await state.set_state(Marketplace.add_title)


@dp.message(Marketplace.enter_address_house)
async def process_address_house(message: types.Message, state: FSMContext):
    if message.text.lower() == "отмена":
        await state.clear()
        await message.answer("Добавление товара отменено.", reply_markup=await marketplace_kb())
        return

    house = message.text.strip()
    if len(house) < 1 or len(house) > 10:
        await message.answer("Номер дома должен быть от 1 до 10 символов. Попробуйте еще раз:")
        return

    await state.update_data(property_house=house)

    # Формируем описание с адресом
    data = await state.get_data()
    city = data.get('property_city', '')
    street = data.get('property_street', '')
    house_number = data.get('property_house', '')

    address_description = f"🏠 Адрес недвижимости: {city}, {street}, {house_number}\n\n"
    await state.update_data(address_description=address_description)

    await message.answer(
        f"✅ Адрес сохранен: {city}, {street}, {house_number}\n\n"
        "Теперь введите название недвижимости (максимум 100 символов):\n"
        "Или напишите 'отмена' для отмены",
        reply_markup=ReplyKeyboardRemove()
    )
    await state.set_state(Marketplace.add_title)


@dp.message(Marketplace.enter_address_street)
async def process_address_street(message: types.Message, state: FSMContext):
    if message.text.lower() == "отмена":
        await state.clear()
        await message.answer("Добавление товара отменено.", reply_markup=await marketplace_kb())
        return

    street = message.text.strip()
    if len(street) < 2 or len(street) > 50:
        await message.answer("Название улицы должно быть от 2 до 50 символов. Попробуйте еще раз:")
        return

    await state.update_data(property_street=street)
    await message.answer(
        "Введите номер дома (можно с буквой, например: 15А или 7/2):\nИли напишите 'отмена' для отмены")
    await state.set_state(Marketplace.enter_address_house)


# Обработчик для кнопки "⬅️ Выбрать другую категорию"
@dp.message(Marketplace.view_item, F.text == "⬅️ Выбрать другую категорию")
async def back_to_categories_from_view(message: types.Message, state: FSMContext):
    """Возврат к выбору категории"""
    await message.answer(
        "Выберите категорию товаров:",
        reply_markup=categories_kb()
    )
    await state.set_state(Marketplace.browse_category)


# Обработчик для кнопки "🏠 В меню маркетплейса"
@dp.message(Marketplace.view_item, F.text == "🏠 В меню маркетплейса")
async def back_to_marketplace_from_view(message: types.Message, state: FSMContext):
    """Возврат в меню маркетплейса"""
    await state.clear()
    await message.answer("Возврат в меню маркетплейса.", reply_markup=await marketplace_kb())


# Обновляем функцию показа моих товаров
@dp.message(F.text == "📦 Мои товары")
async def show_my_items(message: types.Message, state: FSMContext):
    user_id = message.from_user.id

    async with aiosqlite.connect("database.db") as db:
        cursor = await db.execute("""
            SELECT id, title, price, quantity, status, created_date 
            FROM marketplace_items 
            WHERE seller_id = ?
            ORDER BY created_date DESC
        """, (user_id,))

        items = await cursor.fetchall()

    if not items:
        await message.answer("У вас пока нет товаров на продажу.")
        return

    response = "📦 <b>Ваши товары</b>\n\n"

    for item_id, title, price, quantity, status, created_date in items:
        status_emoji = "🟢" if status == 'active' and quantity > 0 else "🟡" if status == 'active' else "🔴"
        response += f"{status_emoji} <b>{title}</b>\n"
        response += f"💰 Цена: {price} шуек\n"
        response += f"📊 Количество: {quantity} шт.\n"
        response += f"📊 Статус: {status}\n"
        response += f"📅 {created_date[:10]}\n"
        response += f"🆔 ID: {item_id}\n"
        response += "─" * 30 + "\n"

    response += "\nВведите ID товара, который хотите посмотреть."
    await message.answer(response, parse_mode="HTML")
    await state.set_state(Marketplace.my_items)


# Обработчик отмены в процессе добавления товара
@dp.message(
    StateFilter(Marketplace.add_title, Marketplace.add_description, Marketplace.add_price, Marketplace.add_quantity,
                Marketplace.add_category, Marketplace.add_image), F.text.lower() == "отмена")
async def cancel_item_addition(message: types.Message, state: FSMContext):
    """Отмена добавления товара"""
    await state.clear()
    await message.answer(
        "Добавление товара отменено.",
        reply_markup=await marketplace_kb()
    )


# Обработчик просмотра товара
@dp.message(Marketplace.view_item)
async def view_item_details(message: types.Message, state: FSMContext):
    if message.text.lower() == "к категориям":
        await message.answer(
            "Выберите категорию товаров:",
            reply_markup=categories_kb()
        )
        await state.set_state(Marketplace.browse_category)
        return

    if message.text.lower() == "в меню":
        await state.clear()
        await message.answer("Возврат в меню маркетплейса.", reply_markup=await marketplace_kb())
        return

    try:
        item_id = int(message.text)
    except ValueError:
        await message.answer("Пожалуйста, введите числовой ID товара.")
        return

    async with aiosqlite.connect("database.db") as db:
        cursor = await db.execute("""
            SELECT 
                mi.id, mi.seller_id, mi.title, mi.description, 
                mi.price, mi.quantity, mi.category, mi.status,
                mi.created_date, mi.image_id, 
                u.name as seller_name, u.account_id as seller_account
            FROM marketplace_items mi
            JOIN users u ON mi.seller_id = u.user_id
            WHERE mi.id = ? AND mi.status = 'active' AND mi.quantity > 0
        """, (item_id,))

        item = await cursor.fetchone()

    if not item:
        await message.answer("Товар не найден или уже продан.")
        await state.clear()
        await message.answer("Возврат в меню маркетплейса.", reply_markup=await marketplace_kb())
        return

    (item_id, seller_id, title, description, price, quantity,
     category, status, created_date, image_id, seller_name, seller_account) = item

    seller_info = f"{seller_name} (ID: {seller_account})"

    response = (
        f"📦 <b>{title}</b>\n\n"
        f"📝 Описание: {description or 'Нет описания'}\n"
        f"💰 Цена: <b>{price}</b> шуек\n"
        f"📊 В наличии: <b>{quantity}</b> шт.\n"
        f"📂 Категория: {category}\n"
        f"👤 Продавец: {seller_info}\n"
        f"📅 Размещено: {created_date[:10]}\n\n"
    )

    if image_id:
        try:
            await message.answer_photo(image_id, caption=response, parse_mode="HTML")
        except:
            await message.answer(response + "\n🖼 Фото прилагается", parse_mode="HTML")
    else:
        await message.answer(response, parse_mode="HTML")

    await state.update_data(item_id=item_id, price=price, seller_id=seller_id,
                            title=title, quantity=quantity, category=category)

    await message.answer("Хотите купить этот товар?", reply_markup=purchase_kb())
    await state.set_state(Marketplace.confirm_purchase)


# Обработчик покупки товара
@dp.message(Marketplace.confirm_purchase, F.text == "✅ Купить")
async def purchase_item(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    data = await state.get_data()
    item_id = data['item_id']
    price = data['price']
    seller_id = data['seller_id']
    available_quantity = data['quantity']
    item_title = data.get('title', 'Неизвестный товар')
    category = data.get('category')

    if user_id == seller_id:
        await message.answer("❌ Вы не можете купить свой собственный товар.")
        await state.clear()
        await message.answer("Возврат в меню маркетплейса.", reply_markup=await marketplace_kb())
        return

    # Для товаров с количеством > 1 спрашиваем количество
    if available_quantity > 1:
        await message.answer(
            f"Введите количество товара для покупки (доступно: {available_quantity} шт.):",
            reply_markup=ReplyKeyboardRemove()
        )
        await state.set_state(Marketplace.enter_purchase_quantity)
    else:
        # Для единичного товара сразу переходим к подтверждению
        await state.update_data(purchase_quantity=1, total_price=price)

        text = (f"Вы собираетесь купить товар:\n"
                f"📦 {item_title}\n"
                f"💰 Цена: {price} шуек\n\n"
                f"Подтверждаете покупку?")

        await message.answer(text, reply_markup=confirm_kb())
        await state.set_state(Marketplace.confirm_purchase_final)


# 10. Обработчик ввода количества покупки
@dp.message(Marketplace.enter_purchase_quantity)
async def process_purchase_quantity(message: types.Message, state: FSMContext):
    try:
        purchase_quantity = int(message.text.strip())
        if purchase_quantity <= 0:
            await message.answer("Количество должно быть положительным числом.")
            return
    except ValueError:
        await message.answer("Пожалуйста, введите корректное количество (целое число).")
        return

    data = await state.get_data()
    available_quantity = data['quantity']

    if purchase_quantity > available_quantity:
        await message.answer(f"❌ Недостаточно товара. Доступно только {available_quantity} шт.")
        return

    total_price = data['price'] * purchase_quantity

    await state.update_data(purchase_quantity=purchase_quantity, total_price=total_price)

    text = (f"Вы собираетесь купить {purchase_quantity} шт. товара\n"
            f"Цена за единицу: {data['price']} шуек\n"
            f"Общая сумма: {total_price} шуек\n\n"
            f"Подтверждаете покупку?")

    await message.answer(text, reply_markup=confirm_kb())
    await state.set_state(Marketplace.confirm_purchase_final)


# Функция отправки запроса подтверждения
async def send_confirmation_request_to_buyer(message: types.Message, state: FSMContext,
                                             transaction_id: int, item_title: str,
                                             price_per_unit: float, quantity: int,
                                             total_price: float, expiration_time: datetime):
    """Отправляет запрос на подтверждение покупателю"""
    buyer_message = (
        f"🛒 Запрос на подтверждение покупки\n\n"
        f"📦 Товар: {item_title}\n"
        f"📊 Количество: {quantity} шт.\n"
        f"💰 Цена за единицу: {price_per_unit} шуек\n"
        f"💵 Общая сумма: {total_price} шуек\n"
        f"⏰ Подтвердите в течение 3 часов (до {expiration_time.strftime('%H:%M')})\n\n"
        f"После подтверждения средства будут зарезервированы."
    )

    # Клавиатура для подтверждения покупателем
    builder = ReplyKeyboardBuilder()
    builder.add(KeyboardButton(text="✅ Подтвердить покупку"))
    builder.add(KeyboardButton(text="❌ Отменить покупку"))

    await message.answer(buyer_message, reply_markup=builder.as_markup(resize_keyboard=True))

    # Устанавливаем состояние ожидания подтверждения
    await state.set_state(Marketplace.wait_buyer_confirmation)


async def start_confirmation_timer(transaction_id: int, expiration_time: datetime):
    """Запускает таймер для автоматической отмены сделки"""
    time_until_expiration = (expiration_time - datetime.now()).total_seconds()

    if time_until_expiration <= 0:
        await auto_cancel_transaction(transaction_id)
        return

    # Ждем до времени истечения
    await asyncio.sleep(time_until_expiration)

    # Проверяем, не подтверждена ли уже сделка
    async with aiosqlite.connect("database.db") as db:
        cursor = await db.execute("""
            SELECT status FROM marketplace_transactions 
            WHERE id = ? AND status = 'pending_confirmation'
        """, (transaction_id,))

        result = await cursor.fetchone()
        if result:
            # Сделка все еще ожидает подтверждения - отменяем
            await auto_cancel_transaction(transaction_id)


async def auto_cancel_transaction(transaction_id: int):
    """Автоматически отменяет сделку при истечении времени"""
    async with aiosqlite.connect("database.db") as db:
        try:
            await db.execute("BEGIN TRANSACTION")

            # Получаем информацию о сделке
            cursor = await db.execute("""
                SELECT buyer_id, seller_id, price, quantity, item_id 
                FROM marketplace_transactions 
                WHERE id = ? AND status = 'pending_confirmation'
            """, (transaction_id,))

            result = await cursor.fetchone()
            if not result:
                return

            buyer_id, seller_id, price_per_unit, quantity, item_id = result
            total_price = price_per_unit * quantity

            # Возвращаем средства покупателю
            await db.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (total_price, buyer_id))

            # Возвращаем товар (увеличиваем количество)
            await db.execute(
                "UPDATE marketplace_items SET quantity = quantity + ? WHERE id = ?",
                (quantity, item_id)
            )

            # Если товар был зарезервирован, возвращаем статус
            await db.execute("""
                UPDATE marketplace_items 
                SET status = CASE WHEN quantity + ? > 0 THEN 'active' ELSE status END 
                WHERE id = ?
            """, (quantity, item_id))

            # Обновляем статус сделки
            await db.execute("""
                UPDATE marketplace_transactions 
                SET status = 'cancelled', 
                    cancellation_reason = 'Истекло время подтверждения',
                    cancelled_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (transaction_id,))

            await db.commit()

            # Уведомляем покупателя
            try:
                await bot.send_message(
                    buyer_id,
                    f"❌ Сделка отменена\n\n"
                    f"Время подтверждения истекло (3 часа).\n"
                    f"Средства возвращены на ваш баланс.",
                    reply_markup=await marketplace_kb()
                )
            except Exception as e:
                print(f"Ошибка уведомления покупателя: {e}")

            # Уведомляем продавца
            try:
                await bot.send_message(
                    seller_id,
                    f"❌ Сделка отменена\n\n"
                    f"Покупатель не подтвердил покупку в течение 3 часов.\n"
                    f"Товар снова доступен для продажи.",
                    reply_markup=await marketplace_kb()
                )
            except Exception as e:
                print(f"Ошибка уведомления продавца: {e}")

        except Exception as e:
            await db.rollback()
            print(f"Ошибка при автоматической отмене сделки: {e}")


async def complete_transaction(db, transaction_id, seller_id, total_price, item_id, quantity):
    """Завершает сделку и переводит средства"""
    # Обновляем статус транзакции
    await db.execute("""
        UPDATE marketplace_transactions 
        SET status = 'completed', completed_at = datetime('now')
        WHERE id = ?
    """, (transaction_id,))

    # Зачисляем средства продавцу
    await db.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (total_price, seller_id))

    # Помечаем товар как проданный
    await db.execute("UPDATE marketplace_items SET quantity = quantity - ? WHERE id = ?", (quantity, item_id))

    # Если товар закончился, меняем статус
    cursor = await db.execute("SELECT quantity FROM marketplace_items WHERE id = ?", (item_id,))
    remaining_quantity = (await cursor.fetchone())[0]

    if remaining_quantity <= 0:
        await db.execute("UPDATE marketplace_items SET status = 'sold' WHERE id = ?", (item_id,))


# Обработчик для кнопки "➕ Добавить товар в эту категорию"
@dp.message(Marketplace.view_item, F.text == "➕ Добавить товар в эту категорию")
async def add_item_to_existing_category(message: types.Message, state: FSMContext):
    """Добавление товара в непустую категорию"""
    # Получаем сохраненную категорию из состояния
    data = await state.get_data()
    category = data.get('selected_category')

    if not category:
        await message.answer("❌ Ошибка: категория не определена.")
        return

    # Сохраняем категорию и переходим к процессу добавления
    await state.update_data(category=category)

    await message.answer(
        f"➕ Вы добавляете товар в категорию '{category}'!\n\n"
        "Введите название товара (максимум 100 символов):\n"
        "Или напишите 'отмена' для отмены",
        reply_markup=ReplyKeyboardRemove()
    )
    await state.set_state(Marketplace.add_title)


# Обработчик подтверждения покупки покупателем
@dp.message(Marketplace.confirm_purchase_final, F.text == "Подтвердить")
async def confirm_purchase_final(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    data = await state.get_data()
    item_id = data['item_id']
    price_per_unit = data['price']
    purchase_quantity = data['purchase_quantity']
    total_price = data['total_price']
    seller_id = data['seller_id']
    item_title = data.get('title', 'Неизвестный товар')
    category = data.get('category')

    async with aiosqlite.connect("database.db") as db:
        try:
            # Проверяем актуальное количество
            cursor = await db.execute("SELECT quantity FROM marketplace_items WHERE id = ?", (item_id,))
            result = await cursor.fetchone()
            if not result or result[0] < purchase_quantity:
                await message.answer("❌ Недостаточно товара. Возможно, его уже купили.")
                await state.clear()
                return

            # Проверяем баланс покупателя
            cursor = await db.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,))
            buyer_balance = (await cursor.fetchone())[0]

            if buyer_balance < total_price:
                await message.answer("❌ Недостаточно средств для покупки.")
                await state.clear()
                return

            # Начинаем транзакцию
            await db.execute("BEGIN TRANSACTION")

            # Резервируем средства у покупателя
            await db.execute("UPDATE users SET balance = balance - ? WHERE user_id = ?", (total_price, user_id))

            # Создаем запись о сделке
            expiration_time = datetime.now() + timedelta(hours=3)

            await db.execute("""
                INSERT INTO marketplace_transactions 
                (item_id, buyer_id, seller_id, price, quantity, status, 
                 confirmation_expires_at, buyer_confirmed, seller_confirmed)
                VALUES (?, ?, ?, ?, ?, 'pending_confirmation', ?, 0, 0)
            """, (item_id, user_id, seller_id, price_per_unit, purchase_quantity, expiration_time))

            # Получаем ID созданной транзакции
            cursor = await db.execute("SELECT last_insert_rowid()")
            transaction_id = (await cursor.fetchone())[0]

            # Резервируем товар (уменьшаем количество)
            await db.execute(
                "UPDATE marketplace_items SET quantity = quantity - ? WHERE id = ?",
                (purchase_quantity, item_id)
            )

            # Если товар закончился, меняем статус
            await db.execute("""
                UPDATE marketplace_items 
                SET status = CASE WHEN quantity - ? <= 0 THEN 'reserved' ELSE status END 
                WHERE id = ?
            """, (purchase_quantity, item_id))

            await db.commit()

            # Сохраняем transaction_id для покупателя
            await state.update_data(transaction_id=transaction_id)

            # Отправляем запрос на подтверждение покупателю
            await send_confirmation_request_to_buyer(
                message, state, transaction_id, item_title,
                price_per_unit, purchase_quantity, total_price, expiration_time
            )

            # Запускаем таймер для автоматической отмены
            asyncio.create_task(start_confirmation_timer(transaction_id, expiration_time))

        except Exception as e:
            await db.rollback()
            await message.answer("❌ Произошла ошибка при покупке. Попробуйте позже.")
            print(f"Ошибка покупки: {e}")
            await state.clear()


@dp.message(Marketplace.confirm_purchase_final, F.text == "Отмена")
async def cancel_purchase_final(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("Покупка отменена.", reply_markup=await marketplace_kb())


@dp.message(Marketplace.enter_purchase_quantity, F.text == "Отмена")
async def cancel_purchase_quantity(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("Покупка отменена.", reply_markup=await marketplace_kb())


@dp.message(Marketplace.wait_buyer_confirmation, F.text == "✅ Подтвердить покупку")
async def buyer_confirm_purchase(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    data = await state.get_data()
    transaction_id = data.get('transaction_id')

    if not transaction_id:
        await message.answer("❌ Ошибка: ID транзакции не найден.")
        await state.clear()
        return

    async with aiosqlite.connect("database.db") as db:
        try:
            await db.execute("BEGIN TRANSACTION")

            # Проверяем, не истекло ли время подтверждения
            cursor = await db.execute("""
                SELECT confirmation_expires_at, price, quantity, item_id, seller_id 
                FROM marketplace_transactions 
                WHERE id = ? AND buyer_id = ? AND status = 'pending_confirmation'
            """, (transaction_id, user_id))

            result = await cursor.fetchone()
            if not result:
                await message.answer("❌ Транзакция не найдена или уже обработана.")
                await db.rollback()
                await state.clear()
                return

            expiration_time_str, price_per_unit, quantity, item_id, seller_id = result
            expiration_time = datetime.fromisoformat(expiration_time_str)

            if datetime.now() > expiration_time:
                await message.answer("❌ Время подтверждения истекло. Сделка отменена.")
                await db.rollback()
                await state.clear()
                return

            # Обновляем статус сделки
            await db.execute("""
                UPDATE marketplace_transactions 
                SET status = 'pending_completion', 
                    buyer_confirmed = 1,
                    confirmed_at = datetime('now')
                WHERE id = ?
            """, (transaction_id,))

            await db.commit()

            total_price = price_per_unit * quantity

            # Сообщение покупателю
            buyer_message = (
                f"✅ Покупка подтверждена!\n"
                f"📦 Товар: {data.get('title', 'Неизвестный товар')}\n"
                f"📊 Количество: {quantity} шт.\n"
                f"💰 Общая сумма: {total_price} шуек\n\n"
                f"После получения товара подтвердите завершение сделки."
            )

            await message.answer(buyer_message)

            # Клавиатура для покупателя
            builder = ReplyKeyboardBuilder()
            builder.add(KeyboardButton(text="✅ Подтвердить получение"))
            builder.add(KeyboardButton(text="❌ Возникли проблемы"))

            await message.answer(
                "Подтвердите получение товара, когда он будет у вас:",
                reply_markup=builder.as_markup(resize_keyboard=True)
            )

            # Сообщение продавцу
            seller_message = (
                f"🎉 Покупатель подтвердил покупку!\n"
                f"📦 Товар: {data.get('title', 'Неизвестный товар')}\n"
                f"📊 Количество: {quantity} шт.\n"
                f"💰 Сумма: {total_price} шуек\n"
                f"👤 Покупатель: {message.from_user.full_name}"
            )

            if message.from_user.username:
                seller_message += f" (@{message.from_user.username})"

            seller_message += (
                f"\n\n💰 Средства покупателя зарезервированы.\n"
                f"✅ После передачи товара подтвердите завершение сделки."
            )

            # Клавиатура для продавца
            seller_builder = ReplyKeyboardBuilder()
            seller_builder.add(KeyboardButton(text="✅ Подтвердить передачу"))
            seller_builder.add(KeyboardButton(text="❌ Отменить сделку"))

            # Отправляем уведомление продавцу
            try:
                await bot.send_message(
                    seller_id,
                    seller_message,
                    reply_markup=seller_builder.as_markup(resize_keyboard=True)
                )
            except Exception as e:
                print(f"Ошибка уведомления продавца: {e}")
                await message.answer("⚠️ Не удалось уведомить продавца. Свяжитесь с ним самостоятельно.")

            # Устанавливаем состояние для покупателя
            await state.set_state(Marketplace.confirm_completion_buyer)

        except Exception as e:
            await db.rollback()
            await message.answer("❌ Произошла ошибка при подтверждении. Попробуйте позже.")
            print(f"Ошибка подтверждения покупки: {e}")
            await state.clear()


# подтверждение передачи
@dp.message(F.text == "✅ Подтвердить передачу")
async def seller_confirm_completion(message: types.Message):
    user_id = message.from_user.id

    async with aiosqlite.connect("database.db") as db:
        try:
            # Ищем активную сделку для этого продавца
            cursor = await db.execute("""
                SELECT id, buyer_id, price, quantity, item_id, buyer_confirmed 
                FROM marketplace_transactions 
                WHERE seller_id = ? AND status = 'pending_completion'
                ORDER BY id DESC
                LIMIT 1
            """, (user_id,))

            result = await cursor.fetchone()
            if not result:
                await message.answer("❌ Активных сделок не найдено.")
                return

            transaction_id, buyer_id, price_per_unit, quantity, item_id, buyer_confirmed = result

            await db.execute("BEGIN TRANSACTION")

            # Отмечаем подтверждение продавца
            await db.execute("""
                UPDATE marketplace_transactions 
                SET seller_confirmed = 1 
                WHERE id = ? AND seller_id = ?
            """, (transaction_id, user_id))

            if buyer_confirmed:
                # Оба подтвердили - завершаем сделку
                total_price = price_per_unit * quantity
                await complete_transaction(db, transaction_id, user_id, total_price, item_id, quantity)
                await db.commit()

                # Получаем информацию о товаре для предложения переезда
                cursor = await db.execute("""
                    SELECT mi.title, mi.category, mi.description 
                    FROM marketplace_items mi 
                    WHERE mi.id = ?
                """, (item_id,))
                item_info = await cursor.fetchone()

                if item_info:
                    title, category, description = item_info

                    # Если это недвижимость, предлагаем переезд
                    if category == "🏠 Недвижимость":
                        # Получаем адрес из описания
                        address_info = extract_address_from_description(description)

                        if address_info:
                            await offer_relocation_to_buyer(buyer_id, user_id, item_id, address_info, title,
                                                            transaction_id)
                        else:
                            # Если адрес не найден в описании, запрашиваем у продавца
                            # УДАЛЯЕМ ВЫЗОВ ФУНКЦИИ, КОТОРАЯ ТРЕБУЕТ STATE
                            # Вместо этого просто завершаем сделку
                            await message.answer(
                                "🎉 Сделка завершена! Средства зачислены на ваш счет.\n"
                                "ℹ️ Адрес недвижимости не указан в описании.",
                                reply_markup=await marketplace_kb()
                            )

                            # Уведомляем покупателя
                            try:
                                await bot.send_message(
                                    buyer_id,
                                    "🎉 Сделка завершена! Средства переведены продавцу.\n"
                                    "ℹ️ Адрес недвижимости не указан.",
                                    reply_markup=await marketplace_kb()
                                )
                            except:
                                pass
                    else:
                        # Для других категорий просто завершаем
                        await message.answer(
                            "🎉 Сделка завершена! Средства зачислены на ваш счет.",
                            reply_markup=await marketplace_kb()
                        )

                        # Уведомляем покупателя
                        try:
                            await bot.send_message(
                                buyer_id,
                                "🎉 Сделка завершена! Средства переведены продавцу.",
                                reply_markup=await marketplace_kb()
                            )
                        except:
                            pass
            else:
                # Ждем подтверждения покупателя
                await db.commit()
                await message.answer(
                    "✅ Вы подтвердили передачу товара.\n"
                    "⏳ Ожидаем подтверждения от покупателя...",
                    reply_markup=ReplyKeyboardRemove()
                )

                # Уведомляем покупателя
                try:
                    await bot.send_message(
                        buyer_id,
                        "✅ Продавец подтвердил передачу товара!\n"
                        "Подтвердите получение для завершения сделки."
                    )
                except:
                    pass

        except Exception as e:
            await db.rollback()
            await message.answer("❌ Произошла ошибка. Попробуйте позже.")
            print(f"Ошибка подтверждения продавца: {e}")


# Обработчик отмены сделки продавцом
@dp.message(F.text == "❌ Отменить сделку")
async def seller_cancel_deal(message: types.Message, state: FSMContext):
    user_id = message.from_user.id

    async with aiosqlite.connect("database.db") as db:
        try:
            # Ищем активную сделку для этого продавца
            cursor = await db.execute("""
                SELECT id, buyer_id, price, item_id 
                FROM marketplace_transactions 
                WHERE seller_id = ? AND status IN ('pending_confirmation', 'pending_completion')
            """, (user_id,))

            result = await cursor.fetchone()
            if not result:
                await message.answer("❌ Активных сделок не найдено.")
                return

            transaction_id, buyer_id, price, item_id = result

            await db.execute("BEGIN TRANSACTION")

            # Возвращаем средства покупателю
            await db.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (price, buyer_id))

            # Обновляем статус сделки
            await db.execute("""
                UPDATE marketplace_transactions 
                SET status = 'cancelled', 
                    cancellation_reason = 'Отменено продавцом',
                    cancelled_at = datetime('now')
                WHERE id = ?
            """, (transaction_id,))

            # Возвращаем товар в активный статус
            await db.execute("UPDATE marketplace_items SET status = 'active' WHERE id = ?", (item_id,))

            await db.commit()

            await message.answer(
                "❌ Сделка отменена. Товар снова доступен для продажи.",
                reply_markup=await marketplace_kb()
            )

            # Уведомляем покупателя
            try:
                await bot.send_message(
                    buyer_id,
                    f"❌ Продавец отменил сделку.\n"
                    f"Средства возвращены на ваш баланс.",
                    reply_markup=await marketplace_kb()
                )
            except:
                pass

        except Exception as e:
            await db.rollback()
            await message.answer("❌ Произошла ошибка при отмене. Попробуйте позже.")
            print(f"Ошибка отмены сделки: {e}")


def extract_address_from_description(description: str):
    """Пытается извлечь адрес из описания недвижимости"""
    if not description:
        return None

    # Паттерны для поиска адреса
    patterns = [
        r'город\s+([А-Яа-яЁёA-Za-z\s\-]+)(?:,|\s|$)',
        r'г\.\s*([А-Яа-яЁёA-Za-z\s\-]+)(?:,|\s|$)',
        r'улица\s+([А-Яа-яЁёA-Za-z\s\-\.]+)(?:,|\s|$)',
        r'ул\.\s*([А-Яа-яЁёA-Za-z\s\-\.]+)(?:,|\s|$)',
        r'дом\s+([0-9А-Яа-яЁёA-Za-z\-\/]+)(?:,|\s|$)',
        r'д\.\s*([0-9А-Яа-яЁёA-Za-z\-\/]+)(?:,|\s|$)',
    ]

    address_parts = {}

    for pattern in patterns:
        matches = re.findall(pattern, description, re.IGNORECASE)
        if matches:
            key = pattern.split('\\')[0].replace('r', '')
            if 'город' in key or 'г.' in key:
                address_parts['city'] = matches[0].strip()
            elif 'улица' in key or 'ул.' in key:
                address_parts['street'] = matches[0].strip()
            elif 'дом' in key or 'д.' in key:
                address_parts['house'] = matches[0].strip()

    return address_parts if address_parts else None


# Вспомогательная функция завершения сделки
async def complete_address_change(message: types.Message, state: FSMContext, city_name: str, street_name: str,
                                  house_number: str):
    """Завершает смену адреса с учетом переезда после покупки"""
    user_id = message.from_user.id
    data = await state.get_data()

    is_after_purchase = data.get('after_purchase', False)
    relocation_offer_id = data.get('relocation_offer_id')
    purchase_transaction_id = data.get('purchase_transaction_id')

    # Проверяем, является ли пользователь мэром этого города
    is_mayor_of_city = False
    async with aiosqlite.connect("database.db") as db:
        cursor = await db.execute(
            "SELECT 1 FROM cities WHERE name = ? AND mayor_id = ?",
            (city_name, user_id)
        )
        is_mayor_of_city = await cursor.fetchone() is not None

    async with aiosqlite.connect("database.db") as db:
        try:
            # Получаем старый город для обновления статистики населения
            cursor = await db.execute(
                "SELECT city FROM users WHERE user_id = ?",
                (user_id,)
            )
            old_city_result = await cursor.fetchone()
            old_city = old_city_result[0] if old_city_result else None

            # Обновляем адрес пользователя
            await db.execute(
                "UPDATE users SET city = ?, street = ?, house_number = ? WHERE user_id = ?",
                (city_name, street_name, house_number, user_id)
            )

            # Обновляем статистику населения городов
            if old_city and old_city != city_name:
                # Уменьшаем население старого города
                await db.execute(
                    "UPDATE cities SET population = population - 1 WHERE name = ?",
                    (old_city,)
                )
                # Увеличиваем население нового города
                await db.execute(
                    "UPDATE cities SET population = population + 1 WHERE name = ?",
                    (city_name,)
                )

            # Если это переезд после покупки, обновляем статусы
            if is_after_purchase and relocation_offer_id:
                await db.execute("""
                    UPDATE relocation_offers 
                    SET status = 'manual_address_set',
                        city = ?,
                        street = ?,
                        house = ?
                    WHERE id = ?
                """, (city_name, street_name, house_number, relocation_offer_id))

                if purchase_transaction_id:
                    await db.execute("""
                        UPDATE marketplace_transactions 
                        SET status = 'completed_with_manual_relocation'
                        WHERE id = ?
                    """, (purchase_transaction_id,))

            await db.commit()

            # Формируем сообщение
            if is_mayor_of_city:
                message_text = (
                    f"✅ Адрес успешно изменен! 👑\n\n"
                    f"📍 <b>Ваш новый адрес как мэра:</b>\n"
                    f"🏙️ Город: {city_name}\n"
                    f"🏘️ Улица: {street_name}\n"
                    f"🏠 Дом: {house_number}\n\n"
                    f"Переезд подтверждён автоматически, так как вы мэр этого города."
                )
            elif is_after_purchase:
                message_text = (
                    f"✅ Адрес успешно изменен!\n\n"
                    f"📍 Ваш новый адрес:\n"
                    f"🏙️ Город: {city_name}\n"
                    f"🏘️ Улица: {street_name}\n"
                    f"🏠 Дом: {house_number}\n\n"
                    f"🎉 Поздравляем с новосельем и успешной покупкой!"
                )
            else:
                message_text = (
                    f"✅ Адрес успешно изменен!\n\n"
                    f"📍 Ваш новый адрес:\n"
                    f"🏙️ Город: {city_name}\n"
                    f"🏘️ Улица: {street_name}\n"
                    f"🏠 Дом: {house_number}"
                )

            await message.answer(
                message_text,
                parse_mode="HTML" if is_mayor_of_city else None,
                reply_markup=await main_menu_kb(user_id)
            )

        except Exception as e:
            await message.answer("❌ Произошла ошибка при смене адреса. Попробуйте позже.")
            print(f"Ошибка смены адреса: {e}")

    await state.clear()
    await show_main_menu(message, user_id)


async def offer_relocation_to_buyer(buyer_id: int, seller_id: int, item_id: int, address_info: dict, item_title: str,
                                    transaction_id: int):
    """Предлагает покупателю переехать в купленную недвижимость"""

    # Формируем текст адреса
    address_text = ""
    if 'city' in address_info:
        address_text += f"🏙️ Город: {address_info['city']}\n"
    if 'street' in address_info:
        address_text += f"🏘️ Улица: {address_info['street']}\n"
    if 'house' in address_info:
        address_text += f"🏠 Дом: {address_info['house']}\n"

    # Создаем клавиатуру с предложением
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="✅ Да, переехать в новую недвижимость")],
            [KeyboardButton(text="❌ Нет, остаться на текущем месте")],
            [KeyboardButton(text="🔄 Изменить адрес вручную")]
        ],
        resize_keyboard=True
    )

    message_text = (
        f"🏠 <b>Поздравляем с покупкой недвижимости!</b>\n\n"
        f"📦 Товар: {item_title}\n"
        f"📍 Адрес:\n{address_text}\n"
        f"Хотите переехать по этому адресу?\n\n"
        f"<i>Если адрес указан неверно, вы можете изменить его вручную.</i>"
    )

    try:
        # Отправляем предложение покупателю
        await bot.send_message(
            buyer_id,
            message_text,
            parse_mode="HTML",
            reply_markup=keyboard
        )

        # Сохраняем информацию для последующей обработки
        async with aiosqlite.connect("database.db") as db:
            await db.execute("""
                UPDATE marketplace_transactions 
                SET status = 'awaiting_relocation_decision'
                WHERE id = ?
            """, (transaction_id,))
            await db.commit()

        # Сохраняем данные в кэш или временную таблицу
        await save_relocation_offer_data(
            buyer_id=buyer_id,
            seller_id=seller_id,
            item_id=item_id,
            address_info=address_info,
            transaction_id=transaction_id
        )

    except Exception as e:
        print(f"Ошибка отправки предложения переезда: {e}")

        # Если не удалось отправить, просто завершаем сделку
        try:
            await bot.send_message(
                buyer_id,
                "🎉 Сделка завершена! Средства переведены продавцу.",
                reply_markup=await marketplace_kb()
            )
        except:
            pass


# 6. Функция сохранения данных предложения переезда
async def save_relocation_offer_data(buyer_id: int, seller_id: int, item_id: int, address_info: dict,
                                     transaction_id: int):
    """Сохраняет данные предложения переезда"""
    try:
        async with aiosqlite.connect("database.db") as db:
            # Создаем таблицу если ее нет
            await db.execute("""
                CREATE TABLE IF NOT EXISTS relocation_offers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    transaction_id INTEGER NOT NULL,
                    buyer_id INTEGER NOT NULL,
                    seller_id INTEGER NOT NULL,
                    item_id INTEGER NOT NULL,
                    city TEXT,
                    street TEXT,
                    house TEXT,
                    status TEXT DEFAULT 'pending',
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (transaction_id) REFERENCES marketplace_transactions(id)
                )
            """)

            await db.execute("""
                INSERT INTO relocation_offers 
                (transaction_id, buyer_id, seller_id, item_id, city, street, house, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'pending')
            """, (
                transaction_id, buyer_id, seller_id, item_id,
                address_info.get('city'),
                address_info.get('street'),
                address_info.get('house')
            ))
            await db.commit()

    except Exception as e:
        print(f"Ошибка сохранения данных переезда: {e}")


# 7. Обработчики выбора покупателя
@dp.message(F.text == "✅ Да, переехать в новую недвижимость")
async def accept_relocation(message: types.Message, state: FSMContext):
    """Обработчик согласия на переезд"""
    user_id = message.from_user.id

    try:
        async with aiosqlite.connect("database.db") as db:
            # Получаем предложение переезда
            cursor = await db.execute("""
                SELECT id, city, street, house, transaction_id 
                FROM relocation_offers 
                WHERE buyer_id = ? AND status = 'pending'
                ORDER BY created_at DESC 
                LIMIT 1
            """, (user_id,))

            offer = await cursor.fetchone()

            if not offer:
                await message.answer("❌ Предложение переезда не найдено или уже обработано.")
                return

            offer_id, city, street, house, transaction_id = offer

            # Проверяем существование города
            cursor = await db.execute("SELECT 1 FROM cities WHERE name = ?", (city,))
            city_exists = await cursor.fetchone()

            if not city_exists:
                # Если города нет, создаем его
                await db.execute(
                    "INSERT INTO cities (name, mayor_id) VALUES (?, ?)",
                    (city, user_id)
                )
                await db.commit()

            # Обновляем адрес пользователя
            await db.execute("""
                UPDATE users 
                SET city = ?, street = ?, house_number = ? 
                WHERE user_id = ?
            """, (city, street, house, user_id))

            # Обновляем статус предложения
            await db.execute("""
                UPDATE relocation_offers 
                SET status = 'accepted' 
                WHERE id = ?
            """, (offer_id,))

            # Обновляем статус транзакции
            await db.execute("""
                UPDATE marketplace_transactions 
                SET status = 'completed_with_relocation'
                WHERE id = ?
            """, (transaction_id,))

            await db.commit()

            # Отправляем подтверждение
            await message.answer(
                f"🎉 Поздравляем с новосельем!\n\n"
                f"📍 Ваш новый адрес:\n"
                f"🏙️ Город: {city}\n"
                f"🏘️ Улица: {street}\n"
                f"🏠 Дом: {house}\n\n"
                f"Теперь это ваше официальное место жительства!",
                reply_markup=await main_menu_kb(user_id)
            )

    except Exception as e:
        await message.answer("❌ Произошла ошибка при обработке переезда.")
        print(f"Ошибка переезда: {e}")


@dp.message(F.text == "❌ Нет, остаться на текущем месте")
async def reject_relocation(message: types.Message, state: FSMContext):
    """Обработчик отказа от переезда"""
    user_id = message.from_user.id

    try:
        async with aiosqlite.connect("database.db") as db:
            # Получаем предложение переезда
            cursor = await db.execute("""
                SELECT id, transaction_id FROM relocation_offers 
                WHERE buyer_id = ? AND status = 'pending'
                ORDER BY created_at DESC 
                LIMIT 1
            """, (user_id,))

            offer = await cursor.fetchone()

            if offer:
                offer_id, transaction_id = offer

                # Обновляем статус предложения
                await db.execute("UPDATE relocation_offers SET status = 'rejected' WHERE id = ?", (offer_id,))

                # Обновляем статус транзакции
                await db.execute("""
                    UPDATE marketplace_transactions 
                    SET status = 'completed'
                    WHERE id = ?
                """, (transaction_id,))

                await db.commit()

            await message.answer(
                "✅ Вы решили остаться на текущем месте жительства.\n"
                "Сделка успешно завершена!",
                reply_markup=await main_menu_kb(user_id)
            )

    except Exception as e:
        await message.answer("❌ Произошла ошибка.")
        print(f"Ошибка отказа от переезда: {e}")


@dp.message(F.text == "🔄 Изменить адрес вручную")
async def change_address_manually(message: types.Message, state: FSMContext):
    """Обработчик ручного изменения адреса"""
    user_id = message.from_user.id

    # Сохраняем информацию о том, что это переезд после покупки
    await state.update_data(after_purchase=True)

    try:
        async with aiosqlite.connect("database.db") as db:
            # Получаем информацию о сделке
            cursor = await db.execute("""
                SELECT ro.id, ro.transaction_id 
                FROM relocation_offers ro
                WHERE ro.buyer_id = ? AND ro.status = 'pending'
                ORDER BY ro.created_at DESC 
                LIMIT 1
            """, (user_id,))

            offer = await cursor.fetchone()

            if offer:
                offer_id, transaction_id = offer
                await state.update_data(
                    relocation_offer_id=offer_id,
                    purchase_transaction_id=transaction_id
                )
    except Exception as e:
        print(f"Ошибка получения данных переезда: {e}")

    # Запускаем процесс изменения адреса
    await change_address_start(message, state)


# 9. Дополнительная функция для запроса адреса у продавца
async def ask_seller_for_address(message: types.Message, state: FSMContext, transaction_id: int, buyer_id: int,
                                 seller_id: int, item_id: int, title: str):
    """Запрашивает у продавца адрес недвижимости"""

    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📍 Указать адрес недвижимости")],
            [KeyboardButton(text="🚫 Без адреса (виртуальная недвижимость)")]
        ],
        resize_keyboard=True
    )

    await bot.send_message(
        seller_id,
        f"🏠 Покупатель приобрел вашу недвижимость: {title}\n\n"
        f"Пожалуйста, укажите адрес недвижимости для переезда покупателя:",
        reply_markup=keyboard
    )

    # Сохраняем данные для обработки ответа продавца
    await state.update_data(
        awaiting_seller_address=True,
        address_transaction_id=transaction_id,
        address_buyer_id=buyer_id,
        address_seller_id=seller_id,
        address_item_id=item_id,
        address_title=title
    )


# 10. Обработчик ответа продавца с адресом
@dp.message(F.text == "📍 Указать адрес недвижимости")
async def seller_provide_address(message: types.Message, state: FSMContext):
    """Обработчик предоставления адреса продавцом"""
    data = await state.get_data()

    if not data.get('awaiting_seller_address'):
        return

    await message.answer(
        "Пожалуйста, введите адрес недвижимости в формате:\n"
        "Город, Улица, Дом\n\n"
        "Пример: Москва, Тверская, 15",
        reply_markup=ReplyKeyboardRemove()
    )

    await state.set_state("waiting_for_property_address")


# 11. Обработчик ввода адреса продавцом
@dp.message(StateFilter("waiting_for_property_address"))
async def process_property_address(message: types.Message, state: FSMContext):
    """Обрабатывает введенный продавцом адрес"""
    address_text = message.text.strip()
    data = await state.get_data()

    # Парсим адрес
    address_parts = address_text.split(',')
    if len(address_parts) < 3:
        await message.answer("❌ Неверный формат адреса. Пожалуйста, используйте формат: Город, Улица, Дом")
        return

    city = address_parts[0].strip()
    street = address_parts[1].strip()
    house = address_parts[2].strip()

    # Сохраняем адрес
    address_info = {
        'city': city,
        'street': street,
        'house': house
    }

    # Получаем данные из состояния
    transaction_id = data.get('address_transaction_id')
    buyer_id = data.get('address_buyer_id')
    seller_id = data.get('address_seller_id')
    item_id = data.get('address_item_id')
    title = data.get('address_title')

    # Отправляем предложение переезда покупателю
    await offer_relocation_to_buyer(buyer_id, seller_id, item_id, address_info, title, transaction_id)

    await message.answer(
        f"✅ Адрес успешно указан и отправлен покупателю!\n\n"
        f"🏙️ Город: {city}\n"
        f"🏘️ Улица: {street}\n"
        f"🏠 Дом: {house}",
        reply_markup=await marketplace_kb()
    )

    await state.clear()


# 12. Обработчик для виртуальной недвижимости
@dp.message(F.text == "🚫 Без адреса (виртуальная недвижимость)")
async def no_address_provided(message: types.Message, state: FSMContext):
    """Обработчик для виртуальной недвижимости без адреса"""
    data = await state.get_data()

    transaction_id = data.get('address_transaction_id')
    buyer_id = data.get('address_buyer_id')

    # Завершаем сделку без предложения переезда
    try:
        await bot.send_message(
            buyer_id,
            "🎉 Сделка завершена! Средства переведены продавцу.\n\n"
            "ℹ️ Это виртуальная недвижимость без физического адреса.",
            reply_markup=await marketplace_kb()
        )
    except Exception as e:
        print(f"Ошибка уведомления покупателя: {e}")

    # Обновляем статус транзакции
    async with aiosqlite.connect("database.db") as db:
        await db.execute("""
            UPDATE marketplace_transactions 
            SET status = 'completed' 
            WHERE id = ?
        """, (transaction_id,))
        await db.commit()

    await message.answer(
        "✅ Сделка завершена. Покупатель уведомлен.",
        reply_markup=await marketplace_kb()
    )

    await state.clear()


# Обработчик добавления товара
@dp.message(F.text == "➕ Добавить товар")
async def add_item_start(message: types.Message, state: FSMContext):
    """Начинает процесс добавления товара из меню"""
    await message.answer(
        "Выберите категорию товара:",
        reply_markup=categories_kb()
    )
    # Используем другое состояние для добавления из меню
    await state.set_state(Marketplace.choose_category_for_add)


@dp.message(F.text == "📊 Статистика")
async def show_marketplace_stats(message: types.Message):
    """Показывает статистику маркетплейса"""

    async with aiosqlite.connect("database.db") as db:
        # Общая статистика
        cursor = await db.execute("""
            SELECT 
                COUNT(*) as total_items,
                SUM(quantity) as total_quantity,
                SUM(price * quantity) as total_value,
                COUNT(DISTINCT seller_id) as total_sellers,
                COUNT(DISTINCT category) as total_categories
            FROM marketplace_items 
            WHERE status = 'active' AND quantity > 0
        """)
        stats = await cursor.fetchone()

        # Новые товары за последние 7 дней
        cursor = await db.execute("""
            SELECT COUNT(*) as new_items
            FROM marketplace_items 
            WHERE status = 'active' AND quantity > 0
            AND date(created_date) >= date('now', '-7 days')
        """)
        new_items = (await cursor.fetchone())[0]

        # Самые популярные категории
        cursor = await db.execute("""
            SELECT category, COUNT(*) as count
            FROM marketplace_items 
            WHERE status = 'active' AND quantity > 0
            GROUP BY category 
            ORDER BY count DESC 
            LIMIT 5
        """)
        top_categories = await cursor.fetchall()

    if not stats or stats[0] == 0:
        await message.answer("📊 На маркетплейсе пока нет товаров.")
        return

    total_items, total_quantity, total_value, total_sellers, total_categories = stats

    response = "📊 <b>Статистика маркетплейса</b>\n\n"
    response += f"📦 Всего товаров: <b>{total_items}</b>\n"
    response += f"🧮 Общее количество: <b>{total_quantity} шт.</b>\n"
    response += f"💰 Общая стоимость: <b>{total_value} шуек</b>\n"
    response += f"👥 Продавцов: <b>{total_sellers}</b>\n"
    response += f"📂 Категорий: <b>{total_categories}</b>\n"
    response += f"🆕 Новых за неделю: <b>{new_items}</b>\n\n"

    if top_categories:
        response += "<b>🏆 Топ категорий:</b>\n"
        for idx, (category, count) in enumerate(top_categories, 1):
            response += f"{idx}. {category}: {count} товаров\n"

    response += "\n📈 Используйте /stats для подробной статистики."

    await message.answer(response, parse_mode="HTML")


# Обработчик выбора другой категории
@dp.message(Marketplace.add_first_item, F.text == "🔄 Выбрать другую категорию")
async def choose_another_category(message: types.Message, state: FSMContext):
    """Возврат к выбору категории"""
    await message.answer(
        "Выберите категорию товаров:",
        reply_markup=categories_kb()
    )
    await state.set_state(Marketplace.browse_category)


# Обработчик выбора другой категории
@dp.message(Marketplace.add_first_item, F.text == "🔄 Выбрать другую категорию")
async def choose_another_category_from_empty(message: types.Message, state: FSMContext):
    """Возврат к выбору категории из пустого списка"""
    await message.answer(
        "Выберите категорию товаров:",
        reply_markup=categories_kb()
    )
    await state.set_state(Marketplace.browse_category)


@dp.message(Marketplace.view_new_items, F.text == "🔄 Изменить период")
async def change_new_items_period(message: types.Message, state: FSMContext):
    """Изменение периода просмотра новых товаров"""
    await message.answer("Выберите период:", reply_markup=new_items_period_kb())
    await state.set_state(Marketplace.choose_new_items_period)


@dp.message(Marketplace.view_new_items, F.text == "📋 По категориям")
async def show_new_items_by_category(message: types.Message, state: FSMContext):
    """Показывает новые товары сгруппированные по категориям"""
    data = await state.get_data()
    days = data.get('new_items_period', 7)
    period_text = data.get('period_text', 'За неделю')

    await show_new_items_list(message, state, days, period_text)


@dp.message(Marketplace.view_new_items, F.text == "🎯 Самые дешёвые")
async def show_cheapest_new_items(message: types.Message, state: FSMContext):
    """Показывает самые дешёвые новые товары"""
    data = await state.get_data()
    days = data.get('new_items_period', 7)
    period_text = data.get('period_text', 'За неделю')

    await show_cheapest_items(message, state, days, period_text)


async def show_cheapest_items(message: types.Message, state: FSMContext, days: int, period_text: str):
    """Показывает самые дешёвые новые товары"""

    async with aiosqlite.connect("database.db") as db:
        if days == 365:
            cursor = await db.execute("""
                SELECT mi.id, mi.title, mi.price, mi.quantity, mi.category, 
                       mi.created_date, u.name as seller_name
                FROM marketplace_items mi
                JOIN users u ON mi.seller_id = u.user_id
                WHERE mi.status = 'active' AND mi.quantity > 0
                ORDER BY mi.price ASC, mi.created_date DESC
                LIMIT 20
            """)
        else:
            cursor = await db.execute("""
                SELECT mi.id, mi.title, mi.price, mi.quantity, mi.category, 
                       mi.created_date, u.name as seller_name
                FROM marketplace_items mi
                JOIN users u ON mi.seller_id = u.user_id
                WHERE mi.status = 'active' AND mi.quantity > 0
                AND date(mi.created_date) >= date('now', ?)
                ORDER BY mi.price ASC, mi.created_date DESC
                LIMIT 20
            """, (f'-{days} days',))

        items = await cursor.fetchall()

    if not items:
        await message.answer(f"😔 За выбранный период не найдено товаров.")
        return

    response = f"🎯 <b>Самые дешёвые новые товары ({period_text.lower()})</b>\n\n"

    for idx, (item_id, title, price, quantity, category, created_date, seller_name) in enumerate(items, 1):
        time_ago = get_time_ago(created_date)
        response += f"{idx}. <b>{title}</b>\n"
        response += f"   💰 <b>{price} шуек</b> | 📦 {quantity} шт.\n"
        response += f"   📂 {category} | 👤 {seller_name}\n"
        response += f"   🕐 {time_ago} | 🆔 ID: {item_id}\n"
        response += "─" * 30 + "\n"

    response += "\n📌 Введите ID товара для покупки."

    await message.answer(response, parse_mode="HTML")


@dp.message(F.text == "⬅️ В меню маркетплейса")
async def back_to_marketplace_from_new_items(message: types.Message, state: FSMContext):
    """Возврат в меню маркетплейса"""
    await state.clear()
    await message.answer("Возврат в меню маркетплейса.", reply_markup=await marketplace_kb())


# Обработчик названия товара
@dp.message(Marketplace.add_title)
async def process_item_title(message: types.Message, state: FSMContext):
    if message.text.lower() == "отмена":
        await state.clear()
        await message.answer("Добавление товара отменено.", reply_markup=await marketplace_kb())
        return

    title = message.text.strip()
    if len(title) > 100:
        await message.answer("Название слишком длинное. Максимум 100 символов.")
        return
    if len(title) < 3:
        await message.answer("Название слишком короткое. Минимум 3 символа.")
        return

    await state.update_data(title=title)

    # Для недвижимости с адресом показываем текущее описание
    data = await state.get_data()
    category = data.get('category')
    address_description = data.get('address_description', '')

    if category == "🏠 Недвижимость" and address_description:
        await message.answer(
            f"Название: {title}\n\n"
            f"Текущее описание с адресом:\n{address_description}"
            "Вы можете дополнить описание (максимум 500 символов):\n"
            "Или напишите 'пропустить' чтобы оставить только адрес",
            reply_markup=ReplyKeyboardRemove()
        )
    else:
        await message.answer("Введите описание товара (максимум 500 символов):")

    await state.set_state(Marketplace.add_description)


# Обработчик описания товара
@dp.message(Marketplace.add_description)
async def process_item_description(message: types.Message, state: FSMContext):
    data = await state.get_data()
    category = data.get('category')
    address_description = data.get('address_description', '')

    if message.text.lower() == "пропустить":
        description = address_description if address_description else None
    else:
        description = message.text.strip()
        if len(description) > 500:
            await message.answer("Описание слишком длинное. Максимум 500 символов.")
            return

        # Для недвижимости с адресом добавляем адрес к описанию
        if category == "🏠 Недвижимость" and address_description:
            description = address_description + description

    await state.update_data(description=description)
    await message.answer("Введите цену товара (целое число):")
    await state.set_state(Marketplace.add_price)


# Обработчик цены товара
@dp.message(Marketplace.add_price)
async def process_item_price(message: types.Message, state: FSMContext):
    try:
        price = int(message.text.strip())
        if price <= 0:
            await message.answer("Цена должна быть положительным числом.")
            return
    except ValueError:
        await message.answer("Пожалуйста, введите корректную цену (целое число).")
        return

    await state.update_data(price=price)
    await message.answer("Введите количество товара (целое число, минимум 1):")
    await state.set_state(Marketplace.add_quantity)


# 4. Обработчик ввода количества
@dp.message(Marketplace.add_quantity)
async def process_item_quantity(message: types.Message, state: FSMContext):
    try:
        quantity = int(message.text.strip())
        if quantity <= 0:
            await message.answer("Количество должно быть положительным числом.")
            return
        if quantity > 1000:  # Максимальное количество
            await message.answer("Максимальное количество - 1000 единиц.")
            return
    except ValueError:
        await message.answer("Пожалуйста, введите корректное количество (целое число).")
        return

    await state.update_data(quantity=quantity)

    data = await state.get_data()
    category = data.get('category')

    if category == "🏠 Недвижимость":
        await message.answer(
            "Отправьте фото недвижимости (опционально).\n"
            "Если не хотите добавлять фото, отправьте 'пропустить'",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="пропустить")]],
                resize_keyboard=True
            )
        )
    else:
        await message.answer(
            "Отправьте фото товара (опционально).\n"
            "Если не хотите добавлять фото, отправьте 'пропустить'",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="пропустить")]],
                resize_keyboard=True
            )
        )

    await state.set_state(Marketplace.add_image)


# Обработчик изображения товара
@dp.message(Marketplace.add_image)
async def process_item_image(message: types.Message, state: FSMContext):
    image_id = None

    if message.text and message.text.lower() == "пропустить":
        pass
    elif message.photo:
        image_id = message.photo[-1].file_id
    else:
        await message.answer("Пожалуйста, отправьте фото или нажмите 'пропустить'.")
        return

    data = await state.get_data()
    user_id = message.from_user.id

    async with aiosqlite.connect("database.db") as db:
        try:
            # Получаем текущее описание
            description = data.get('description', '')

            # Если есть данные адреса - добавляем их к описанию
            if 'property_city' in data:
                address_parts = []
                if data.get('property_city'):
                    address_parts.append(f"город {data['property_city']}")
                if data.get('property_street'):
                    address_parts.append(f"улица {data['property_street']}")
                if data.get('property_house'):
                    address_parts.append(f"дом {data['property_house']}")

                if address_parts:
                    address_text = "📍 Адрес: " + ", ".join(address_parts)
                    if description:
                        description = address_text + "\n\n" + description
                    else:
                        description = address_text

            # Вставляем товар в БД
            await db.execute("""
                INSERT INTO marketplace_items 
                (seller_id, title, description, price, quantity, category, image_id)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (user_id, data['title'], description, data['price'],
                  data['quantity'], data['category'], image_id))
            await db.commit()

            # Формируем ответ
            response_text = f"✅ Товар успешно добавлен!\n\n📦 Название: {data['title']}\n💰 Цена: {data['price']} шуек\n📂 Категория: {data['category']}\n📊 Количество: {data['quantity']} шт."

            # Показываем адрес если он был указан
            if 'property_city' in data:
                address_display = []
                if data.get('property_city'):
                    address_display.append(data['property_city'])
                if data.get('property_street'):
                    address_display.append(data['property_street'])
                if data.get('property_house'):
                    address_display.append(data['property_house'])

                if address_display:
                    response_text += f"\n📍 Адрес: {', '.join(address_display)}"

            await message.answer(response_text, reply_markup=await marketplace_kb())

        except Exception as e:
            await message.answer("❌ Произошла ошибка при добавлении товара.")
            print(f"Ошибка добавления товара: {e}")

    await state.clear()


# Обработчик категории товара
@dp.message(Marketplace.add_category)
async def process_item_category(message: types.Message, state: FSMContext):
    category = message.text

    if category == "❌ Отмена":
        await state.clear()
        await message.answer("Добавление товара отменено.", reply_markup=await marketplace_kb())
        return

    if category not in MARKETPLACE_CATEGORIES:
        await message.answer("Пожалуйста, выберите категорию из списка.")
        return

    await state.update_data(category=category)
    await message.answer(
        "Отправьте фото товара (опционально).\n"
        "Если не хотите добавлять фото, отправьте 'пропустить'",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="пропустить")]],
            resize_keyboard=True
        )
    )
    await state.set_state(Marketplace.add_image)


# Обработчик изменения названия товара
@dp.message(F.text == "✏️ Изменить название")
async def edit_item_title_start(message: types.Message, state: FSMContext):
    data = await state.get_data()
    if 'item_id' not in data:
        await message.answer("❌ Ошибка: товар не найден.", reply_markup=await marketplace_kb())
        await state.clear()
        return

    await message.answer(
        "Введите новое название товара (максимум 100 символов):\n"
        "Или напишите 'отмена' для отмены",
        reply_markup=ReplyKeyboardRemove()
    )
    await state.set_state(Marketplace.edit_title)


@dp.message(Marketplace.edit_title)
async def process_edit_title(message: types.Message, state: FSMContext):
    if message.text.lower() == "отмена":
        await state.clear()
        await message.answer("Изменение отменено.", reply_markup=await marketplace_kb())
        return

    title = message.text.strip()
    if len(title) > 100:
        await message.answer("Название слишком длинное. Максимум 100 символов.")
        return
    if len(title) < 3:
        await message.answer("Название слишком короткое. Минимум 3 символа.")
        return

    data = await state.get_data()
    item_id = data['item_id']
    user_id = message.from_user.id

    try:
        async with aiosqlite.connect("database.db") as db:
            await db.execute(
                "UPDATE marketplace_items SET title = ? WHERE id = ? AND seller_id = ?",
                (title, item_id, user_id)
            )
            await db.commit()

        await message.answer(
            f"✅ Название товара успешно изменено на: {title}",
            reply_markup=await marketplace_kb()
        )

    except Exception as e:
        await message.answer(
            "❌ Произошла ошибка при изменении названия.",
            reply_markup=await marketplace_kb()
        )
        print(f"Ошибка изменения названия: {e}")

    await state.clear()


# Обработчик изменения описания товара
@dp.message(F.text == "✏️ Изменить описание")
async def edit_item_description_start(message: types.Message, state: FSMContext):
    data = await state.get_data()
    if 'item_id' not in data:
        await message.answer("❌ Ошибка: товар не найден.", reply_markup=await marketplace_kb())
        await state.clear()
        return

    await message.answer(
        "Введите новое описание товара (максимум 500 символов):\n"
        "Или напишите 'отмена' для отмены",
        reply_markup=ReplyKeyboardRemove()
    )
    await state.set_state(Marketplace.edit_description)


@dp.message(Marketplace.edit_description)
async def process_edit_description(message: types.Message, state: FSMContext):
    if message.text.lower() == "отмена":
        await state.clear()
        await message.answer("Изменение отменено.", reply_markup=await marketplace_kb())
        return

    description = message.text.strip()
    if len(description) > 500:
        await message.answer("Описание слишком длинное. Максимум 500 символов.")
        return

    data = await state.get_data()
    item_id = data['item_id']
    user_id = message.from_user.id

    try:
        async with aiosqlite.connect("database.db") as db:
            await db.execute(
                "UPDATE marketplace_items SET description = ? WHERE id = ? AND seller_id = ?",
                (description, item_id, user_id)
            )
            await db.commit()

        await message.answer(
            "✅ Описание товара успешно изменено!",
            reply_markup=await marketplace_kb()
        )

    except Exception as e:
        await message.answer(
            "❌ Произошла ошибка при изменении описания.",
            reply_markup=await marketplace_kb()
        )
        print(f"Ошибка изменения описания: {e}")

    await state.clear()


# Обработчик изменения цены товара
@dp.message(F.text == "💰 Изменить цену")
async def edit_item_price_start(message: types.Message, state: FSMContext):
    data = await state.get_data()
    if 'item_id' not in data:
        await message.answer("❌ Ошибка: товар не найден.", reply_markup=await marketplace_kb())
        await state.clear()
        return

    await message.answer(
        "Введите новую цену товара (целое число):\n"
        "Или напишите 'отмена' для отмены",
        reply_markup=ReplyKeyboardRemove()
    )
    await state.set_state(Marketplace.edit_price)


@dp.message(Marketplace.edit_price)
async def process_edit_price(message: types.Message, state: FSMContext):
    if message.text.lower() == "отмена":
        await state.clear()
        await message.answer("Изменение отменено.", reply_markup=await marketplace_kb())
        return

    try:
        price = int(message.text.strip())
        if price <= 0:
            await message.answer("Цена должна быть положительным числом.")
            return
        if price > 1000000:
            await message.answer("Цена слишком высокая. Максимум 1,000,000 шуек.")
            return
    except ValueError:
        await message.answer("Пожалуйста, введите корректную цену (целое число).")
        return

    data = await state.get_data()
    item_id = data['item_id']
    user_id = message.from_user.id

    try:
        async with aiosqlite.connect("database.db") as db:
            await db.execute(
                "UPDATE marketplace_items SET price = ? WHERE id = ? AND seller_id = ?",
                (price, item_id, user_id)
            )
            await db.commit()

        await message.answer(
            f"✅ Цена товара успешно изменена на: {price} шуек",
            reply_markup=await marketplace_kb()
        )

    except Exception as e:
        await message.answer(
            "❌ Произошла ошибка при изменении цены.",
            reply_markup=await marketplace_kb()
        )
        print(f"Ошибка изменения цены: {e}")

    await state.clear()


# Обработчик изменения фото товара
@dp.message(F.text == "🖼 Изменить фото")
async def edit_item_image_start(message: types.Message, state: FSMContext):
    data = await state.get_data()
    if 'item_id' not in data:
        await message.answer("❌ Ошибка: товар не найден.", reply_markup=await marketplace_kb())
        await state.clear()
        return

    await message.answer(
        "Отправьте новое фото товара:\n"
        "Или напишите 'пропустить' чтобы удалить текущее фото",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="пропустить")]],
            resize_keyboard=True
        )
    )
    await state.set_state(Marketplace.edit_image)


# Обработчик нового фото товара
@dp.message(Marketplace.edit_image)
async def process_edit_image(message: types.Message, state: FSMContext):
    data = await state.get_data()
    item_id = data['item_id']
    user_id = message.from_user.id

    if message.text and message.text.lower() == "пропустить":
        # Удаляем текущее фото
        try:
            async with aiosqlite.connect("database.db") as db:
                await db.execute(
                    "UPDATE marketplace_items SET image_id = NULL WHERE id = ? AND seller_id = ?",
                    (item_id, user_id)
                )
                await db.commit()

            await message.answer(
                "✅ Фото товара удалено!",
                reply_markup=await marketplace_kb()
            )
        except Exception as e:
            await message.answer(
                "❌ Произошла ошибка при удалении фото.",
                reply_markup=await marketplace_kb()
            )
            print(f"Ошибка удаления фото: {e}")

        await state.clear()
        return

    if not message.photo:
        await message.answer("Пожалуйста, отправьте фото или напишите 'пропустить'.")
        return

    # Получаем самое большое фото
    photo = message.photo[-1]
    image_id = photo.file_id

    try:
        async with aiosqlite.connect("database.db") as db:
            await db.execute(
                "UPDATE marketplace_items SET image_id = ? WHERE id = ? AND seller_id = ?",
                (image_id, item_id, user_id)
            )
            await db.commit()

        await message.answer(
            "✅ Фото товара успешно обновлено!",
            reply_markup=await marketplace_kb()
        )

    except Exception as e:
        await message.answer(
            "❌ Произошла ошибка при обновлении фото.",
            reply_markup=await marketplace_kb()
        )
        print(f"Ошибка обновления фото: {e}")

    await state.clear()


# Обработчик удаления товара
@dp.message(F.text == "❌ Удалить товар")
async def delete_item_start(message: types.Message, state: FSMContext):
    data = await state.get_data()
    item_id = data.get('item_id')

    if not item_id:
        await message.answer("❌ Ошибка: товар не найден.", reply_markup=await marketplace_kb())
        await state.clear()
        return

    # Клавиатура подтверждения удаления
    confirm_kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="✅ Да, удалить"), KeyboardButton(text="❌ Нет, отмена")]
        ],
        resize_keyboard=True
    )

    await message.answer(
        "❌ Вы уверены, что хотите удалить этот товар?\n"
        "Это действие нельзя отменить!",
        reply_markup=confirm_kb
    )

    await state.set_state(Marketplace.confirm_delete)


# Обработчик подтверждения удаления
@dp.message(Marketplace.confirm_delete, F.text == "✅ Да, удалить")
async def confirm_delete_item(message: types.Message, state: FSMContext):
    data = await state.get_data()
    item_id = data.get('item_id')
    user_id = message.from_user.id

    try:
        async with aiosqlite.connect("database.db") as db:
            await db.execute(
                "DELETE FROM marketplace_items WHERE id = ? AND seller_id = ?",
                (item_id, user_id)
            )
            await db.commit()

        await message.answer(
            "✅ Товар успешно удален!",
            reply_markup=await marketplace_kb()
        )

    except Exception as e:
        await message.answer(
            "❌ Произошла ошибка при удалении товара.",
            reply_markup=await marketplace_kb()
        )
        print(f"Ошибка удаления товара: {e}")

    await state.clear()


# Обработчик отмены удаления
@dp.message(Marketplace.confirm_delete, F.text == "❌ Нет, отмена")
async def cancel_delete_item(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("Удаление отменено.", reply_markup=await marketplace_kb())


# Обработчик отмены для всех состояний редактирования
@dp.message(
    StateFilter(Marketplace.edit_title, Marketplace.edit_description, Marketplace.edit_price, Marketplace.edit_image,
                Marketplace.confirm_delete),
    F.text.lower() == "отмена")
async def cancel_edit_operation(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("Операция отменена.", reply_markup=await marketplace_kb())


# Обработчик изменения названия товара
@dp.message(F.text == "✏️ Изменить название")
async def edit_item_title_start(message: types.Message, state: FSMContext):
    data = await state.get_data()
    if 'item_id' not in data:
        await message.answer("❌ Ошибка: товар не найден.", reply_markup=await marketplace_kb())
        await state.clear()
        return

    await message.answer(
        "Введите новое название товара (максимум 100 символов):\n"
        "Или напишите 'отмена' для отмены",
        reply_markup=ReplyKeyboardRemove()
    )
    await state.set_state(Marketplace.edit_title)


@dp.message(Marketplace.edit_title)
async def process_edit_title(message: types.Message, state: FSMContext):
    if message.text.lower() == "отмена":
        await state.clear()
        await message.answer("Изменение отменено.", reply_markup=await marketplace_kb())
        return

    title = message.text.strip()
    if len(title) > 100:
        await message.answer("Название слишком длинное. Максимум 100 символов.")
        return
    if len(title) < 3:
        await message.answer("Название слишком короткое. Минимум 3 символа.")
        return

    data = await state.get_data()
    item_id = data['item_id']
    user_id = message.from_user.id

    try:
        async with aiosqlite.connect("database.db") as db:
            await db.execute(
                "UPDATE marketplace_items SET title = ? WHERE id = ? AND seller_id = ?",
                (title, item_id, user_id)
            )
            await db.commit()

        await message.answer(
            f"✅ Название товара успешно изменено на: {title}",
            reply_markup=await marketplace_kb()
        )

    except Exception as e:
        await message.answer(
            "❌ Произошла ошибка при изменении названия.",
            reply_markup=await marketplace_kb()
        )
        print(f"Ошибка изменения названия: {e}")

    await state.clear()


# Обработчик изменения описания товара
@dp.message(F.text == "✏️ Изменить описание")
async def edit_item_description_start(message: types.Message, state: FSMContext):
    data = await state.get_data()
    if 'item_id' not in data:
        await message.answer("❌ Ошибка: товар не найден.", reply_markup=await marketplace_kb())
        await state.clear()
        return

    await message.answer(
        "Введите новое описание товара (максимум 500 символов):\n"
        "Или напишите 'отмена' для отмены",
        reply_markup=ReplyKeyboardRemove()
    )
    await state.set_state(Marketplace.edit_description)


@dp.message(Marketplace.edit_description)
async def process_edit_description(message: types.Message, state: FSMContext):
    if message.text.lower() == "отмена":
        await state.clear()
        await message.answer("Изменение отменено.", reply_markup=await marketplace_kb())
        return

    description = message.text.strip()
    if len(description) > 500:
        await message.answer("Описание слишком длинное. Максимум 500 символов.")
        return

    data = await state.get_data()
    item_id = data['item_id']
    user_id = message.from_user.id

    try:
        async with aiosqlite.connect("database.db") as db:
            await db.execute(
                "UPDATE marketplace_items SET description = ? WHERE id = ? AND seller_id = ?",
                (description, item_id, user_id)
            )
            await db.commit()

        await message.answer(
            "✅ Описание товара успешно изменено!",
            reply_markup=await marketplace_kb()
        )

    except Exception as e:
        await message.answer(
            "❌ Произошла ошибка при изменении описания.",
            reply_markup=await marketplace_kb()
        )
        print(f"Ошибка изменения описания: {e}")

    await state.clear()


# Обработчик изменения цены товара
@dp.message(F.text == "💰 Изменить цену")
async def edit_item_price_start(message: types.Message, state: FSMContext):
    data = await state.get_data()
    if 'item_id' not in data:
        await message.answer("❌ Ошибка: товар не найден.", reply_markup=await marketplace_kb())
        await state.clear()
        return

    await message.answer(
        "Введите новую цену товара (целое число):\n"
        "Или напишите 'отмена' для отмены",
        reply_markup=ReplyKeyboardRemove()
    )
    await state.set_state(Marketplace.edit_price)


@dp.message(Marketplace.edit_price)
async def process_edit_price(message: types.Message, state: FSMContext):
    if message.text.lower() == "отмена":
        await state.clear()
        await message.answer("Изменение отменено.", reply_markup=await marketplace_kb())
        return

    try:
        price = int(message.text.strip())
        if price <= 0:
            await message.answer("Цена должна быть положительным числом.")
            return
        if price > 1000000:
            await message.answer("Цена слишком высокая. Максимум 1,000,000 шуек.")
            return
    except ValueError:
        await message.answer("Пожалуйста, введите корректную цену (целое число).")
        return

    data = await state.get_data()
    item_id = data['item_id']
    user_id = message.from_user.id

    try:
        async with aiosqlite.connect("database.db") as db:
            await db.execute(
                "UPDATE marketplace_items SET price = ? WHERE id = ? AND seller_id = ?",
                (price, item_id, user_id)
            )
            await db.commit()

        await message.answer(
            f"✅ Цена товара успешно изменена на: {price} шуек",
            reply_markup=await marketplace_kb()
        )

    except Exception as e:
        await message.answer(
            "❌ Произошла ошибка при изменении цены.",
            reply_markup=await marketplace_kb()
        )
        print(f"Ошибка изменения цены: {e}")

    await state.clear()


# Обработчик изменения фото товара
@dp.message(F.text == "🖼 Изменить фото")
async def edit_item_image_start(message: types.Message, state: FSMContext):
    data = await state.get_data()
    if 'item_id' not in data:
        await message.answer("❌ Ошибка: товар не найден.", reply_markup=await marketplace_kb())
        await state.clear()
        return

    await message.answer(
        "Отправьте новое фото товара:\n"
        "Или напишите 'пропустить' чтобы удалить текущее фото",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="пропустить")]],
            resize_keyboard=True
        )
    )
    await state.set_state(Marketplace.add_image)


# Обработчик удаления товара
@dp.message(F.text == "❌ Удалить товар")
async def delete_item_start(message: types.Message, state: FSMContext):
    data = await state.get_data()
    item_id = data.get('item_id')

    if not item_id:
        await message.answer("❌ Ошибка: товар не найден.", reply_markup=await marketplace_kb())
        await state.clear()
        return

    # Клавиатура подтверждения удаления
    confirm_kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="✅ Да, удалить"), KeyboardButton(text="❌ Нет, отмена")]
        ],
        resize_keyboard=True
    )

    await message.answer(
        "❌ Вы уверены, что хотите удалить этот товар?\n"
        "Это действие нельзя отменить!",
        reply_markup=confirm_kb
    )

    await state.set_state(Marketplace.confirm_delete)


# Обработчик подтверждения удаления
@dp.message(Marketplace.confirm_delete, F.text == "✅ Да, удалить")
async def confirm_delete_item(message: types.Message, state: FSMContext):
    data = await state.get_data()
    item_id = data.get('item_id')
    user_id = message.from_user.id

    try:
        async with aiosqlite.connect("database.db") as db:
            await db.execute(
                "DELETE FROM marketplace_items WHERE id = ? AND seller_id = ?",
                (item_id, user_id)
            )
            await db.commit()

        await message.answer(
            "✅ Товар успешно удален!",
            reply_markup=await marketplace_kb()
        )

    except Exception as e:
        await message.answer(
            "❌ Произошла ошибка при удалении товара.",
            reply_markup=await marketplace_kb()
        )
        print(f"Ошибка удаления товара: {e}")

    await state.clear()


# Обработчик отмены удаления
@dp.message(Marketplace.confirm_delete, F.text == "❌ Нет, отмена")
async def cancel_delete_item(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("Удаление отменено.", reply_markup=await marketplace_kb())


# Обработчик отмены для всех состояний редактирования
@dp.message(StateFilter(Marketplace.edit_title, Marketplace.edit_description, Marketplace.edit_price,
                        Marketplace.confirm_delete), F.text.lower() == "отмена")
async def cancel_edit_operation(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("Операция отменена.", reply_markup=await marketplace_kb())


# Обработчик кнопки "Назад" в управлении товаром
@dp.message(StateFilter(Marketplace.edit_title, Marketplace.edit_description, Marketplace.edit_price,
                        Marketplace.confirm_delete), F.text == "⬅️ Назад")
async def back_from_edit(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("Возврат в меню маркетплейса.", reply_markup=await marketplace_kb())


# Обработчик моих товаров
# 12. Обновляем отображение моих товаров
@dp.message(F.text == "📦 Мои товары")
async def show_my_items(message: types.Message, state: FSMContext):
    user_id = message.from_user.id

    async with aiosqlite.connect("database.db") as db:
        cursor = await db.execute("""
            SELECT id, title, price, quantity, status, created_date 
            FROM marketplace_items 
            WHERE seller_id = ?
            ORDER BY created_date DESC
        """, (user_id,))

        items = await cursor.fetchall()

    if not items:
        await message.answer("У вас пока нет товаров на продажу.")
        return

    response = "📦 <b>Ваши товары</b>\n\n"

    for item_id, title, price, quantity, status, created_date in items:
        status_emoji = "🟢" if status == 'active' and quantity > 0 else "🟡" if status == 'active' else "🔴"
        response += f"{status_emoji} <b>{title}</b>\n"
        response += f"💰 Цена: {price} шуек\n"
        response += f"📊 Количество: {quantity} шт.\n"
        response += f"📊 Статус: {status}\n"
        response += f"📅 {created_date[:10]}\n"
        response += f"🆔 ID: {item_id}\n"
        response += "─" * 30 + "\n"

    await message.answer(response, parse_mode="HTML")
    await state.set_state(Marketplace.my_items)


# Обработчик управления товарами
@dp.message(Marketplace.my_items)
async def manage_my_items(message: types.Message, state: FSMContext):
    if message.text.lower() == "назад":
        await state.clear()
        await message.answer("Возврат в меню маркетплейса.", reply_markup=await marketplace_kb())
        return

    try:
        item_id = int(message.text)
    except ValueError:
        await message.answer("Пожалуйста, введите числовой ID товара.")
        return

    user_id = message.from_user.id

    async with aiosqlite.connect("database.db") as db:
        cursor = await db.execute("""
            SELECT id, title, description, price, status, image_id 
            FROM marketplace_items 
            WHERE id = ? AND seller_id = ?
        """, (item_id, user_id))

        item = await cursor.fetchone()

    if not item:
        await message.answer("Товар не найден или у вас нет прав для его управления.")
        await state.clear()
        await message.answer("Возврат в меню маркетплейса.", reply_markup=await marketplace_kb())
        return

    item_id, title, description, price, status, image_id = item

    if status == 'sold':
        await message.answer(
            f"📦 Товар: {title}\n"
            f"💰 Цена: {price} шуек\n"
            f"📊 Статус: Продан\n\n"
            "Этот товар уже продан и не может быть изменен."
        )
        await state.clear()
        await message.answer("Возврат в меню маркетплейса.", reply_markup=await marketplace_kb())
        return

    if status == 'reserved':
        await message.answer(
            f"📦 Товар: {title}\n"
            f"💰 Цена: {price} шуек\n"
            f"📊 Статус: В процессе продажи\n\n"
            "Этот товар зарезервирован для покупки и не может быть изменен."
        )
        await state.clear()
        await message.answer("Возврат в меню маркетплейса.", reply_markup=await marketplace_kb())
        return

    # Клавиатура для управления товаром
    manage_kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="✏️ Изменить название"), KeyboardButton(text="✏️ Изменить описание")],
            [KeyboardButton(text="💰 Изменить цену"), KeyboardButton(text="🖼 Изменить фото")],
            [KeyboardButton(text="❌ Удалить товар")],
            [KeyboardButton(text="⬅️ Назад")]
        ],
        resize_keyboard=True
    )

    item_info = (
        f"📦 <b>Управление товаром:</b>\n"
        f"🆔 ID: {item_id}\n"
        f"📝 Название: {title}\n"
        f"📄 Описание: {description or 'Нет описания'}\n"
        f"💰 Цена: {price} шуек\n"
        f"📊 Статус: {status}\n\n"
        f"Выберите действие:"
    )

    if image_id:
        try:
            await message.answer_photo(image_id, caption=item_info, parse_mode="HTML", reply_markup=manage_kb)
        except:
            await message.answer(item_info + "\n🖼 Фото прилагается", parse_mode="HTML", reply_markup=manage_kb)
    else:
        await message.answer(item_info, parse_mode="HTML", reply_markup=manage_kb)

    await state.update_data(item_id=item_id, current_title=title,
                            current_description=description, current_price=price)


# Обработчик моих покупок
@dp.message(F.text == "💰 Мои покупки")
async def show_my_purchases(message: types.Message):
    user_id = message.from_user.id

    try:
        async with aiosqlite.connect("database.db") as db:
            cursor = await db.execute("""
                SELECT 
                    mt.id, 
                    mi.title, 
                    mi.price, 
                    u.name as seller_name,
                    u.account_id as seller_account_id,  -- Добавляем ГосID продавца
                    mt.status, 
                    mt.buyer_confirmed, 
                    mt.seller_confirmed, 
                    mt.created_at,
                    mt.quantity, 
                    mt.price as unit_price,
                    mi.category  -- Добавляем категорию товара
                FROM marketplace_transactions mt
                JOIN marketplace_items mi ON mt.item_id = mi.id
                JOIN users u ON mt.seller_id = u.user_id
                WHERE mt.buyer_id = ?
                ORDER BY mt.created_at DESC
                LIMIT 20
            """, (user_id,))

            purchases = await cursor.fetchall()

        if not purchases:
            await message.answer("🛍 У вас пока нет покупок.")
            return

        # Формируем детальный ответ
        response = "🛍 <b>История ваших покупок:</b>\n\n"

        total_spent = 0
        purchase_count = 0

        for purchase in purchases:
            (purchase_id, title, total_price, seller_name, seller_account_id,
             status, buyer_conf, seller_conf, date, quantity, unit_price, category) = purchase

            # Рассчитываем общую стоимость
            purchase_total = total_price if quantity <= 1 else unit_price * quantity
            total_spent += purchase_total
            purchase_count += 1

            # Эмодзи статуса
            if status == 'completed':
                status_emoji = "✅"
                status_text = "Завершена"
            elif status == 'completed_with_relocation':
                status_emoji = "🏠"
                status_text = "Завершена (с переездом)"
            elif status == 'completed_with_manual_relocation':
                status_emoji = "🏠"
                status_text = "Завершена (ручной переезд)"
            elif status == 'pending_completion':
                status_emoji = "⏳"
                status_text = "В процессе"
            elif status == 'pending_confirmation':
                status_emoji = "⏳"
                status_text = "Ожидает подтверждения"
            elif status == 'cancelled':
                status_emoji = "❌"
                status_text = "Отменена"
            elif status == 'awaiting_relocation_decision':
                status_emoji = "🤔"
                status_text = "Ожидает решения о переезде"
            else:
                status_emoji = "📝"
                status_text = status

            response += f"{status_emoji} <b>{title}</b>\n"

            if quantity > 1:
                response += f"📦 Количество: {quantity} шт.\n"
                response += f"💰 Цена за единицу: {unit_price} шуек\n"
                response += f"💵 Общая сумма: {purchase_total} шуек\n"
            else:
                response += f"💰 Сумма: {purchase_total} шуек\n"

            # Категория товара
            if category:
                response += f"📂 Категория: {category}\n"

            # Продавец с ГосID (моноширинный выделение)
            response += f"👤 Продавец: {seller_name}\n"
            response += f"💳 ГосID продавца: <code>{seller_account_id}</code>\n"

            response += f"📊 Статус: {status_text}\n"
            response += f"📅 Дата покупки: {date[:16] if date else 'Неизвестно'}\n"
            response += f"🆔 ID транзакции: <code>{purchase_id}</code>\n"
            response += "─" * 30 + "\n\n"

        # Добавляем статистику
        response += f"\n📊 <b>Статистика покупок:</b>\n"
        response += f"📦 Всего покупок: {purchase_count}\n"
        response += f"💸 Общая сумма: {total_spent} шуек\n"

        # Средняя стоимость покупки
        if purchase_count > 0:
            avg_purchase = total_spent // purchase_count
            response += f"📈 Средний чек: {avg_purchase} шуек\n"

        # Добавляем инструкцию для детального просмотра
        response += f"\n📌 Для деталей покупки используйте:\n"
        response += f"<code>/purchase_info ID_покупки</code>\n"
        response += f"Пример: <code>/purchase_info {purchases[0][0]}</code>"

        await message.answer(response, parse_mode="HTML")

    except Exception as e:
        await message.answer("❌ Произошла ошибка при загрузке покупок.")
        print(f"Ошибка загрузки покупок: {e}")


async def recreate_marketplace_transactions_table():
    async with aiosqlite.connect("database.db") as db:
        try:
            # Сначала делаем резервную копию данных (если они есть)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS marketplace_transactions_backup AS 
                SELECT * FROM marketplace_transactions
            """)

            # Удаляем старую таблицу
            await db.execute("DROP TABLE IF EXISTS marketplace_transactions")

            # Создаем новую таблицу с правильной структурой (включая quantity)
            await db.execute("""
                CREATE TABLE marketplace_transactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER NOT NULL,
                    buyer_id INTEGER NOT NULL,
                    seller_id INTEGER NOT NULL,
                    price REAL NOT NULL,
                    quantity INTEGER NOT NULL DEFAULT 1,
                    status TEXT NOT NULL DEFAULT 'pending_confirmation',
                    buyer_confirmed INTEGER NOT NULL DEFAULT 0,
                    seller_confirmed INTEGER NOT NULL DEFAULT 0,
                    confirmation_expires_at DATETIME,
                    confirmed_at DATETIME,
                    cancelled_at DATETIME,
                    cancellation_reason TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    completed_at DATETIME,
                    FOREIGN KEY (item_id) REFERENCES marketplace_items (id),
                    FOREIGN KEY (buyer_id) REFERENCES users (user_id),
                    FOREIGN KEY (seller_id) REFERENCES users (user_id)
                )
            """)

            # Создаем индексы
            await db.execute("CREATE INDEX idx_transactions_buyer ON marketplace_transactions (buyer_id)")
            await db.execute("CREATE INDEX idx_transactions_seller ON marketplace_transactions (seller_id)")
            await db.execute("CREATE INDEX idx_transactions_status ON marketplace_transactions (status)")

            await db.commit()
            print("Таблица marketplace_transactions пересоздана с правильной структурой")

        except Exception as e:
            await db.rollback()
            print(f"Ошибка при пересоздании таблицы: {e}")
            raise


@dp.message(Marketplace.choose_new_items_period)
async def process_new_items_period(message: types.Message, state: FSMContext):
    """Обработчик выбора периода для новых товаров"""

    if message.text == "⬅️ Назад в маркетплейс":
        await state.clear()
        await message.answer("Возврат в меню маркетплейса.", reply_markup=await marketplace_kb())
        return

    period_text = message.text
    period_days = {
        "🕐 Сегодня": 1,
        "📅 За неделю": 7,
        "🗓️ За месяц": 30,
        "🎯 Все новые": 365
    }

    if period_text not in period_days:
        await message.answer("Пожалуйста, выберите период из списка.")
        return

    days = period_days[period_text]
    await state.update_data(new_items_period=days, period_text=period_text)

    await show_new_items_list(message, state, days, period_text)


async def show_new_items_list(message: types.Message, state: FSMContext, days: int, period_text: str):
    """Показывает список новых товаров за указанный период"""

    async with aiosqlite.connect("database.db") as db:
        if days == 365:  # Все новые
            cursor = await db.execute("""
                SELECT mi.id, mi.title, mi.price, mi.quantity, mi.category, 
                       mi.created_date, u.name as seller_name
                FROM marketplace_items mi
                JOIN users u ON mi.seller_id = u.user_id
                WHERE mi.status = 'active' AND mi.quantity > 0
                ORDER BY mi.created_date DESC
                LIMIT 50
            """)
        else:
            cursor = await db.execute("""
                SELECT mi.id, mi.title, mi.price, mi.quantity, mi.category, 
                       mi.created_date, u.name as seller_name
                FROM marketplace_items mi
                JOIN users u ON mi.seller_id = u.user_id
                WHERE mi.status = 'active' AND mi.quantity > 0
                AND date(mi.created_date) >= date('now', ?)
                ORDER BY mi.created_date DESC
                LIMIT 50
            """, (f'-{days} days',))

        items = await cursor.fetchall()

    if not items:
        await message.answer(
            f"😔 За выбранный период ({period_text.lower()}) новых товаров не найдено.",
            reply_markup=await marketplace_kb()
        )
        await state.clear()
        return

    # Статистика
    total_items = len(items)
    total_value = sum(item[2] * item[3] for item in items)  # цена * количество

    response = f"🆕 <b>Новые товары ({period_text.lower()})</b>\n\n"
    response += f"📊 Найдено товаров: {total_items}\n"
    response += f"💰 Общая стоимость: {total_value} шуек\n\n"

    # Группируем по категориям
    categories = {}
    for item_id, title, price, quantity, category, created_date, seller_name in items:
        if category not in categories:
            categories[category] = []
        categories[category].append((item_id, title, price, quantity, created_date, seller_name))

    # Выводим по категориям
    for category, cat_items in categories.items():
        response += f"<b>{category}</b> ({len(cat_items)}):\n"

        for idx, (item_id, title, price, quantity, created_date, seller_name) in enumerate(cat_items[:5], 1):
            time_ago = get_time_ago(created_date)
            response += f"{idx}. <b>{title}</b>\n"
            response += f"   💰 {price} шуек | 📦 {quantity} шт.\n"
            response += f"   👤 {seller_name} | 🕐 {time_ago}\n"
            response += f"   🆔 ID: {item_id}\n\n"

        if len(cat_items) > 5:
            response += f"   ... и ещё {len(cat_items) - 5} товаров\n\n"

    response += "\n📌 Введите ID товара для просмотра подробностей."
    response += "\nИли используйте кнопки ниже:"

    # Клавиатура для навигации
    nav_kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🔄 Изменить период")],
            [KeyboardButton(text="📋 По категориям"), KeyboardButton(text="🎯 Самые дешёвые")],
            [KeyboardButton(text="⬅️ В меню маркетплейса")]
        ],
        resize_keyboard=True
    )

    await message.answer(response, parse_mode="HTML", reply_markup=nav_kb)
    await state.set_state(Marketplace.view_new_items)


def get_time_ago(created_date):
    """Возвращает строку вида '2 часа назад', '3 дня назад' и т.д."""
    from datetime import datetime

    if isinstance(created_date, str):
        try:
            created = datetime.strptime(created_date, '%Y-%m-%d %H:%M:%S')
        except:
            created = datetime.strptime(created_date, '%Y-%m-%d')
    else:
        created = created_date

    now = datetime.now()
    diff = now - created

    if diff.days > 30:
        months = diff.days // 30
        return f"{months} мес. назад"
    elif diff.days > 0:
        return f"{diff.days} дн. назад"
    elif diff.seconds > 3600:
        hours = diff.seconds // 3600
        return f"{hours} час. назад"
    elif diff.seconds > 60:
        minutes = diff.seconds // 60
        return f"{minutes} мин. назад"
    else:
        return "только что"


"""Недвижимость Недвижка дома"""


@dp.message(F.text == "🏘️ Моя недвижимость")
async def show_my_properties(message: types.Message, state: FSMContext):
    """Показывает всю недвижимость пользователя"""
    user_id = message.from_user.id

    async with aiosqlite.connect("database.db") as db:
        # Получаем всю недвижимость пользователя
        cursor = await db.execute("""
            SELECT 
                mi.id, mi.title, mi.description, mi.price, mi.quantity,
                mi.created_date, mi.status, mi.image_id,
                u.city as property_city, u.street as property_street, 
                u.house_number as property_house
            FROM marketplace_items mi
            LEFT JOIN users u ON mi.seller_id = u.user_id
            WHERE mi.seller_id = ? 
            AND mi.category = '🏠 Недвижимость'
            AND mi.status != 'deleted'
            ORDER BY 
                CASE mi.status 
                    WHEN 'active' THEN 1
                    WHEN 'sold' THEN 2
                    WHEN 'reserved' THEN 3
                    ELSE 4
                END,
                mi.created_date DESC
        """, (user_id,))

        properties = await cursor.fetchall()

        # Получаем текущий адрес проживания
        cursor = await db.execute("""
            SELECT city, street, house_number FROM users WHERE user_id = ?
        """, (user_id,))
        current_address = await cursor.fetchone()

    if not properties:
        await message.answer(
            "🏘️ У вас пока нет недвижимости на продажу.\n\n"
            "Вы можете добавить недвижимость через маркетплейс."
        )
        return

    # Текущий адрес
    response = "🏘️ <b>Ваша недвижимость</b>\n\n"
    if current_address:
        city, street, house = current_address
        if city or street or house:
            address_parts = []
            if city: address_parts.append(f"🏙️ {city}")
            if street: address_parts.append(f"🏘️ {street}")
            if house: address_parts.append(f"🏠 {house}")
            response += f"📍 <b>Текущий адрес проживания:</b>\n{' | '.join(address_parts)}\n\n"

    # Статистика
    active_count = sum(1 for p in properties if p[6] == 'active')
    sold_count = sum(1 for p in properties if p[6] == 'sold')
    reserved_count = sum(1 for p in properties if p[6] == 'reserved')
    total_value = sum(p[3] * p[4] for p in properties if p[6] == 'active')

    response += f"📊 <b>Статистика:</b>\n"
    response += f"• 🟢 Активных: {active_count}\n"
    response += f"• 🟡 В процессе: {reserved_count}\n"
    response += f"• 🔴 Проданных: {sold_count}\n"
    response += f"• 💰 Общая стоимость: {total_value} шуек\n\n"

    # Список недвижимости
    response += "<b>🏠 Список недвижимости:</b>\n"

    for idx, (prop_id, title, description, price, quantity, created_date,
              status, image_id, prop_city, prop_street, prop_house) in enumerate(properties[:10], 1):

        # Эмодзи статуса
        status_emoji = "🟢" if status == 'active' else "🟡" if status == 'reserved' else "🔴"

        response += f"\n{status_emoji} <b>{idx}. {title}</b>\n"
        response += f"💰 Цена: {price} шуек\n"

        # Адрес из описания или из полей
        address = extract_address_from_description(description)
        if not address and (prop_city or prop_street or prop_house):
            address_parts = []
            if prop_city: address_parts.append(prop_city)
            if prop_street: address_parts.append(prop_street)
            if prop_house: address_parts.append(prop_house)
            if address_parts:
                response += f"📍 Адрес: {', '.join(address_parts)}\n"
        elif address:
            response += f"📍 Адрес: {address}\n"

        response += f"📊 Статус: {status}\n"
        response += f"📅 Добавлено: {created_date[:10]}\n"
        response += f"🆔 ID: {prop_id}\n"
        response += "─" * 30

    if len(properties) > 10:
        response += f"\n\n... и ещё {len(properties) - 10} объектов"

    # Клавиатура
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🏠 Активная недвижимость"), KeyboardButton(text="💰 Проданная недвижимость")],
            [KeyboardButton(text="📍 По адресу"), KeyboardButton(text="💵 По цене")],
            [KeyboardButton(text="📝 Добавить недвижимость"), KeyboardButton(text="🗺️ На карте")],
            [KeyboardButton(text="⬅️ Назад в настройки")]
        ],
        resize_keyboard=True
    )

    await message.answer(response, parse_mode="HTML", reply_markup=keyboard)
    await state.set_state(PropertyManagement.viewing_properties)

    # Сохраняем данные для фильтрации
    await state.update_data(
        all_properties=properties,
        user_id=user_id,
        current_address=current_address
    )


def extract_address_from_description(description: str) -> str:
    """Извлекает адрес из описания недвижимости"""
    if not description:
        return ""

    # Ищем паттерны адреса
    import re

    # Паттерны для поиска адреса
    patterns = [
        r'город[:\s]+([^\n,]+)',
        r'г\.\s*([^\n,]+)',
        r'улица[:\s]+([^\n,]+)',
        r'ул\.\s*([^\n,]+)',
        r'дом[:\s]+([^\n,]+)',
        r'д\.\s*([^\n,]+)',
        r'адрес[:\s]+([^\n]+)'
    ]

    address_parts = {}

    for pattern in patterns:
        matches = re.findall(pattern, description, re.IGNORECASE)
        if matches:
            key = pattern[:3]
            if 'город' in key or 'г.' in key:
                address_parts['city'] = matches[0].strip()
            elif 'улица' in key or 'ул.' in key:
                address_parts['street'] = matches[0].strip()
            elif 'дом' in key or 'д.' in key:
                address_parts['house'] = matches[0].strip()
            elif 'адрес' in key:
                return matches[0].strip()

    if address_parts:
        parts = []
        if 'city' in address_parts:
            parts.append(address_parts['city'])
        if 'street' in address_parts:
            parts.append(address_parts['street'])
        if 'house' in address_parts:
            parts.append(address_parts['house'])
        return ', '.join(parts)

    return ""


@dp.message(PropertyManagement.viewing_properties, F.text == "🏠 Активная недвижимость")
async def show_active_properties(message: types.Message, state: FSMContext):
    """Показывает только активную недвижимость"""
    data = await state.get_data()
    properties = data.get('all_properties', [])

    active_properties = [p for p in properties if p[6] == 'active']

    if not active_properties:
        await message.answer("🟢 У вас нет активной недвижимости на продаже.")
        return

    response = "🟢 <b>Активная недвижимость</b>\n\n"

    for idx, (prop_id, title, description, price, quantity, created_date,
              status, image_id, prop_city, prop_street, prop_house) in enumerate(active_properties[:10], 1):

        response += f"<b>{idx}. {title}</b>\n"
        response += f"💰 {price} шуек\n"

        address = extract_address_from_description(description)
        if address:
            response += f"📍 {address}\n"

        response += f"📅 {created_date[:10]}\n"
        response += f"🆔 ID: {prop_id}\n"
        response += "─" * 30 + "\n"

    await message.answer(response, parse_mode="HTML")


@dp.message(PropertyManagement.viewing_properties, F.text == "💰 Проданная недвижимость")
async def show_sold_properties(message: types.Message, state: FSMContext):
    """Показывает проданную недвижимость"""
    data = await state.get_data()
    properties = data.get('all_properties', [])

    sold_properties = [p for p in properties if p[6] == 'sold']

    if not sold_properties:
        await message.answer("🔴 У вас нет проданной недвижимости.")
        return

    response = "🔴 <b>Проданная недвижимость</b>\n\n"

    for idx, (prop_id, title, description, price, quantity, created_date,
              status, image_id, prop_city, prop_street, prop_house) in enumerate(sold_properties[:10], 1):

        response += f"<b>{idx}. {title}</b>\n"
        response += f"💰 Продано за: {price} шуек\n"

        address = extract_address_from_description(description)
        if address:
            response += f"📍 {address}\n"

        response += f"📅 Продано: {created_date[:10]}\n"
        response += f"🆔 ID: {prop_id}\n"
        response += "─" * 30 + "\n"

    await message.answer(response, parse_mode="HTML")


@dp.message(PropertyManagement.viewing_properties, F.text == "📍 По адресу")
async def show_properties_by_address(message: types.Message, state: FSMContext):
    """Группирует недвижимость по городам"""
    data = await state.get_data()
    properties = data.get('all_properties', [])

    # Группировка по городам
    cities = {}
    for prop in properties:
        description = prop[2]
        address = extract_address_from_description(description)

        if address:
            # Извлекаем город из адреса
            city = address.split(',')[0].strip() if ',' in address else address
        else:
            city = "Без адреса"

        if city not in cities:
            cities[city] = []
        cities[city].append(prop)

    response = "📍 <b>Недвижимость по городам</b>\n\n"

    for city, city_properties in sorted(cities.items()):
        response += f"<b>🏙️ {city}</b> ({len(city_properties)}):\n"

        for idx, prop in enumerate(city_properties[:3], 1):
            prop_id, title, description, price, quantity, created_date, status, *_ = prop
            status_emoji = "🟢" if status == 'active' else "🟡" if status == 'reserved' else "🔴"

            response += f"  {status_emoji} <b>{title}</b>\n"
            response += f"     💰 {price} шуек | 🆔 {prop_id}\n"

        if len(city_properties) > 3:
            response += f"     ... и ещё {len(city_properties) - 3}\n"

        response += "\n"

    await message.answer(response, parse_mode="HTML")


@dp.message(PropertyManagement.viewing_properties, F.text == "💵 По цене")
async def show_properties_by_price(message: types.Message, state: FSMContext):
    """Сортирует недвижимость по цене"""
    data = await state.get_data()
    properties = data.get('all_properties', [])

    # Сортируем по цене (дорогие сначала)
    sorted_properties = sorted(properties, key=lambda x: x[3], reverse=True)

    response = "💵 <b>Недвижимость по цене</b>\n\n"

    for idx, (prop_id, title, description, price, quantity, created_date,
              status, image_id, *_) in enumerate(sorted_properties[:10], 1):

        status_emoji = "🟢" if status == 'active' else "🟡" if status == 'reserved' else "🔴"

        response += f"{idx}. {status_emoji} <b>{title}</b>\n"
        response += f"   💰 <b>{price} шуек</b>\n"

        address = extract_address_from_description(description)
        if address:
            short_address = address[:40] + "..." if len(address) > 40 else address
            response += f"   📍 {short_address}\n"

        response += f"   🆔 {prop_id} | 📅 {created_date[:10]}\n"
        response += "─" * 40 + "\n"

    # Статистика цен
    if properties:
        active_prices = [p[3] for p in properties if p[6] == 'active']
        if active_prices:
            avg_price = sum(active_prices) // len(active_prices)
            response += f"\n📊 Средняя цена: {avg_price} шуек\n"
            response += f"🏆 Самая дорогая: {max(active_prices)} шуек\n"
            response += f"💰 Самая дешёвая: {min(active_prices)} шуек"

    await message.answer(response, parse_mode="HTML")


@dp.message(PropertyManagement.viewing_properties, F.text == "📝 Добавить недвижимость")
async def add_property_from_menu(message: types.Message, state: FSMContext):
    """Переход к добавлению недвижимости через маркетплейс"""
    await state.clear()

    # Устанавливаем категорию "🏠 Недвижимость" и переходим к добавлению
    await state.update_data(category="🏠 Недвижимость")

    await message.answer(
        "🏠 Вы добавляете недвижимость. Для удобства покупателей "
        "рекомендуется указать адрес.\n\n"
        "Хотите указать адрес недвижимости?",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="✅ Да, указать адрес")],
                [KeyboardButton(text="❌ Нет, пропустить")],
                [KeyboardButton(text="↩ Отмена")]
            ],
            resize_keyboard=True
        )
    )
    await state.set_state(Marketplace.add_property_address)


async def check_table_structure():
    """Проверяет структуру таблицы cities"""
    try:
        async with aiosqlite.connect("database.db") as db:
            # Создаем таблицу cities если её нет
            await db.execute("""
                CREATE TABLE IF NOT EXISTS cities (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE NOT NULL,
                    mayor_id INTEGER,
                    population INTEGER DEFAULT 0,
                    coord_x INTEGER,
                    coord_z INTEGER,
                    created_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                    is_verified INTEGER DEFAULT 0,
                    FOREIGN KEY (mayor_id) REFERENCES users(user_id)
                )
            """)
            await db.commit()
            print("✅ Таблица cities создана/проверена")
            return True
    except Exception as e:
        print(f"❌ Ошибка при создании таблицы cities: {e}")
        return False


@dp.message(Command("fix_cities_table"))
async def fix_cities_table(message: types.Message):
    """Создает новую таблицу cities с колонкой is_verified"""
    if message.from_user.id not in ADMIN_ID:
        await message.answer("❌ У вас нет прав администратора.")
        return

    await message.answer("🔄 Пересоздаю таблицу cities с колонкой is_verified...")

    try:
        async with aiosqlite.connect("database.db") as db:
            # 1. Создаем временную таблицу с правильной структурой
            await db.execute("""
                CREATE TABLE IF NOT EXISTS cities_temp (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE NOT NULL,
                    mayor_id INTEGER,
                    population INTEGER DEFAULT 0,
                    coord_x INTEGER,
                    coord_z INTEGER,
                    created_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                    is_verified INTEGER DEFAULT 0,
                    FOREIGN KEY (mayor_id) REFERENCES users(user_id)
                )
            """)

            # 2. Копируем данные из старой таблицы (если она существует)
            try:
                await db.execute("""
                    INSERT INTO cities_temp (id, name, mayor_id, population, coord_x, coord_z, created_date, is_verified)
                    SELECT id, name, mayor_id, population, coord_x, coord_z, created_date, 0 
                    FROM cities
                """)
                print("✅ Данные скопированы из старой таблицы")
            except Exception as copy_error:
                print(f"⚠️ Не удалось скопировать данные: {copy_error}")
                # Таблица может не существовать или быть пустой

            # 3. Удаляем старую таблицу
            try:
                await db.execute("DROP TABLE IF EXISTS cities")
                print("✅ Старая таблица удалена")
            except Exception as drop_error:
                print(f"⚠️ Не удалось удалить старую таблицу: {drop_error}")

            # 4. Переименовываем временную таблицу
            await db.execute("ALTER TABLE cities_temp RENAME TO cities")

            await db.commit()

            # Проверяем результат
            cursor = await db.execute("PRAGMA table_info(cities)")
            columns = await cursor.fetchall()
            column_names = [col[1] for col in columns]

            response = "✅ <b>Таблица cities успешно пересоздана!</b>\n\n"
            response += f"📋 Колонки в таблице cities:\n"
            for col_name in column_names:
                response += f"• {col_name}\n"

            if 'is_verified' in column_names:
                response += f"\n✅ Колонка <b>is_verified</b> успешно добавлена!"
            else:
                response += f"\n❌ Колонка is_verified не добавлена!"

            await message.answer(response, parse_mode="HTML")

    except Exception as e:
        await message.answer(f"❌ Ошибка при пересоздании таблицы: {str(e)[:200]}")
        print(f"Ошибка fix_cities_table: {e}")

"""
корпорации
"""
@dp.message(F.text == "Создать корпорацию")
@dp.message(Command("create_corporation"))
async def cmd_create_corporation(message: Message, state: FSMContext):
    user_id = message.from_user.id
    async with aiosqlite.connect('database.db') as db:
        cursor = await db.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,))
        user = await cursor.fetchone()
        if not user:
            await message.answer("❌ Вы не зарегистрированы. Используйте /start для регистрации.")
            return
    await message.answer("🏢 Введите название вашей корпорации (до 50 символов):")
    await state.set_state(CorporationStates.waiting_for_name)

@dp.message(CorporationStates.waiting_for_name)
async def process_corp_name(message: Message, state: FSMContext):
    name = message.text.strip()
    if len(name) > 50:
        await message.answer("❌ Название слишком длинное. Введите до 50 символов:")
        return
    if len(name) < 3:
        await message.answer("❌ Название должно содержать минимум 3 символа:")
        return
    async with aiosqlite.connect('database.db') as db:
        cursor = await db.execute("SELECT id FROM corporations WHERE name = ?", (name,))
        existing = await cursor.fetchone()
        if existing:
            await message.answer("❌ Корпорация с таким названием уже существует. Придумайте другое:")
            return
    await state.update_data(corp_name=name)
    await message.answer("📝 Введите описание корпорации (можно оставить пустым, отправив прочерк или просто пропустите):")
    await state.set_state(CorporationStates.waiting_for_description)

@dp.message(CorporationStates.waiting_for_description)
async def process_corp_description(message: Message, state: FSMContext):
    description = message.text.strip()
    if description in ["-", "—", "пропуск", "нет", ""]:
        description = ""
    data = await state.get_data()
    corp_name = data['corp_name']
    user_id = message.from_user.id

    async with aiosqlite.connect('database.db') as db:
        # Создаём корпорацию
        cursor = await db.execute(
            "INSERT INTO corporations (name, description, owner_id) VALUES (?, ?, ?)",
            (corp_name, description, user_id)
        )
        corporation_id = cursor.lastrowid

        # Создаём стандартные роли
        cursor = await db.execute(
            "INSERT INTO corporation_roles (corporation_id, name) VALUES (?, ?)",
            (corporation_id, "Владелец")
        )
        owner_role_id = cursor.lastrowid

        cursor = await db.execute(
            "INSERT INTO corporation_roles (corporation_id, name) VALUES (?, ?)",
            (corporation_id, "Администратор")
        )
        admin_role_id = cursor.lastrowid

        cursor = await db.execute(
            "INSERT INTO corporation_roles (corporation_id, name) VALUES (?, ?)",
            (corporation_id, "Участник")
        )
        member_role_id = cursor.lastrowid

        # Добавляем создателя как владельца
        await db.execute(
            "INSERT INTO corporation_members (corporation_id, user_id, role_id) VALUES (?, ?, ?)",
            (corporation_id, user_id, owner_role_id)
        )
        await db.commit()

    await message.answer(
        f"✅ Корпорация «{corp_name}» успешно создана!\n"
        f"ID: {corporation_id}"
    )
    await state.clear()

@dp.message(F.text == "Мои корпорации")
@dp.message(Command("my_corporations"))
async def cmd_my_corporations(message: Message):
    user_id = message.from_user.id
    async with aiosqlite.connect('database.db') as db:
        cursor = await db.execute("""
            SELECT c.id, c.name, c.description, cr.name as role_name
            FROM corporations c
            JOIN corporation_members cm ON c.id = cm.corporation_id
            JOIN corporation_roles cr ON cm.role_id = cr.id
            WHERE cm.user_id = ?
            ORDER BY c.created_at DESC
        """, (user_id,))
        corps = await cursor.fetchall()
    if not corps:
        await message.answer("Вы пока не состоите ни в одной корпорации.")
        return
    text = "🏢 Ваши корпорации:\n\n"
    for corp_id, name, desc, role_name in corps:
        text += f"• <b>{name}</b> (ID: {corp_id}) — роль: {role_name}\n"
        if desc:
            text += f"  {desc[:50]}{'...' if len(desc)>50 else ''}\n"
        text += "\n"
    await message.answer(text, parse_mode="HTML")

@dp.message(F.text == "Вступить в корпорацию")
@dp.message(Command("join_corporation"))
async def cmd_join_corporation(message: Message, state: FSMContext):
    await message.answer("Введите ID корпорации, в которую хотите вступить:")
    await state.set_state(CorporationJoinStates.waiting_for_corp_id)

@dp.message(CorporationJoinStates.waiting_for_corp_id)
async def process_join_corp_id(message: Message, state: FSMContext):
    try:
        corp_id = int(message.text.strip())
    except ValueError:
        await message.answer("❌ Введите корректный числовой ID.")
        return

    user_id = message.from_user.id
    async with aiosqlite.connect('database.db') as db:
        # Проверяем существование корпорации
        cursor = await db.execute("SELECT id, name FROM corporations WHERE id = ?", (corp_id,))
        corp = await cursor.fetchone()
        if not corp:
            await message.answer("❌ Корпорация с таким ID не найдена.")
            return

        # Проверяем, не состоит ли уже пользователь
        cursor = await db.execute(
            "SELECT id FROM corporation_members WHERE corporation_id = ? AND user_id = ?",
            (corp_id, user_id)
        )
        member = await cursor.fetchone()
        if member:
            await message.answer("❌ Вы уже состоите в этой корпорации.")
            return

        # Проверяем, не подавал ли уже заявку
        cursor = await db.execute(
            "SELECT id FROM corporation_applications WHERE corporation_id = ? AND user_id = ? AND status = 'pending'",
            (corp_id, user_id)
        )
        app = await cursor.fetchone()
        if app:
            await message.answer("❌ Вы уже подали заявку в эту корпорацию. Ожидайте решения.")
            return

    await state.update_data(corp_id=corp_id, corp_name=corp[1])
    await message.answer("📝 Напишите сопроводительное сообщение (почему хотите вступить, можно оставить пустым):")
    await state.set_state(CorporationJoinStates.waiting_for_join_message)

@dp.message(CorporationJoinStates.waiting_for_join_message)
async def process_join_message(message: Message, state: FSMContext):
    join_msg = message.text.strip()
    if join_msg in ["-", "—", "пропуск", "нет", ""]:
        join_msg = ""
    data = await state.get_data()
    corp_id = data['corp_id']
    corp_name = data['corp_name']
    user_id = message.from_user.id

    async with aiosqlite.connect('database.db') as db:
        await db.execute(
            "INSERT INTO corporation_applications (corporation_id, user_id, message) VALUES (?, ?, ?)",
            (corp_id, user_id, join_msg)
        )
        await db.commit()

    await message.answer(
        f"✅ Заявка в корпорацию «{corp_name}» отправлена! Ожидайте решения администрации."
    )
    await state.clear()

async def is_corp_admin_or_owner(user_id: int, corporation_id: int) -> bool:
    """Проверяет, является ли пользователь владельцем или администратором корпорации."""
    async with aiosqlite.connect('database.db') as db:
        cursor = await db.execute("""
            SELECT cr.name
            FROM corporation_members cm
            JOIN corporation_roles cr ON cm.role_id = cr.id
            WHERE cm.corporation_id = ? AND cm.user_id = ?
        """, (corporation_id, user_id))
        role = await cursor.fetchone()
        if role and role[0] in ("Владелец", "Администратор"):
            return True
    return False

@dp.message(F.text == "Управление заявками")
async def cmd_manage_applications(message: Message, state: FSMContext):
    user_id = message.from_user.id
    async with aiosqlite.connect('database.db') as db:
        # Ищем корпорации, где пользователь админ/владелец
        cursor = await db.execute("""
            SELECT c.id, c.name
            FROM corporations c
            JOIN corporation_members cm ON c.id = cm.corporation_id
            JOIN corporation_roles cr ON cm.role_id = cr.id
            WHERE cm.user_id = ? AND cr.name IN ('Владелец', 'Администратор')
        """, (user_id,))
        corps = await cursor.fetchall()

    if not corps:
        await message.answer("У вас нет прав на управление заявками (вы не админ и не владелец ни одной корпорации).")
        return

    # Если несколько корпораций, предложим выбрать
    if len(corps) == 1:
        corp_id, corp_name = corps[0]
        await show_applications(message, corp_id, corp_name)
    else:
        kb = InlineKeyboardBuilder()
        for corp_id, corp_name in corps:
            kb.button(text=corp_name, callback_data=f"show_apps:{corp_id}")
        kb.button(text="❌ Отмена", callback_data="cancel")
        kb.adjust(1)
        await message.answer("Выберите корпорацию для управления заявками:", reply_markup=kb.as_markup())
        await state.set_state(ApplicationManagementStates.viewing_applications)

async def show_applications(message: Message, corp_id: int, corp_name: str):
    async with aiosqlite.connect('database.db') as db:
        cursor = await db.execute("""
            SELECT a.id, a.user_id, a.message, a.created_at, u.name
            FROM corporation_applications a
            JOIN users u ON a.user_id = u.user_id
            WHERE a.corporation_id = ? AND a.status = 'pending'
            ORDER BY a.created_at ASC
        """, (corp_id,))
        apps = await cursor.fetchall()

    if not apps:
        await message.answer(f"В корпорации «{corp_name}» нет новых заявок.")
        return

    for app_id, user_id, msg, created_at, user_name in apps:
        text = f"📨 Заявка #{app_id}\n"
        text += f"От: {user_name} (ID: {user_id})\n"
        text += f"Дата: {created_at}\n"
        if msg:
            text += f"Сообщение: {msg}\n"
        kb = InlineKeyboardBuilder()
        kb.button(text="✅ Принять", callback_data=f"app_accept:{app_id}")
        kb.button(text="❌ Отклонить", callback_data=f"app_reject:{app_id}")
        kb.adjust(2)
        await message.answer(text, reply_markup=kb.as_markup())

@dp.callback_query(F.data.startswith("show_apps:"))
async def show_apps_callback(callback: CallbackQuery, state: FSMContext):
    corp_id = int(callback.data.split(":")[1])
    async with aiosqlite.connect('database.db') as db:
        cursor = await db.execute("SELECT name FROM corporations WHERE id = ?", (corp_id,))
        corp = await cursor.fetchone()
        if not corp:
            await callback.answer("❌ Корпорация не найдена", show_alert=True)
            return
        corp_name = corp[0]
    # Вызываем функцию отображения заявок (она уже должна быть определена)
    await show_applications(callback.message, corp_id, corp_name)
    await callback.answer()
    await state.clear()  # очищаем состояние, если оно было установлено



@dp.callback_query(F.data.startswith("app_accept"))
async def accept_application(callback: CallbackQuery):
    app_id = int(callback.data.split(":")[1])
    admin_id = callback.from_user.id

    async with aiosqlite.connect('database.db') as db:
        # Получаем данные заявки
        cursor = await db.execute("""
            SELECT corporation_id, user_id FROM corporation_applications WHERE id = ?
        """, (app_id,))
        app = await cursor.fetchone()
        if not app:
            await callback.answer("Заявка не найдена")
            return
        corp_id, applicant_id = app

        # Проверяем права админа
        if not await is_corp_admin_or_owner(admin_id, corp_id):
            await callback.answer("У вас нет прав для этого действия", show_alert=True)
            return

        # Получаем роль "Участник" для этой корпорации
        cursor = await db.execute("""
            SELECT id FROM corporation_roles
            WHERE corporation_id = ? AND name = 'Участник'
        """, (corp_id,))
        role = await cursor.fetchone()
        if not role:
            await callback.answer("Ошибка: роль 'Участник' не найдена", show_alert=True)
            return
        member_role_id = role[0]

        # Добавляем пользователя в члены
        await db.execute(
            "INSERT OR IGNORE INTO corporation_members (corporation_id, user_id, role_id) VALUES (?, ?, ?)",
            (corp_id, applicant_id, member_role_id)
        )
        # Обновляем статус заявки
        await db.execute(
            "UPDATE corporation_applications SET status = 'accepted', reviewed_at = CURRENT_TIMESTAMP, reviewed_by = ? WHERE id = ?",
            (admin_id, app_id)
        )
        await db.commit()

    await callback.message.edit_text(f"{callback.message.text}\n\n✅ Заявка принята!")
    await callback.answer("Заявка принята")

@dp.callback_query(F.data.startswith("app_reject"))
async def reject_application(callback: CallbackQuery):
    app_id = int(callback.data.split(":")[1])
    admin_id = callback.from_user.id

    async with aiosqlite.connect('database.db') as db:
        cursor = await db.execute("SELECT corporation_id FROM corporation_applications WHERE id = ?", (app_id,))
        app = await cursor.fetchone()
        if not app:
            await callback.answer("Заявка не найдена")
            return
        corp_id = app[0]

        if not await is_corp_admin_or_owner(admin_id, corp_id):
            await callback.answer("У вас нет прав для этого действия", show_alert=True)
            return

        await db.execute(
            "UPDATE corporation_applications SET status = 'rejected', reviewed_at = CURRENT_TIMESTAMP, reviewed_by = ? WHERE id = ?",
            (admin_id, app_id)
        )
        await db.commit()

    await callback.message.edit_text(f"{callback.message.text}\n\n❌ Заявка отклонена")
    await callback.answer("Заявка отклонена")

@dp.message(F.text == "Управление ролями")
async def cmd_manage_roles(message: Message, state: FSMContext):
    user_id = message.from_user.id
    async with aiosqlite.connect('database.db') as db:
        cursor = await db.execute("""
            SELECT c.id, c.name
            FROM corporations c
            JOIN corporation_members cm ON c.id = cm.corporation_id
            JOIN corporation_roles cr ON cm.role_id = cr.id
            WHERE cm.user_id = ? AND cr.name IN ('Владелец', 'Администратор')
        """, (user_id,))
        corps = await cursor.fetchall()

    if not corps:
        await message.answer("У вас нет прав на управление ролями.")
        return

    if len(corps) == 1:
        corp_id, corp_name = corps[0]
        await show_role_management(message, corp_id, corp_name)
    else:
        kb = InlineKeyboardBuilder()
        for corp_id, corp_name in corps:
            kb.button(text=corp_name, callback_data=f"role_manage:{corp_id}")
        kb.button(text="❌ Отмена", callback_data="cancel")
        kb.adjust(1)
        await message.answer("Выберите корпорацию для управления ролями:", reply_markup=kb.as_markup())
        await state.set_state(RoleManagementStates.choosing_action)

async def show_role_management(message: Message, corp_id: int, corp_name: str):
    text = f"🏢 Управление ролями в корпорации «{corp_name}»\n\n"
    kb = InlineKeyboardBuilder()
    kb.button(text="➕ Создать роль", callback_data=f"role_create:{corp_id}")
    kb.button(text="👤 Назначить роль", callback_data=f"role_assign:{corp_id}")
    kb.button(text="🔄 Изменить роль участника", callback_data=f"role_change:{corp_id}")
    kb.button(text="📋 Список ролей", callback_data=f"role_list:{corp_id}")
    kb.button(text="🔙 Назад", callback_data="role_back")
    kb.adjust(1)
    await message.answer(text, reply_markup=kb.as_markup())

@dp.callback_query(F.data.startswith("role_manage:"))
async def role_manage_callback(callback: CallbackQuery, state: FSMContext):
    corp_id = int(callback.data.split(":")[1])
    async with aiosqlite.connect('database.db') as db:
        cursor = await db.execute("SELECT name FROM corporations WHERE id = ?", (corp_id,))
        corp = await cursor.fetchone()
        if not corp:
            await callback.answer("Корпорация не найдена")
            return
        corp_name = corp[0]
    await show_role_management(callback.message, corp_id, corp_name)
    await callback.answer()
    await state.set_state(RoleManagementStates.choosing_action)

# Создание роли
@dp.callback_query(F.data.startswith("role_create:"))
async def role_create_callback(callback: CallbackQuery, state: FSMContext):
    corp_id = int(callback.data.split(":")[1])
    await state.update_data(corp_id=corp_id)
    await callback.message.edit_text("Введите название новой роли:")
    await state.set_state(RoleManagementStates.waiting_for_role_name)
    await callback.answer()

@dp.message(RoleManagementStates.waiting_for_role_name)
async def process_new_role_name(message: Message, state: FSMContext):
    role_name = message.text.strip()
    if len(role_name) > 30:
        await message.answer("❌ Название слишком длинное (макс. 30 символов). Введите заново:")
        return
    data = await state.get_data()
    corp_id = data['corp_id']
    async with aiosqlite.connect('database.db') as db:
        try:
            await db.execute(
                "INSERT INTO corporation_roles (corporation_id, name) VALUES (?, ?)",
                (corp_id, role_name)
            )
            await db.commit()
            await message.answer(f"✅ Роль «{role_name}» успешно создана!")
        except aiosqlite.IntegrityError:
            await message.answer("❌ Роль с таким названием уже существует в этой корпорации.")
    await state.clear()

# Список ролей
@dp.callback_query(F.data.startswith("role_list:"))
async def role_list_callback(callback: CallbackQuery):
    corp_id = int(callback.data.split(":")[1])
    async with aiosqlite.connect('database.db') as db:
        cursor = await db.execute("""
            SELECT id, name FROM corporation_roles
            WHERE corporation_id = ?
            ORDER BY name
        """, (corp_id,))
        roles = await cursor.fetchall()
    if not roles:
        await callback.message.edit_text("В этой корпорации пока нет ролей.")
        return
    text = "📋 Список ролей:\n"
    for role_id, role_name in roles:
        text += f"• {role_name} (ID: {role_id})\n"
    await callback.message.edit_text(text)
    await callback.answer()

# Назначение роли пользователю (выбор роли)
@dp.callback_query(F.data.startswith("role_assign:"))
async def role_assign_callback(callback: CallbackQuery, state: FSMContext):
    corp_id = int(callback.data.split(":")[1])
    await state.update_data(corp_id=corp_id, action="assign")
    await callback.message.edit_text("Введите ID пользователя, которому хотите назначить роль:")
    await state.set_state(RoleManagementStates.waiting_for_user_to_assign)
    await callback.answer()

@dp.message(RoleManagementStates.waiting_for_user_to_assign)
async def process_user_for_assign(message: Message, state: FSMContext):
    try:
        target_user_id = int(message.text.strip())
    except ValueError:
        await message.answer("❌ Введите корректный числовой ID.")
        return
    data = await state.get_data()
    corp_id = data['corp_id']
    # Проверяем, существует ли пользователь и не состоит ли уже
    async with aiosqlite.connect('database.db') as db:
        cursor = await db.execute("SELECT user_id, name FROM users WHERE user_id = ?", (target_user_id,))
        user = await cursor.fetchone()
        if not user:
            await message.answer("❌ Пользователь с таким ID не зарегистрирован в боте.")
            return
        cursor = await db.execute("""
            SELECT id FROM corporation_members
            WHERE corporation_id = ? AND user_id = ?
        """, (corp_id, target_user_id))
        member = await cursor.fetchone()
        if not member:
            await message.answer("❌ Этот пользователь не состоит в корпорации. Сначала он должен вступить.")
            return
    await state.update_data(target_user_id=target_user_id, target_user_name=user[1])
    # Показываем список ролей для выбора
    async with aiosqlite.connect('database.db') as db:
        cursor = await db.execute("""
            SELECT id, name FROM corporation_roles
            WHERE corporation_id = ?
            ORDER BY name
        """, (corp_id,))
        roles = await cursor.fetchall()
    if not roles:
        await message.answer("❌ В корпорации нет ролей.")
        await state.clear()
        return
    kb = InlineKeyboardBuilder()
    for role_id, role_name in roles:
        kb.button(text=role_name, callback_data=f"set_role:{role_id}")
    kb.button(text="❌ Отмена", callback_data="cancel")
    kb.adjust(1)
    await message.answer(f"Выберите роль для пользователя {user[1]}:", reply_markup=kb.as_markup())
    await state.set_state(RoleManagementStates.waiting_for_role_to_assign)

@dp.callback_query(F.data.startswith("set_role:"), RoleManagementStates.waiting_for_role_to_assign)
async def set_role_callback(callback: CallbackQuery, state: FSMContext):
    role_id = int(callback.data.split(":")[1])
    data = await state.get_data()
    corp_id = data['corp_id']
    target_user_id = data['target_user_id']
    async with aiosqlite.connect('database.db') as db:
        # Проверяем, что роль принадлежит этой корпорации
        cursor = await db.execute("SELECT corporation_id FROM corporation_roles WHERE id = ?", (role_id,))
        role_corp = await cursor.fetchone()
        if not role_corp or role_corp[0] != corp_id:
            await callback.answer("Ошибка: роль не принадлежит этой корпорации", show_alert=True)
            return
        # Обновляем роль пользователя
        await db.execute("""
            UPDATE corporation_members
            SET role_id = ?
            WHERE corporation_id = ? AND user_id = ?
        """, (role_id, corp_id, target_user_id))
        await db.commit()
    await callback.message.edit_text(f"✅ Роль пользователю {data['target_user_name']} успешно назначена!")
    await state.clear()
    await callback.answer()

# Изменение роли участника (можно реализовать аналогично)
# Для краткости оставим заглушку, но в реальном боте нужно добавить аналогичную логику
@dp.callback_query(F.data.startswith("role_change:"))
async def role_change_callback(callback: CallbackQuery):
    await callback.answer("Функция в разработке", show_alert=True)

@dp.callback_query(F.data == "role_back")
async def role_back_callback(callback: CallbackQuery, state: FSMContext):
    await callback.message.delete()
    await state.clear()
    await callback.answer()

@dp.callback_query(F.data == "cancel")
async def cancel_callback(callback: CallbackQuery, state: FSMContext):
    await callback.message.delete()
    await state.clear()
    await callback.answer()


async def main():
    await init_db()
    await dp.start_polling(bot)
    await update_existing_cities()

if __name__ == "__main__":
    asyncio.run(main())
