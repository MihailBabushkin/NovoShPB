import asyncio
import logging
import os
import subprocess
import tempfile
import re
import aiohttp

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
import speech_recognition as sr

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Токен бота (замените на свой)
BOT_TOKEN = "8672862279:AAGwWOArOMHDhakGp04XhwL4d3zTG4rvepQ"

# Путь к FFmpeg (автоопределение)
try:
    import ffmpeg_downloader as ffdl
    FFMPEG_PATH = ffdl.binary_path('ffmpeg')
    logger.info(f"✅ FFmpeg найден через ffmpeg-downloader: {FFMPEG_PATH}")
except:
    import shutil
    FFMPEG_PATH = shutil.which('ffmpeg')
    if FFMPEG_PATH:
        logger.info(f"✅ FFmpeg найден в системе: {FFMPEG_PATH}")
    else:
        FFMPEG_PATH = 'ffmpeg'
        logger.warning("⚠️ FFmpeg не найден, будет использоваться 'ffmpeg' из PATH")

# Константы
MAX_DURATION = 3600  # 1 час
MAX_FILE_SIZE = 50 * 1024 * 1024  # 50 МБ

# Состояния FSM
class VoiceStates(StatesGroup):
    waiting_for_summary = State()

# Инициализация хранилища и диспетчера
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# Создаём бота с большим таймаутом (10 минут)
timeout = aiohttp.ClientTimeout(total=600)
bot = Bot(token=BOT_TOKEN, timeout=timeout)

# Функция конвертации аудио
def convert_ogg_to_wav(ogg_path: str, wav_path: str) -> bool:
    try:
        cmd = [
            FFMPEG_PATH,
            '-i', ogg_path,
            '-acodec', 'pcm_s16le',
            '-ar', '16000',
            '-ac', '1',
            '-y', wav_path
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            logger.error(f"Ошибка конвертации: {result.stderr}")
        return result.returncode == 0
    except Exception as e:
        logger.error(f"Исключение при конвертации: {e}")
        return False

# Функция распознавания речи
def recognize_speech(wav_path: str) -> str | None:
    recognizer = sr.Recognizer()
    try:
        with sr.AudioFile(wav_path) as source:
            recognizer.adjust_for_ambient_noise(source, duration=0.5)
            audio_data = recognizer.record(source)
            text = recognizer.recognize_google(audio_data, language="ru-RU")
            logger.info(f"Распознано {len(text)} символов")
            return text
    except sr.UnknownValueError:
        logger.warning("Речь не распознана")
        return None
    except Exception as e:
        logger.error(f"Ошибка распознавания: {e}")
        return None

# Функция создания пересказа
def create_summary(text: str, summary_type: str = "medium") -> str:
    if not text:
        return "Текст отсутствует"
    sentences = re.split(r'[.!?]+', text)
    sentences = [s.strip() for s in sentences if s.strip()]
    if not sentences:
        return text[:200] + "..." if len(text) > 200 else text

    if summary_type == "short":
        num = min(3, len(sentences))
    elif summary_type == "medium":
        num = min(6, len(sentences))
    else:  # detailed
        num = min(10, len(sentences))

    # Выбираем равномерно по тексту
    if len(sentences) > num:
        step = len(sentences) // num
        indices = [i * step for i in range(num)]
        selected = [sentences[i] for i in indices]
    else:
        selected = sentences
    return '. '.join(selected) + '.'

# Клавиатура выбора пересказа


# Команда /start
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    await message.answer(
        "🎤 Голосовой бот\n\n"
        "Просто отправь мне голосовое сообщение, и я преобразую его в текст и сделаю краткий пересказ.\n\nБот создан Шуйский It отделом по заказу Администрации Города Дамкрат"
    )

# Обработчик голосовых сообщений
@dp.message(lambda message: message.voice is not None)
async def handle_voice(message: types.Message, state: FSMContext):
    status_msg = None
    temp_files = []

    try:
        status_msg = await message.answer("🔄 Начинаю обработку...")

        voice = message.voice
        file_size = voice.file_size
        duration = voice.duration

        # Проверка размера
        if file_size > MAX_FILE_SIZE:
            await message.answer(
                f"❌ Файл слишком большой ({file_size/1024/1024:.1f} МБ). "
                f"Максимум {MAX_FILE_SIZE/1024/1024} МБ."
            )
            return

        # Проверка длительности
        if duration > MAX_DURATION:
            await message.answer("❌ Сообщение длиннее 1 часа. Пожалуйста, отправьте более короткое.")
            return

        # Информируем о длительности
        if duration > 300:
            minutes = duration // 60
            seconds = duration % 60
            await status_msg.edit_text(
                f"🔄 Обрабатываю длинное сообщение ({minutes}м {seconds}с)...\n"
                f"Это может занять несколько минут."
            )

        # Скачивание файла
        file = await bot.get_file(voice.file_id)
        ogg_file = tempfile.NamedTemporaryFile(suffix='.ogg', delete=False)
        ogg_path = ogg_file.name
        ogg_file.close()
        temp_files.append(ogg_path)

        wav_path = ogg_path.replace('.ogg', '.wav')
        temp_files.append(wav_path)

        await status_msg.edit_text("📥 Скачиваю файл...")
        await bot.download_file(file.file_path, ogg_path)

        await status_msg.edit_text("🔄 Конвертирую в WAV...")
        if not convert_ogg_to_wav(ogg_path, wav_path):
            await message.answer("❌ Ошибка конвертации аудио.")
            return

        await status_msg.edit_text("🎤 Распознаю речь...")
        text = recognize_speech(wav_path)
        if not text:
            await message.answer("❌ Не удалось распознать речь.")
            return

        # Сохраняем текст в состоянии
        await state.update_data(voice_text=text)
        await state.set_state(VoiceStates.waiting_for_summary)

        # Отправляем результат распознавания
        preview = text[:300] + "..." if len(text) > 300 else text
        await message.answer(
            f"📄Распознанный текст:\n\n"
        )
        await message.answer(
            f"{preview}"
        )

    except asyncio.TimeoutError:
        logger.error("Timeout при скачивании файла")
        await message.answer("⏱️ Превышено время ожидания при скачивании файла. Возможно, файл слишком большой или медленное соединение. Попробуйте позже или отправьте более короткое сообщение.")
    except Exception as e:
        logger.exception("Ошибка в handle_voice")
        await message.answer(f"❌ Произошла ошибка: {str(e)}")
    finally:
        # Очистка временных файлов
        for f in temp_files:
            try:
                if os.path.exists(f):
                    os.unlink(f)
            except:
                pass
        if status_msg:
            await status_msg.delete()



# Обработчик остальных сообщений
@dp.message()
async def handle_other(message: types.Message):
    await message.answer("Отправьте голосовое сообщение для распознавания.")

# Запуск бота
async def main():
    logger.info(f"🚀 Бот запущен с таймаутом {timeout.total} секунд")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
