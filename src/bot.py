from __future__ import annotations
import os, asyncio, logging, tempfile, pandas as pd, subprocess
from datetime import datetime
from pathlib import Path

from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import CommandStart
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardButton, InlineKeyboardMarkup, FSInputFile
)
from dotenv import load_dotenv

from db import get_conn

# конфиг
load_dotenv()
BOT_TOKEN = os.getenv("BOT_KEY")
if not BOT_TOKEN:
    raise RuntimeError("Переменная BOT_KEY не найдена в окружении (.env)")

BASE_DIR  = Path(__file__).resolve().parent
MAIN_PATH = BASE_DIR / "main.py"


logging.basicConfig(level=logging.INFO)
bot = Bot(BOT_TOKEN)
dp  = Dispatcher()
rt  = Router()
dp.include_router(rt)


def export_mentions_excel() -> str:
    #читает таблицу mentions и сохраняет во временный excel, возвращает путь.
    with get_conn() as conn:
        df = pd.read_sql("""
            SELECT date(substr(a.published,1,10)) AS day,
                   a.title,
                   m.subject,
                   m.sentiment,
                   m.context_snippet,
                   a.url
            FROM   mentions m
            JOIN   articles a ON a.id = m.article_id
            ORDER  BY a.published DESC
        """, conn)

    fd, path = tempfile.mkstemp(
        suffix=f"-mentions-{datetime.now():%Y%m%d-%H%M%S}.xlsx"
    )
    os.close(fd)
    df.to_excel(path, index=False)
    return path

# хэндлеры
@rt.message(CommandStart())
async def cmd_start(msg: Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(
            text="🔄 Обновить данные и скачать отчёт",
            callback_data="etl_report"
        )
    ]])
    await msg.answer(
        "Привет! Я соберу свежие новости, обновлю базу и пришлю Excel‑отчёт.",
        reply_markup=kb
    )

@rt.callback_query(F.data == "etl_report")
async def etl_and_report(cb: CallbackQuery):
    await cb.answer()  # закрыть "часики"
    await cb.message.edit_text("⏳ Сбор свежих новостей…")

    # 1. запускаем ETL (src/main.py) как отдельный процесс
    proc = await asyncio.create_subprocess_exec(
        "python", str(MAIN_PATH),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    stdout, stderr = await proc.communicate()

    if proc.returncode != 0:
        err = stderr.decode()[:4000] or "unknown error"
        await cb.message.answer(f"❌ main.py завершился с ошибкой:\n{err}")
        return

    # 2. формируем Excel и отправляем
    try:
        path = export_mentions_excel()
        await bot.send_document(
            chat_id=cb.from_user.id,
            document=FSInputFile(path),
            caption="📊 Сводка упоминаний за последние обновления"
        )
    except Exception as e:
        await cb.message.answer(f"Не удалось создать отчёт: {e}")
    finally:
        if 'path' in locals() and os.path.exists(path):
            os.remove(path)

# main
async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
