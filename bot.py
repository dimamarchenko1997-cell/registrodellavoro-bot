#!/usr/bin/env python3
# bot.py - Telegram presence bot con Google Sheets, FastAPI webhook e scheduler interno

import os
import asyncio
import calendar
import json
import csv
import io
import logging
from datetime import datetime, date
from math import radians, sin, cos, sqrt, atan2
from typing import Optional, List, Any

from dotenv import load_dotenv
import pytz

from aiogram import Bot, Dispatcher, F, types
from aiogram.types import Message, CallbackQuery, ReplyKeyboardMarkup, KeyboardButton
from aiogram.client.default import DefaultBotProperties
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager

import gspread
from gspread.worksheet import Worksheet
from google.oauth2.service_account import Credentials

# ---------------- CONFIG ----------------
load_dotenv()
TOKEN = os.getenv("BOT_TOKEN")  # Telegram bot token
SHEET_ID = os.getenv("GOOGLE_SHEETS_ID")  # Google Sheets ID
CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS")  # JSON string delle credenziali (opzionale)
CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE")  # oppure path a file .json
PORT = int(os.getenv("PORT", 8000))
TIMEZONE = pytz.timezone("Europe/Rome")

# logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# Bot + Dispatcher (FSM memory storage)
bot = Bot(token=TOKEN or "", default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher(storage=MemoryStorage())

# Globals scheduler (servono per non inviare più volte nello stesso giorno)
_last_ingresso_date: Optional[date] = None
_last_uscita_date: Optional[date] = None

# ---------------- Google Sheets helper ----------------
def get_sheet(sheet_name: str = "Registro") -> Worksheet:
    """
    Restituisce la worksheet richiesta. Supporta:
      - GOOGLE_CREDENTIALS come stringa JSON (con \n escaped nella private_key)
      - GOOGLE_CREDENTIALS_FILE -> path su filesystem
    """
    if not (CREDENTIALS_JSON or CREDENTIALS_FILE):
        raise ValueError("Devi impostare GOOGLE_CREDENTIALS (JSON string) o GOOGLE_CREDENTIALS_FILE (path).")

    # Ottieni dict credenziali
    if CREDENTIALS_JSON:
        try:
            credentials_dict = json.loads(CREDENTIALS_JSON)
        except Exception as e:
            logger.exception("Errore parsing GOOGLE_CREDENTIALS: %s", e)
            raise

        # converti sequenze "\\n" -> newline reale nella private_key
        if "private_key" in credentials_dict and isinstance(credentials_dict["private_key"], str):
            credentials_dict["private_key"] = credentials_dict["private_key"].replace("\\n", "\n")

        scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_info(credentials_dict, scopes=scope)
    else:
        # usa file JSON
        scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scope)

    client = gspread.authorize(creds)
    try:
        sheet = client.open_by_key(SHEET_ID).worksheet(sheet_name)
    except Exception as e:
        logger.exception("Errore aprendo il foglio '%s': %s", sheet_name, e)
        raise
    return sheet

# ---------------- Costanti e luoghi ----------------
WORK_LOCATIONS = {
    "Ufficio Centrale": (45.6204762, 9.2401744),
    "Iveco Cornaredo": (45.480555, 9.034716),
    "Iveco Vasto": (42.086621, 14.731960),
}
MAX_DISTANCE_METERS = 200

# ---------------- FSM States ----------------
class RegistroForm(StatesGroup):
    waiting_ingresso_location = State()
    waiting_uscita_location = State()

class PermessiForm(StatesGroup):
    waiting_for_start = State()
    waiting_for_end = State()
    waiting_for_reason = State()

# ---------------- Sheets functions ----------------
def init_sheets() -> None:
    try:
        sheet_reg = get_sheet("Registro")
        if not sheet_reg.row_values(1):
            sheet_reg.append_row(["Data", "Utente", "Ingresso ora", "Posizione ingresso", "Uscita ora", "Posizione uscita"])
        sheet_perm = get_sheet("Permessi")
        if not sheet_perm.row_values(1):
            sheet_perm.append_row(["Data richiesta", "Utente", "Dal", "Al", "Motivo"])
        logger.info("Sheets inizializzati (Registro, Permessi).")
    except Exception as e:
        logger.error("Errore init_sheets: %s", e)

def save_ingresso(user: types.User, time_str: str, location_name: str) -> bool:
    try:
        sheet = get_sheet("Registro")
        now_local = datetime.now(TIMEZONE)
        today = now_local.strftime("%d.%m.%Y")
        user_id = f"{user.full_name} | {user.id}"
        rows = sheet.get_all_values()
        for row in rows[1:]:
            if len(row) > 1 and row[0] == today and row[1] == user_id:
                logger.warning("Ingresso già registrato per %s oggi.", user_id)
                return False
        sheet.append_row([today, user_id, time_str, location_name, "", ""])
        return True
    except Exception as e:
        logger.exception("Errore save_ingresso: %s", e)
        return False

def save_uscita(user: types.User, time_str: str, location_name: str) -> bool:
    try:
        sheet = get_sheet("Registro")
        now_local = datetime.now(TIMEZONE)
        today = now_local.strftime("%d.%m.%Y")
        user_id = f"{user.full_name} | {user.id}"
        rows = sheet.get_all_values()
        for i, row in enumerate(rows[1:], start=2):
            # colonna 5 (index 4) è uscita ora
            if len(row) > 4 and row[0] == today and row[1] == user_id and not row[4]:
                sheet.update_cell(i, 5, time_str)  # colonna E (5)
                sheet.update_cell(i, 6, location_name)  # colonna F (6)
                return True
        logger.warning("Nessun ingresso trovato per %s oggi.", user_id)
        return False
    except Exception as e:
        logger.exception("Errore save_uscita: %s", e)
        return False

def save_permesso(user: types.User, start_date: str, end_date: str, reason: str) -> bool:
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        if end_dt < start_dt:
            logger.warning("Data fine precedente alla data inizio.")
            return False
        sheet = get_sheet("Permessi")
        now_local = datetime.now(TIMEZONE)
        created = now_local.strftime("%d.%m.%Y %H:%M")
        user_id = f"{user.full_name} | {user.id}"
        sheet.append_row([created, user_id, start_date, end_date, reason])
        return True
    except Exception as e:
        logger.exception("Errore save_permesso: %s", e)
        return False

async def get_riepilogo(user: types.User) -> Optional[io.StringIO]:
    try:
        sheet = get_sheet("Registro")
        rows = sheet.get_all_values()
        user_id = f"{user.full_name} | {user.id}"
        user_rows = [row for row in rows if len(row) > 1 and row[1] == user_id]
        if not user_rows:
            return None
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["Data", "Utente", "Ingresso ora", "Posizione ingresso", "Uscita ora", "Posizione uscita"])
        writer.writerows(user_rows)
        output.seek(0)
        return output
    except Exception as e:
        logger.exception("Errore get_riepilogo: %s", e)
        return None

# ---------------- Location utils ----------------
def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371000
    dlat, dlon = radians(lat2 - lat1), radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    return R * 2 * atan2(sqrt(a), sqrt(1 - a))

def check_location(lat: float, lon: float) -> Optional[str]:
    for name, (wlat, wlon) in WORK_LOCATIONS.items():
        if haversine(lat, lon, wlat, wlon) <= MAX_DISTANCE_METERS:
            return name
    return None

# ---------------- Keyboards ----------------
main_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="🕓 Ingresso")],
        [KeyboardButton(text="🚪 Uscita")],
        [KeyboardButton(text="📝 Richiesta permessi")],
        [KeyboardButton(text="📄 Riepilogo")]
    ],
    resize_keyboard=True
)

# ---------------- Calendar builder ----------------
def mese_nome(month: int) -> str:
    mesi = ["Gennaio", "Febbraio", "Marzo", "Aprile", "Maggio", "Giugno",
            "Luglio", "Agosto", "Settembre", "Ottobre", "Novembre", "Dicembre"]
    return mesi[month - 1]

def build_calendar(year: int, month: int, phase: str):
    kb = InlineKeyboardBuilder()
    today = datetime.now(TIMEZONE)
    giorni = ["Lu", "Ma", "Me", "Gi", "Ve", "Sa", "Do"]

    # titolo
    kb.button(text=f"{mese_nome(month)} {year}", callback_data="ignore")

    # giorni della settimana
    for g in giorni:
        kb.button(text=g, callback_data="ignore")

    # numeri del mese - assicurati sempre 6 settimane per dimensione costante
    weeks = calendar.monthcalendar(year, month)
    while len(weeks) < 6:
        weeks.append([0] * 7)

    for week in weeks:
        for day in week:
            if day == 0:
                kb.button(text=" ", callback_data="ignore")
            else:
                text_day = f"🔵{day}" if (day == today.day and month == today.month and year == today.year) else str(day)
                kb.button(text=text_day, callback_data=f"perm:{phase}:day:{year}:{month}:{day}")

    # frecce di navigazione (due bottoni)
    kb.button(text="◀️", callback_data=f"perm:{phase}:nav:{year}:{month}:prev")
    kb.button(text="▶️", callback_data=f"perm:{phase}:nav:{year}:{month}:next")

    # costruisci righe: 1 (title), 7 (weekday names), 6 righe da 7 (mesi), 2 (frecce)
    kb.adjust(1, 7, *([7] * len(weeks)), 2)
    return kb.as_markup()

# ---------------- Handlers ----------------
@dp.message(F.text == "/start")
async def start_handler(message: Message):
    await message.answer("Benvenuto! Scegli un'opzione:", reply_markup=main_kb)

@dp.message(F.text == "🕓 Ingresso")
async def ingresso_start(message: Message, state: FSMContext):
    await state.set_state(RegistroForm.waiting_ingresso_location)
    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📍 Invia posizione", request_location=True)]],
        resize_keyboard=True
    )
    await message.answer("Invia la tua posizione per registrare l'ingresso:", reply_markup=kb)

@dp.message(RegistroForm.waiting_ingresso_location, F.location)
async def ingresso_location(message: Message, state: FSMContext):
    loc = message.location
    location_name = check_location(loc.latitude, loc.longitude)
    if location_name:
        now_local = datetime.now(TIMEZONE).strftime("%H:%M")
        if save_ingresso(message.from_user, now_local, location_name):
            await message.answer("✅ Ingresso registrato!", reply_markup=main_kb)
        else:
            await message.answer("❌ Ingresso già registrato per oggi.", reply_markup=main_kb)
    else:
        await message.answer("❌ Non sei in un luogo autorizzato.", reply_markup=main_kb)
    await state.clear()

@dp.message(F.text == "🚪 Uscita")
async def uscita_start(message: Message, state: FSMContext):
    await state.set_state(RegistroForm.waiting_uscita_location)
    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📍 Invia posizione", request_location=True)]],
        resize_keyboard=True
    )
    await message.answer("Invia la tua posizione per registrare l'uscita:", reply_markup=kb)

@dp.message(RegistroForm.waiting_uscita_location, F.location)
async def uscita_location(message: Message, state: FSMContext):
    loc = message.location
    location_name = check_location(loc.latitude, loc.longitude)
    if location_name:
        now_local = datetime.now(TIMEZONE).strftime("%H:%M")
        if save_uscita(message.from_user, now_local, location_name):
            await message.answer("✅ Uscita registrata!", reply_markup=main_kb)
        else:
            await message.answer("❌ Nessun ingresso trovato per oggi.", reply_markup=main_kb)
    else:
        await message.answer("❌ Non sei in un luogo autorizzato.", reply_markup=main_kb)
    await state.clear()

@dp.message(F.text == "📝 Richiesta permessi")
async def permessi_start(message: Message, state: FSMContext):
    await state.set_state(PermessiForm.waiting_for_start)
    now = datetime.now(TIMEZONE)
    await message.answer("📅 Seleziona data di inizio:", reply_markup=build_calendar(now.year, now.month, "start"))

@dp.callback_query(F.data.startswith("perm:"))
async def perm_calendar_handler(cb: CallbackQuery, state: FSMContext):
    parts = cb.data.split(":")
    if len(parts) < 3:
        await cb.answer()
        return
    phase, kind = parts[1], parts[2]
    if kind == "nav":
        year = int(parts[3]); month = int(parts[4]); direction = parts[5]
        if direction == "prev":
            if month == 1:
                month = 12; year -= 1
            else:
                month -= 1
        else:
            if month == 12:
                month = 1; year += 1
            else:
                month += 1
        await cb.message.edit_reply_markup(reply_markup=build_calendar(year, month, phase))
        await cb.answer()
        return

    if kind == "day":
        year = int(parts[3]); month = int(parts[4]); day = int(parts[5])
        selected = f"{year}-{month:02d}-{day:02d}"
        if phase == "start":
            await state.update_data(start_date=selected)
            await state.set_state(PermessiForm.waiting_for_end)
            await cb.message.edit_text(
                f"📅 Inizio selezionato: {selected}\nSeleziona la data di fine:",
                reply_markup=build_calendar(year, month, "end"),
            )
        elif phase == "end":
            await state.update_data(end_date=selected)
            await state.set_state(PermessiForm.waiting_for_reason)
            await cb.message.edit_text(f"📅 Fine selezionata: {selected}\nOra scrivi il motivo del permesso:")
        await cb.answer()

@dp.message(PermessiForm.waiting_for_reason)
async def permessi_reason(message: Message, state: FSMContext):
    data = await state.get_data()
    start_date = data.get("start_date")
    end_date = data.get("end_date")
    reason = message.text or ""
    if save_permesso(message.from_user, start_date, end_date, reason):
        await message.answer("✅ Permesso registrato!", reply_markup=main_kb)
    else:
        await message.answer("❌ Errore nella registrazione del permesso.", reply_markup=main_kb)
    await state.clear()

@dp.message(F.text == "📄 Riepilogo")
async def riepilogo_handler(message: Message):
    riepilogo = await get_riepilogo(message.from_user)
    if not riepilogo:
        await message.answer("❌ Nessun dato trovato nel tuo registro.", reply_markup=main_kb)
        return
    buffer = io.BytesIO(riepilogo.getvalue().encode("utf-8"))
    buffer.name = "riepilogo_registro.csv"
    buffer.seek(0)
    await bot.send_document(message.chat.id, types.InputFile(buffer, filename=buffer.name))
    await message.answer("✅ Riepilogo inviato!", reply_markup=main_kb)

# ---------------- Scheduler / Reminders ----------------
async def send_reminder(user_id: int, text: str) -> None:
    try:
        await bot.send_message(user_id, text)
        logger.info("Reminder inviato a %s", user_id)
    except Exception as e:
        logger.error("Errore invio reminder a %s: %s", user_id, e)

async def remind_ingresso() -> None:
    try:
        weekday = datetime.now(TIMEZONE).weekday()
        if weekday >= 5:
            logger.debug("skip remind_ingresso (weekend)")
            return
        today = datetime.now(TIMEZONE).strftime("%d.%m.%Y")
        logger.info("Eseguo remind_ingresso per %s", today)
        sheet = get_sheet("Registro")
        rows = sheet.get_all_values()
        if len(rows) < 2:
            logger.info("Nessun utente nel foglio.")
            return
        all_users = set(row[1] for row in rows[1:] if len(row) > 1 and row[1])
        registered_today = set(row[1] for row in rows[1:] if len(row) > 2 and row[0] == today and row[2])
        missing_users = all_users - registered_today
        logger.info("Utenti totali: %s, registrati oggi: %s, mancanti: %s", len(all_users), len(registered_today), len(missing_users))
        for user_str in missing_users:
            try:
                parts = user_str.rsplit(" | ", 1)
                if len(parts) != 2:
                    logger.warning("Formato user non valido: %s", user_str)
                    continue
                name, uid_str = parts[0], parts[1]
                user_id = int(uid_str)
                await send_reminder(user_id, f"Ciao {name}, ricorda di registrare l'ingresso 🔔")
            except Exception as e:
                logger.error("Errore reminder ingresso per %s: %s", user_str, e)
    except Exception as e:
        logger.exception("Errore remind_ingresso: %s", e)

async def remind_uscita() -> None:
    try:
        weekday = datetime.now(TIMEZONE).weekday()
        if weekday >= 5:
            logger.debug("skip remind_uscita (weekend)")
            return
        today = datetime.now(TIMEZONE).strftime("%d.%m.%Y")
        logger.info("Eseguo remind_uscita per %s", today)
        sheet = get_sheet("Registro")
        rows = sheet.get_all_values()
        if len(rows) < 2:
            logger.info("Nessun dato nel foglio.")
            return
        all_users_today = set(row[1] for row in rows[1:] if len(row) > 2 and row[0] == today and row[2])
        exited_today = set(row[1] for row in rows[1:] if len(row) > 4 and row[0] == today and row[4])
        missing_exit = all_users_today - exited_today
        logger.info("Ingressi oggi: %s, uscite registrate: %s, mancanti: %s", len(all_users_today), len(exited_today), len(missing_exit))
        for user_str in missing_exit:
            try:
                parts = user_str.rsplit(" | ", 1)
                if len(parts) != 2:
                    logger.warning("Formato user non valido: %s", user_str)
                    continue
                name, uid_str = parts[0], parts[1]
                user_id = int(uid_str)
                await send_reminder(user_id, f"Ciao {name}, non dimenticare di registrare l'uscita! 🔔")
            except Exception as e:
                logger.error("Errore reminder uscita per %s: %s", user_str, e)
    except Exception as e:
        logger.exception("Errore remind_uscita: %s", e)

async def scheduler_loop() -> None:
    global _last_ingresso_date, _last_uscita_date
    logger.info("Scheduler loop avviato (controllo ogni 30s, timezone Europe/Rome)")
    try:
        while True:
            try:
                now = datetime.now(TIMEZONE)
                hhmm = now.strftime("%H:%M")
                today_date = now.date()

                # alle 08:30 Europe/Rome -> remind_ingresso
                if hhmm == "08:30" and _last_ingresso_date != today_date:
                    logger.info("Orario 08:30: lancio remind_ingresso")
                    asyncio.create_task(remind_ingresso())
                    _last_ingresso_date = today_date

                # alle 16:00 Europe/Rome -> remind_uscita
                if hhmm == "16:00" and _last_uscita_date != today_date:
                    logger.info("Orario 16:00: lancio remind_uscita")
                    asyncio.create_task(remind_uscita())
                    _last_uscita_date = today_date

            except asyncio.CancelledError:
                logger.info("Scheduler loop cancellato, esco.")
                raise
            except Exception as e:
                logger.exception("Errore nel scheduler loop: %s", e)

            await asyncio.sleep(30)
    except asyncio.CancelledError:
        logger.info("Scheduler loop terminato (cancellato).")

# comando test per forzare i reminder
@dp.message(F.text == "/remindtest")
async def remindtest_handler(message: Message):
    await message.answer("Eseguo test reminder (ingresso + uscita).")
    asyncio.create_task(remind_ingresso())
    asyncio.create_task(remind_uscita())

# ---------------- FastAPI + lifecycle ----------------
async def on_startup() -> None:
    init_sheets()
    # start scheduler in background
    asyncio.create_task(scheduler_loop())
    logger.info("On startup completato: scheduler avviato.")

@asynccontextmanager
async def lifespan(app: FastAPI):
    await on_startup()
    yield
    logger.info("Shutdown completato.")

app = FastAPI(lifespan=lifespan)

@app.post("/webhook")
async def webhook(request: Request):
    try:
        update = types.Update.model_validate(await request.json(), context={"bot": bot})
        await dp.feed_update(bot=bot, update=update)
        return {"ok": True}
    except Exception as e:
        logger.exception("Errore nel webhook: %s", e)
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/")
async def health_check():
    return "Bot is running"

# ---------------- Main (uvicorn entrypoint) ----------------
if __name__ == "__main__":
    import uvicorn
    logger.info("Avvio uvicorn FastAPI + webhook")
    uvicorn.run("bot:app", host="0.0.0.0", port=PORT, log_level="info")
