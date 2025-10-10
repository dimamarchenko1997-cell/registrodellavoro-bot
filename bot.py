import os
import asyncio
import calendar
import json
import csv
import io
from datetime import datetime
from math import radians, sin, cos, sqrt, atan2
from aiogram import Bot, Dispatcher, F, types
from aiogram.types import Message, CallbackQuery, ReplyKeyboardMarkup, KeyboardButton
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
import aioschedule
from dotenv import load_dotenv
import logging
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
import gspread
from google.oauth2.service_account import Credentials
import pytz

# ---------------- CONFIG ----------------
load_dotenv()
TOKEN = os.getenv("BOT_TOKEN") or "PASTE_YOUR_TOKEN_HERE"
SHEET_ID = os.getenv("GOOGLE_SHEETS_ID")
CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS")
TIMEZONE = pytz.timezone("Europe/Rome")

bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
# Dispatcher con MemoryStorage per FSM
dp = Dispatcher(storage=MemoryStorage())
logging.basicConfig(level=logging.INFO)

# ---------------- Google Sheets ----------------
def get_sheet(sheet_name="Registro"):
    if not CREDENTIALS_JSON:
        raise ValueError("GOOGLE_CREDENTIALS non impostata!")
    try:
        credentials_dict = json.loads(CREDENTIALS_JSON)
        # Se la private_key è stata salvata in .env con '\n' (escape), sostituisci con newline reale
        if "private_key" in credentials_dict and isinstance(credentials_dict["private_key"], str):
            credentials_dict["private_key"] = credentials_dict["private_key"].replace("\\n", "\n")
        if "private_key" not in credentials_dict or not credentials_dict["private_key"].startswith("-----BEGIN PRIVATE KEY-----"):
            raise ValueError("Private_key malformata o mancante!")
    except json.JSONDecodeError as e:
        raise ValueError("JSON malformato: " + str(e))

    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(credentials_dict, scopes=scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SHEET_ID).worksheet(sheet_name)
    return sheet

# ---------------- Constants ----------------
WORK_LOCATIONS = {
    "Ufficio Centrale": (45.6204762, 9.2401744),
    "Iveco Cornaredo": (45.480555, 9.034716),
    "Iveco Vasto": (42.086621, 14.731960),
}
MAX_DISTANCE_METERS = 200

# ---------------- FSM ----------------
class RegistroForm(StatesGroup):
    waiting_ingresso_location = State()
    waiting_uscita_location = State()

class PermessiForm(StatesGroup):
    waiting_for_start = State()
    waiting_for_end = State()
    waiting_for_reason = State()

# ---------------- Sheets Functions ----------------
def init_sheets():
    sheet_registro = get_sheet("Registro")
    if not sheet_registro.row_values(1):
        sheet_registro.append_row(["Data", "Utente", "Ingresso ora", "Posizione ingresso", "Uscita ora", "Posizione uscita"])
    sheet_permessi = get_sheet("Permessi")
    if not sheet_permessi.row_values(1):
        sheet_permessi.append_row(["Data richiesta", "Utente", "Dal", "Al", "Motivo"])

def save_ingresso(user: types.User, time, location_name):
    try:
        sheet = get_sheet("Registro")
        now_local = datetime.now(TIMEZONE)
        today = now_local.strftime("%d.%m.%Y")
        time_local = now_local.strftime("%H:%M")
        user_id = f"{user.full_name} | {user.id}"
        rows = sheet.get_all_values()
        for row in rows[1:]:
            if len(row) > 1 and row[0] == today and row[1] == user_id:
                logging.warning(f"Ingresso già registrato per {user_id} oggi.")
                return False
        sheet.append_row([today, user_id, time_local, location_name, "", ""])
        return True
    except Exception as e:
        logging.error(f"Errore durante il salvataggio dell'ingresso: {e}")
        return False

def save_uscita(user: types.User, time, location_name):
    try:
        sheet = get_sheet("Registro")
        now_local = datetime.now(TIMEZONE)
        today = now_local.strftime("%d.%m.%Y")
        time_local = now_local.strftime("%H:%M")
        user_id = f"{user.full_name} | {user.id}"
        rows = sheet.get_all_values()
        for i, row in enumerate(rows[1:], start=2):
            if len(row) > 4 and row[0] == today and row[1] == user_id and not row[4]:
                sheet.update_cell(i, 5, time_local)
                sheet.update_cell(i, 6, location_name)
                return True
        logging.warning(f"Nessun ingresso trovato per {user_id} oggi.")
        return False
    except Exception as e:
        logging.error(f"Errore durante il salvataggio dell'uscita: {e}")
        return False

def save_permesso(user: types.User, start_date, end_date, reason):
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        if end_dt < start_dt:
            raise ValueError("La data di fine deve essere successiva o uguale alla data di inizio.")
        sheet = get_sheet("Permessi")
        now_local = datetime.now(TIMEZONE)
        today = now_local.strftime("%d.%m.%Y %H:%M")
        user_id = f"{user.full_name} | {user.id}"
        sheet.append_row([today, user_id, start_date, end_date, reason])
        return True
    except ValueError as ve:
        logging.error(f"Errore di validazione: {ve}")
        return False
    except Exception as e:
        logging.error(f"Errore durante il salvataggio del permesso: {e}")
        return False

# ---------------- Riepilogo ----------------
async def get_riepilogo(user: types.User):
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
        logging.error(f"Errore durante il recupero del riepilogo: {e}")
        return None

# ---------------- Location check ----------------
def haversine(lat1, lon1, lat2, lon2):
    R = 6371000
    dlat, dlon = radians(lat2 - lat1), radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    return R * 2 * atan2(sqrt(a), sqrt(1 - a))

def check_location(lat, lon):
    for name, (wlat, wlon) in WORK_LOCATIONS.items():
        if haversine(lat, lon, wlat, wlon) <= MAX_DISTANCE_METERS:
            return name
    return None

# ---------------- Keyboards ----------------
main_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Ingresso")],
        [KeyboardButton(text="Uscita")],
        [KeyboardButton(text="Richiesta permessi")],
        [KeyboardButton(text="Riepilogo")]
    ],
    resize_keyboard=True
)

# ---------------- Calendar ----------------
def mese_nome(month: int) -> str:
    mesi = ["Gennaio", "Febbraio", "Marzo", "Aprile", "Maggio", "Giugno",
            "Luglio", "Agosto", "Settembre", "Ottobre", "Novembre", "Dicembre"]
    return mesi[month - 1]

def build_calendar(year: int, month: int, phase: str):
    kb = InlineKeyboardBuilder()
    today = datetime.now(TIMEZONE)
    giorni = ["Lu", "Ma", "Me", "Gi", "Ve", "Sa", "Do"]
    kb.button(text=f"{mese_nome(month)} {year}", callback_data="ignore")
    for g in giorni:
        kb.button(text=g, callback_data="ignore")

    # Forza sempre 6 settimane per mantenere dimensione costante
    weeks = calendar.monthcalendar(year, month)
    while len(weeks) < 6:
        weeks.append([0] * 7)

    for week in weeks:
        for day in week:
            if day == 0:
                kb.button(text=" ", callback_data="ignore")
            else:
                text_day = f"🔵{day}" if day == today.day and month == today.month and year == today.year else str(day)
                kb.button(text=text_day, callback_data=f"perm:{phase}:day:{year}:{month}:{day}")

    kb.button(text="◀️◀️◀️", callback_data=f"perm:{phase}:nav:{year}:{month}:prev")
    kb.button(text="▶️▶️▶️", callback_data=f"perm:{phase}:nav:{year}:{month}:next")

    # Imposta larghezze fisse: 1 (titolo), 7 (intestazione), 6x7 (settimane), 2 (frecce)
    kb.adjust(1, 7, 7, 7, 7, 7, 7, 2)
    return kb.as_markup()

# ---------------- Handlers ----------------
@dp.message(F.text == "/start")
async def start_handler(message: Message):
    await message.answer("Benvenuto! Scegli un'opzione:", reply_markup=main_kb)

@dp.message(F.text == "Ingresso")
async def ingresso_start(message: Message, state: FSMContext):
    await state.set_state(RegistroForm.waiting_ingresso_location)
    kb = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="📍 Invia posizione", request_location=True)]], resize_keyboard=True)
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

@dp.message(F.text == "Uscita")
async def uscita_start(message: Message, state: FSMContext):
    await state.set_state(RegistroForm.waiting_uscita_location)
    kb = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="📍 Invia posizione", request_location=True)]], resize_keyboard=True)
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

# ---------------- Permessi ----------------
@dp.message(F.text == "Richiesta permessi")
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
        year = int(parts[3])
        month = int(parts[4])
        direction = parts[5]
        if direction == "prev":
            if month == 1:
                month = 12
                year -= 1
            else:
                month -= 1
        else:
            if month == 12:
                month = 1
                year += 1
            else:
                month += 1
        await cb.message.edit_reply_markup(reply_markup=build_calendar(year, month, phase))
        await cb.answer()
        return
    if kind == "day":
        year = int(parts[3])
        month = int(parts[4])
        day = int(parts[5])
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

# ---------------- Riepilogo ----------------
@dp.message(F.text == "Riepilogo")
async def riepilogo_handler(message: Message):
    riepilogo = await get_riepilogo(message.from_user)
    if not riepilogo:
        await message.answer("❌ Nessun dato trovato nel tuo registro.", reply_markup=main_kb)
        return
    buffer = io.BytesIO(riepilogo.getvalue().encode("utf-8"))
    buffer.name = "riepilogo_registro.csv"
    buffer.seek(0)
    # usa InputFile con BytesIO
    await bot.send_document(message.chat.id, types.InputFile(buffer, filename=buffer.name))
    await message.answer("✅ Riepilogo inviato!", reply_markup=main_kb)

# ---------------- Scheduler / Reminders ----------------
async def send_reminder(user_id: int, text: str):
    try:
        await bot.send_message(user_id, text)
        logging.info(f"Reminder inviato a {user_id}")
    except Exception as e:
        logging.error(f"Errore nell'invio reminder a {user_id}: {e}")


async def remind_ingresso():
    weekday = datetime.now(TIMEZONE).weekday()
    if weekday >= 5:
        return
    today = datetime.now(TIMEZONE).strftime("%d.%m.%Y")
    sheet = get_sheet("Registro")
    rows = sheet.get_all_values()
    if len(rows) < 2:
        return
    all_users = set(row[1] for row in rows[1:] if len(row) > 1 and row[1])
    registered_today = set(row[1] for row in rows[1:] if len(row) > 2 and row[0] == today and row[2])
    missing_users = all_users - registered_today
    for user_str in missing_users:
        try:
            name, user_id = user_str.rsplit(" | ", 1)
            await send_reminder(int(user_id), f"Ciao {name}, ricorda di registrare l'ingresso 🔔")
        except Exception as e:
            logging.error(f"Reminder ingresso fallito per {user_str}: {e}")


async def remind_uscita():
    weekday = datetime.now(TIMEZONE).weekday()
    if weekday >= 5:
        return
    today = datetime.now(TIMEZONE).strftime("%d.%m.%Y")
    sheet = get_sheet("Registro")
    rows = sheet.get_all_values()
    if len(rows) < 2:
        return
    all_users_today = set(row[1] for row in rows[1:] if len(row) > 2 and row[0] == today and row[2])
    exited_today = set(row[1] for row in rows[1:] if len(row) > 4 and row[0] == today and row[4])
    missing_exit = all_users_today - exited_today
    for user_str in missing_exit:
        try:
            name, user_id = user_str.rsplit(" | ", 1)
            await send_reminder(int(user_id), f"Ciao {name}, non dimenticare di registrare l'uscita! 🔔")
        except Exception as e:
            logging.error(f"Reminder uscita fallito per {user_str}: {e}")


async def scheduler_loop():
    """Controlla continuamente l'orario locale e lancia i reminder agli orari stabiliti."""
    global _last_ingresso_date, _last_uscita_date
    while True:
        now = datetime.now(TIMEZONE)
        hhmm = now.strftime("%H:%M")
        today = now.date()

        # Reminder ingresso (08:30)
        if hhmm == "19:45" and _last_ingresso_date != today:
            logging.info("🔔 Lancio reminder ingresso")
            asyncio.create_task(remind_ingresso())
            _last_ingresso_date = today

        # Reminder uscita (16:00)
        if hhmm == "19:50" and _last_uscita_date != today:
            logging.info("🔔 Lancio reminder uscita")
            asyncio.create_task(remind_uscita())
            _last_uscita_date = today

        await asyncio.sleep(30)  # Controlla ogni 30 secondi


async def on_startup():
    init_sheets()
    asyncio.create_task(scheduler_loop())
    logging.info("Scheduler avviato: controlla ogni 30s Europe/Rome")


# ---------------- FastAPI Setup ----------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    await on_startup()
    yield
    logging.info("Shutdown completato")

app = FastAPI(lifespan=lifespan)

@app.post("/webhook")
async def webhook(request: Request):
    try:
        update = types.Update.model_validate(await request.json(), context={"bot": bot})
        await dp.feed_update(bot=bot, update=update)
        return {"ok": True}
    except Exception as e:
        logging.error(f"Errore nel webhook: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/")
async def health_check():
    return "Bot is running"

# ---------------- Main ----------------
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
