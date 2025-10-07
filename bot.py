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
TIMEZONE = pytz.timezone('Europe/Rome')

bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
logging.basicConfig(level=logging.INFO)

# ---------------- Google Sheets ----------------
def get_credentials():
    if not CREDENTIALS_JSON:
        raise ValueError("GOOGLE_CREDENTIALS non impostata!")
    credentials_dict = json.loads(CREDENTIALS_JSON)
    if "private_key" in credentials_dict:
        credentials_dict["private_key"] = credentials_dict["private_key"].replace("\\n", "\n")
    scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_info(credentials_dict, scopes=scope)
    return creds

def get_sheet(sheet_name="Registro"):
    creds = get_credentials()
    client = gspread.authorize(creds)
    return client.open_by_key(SHEET_ID).worksheet(sheet_name)

def init_sheets():
    for name, headers in [("Registro", ["Data", "Utente", "Ingresso ora", "Posizione ingresso", "Uscita ora", "Posizione uscita"]),
                          ("Permessi", ["Data richiesta", "Utente", "Dal", "Al", "Motivo"])]:
        sheet = get_sheet(name)
        if not sheet.row_values(1):
            sheet.append_row(headers)

# ---------------- Constants ----------------
WORK_LOCATIONS = {
    "Ufficio Centrale": (45.6204762, 9.2401744),
    "Iveco Cornaredo": (45.480555, 9.034716),
    "Iveco Vasto": (42.086621, 14.731960)
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

# ---------------- Utils ----------------
def get_now():
    return datetime.now(TIMEZONE)

def haversine(lat1, lon1, lat2, lon2):
    R = 6371000
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return R * c

def check_location(lat, lon):
    for name, (wlat, wlon) in WORK_LOCATIONS.items():
        if haversine(lat, lon, wlat, wlon) <= MAX_DISTANCE_METERS:
            return name
    return None

def format_user(user: types.User):
    return f"{user.full_name} | {user.id}"

# ---------------- Sheets Operations ----------------
def save_ingresso(user: types.User, location_name: str):
    try:
        sheet = get_sheet("Registro")
        today = get_now().strftime("%d.%m.%Y")
        time_local = get_now().strftime("%H:%M")
        user_id = format_user(user)
        rows = sheet.get_all_values()
        for row in rows[1:]:
            if row[0] == today and row[1] == user_id:
                logging.warning(f"Ingresso già registrato per {user_id}")
                return False
        sheet.append_row([today, user_id, time_local, location_name, "", ""])
        return True
    except Exception as e:
        logging.error(f"Errore save_ingresso: {e}")
        return False

def save_uscita(user: types.User, location_name: str):
    try:
        sheet = get_sheet("Registro")
        today = get_now().strftime("%d.%m.%Y")
        time_local = get_now().strftime("%H:%M")
        user_id = format_user(user)
        rows = sheet.get_all_values()
        for i, row in enumerate(rows[1:], start=2):
            if row[0] == today and row[1] == user_id and not row[4]:
                sheet.update(f"E{i}:F{i}", [[time_local, location_name]])
                return True
        logging.warning(f"Nessun ingresso trovato per {user_id} oggi")
        return False
    except Exception as e:
        logging.error(f"Errore save_uscita: {e}")
        return False

def save_permesso(user: types.User, start_date, end_date, reason):
    try:
        if datetime.strptime(end_date, "%Y-%m-%d") < datetime.strptime(start_date, "%Y-%m-%d"):
            raise ValueError("Data fine minore di data inizio")
        sheet = get_sheet("Permessi")
        today = get_now().strftime("%d.%m.%Y %H:%M")
        user_id = format_user(user)
        sheet.append_row([today, user_id, start_date, end_date, reason])
        return True
    except Exception as e:
        logging.error(f"Errore save_permesso: {e}")
        return False

async def get_riepilogo(user: types.User):
    try:
        sheet = get_sheet("Registro")
        rows = sheet.get_all_values()
        user_id = format_user(user)
        user_rows = [row for row in rows if row[1] == user_id]
        if not user_rows:
            return None
        output = io.StringIO(newline='')
        writer = csv.writer(output)
        writer.writerow(rows[0])
        writer.writerows(user_rows)
        output.seek(0)
        return output
    except Exception as e:
        logging.error(f"Errore get_riepilogo: {e}")
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

def build_calendar(year: int, month: int, phase: str):
    kb = InlineKeyboardBuilder()
    today = get_now()
    giorni = ["Lu", "Ma", "Me", "Gi", "Ve", "Sa", "Do"]

    kb.button(text=f"{calendar.month_name[month]} {year}", callback_data="noop")
    for g in giorni:
        kb.button(text=g, callback_data="noop")

    for week in calendar.monthcalendar(year, month):
        for day in week:
            if day == 0:
                kb.button(text=" ", callback_data="noop")
            else:
                text_day = f"🔵{day}" if day == today.day and month == today.month and year == today.year else str(day)
                kb.button(text=text_day, callback_data=f"perm:{phase}:day:{year}:{month}:{day}")

    kb.button(text="◀️", callback_data=f"perm:{phase}:nav:{year}:{month}:prev")
    kb.button(text="▶️", callback_data=f"perm:{phase}:nav:{year}:{month}:next")
    kb.adjust(1, 7, *(7 for _ in calendar.monthcalendar(year, month)), 2)
    return kb.as_markup()

# ---------------- Handlers ----------------
@dp.message(F.text == "/start")
async def start_handler(message: Message):
    await message.answer("Benvenuto! Scegli un'opzione:", reply_markup=main_kb)

@dp.message(F.text == "Ingresso")
async def ingresso_start(message: Message, state: FSMContext):
    await state.set_state(RegistroForm.waiting_ingresso_location)
    kb = ReplyKeyboardMarkup([[KeyboardButton(text="📍 Invia posizione", request_location=True)]], resize_keyboard=True)
    await message.answer("Invia la tua posizione per registrare l'ingresso:", reply_markup=kb)

@dp.message(RegistroForm.waiting_ingresso_location, F.location)
async def ingresso_location(message: Message, state: FSMContext):
    loc_name = check_location(message.location.latitude, message.location.longitude)
    if loc_name:
        if save_ingresso(message.from_user, loc_name):
            await message.answer("✅ Ingresso registrato!", reply_markup=main_kb)
        else:
            await message.answer("❌ Ingresso già registrato.", reply_markup=main_kb)
    else:
        await message.answer("❌ Luogo non autorizzato.", reply_markup=main_kb)
    await state.clear()

@dp.message(F.text == "Uscita")
async def uscita_start(message: Message, state: FSMContext):
    await state.set_state(RegistroForm.waiting_uscita_location)
    kb = ReplyKeyboardMarkup([[KeyboardButton(text="📍 Invia posizione", request_location=True)]], resize_keyboard=True)
    await message.answer("Invia la tua posizione per registrare l'uscita:", reply_markup=kb)

@dp.message(RegistroForm.waiting_uscita_location, F.location)
async def uscita_location(message: Message, state: FSMContext):
    loc_name = check_location(message.location.latitude, message.location.longitude)
    if loc_name:
        if save_uscita(message.from_user, loc_name):
            await message.answer("✅ Uscita registrata!", reply_markup=main_kb)
        else:
            await message.answer("❌ Nessun ingresso trovato.", reply_markup=main_kb)
    else:
        await message.answer("❌ Luogo non autorizzato.", reply_markup=main_kb)
    await state.clear()

# ---- Permessi ----
@dp.message(F.text == "Richiesta permessi")
async def permessi_start(message: Message, state: FSMContext):
    await state.set_state(PermessiForm.waiting_for_start)
    now = get_now()
    await message.answer("📅 Seleziona data di inizio:", reply_markup=build_calendar(now.year, now.month, "start"))

@dp.callback_query(F.data.startswith("perm:"))
async def perm_calendar_handler(cb: CallbackQuery, state: FSMContext):
    parts = cb.data.split(":")
    if len(parts) < 3:
        await cb.answer()
        return
    phase, kind = parts[1], parts[2]
    if kind == "nav":
        year, month, direction = int(parts[3]), int(parts[4]), parts[5]
        month = month-1 if direction=="prev" else month+1
        if month < 1: month, year = 12, year-1
        if month > 12: month, year = 1, year+1
        await cb.message.edit_reply_markup(build_calendar(year, month, phase))
        await cb.answer()
        return
    if kind == "day":
        year, month, day = int(parts[3]), int(parts[4]), int(parts[5])
        selected = f"{year}-{month:02d}-{day:02d}"
        if phase == "start":
            await state.update_data(start_date=selected)
            await state.set_state(PermessiForm.waiting_for_end)
            await cb.message.edit_text(f"📅 Inizio: {selected}\nSeleziona data di fine:", reply_markup=build_calendar(year, month, "end"))
        elif phase == "end":
            await state.update_data(end_date=selected)
            await state.set_state(PermessiForm.waiting_for_reason)
            await cb.message.edit_text(f"📅 Fine: {selected}\nScrivi il motivo:")
        await cb.answer()

@dp.message(PermessiForm.waiting_for_reason)
async def permessi_reason(message: Message, state: FSMContext):
    data = await state.get_data()
    if save_permesso(message.from_user, data.get("start_date"), data.get("end_date"), message.text):
        await message.answer("✅ Permesso registrato!", reply_markup=main_kb)
    else:
        await message.answer("❌ Errore registrazione permesso.", reply_markup=main_kb)
    await state.clear()

# ---- Riepilogo ----
@dp.message(F.text == "Riepilogo")
async def riepilogo_handler(message: Message):
    csv_file = await get_riepilogo(message.from_user)
    if not csv_file:
        await message.answer("❌ Nessun dato trovato.", reply_markup=main_kb)
        return
    buffer = io.BytesIO(csv_file.getvalue().encode('utf-8'))
    buffer.name = "riepilogo_registro.csv"
    buffer.seek(0)
    await bot.send_document(message.chat.id, types.BufferedInputFile(buffer.read(), filename="riepilogo_registro.csv"))
    await message.answer("✅ Riepilogo inviato!", reply_markup=main_kb)

# ---------------- Scheduler ----------------
async def notify_missing_ingresso():
    today = get_now().strftime("%d.%m.%Y")
    try:
        sheet = get_sheet("Registro")
        rows = sheet.get_all_values()
        registered_users = {row[1] for row in rows[1:] if row[0] == today}
        logging.info(f"[NOTIFY] Utenti con ingresso oggi ({today}): {registered_users}")
    except Exception as e:
        logging.error(f"Errore notify_missing_ingresso: {e}")

async def scheduler():
    while True:
        await aioschedule.run_pending()
        await asyncio.sleep(60)

async def on_startup():
    init_sheets()
    for day in ["monday","tuesday","wednesday","thursday","friday"]:
        getattr(aioschedule.every(), day).at("09:00").do(notify_missing_ingresso)
    asyncio.create_task(scheduler())
    logging.info("🚀 Bot avviato")

# ---------------- FastAPI ----------------
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
        logging.error(f"Errore webhook: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/")
async def health_check():
    return "Bot is running"

# ---------------- Main ----------------
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
