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
from typing import Optional, List, Tuple

from dotenv import load_dotenv
import pytz

from aiogram import Bot, Dispatcher, F, types
from aiogram.types import Message, CallbackQuery, ReplyKeyboardMarkup, KeyboardButton
from aiogram.types import BufferedInputFile, FSInputFile
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
from gspread.exceptions import WorksheetNotFound
from google.oauth2.service_account import Credentials

# ---------------- CONFIG ----------------
load_dotenv()
TOKEN = os.getenv("BOT_TOKEN")  # Telegram bot token
SHEET_ID = os.getenv("GOOGLE_SHEETS_ID")  # Google Sheets ID
CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS")  # JSON string delle credenziali (opzionale)
CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE")  # oppure path a file .json
PORT = int(os.getenv("PORT", 8000))
TIMEZONE = pytz.timezone("Europe/Rome")
ALLOWED_ADMINS = os.getenv("ALLOWED_ADMINS", "")  # CSV di user id autorizzati a gestire zone; se vuoto nessuno

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
    "Unicredit Bologna": (44.486511, 11.338797),
    "Pallazzo Gallo Osimo": (43.486247, 13.484761),
    "PF Ponteggi Tolentino": (43.186848, 13.259663),
    "Unicredit Nocera Umbra": (43.116717, 12.790829),
    "Unicredit Deruta": (42.982390, 12.416994),
}
MAX_DISTANCE_METERS = 200

# Default radius for dynamic zones if not provided
DEFAULT_ZONE_RADIUS = int(os.getenv("DEFAULT_ZONE_RADIUS", str(MAX_DISTANCE_METERS)))

# Zone sheet name and in-memory cache
ZONES_SHEET_NAME = "Zone"
_zones_cache: List[Tuple[str, float, float, float]] = []  # (name, lat, lon, radius_m)

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
        # Inizializza/crea sheet Zone se mancante
        try:
            sheet_zone = get_sheet(ZONES_SHEET_NAME)
            if not sheet_zone.row_values(1):
                sheet_zone.append_row(["Nome", "Latitudine", "Longitudine", "Raggio (m)"])
        except WorksheetNotFound:
            # crea worksheet "Zone" e imposta header
            try:
                if not (CREDENTIALS_JSON or CREDENTIALS_FILE):
                    raise ValueError("Devi impostare credenziali Google per creare lo sheet Zone.")
                if CREDENTIALS_JSON:
                    credentials_dict = json.loads(CREDENTIALS_JSON)
                    if "private_key" in credentials_dict and isinstance(credentials_dict["private_key"], str):
                        credentials_dict["private_key"] = credentials_dict["private_key"].replace("\\n", "\n")
                    scope = [
                        "https://www.googleapis.com/auth/spreadsheets",
                        "https://www.googleapis.com/auth/drive",
                    ]
                    creds = Credentials.from_service_account_info(credentials_dict, scopes=scope)
                else:
                    scope = [
                        "https://www.googleapis.com/auth/spreadsheets",
                        "https://www.googleapis.com/auth/drive",
                    ]
                    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scope)
                client = gspread.authorize(creds)
                sh = client.open_by_key(SHEET_ID)
                sheet_zone = sh.add_worksheet(title=ZONES_SHEET_NAME, rows=100, cols=4)
                sheet_zone.append_row(["Nome", "Latitudine", "Longitudine", "Raggio (m)"])
                logger.info("Creato worksheet Zone con header.")
            except Exception as ce:
                logger.error("Errore creazione sheet Zone: %s", ce)
        except Exception as e:
            logger.error("Impossibile inizializzare lo sheet Zone: %s", e)
        logger.info("Sheets inizializzati (Registro, Permessi, Zone).")
    except Exception as e:
        logger.error("Errore init_sheets: %s", e)

def _parse_admin_ids(env_value: str) -> set[int]:
    ids: set[int] = set()
    for part in env_value.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            ids.add(int(part))
        except ValueError:
            logger.warning("ID admin non numerico in ALLOWED_ADMINS: %s", part)
    return ids

def user_can_manage_zones(user_id: int) -> bool:
    allowed_ids = _parse_admin_ids(ALLOWED_ADMINS)
    # Se la lista è vuota, nessuno è autorizzato finché non viene configurato
    if not allowed_ids:
        return False
    return user_id in allowed_ids

def load_zones_from_sheet() -> List[Tuple[str, float, float, float]]:
    try:
        sheet = get_sheet(ZONES_SHEET_NAME)
        rows = sheet.get_all_values()
        result: List[Tuple[str, float, float, float]] = []
        for row in rows[1:]:
            if len(row) < 3:
                continue
            name = (row[0] or "").strip()
            if not name:
                continue
            try:
                lat = float(row[1])
                lon = float(row[2])
                radius = float(row[3]) if len(row) > 3 and row[3] else float(DEFAULT_ZONE_RADIUS)
            except Exception:
                logger.warning("Riga Zone non valida: %s", row)
                continue
            result.append((name, lat, lon, radius))
        return result
    except Exception as e:
        logger.error("Errore caricando le Zone: %s", e)
        return []

def refresh_zones_cache() -> None:
    global _zones_cache
    _zones_cache = load_zones_from_sheet()
    logger.info("Zone caricate in cache: %s", len(_zones_cache))

def add_zone(name: str, lat: float, lon: float, radius_m: float) -> bool:
    try:
        if radius_m <= 0:
            radius_m = float(DEFAULT_ZONE_RADIUS)
        sheet = get_sheet(ZONES_SHEET_NAME)
        sheet.append_row([name, lat, lon, radius_m])
        # aggiorna cache in memoria
        _zones_cache.append((name, float(lat), float(lon), float(radius_m)))
        logger.info("Zona aggiunta: %s (%.6f, %.6f) r=%sm", name, lat, lon, radius_m)
        return True
    except Exception as e:
        logger.exception("Errore aggiunta zona: %s", e)
        return False

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
    # 1) Verifica zone dinamiche da Google Sheets
    zones = _zones_cache if _zones_cache else []
    for name, wlat, wlon, radius_m in zones:
        if haversine(lat, lon, wlat, wlon) <= float(radius_m):
            return name
    # 2) Fallback alle zone statiche hardcoded
    for name, (wlat, wlon) in WORK_LOCATIONS.items():
        if haversine(lat, lon, wlat, wlon) <= MAX_DISTANCE_METERS:
            return name
    return None

# ---------------- Keyboards ----------------
def build_main_kb_for_user(user_id: int) -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text="🕓 Ingresso")],
        [KeyboardButton(text="🚪 Uscita")],
        [KeyboardButton(text="📝 Richiesta permessi")],
        [KeyboardButton(text="📄 Riepilogo")],
    ]
    if user_can_manage_zones(user_id):
        rows.append([KeyboardButton(text="➕ Aggiungi zona")])
    rows.append([KeyboardButton(text="📘 Istruzioni Bot")])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)

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
    kb = build_main_kb_for_user(message.from_user.id)
    await message.answer("Benvenuto! Scegli un'opzione:", reply_markup=kb)

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
            await message.answer("✅ Ingresso registrato!", reply_markup=build_main_kb_for_user(message.from_user.id))
        else:
            await message.answer("❌ Ingresso già registrato per oggi.", reply_markup=build_main_kb_for_user(message.from_user.id))
    else:
        await message.answer("❌ Non sei in un luogo autorizzato.", reply_markup=build_main_kb_for_user(message.from_user.id))
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
            await message.answer("✅ Uscita registrata!", reply_markup=build_main_kb_for_user(message.from_user.id))
        else:
            await message.answer("❌ Nessun ingresso trovato per oggi.", reply_markup=build_main_kb_for_user(message.from_user.id))
    else:
        await message.answer("❌ Non sei in un luogo autorizzato.", reply_markup=build_main_kb_for_user(message.from_user.id))
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
        await message.answer("✅ Permesso registrato!", reply_markup=build_main_kb_for_user(message.from_user.id))
    else:
        await message.answer("❌ Errore nella registrazione del permesso.", reply_markup=build_main_kb_for_user(message.from_user.id))
    await state.clear()

@dp.message(F.text == "📄 Riepilogo")
async def riepilogo_handler(message: Message):
    riepilogo = await get_riepilogo(message.from_user)
    if not riepilogo:
        await message.answer("❌ Nessun dato trovato nel tuo registro.", reply_markup=build_main_kb_for_user(message.from_user.id))
        return

    # converti StringIO a BytesIO e invia con BufferedInputFile
    csv_bytes = riepilogo.getvalue().encode("utf-8")
    buffer = io.BytesIO(csv_bytes)
    buffer.seek(0)
    input_file = BufferedInputFile(buffer.getvalue(), filename="riepilogo_registro.csv")

    try:
        await bot.send_document(chat_id=message.chat.id, document=input_file)
        await message.answer("✅ Riepilogo inviato!", reply_markup=build_main_kb_for_user(message.from_user.id))
    except Exception as e:
        logger.exception("Errore invio riepilogo: %s", e)
        await message.answer("❌ Errore nell'invio del riepilogo. Contatta l'amministratore.", reply_markup=build_main_kb_for_user(message.from_user.id))
    finally:
        buffer.close()

@dp.message(F.text == "📘 Istruzioni Bot")
async def istruzioni_handler(message: Message):
    istruzioni_text = """<b>🔹 Come utilizzare il bot</b>
<b>Avvio</b>
Apri la chat con il bot e invia il comando /start.
Ti verrà mostrato un menu con le seguenti opzioni:
🕓 Ingresso
🚪 Uscita
📝 Richiesta permessi
📄 Riepilogo
<b>Registrazione ingresso</b>
Premi “Ingresso”.
Il bot ti chiederà di inviare la tua posizione (📍).
Dopo l’invio, il sistema verifica che tu sia in una delle sedi autorizzate e registra data, ora e posizione.
<b>Registrazione uscita</b>
Premi “Uscita” e invia la posizione come sopra.
Il bot aggiorna il tuo registro giornaliero con l’orario di uscita.
<b>Richiesta permessi</b>
Seleziona “Richiesta permessi” e scegli le date dal calendario.
Inserisci il motivo: ferie, malattia, permesso, ecc.
<b>Riepilogo personale</b>
Puoi richiedere un riepilogo completo dei tuoi ingressi e uscite in formato CSV.

<b>🔹 Funzionamento della geolocalizzazione</b>
📍 Il bot NON traccia mai la posizione in automatico.
La localizzazione viene utilizzata solo quando l’utente la invia manualmente durante la registrazione di ingresso o uscita.
✅ Dati registrati:
Data e ora dell’azione
Nome e ID Telegram
Luogo riconosciuto (es. Ufficio Centrale, Iveco Cornaredo…)
Coordinate GPS (latitudine e longitudine)
I dati servono esclusivamente a confermare la presenza sul posto di lavoro e a garantire la correttezza delle registrazioni.
❌ Il bot non raccoglie posizione in background, non effettua tracciamenti continui e non utilizza i dati per altre finalità.

<b>🔹 Tutela della privacy</b>
Questo sistema è conforme al Regolamento Europeo GDPR (UE 2016/679) e rispetta i principi di:
Trasparenza: i dipendenti sanno quali dati vengono raccolti e perché.
Minimizzazione: vengono registrati solo i dati strettamente necessari.
Limitazione temporale: i dati sono conservati solo per il periodo richiesto per la gestione presenze.
Sicurezza: l’accesso ai dati su Google Sheets è riservato ai soli responsabili autorizzati.

<b>🔹 Domande e assistenza</b>
Per problemi tecnici o chiarimenti sulla privacy, contattare:
📧 sserviceitalia@gmail.com - Shust Dmytro (3298333622)
"""
    await message.answer(istruzioni_text, reply_markup=build_main_kb_for_user(message.from_user.id))

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

# ---------------- Gestione Zone (FSM) ----------------
class ZonaForm(StatesGroup):
    waiting_for_name = State()
    waiting_for_location = State()
    waiting_for_radius = State()

@dp.message(F.text == "➕ Aggiungi zona")
async def add_zone_start(message: Message, state: FSMContext):
    if not user_can_manage_zones(message.from_user.id):
        await message.answer("❌ Non sei autorizzato ad aggiungere zone.")
        return
    await state.set_state(ZonaForm.waiting_for_name)
    await message.answer("Inserisci il nome della zona (es. Ufficio Centrale):")

@dp.message(ZonaForm.waiting_for_name)
async def add_zone_wait_location(message: Message, state: FSMContext):
    zone_name = (message.text or "").strip()
    if not zone_name:
        await message.answer("Nome non valido. Riprova.")
        return
    await state.update_data(zone_name=zone_name)
    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📍 Invia posizione", request_location=True)]],
        resize_keyboard=True
    )
    await state.set_state(ZonaForm.waiting_for_location)
    await message.answer("Ora invia la posizione per la zona:", reply_markup=kb)

@dp.message(ZonaForm.waiting_for_location, F.location)
async def add_zone_wait_radius(message: Message, state: FSMContext):
    loc = message.location
    await state.update_data(lat=loc.latitude, lon=loc.longitude)
    await state.set_state(ZonaForm.waiting_for_radius)
    await message.answer(
        f"Inserisci il raggio in metri (invio vuoto per {DEFAULT_ZONE_RADIUS} m):",
        reply_markup=build_main_kb_for_user(message.from_user.id),
    )

@dp.message(ZonaForm.waiting_for_location)
async def add_zone_wait_location_invalid(message: Message):
    await message.answer("Per favore usa il pulsante '📍 Invia posizione' per inviare la posizione della zona.")

@dp.message(ZonaForm.waiting_for_radius)
async def add_zone_finalize(message: Message, state: FSMContext):
    data = await state.get_data()
    zone_name = data.get("zone_name", "")
    lat = float(data.get("lat"))
    lon = float(data.get("lon"))
    radius_text = (message.text or "").strip()
    try:
        radius = float(radius_text) if radius_text else float(DEFAULT_ZONE_RADIUS)
        if radius <= 0:
            radius = float(DEFAULT_ZONE_RADIUS)
    except Exception:
        radius = float(DEFAULT_ZONE_RADIUS)
    if add_zone(zone_name, lat, lon, radius):
        await message.answer(
            f"✅ Zona aggiunta: {zone_name}\nLat: {lat:.6f}, Lon: {lon:.6f}\nRaggio: {int(radius)} m",
            reply_markup=build_main_kb_for_user(message.from_user.id),
        )
    else:
        await message.answer("❌ Errore durante l'aggiunta della zona.", reply_markup=build_main_kb_for_user(message.from_user.id))
    await state.clear()

# ---------------- FastAPI + lifecycle ----------------
async def on_startup() -> None:
    init_sheets()
    # carica cache zone
    refresh_zones_cache()
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
