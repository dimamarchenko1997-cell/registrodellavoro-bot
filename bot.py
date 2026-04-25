import os
import asyncio
import calendar
import json
import csv
import io
import logging
from datetime import datetime, date
from math import radians, sin, cos, sqrt, atan2
from typing import Optional, Dict, Tuple

from dotenv import load_dotenv
import pytz

from aiogram import Bot, Dispatcher, F, types
from aiogram.types import Message, CallbackQuery, ReplyKeyboardMarkup, KeyboardButton, BufferedInputFile
from aiogram.client.default import DefaultBotProperties
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage

from fastapi import FastAPI, Request
from contextlib import asynccontextmanager

import gspread
from gspread.worksheet import Worksheet
from google.oauth2.service_account import Credentials

# ============================================================
# CONFIG
# ============================================================
load_dotenv()

TOKEN = os.getenv("BOT_TOKEN")
SHEET_ID = os.getenv("GOOGLE_SHEETS_ID")
CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS")
CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
PORT = int(os.getenv("PORT", 8000))
TIMEZONE = pytz.timezone("Europe/Rome")

if not TOKEN:
    raise RuntimeError("BOT_TOKEN non impostato nelle variabili d'ambiente.")

if not SHEET_ID:
    raise RuntimeError("GOOGLE_SHEETS_ID non impostato nelle variabili d'ambiente.")

ADMINS = {614102287}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher(storage=MemoryStorage())

_sent_ingresso_today: Dict[int, date] = {}
_sent_uscita_today: Dict[int, date] = {}

# ============================================================
# Google Sheets client con caching
# ============================================================
_gspread_client: Optional[gspread.Client] = None


def _get_client() -> gspread.Client:
    global _gspread_client

    if _gspread_client is not None:
        return _gspread_client

    if not (CREDENTIALS_JSON or CREDENTIALS_FILE):
        raise ValueError("Devi impostare GOOGLE_CREDENTIALS o GOOGLE_CREDENTIALS_FILE.")

    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    if CREDENTIALS_JSON:
        credentials_dict = json.loads(CREDENTIALS_JSON)
        if "private_key" in credentials_dict and isinstance(credentials_dict["private_key"], str):
            credentials_dict["private_key"] = credentials_dict["private_key"].replace("\\n", "\n")
        creds = Credentials.from_service_account_info(credentials_dict, scopes=scope)
    else:
        creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scope)

    _gspread_client = gspread.authorize(creds)
    return _gspread_client


def get_sheet(sheet_name: str = "Registro") -> Worksheet:
    global _gspread_client

    try:
        return _get_client().open_by_key(SHEET_ID).worksheet(sheet_name)
    except gspread.exceptions.APIError as e:
        status_code = getattr(getattr(e, "response", None), "status_code", None)
        if status_code == 401:
            _gspread_client = None
            logger.warning("Token gspread scaduto, client resettato.")
        logger.exception("Errore aprendo il foglio '%s': %s", sheet_name, e)
        raise
    except Exception as e:
        logger.exception("Errore aprendo il foglio '%s': %s", sheet_name, e)
        raise


# ============================================================
# Zone lavoro cache
# ============================================================
_work_locations_cache: Optional[Dict[str, Tuple[float, float]]] = None
_work_locations_cache_time: Optional[datetime] = None
_CACHE_TTL_SECONDS = 300

WORK_LOCATIONS = {
    "Ufficio Centrale": (45.6204762, 9.2401744),
}
MAX_DISTANCE_METERS = 200


def get_work_locations() -> Dict[str, Tuple[float, float]]:
    global _work_locations_cache, _work_locations_cache_time

    now = datetime.now(TIMEZONE)
    cache_valid = (
        _work_locations_cache is not None
        and _work_locations_cache_time is not None
        and (now - _work_locations_cache_time).total_seconds() < _CACHE_TTL_SECONDS
    )
    if cache_valid:
        return _work_locations_cache

    try:
        sheet = get_sheet("ZoneLavoro")
        rows = sheet.get_all_values()
        locs: Dict[str, Tuple[float, float]] = {}

        for row in rows[1:]:
            if len(row) >= 3:
                name = row[0].strip()
                if not name:
                    continue
                try:
                    lat, lon = float(row[1]), float(row[2])
                except ValueError:
                    continue
                locs[name] = (lat, lon)

        result = locs if locs else WORK_LOCATIONS.copy()
        _work_locations_cache = result
        _work_locations_cache_time = now
        return result
    except Exception as e:
        logger.warning("Impossibile leggere ZoneLavoro, uso fallback statico: %s", e)
        return WORK_LOCATIONS.copy()


def _invalidate_locations_cache() -> None:
    global _work_locations_cache, _work_locations_cache_time
    _work_locations_cache = None
    _work_locations_cache_time = None


def save_new_zone(name: str, lat: float, lon: float) -> bool:
    try:
        sheet = get_sheet("ZoneLavoro")
        sheet.append_row([name, str(lat), str(lon)])
        _invalidate_locations_cache()
        return True
    except Exception as e:
        logger.exception("Errore salvataggio zona: %s", e)
        return False


def update_zone_name(old_name: str, new_name: str) -> bool:
    try:
        sheet = get_sheet("ZoneLavoro")
        rows = sheet.get_all_values()
        for i, row in enumerate(rows[1:], start=2):
            if len(row) >= 3 and row[0] == old_name:
                sheet.update_cell(i, 1, new_name)
                _invalidate_locations_cache()
                return True
        return False
    except Exception as e:
        logger.exception("Errore aggiornamento zona: %s", e)
        return False


def delete_zone(name: str) -> bool:
    try:
        sheet = get_sheet("ZoneLavoro")
        rows = sheet.get_all_values()
        for i, row in enumerate(rows[1:], start=2):
            if len(row) >= 3 and row[0] == name:
                sheet.delete_rows(i)
                _invalidate_locations_cache()
                return True
        return False
    except Exception as e:
        logger.exception("Errore rimozione zona: %s", e)
        return False


# ============================================================
# Operazioni Registro / Permessi
# ============================================================
async def async_save_ingresso(user: types.User, time_str: str, location_name: str) -> bool:
    return await asyncio.to_thread(_sync_save_ingresso, user, time_str, location_name)


def _sync_save_ingresso(user: types.User, time_str: str, location_name: str) -> bool:
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
        upsert_user_notifiche(user.id, user.full_name)
        return True
    except Exception as e:
        logger.exception("Errore save_ingresso: %s", e)
        return False


async def async_save_uscita(user: types.User, time_str: str, location_name: str) -> bool:
    return await asyncio.to_thread(_sync_save_uscita, user, time_str, location_name)


def _sync_save_uscita(user: types.User, time_str: str, location_name: str) -> bool:
    try:
        sheet = get_sheet("Registro")
        now_local = datetime.now(TIMEZONE)
        today = now_local.strftime("%d.%m.%Y")
        user_id = f"{user.full_name} | {user.id}"
        rows = sheet.get_all_values()

        for i, row in enumerate(rows[1:], start=2):
            if len(row) > 4 and row[0] == today and row[1] == user_id and not row[4]:
                col_e = gspread.utils.rowcol_to_a1(i, 5)
                col_f = gspread.utils.rowcol_to_a1(i, 6)
                sheet.batch_update([{
                    "range": f"{col_e}:{col_f}",
                    "values": [[time_str, location_name]],
                }])
                return True

        logger.warning("Nessun ingresso trovato per %s oggi.", user_id)
        return False
    except Exception as e:
        logger.exception("Errore save_uscita: %s", e)
        return False


async def async_save_permesso(user: types.User, start_date: str, end_date: str, reason: str) -> bool:
    return await asyncio.to_thread(_sync_save_permesso, user, start_date, end_date, reason)


def _sync_save_permesso(user: types.User, start_date: str, end_date: str, reason: str) -> bool:
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


async def get_riepilogo(user: types.User, year: int, month: int) -> Optional[io.StringIO]:
    return await asyncio.to_thread(_sync_get_riepilogo, user, year, month)


def _sync_get_riepilogo(user: types.User, year: int, month: int) -> Optional[io.StringIO]:
    try:
        sheet = get_sheet("Registro")
        rows = sheet.get_all_values()
        user_id = f"{user.full_name} | {user.id}"
        month_filter = f"{month:02d}.{year}"
        user_rows = [
            row for row in rows[1:]
            if len(row) > 1
            and row[1] == user_id
            and len(row[0]) >= 7
            and row[0][3:10] == month_filter
        ]

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


def init_sheets() -> None:
    try:
        sheet_reg = get_sheet("Registro")
        if not sheet_reg.row_values(1):
            sheet_reg.append_row(["Data", "Utente", "Ingresso ora", "Posizione ingresso", "Uscita ora", "Posizione uscita"])

        sheet_perm = get_sheet("Permessi")
        if not sheet_perm.row_values(1):
            sheet_perm.append_row(["Data richiesta", "Utente", "Dal", "Al", "Motivo"])

        try:
            sheet_zone = get_sheet("ZoneLavoro")
            if not sheet_zone.row_values(1):
                sheet_zone.append_row(["Nome", "Latitudine", "Longitudine"])
        except Exception:
            logger.exception("Errore inizializzazione ZoneLavoro")

        try:
            sheet_notif = get_sheet("Notifiche")
            if not sheet_notif.row_values(1):
                sheet_notif.append_row([
                    "Telegram ID", "Nome",
                    "Reminder Ingresso", "Orario Ingresso",
                    "Reminder Uscita", "Orario Uscita",
                ])
        except Exception:
            logger.exception("Errore inizializzazione Notifiche")

        logger.info("Sheets inizializzati.")
    except Exception as e:
        logger.error("Errore init_sheets: %s", e)


# ============================================================
# Notifiche helpers
# ============================================================
def get_notifiche_settings() -> Dict[int, dict]:
    try:
        sheet = get_sheet("Notifiche")
        rows = sheet.get_all_values()
        result: Dict[int, dict] = {}

        for i, row in enumerate(rows[1:], start=2):
            if len(row) < 6 or not row[0].strip():
                continue
            try:
                uid = int(row[0].strip())
            except ValueError:
                continue

            result[uid] = {
                "nome": row[1],
                "reminder_ingresso": row[2].strip().upper() == "TRUE",
                "orario_ingresso": row[3].strip() or "08:00",
                "reminder_uscita": row[4].strip().upper() == "TRUE",
                "orario_uscita": row[5].strip() or "17:00",
                "row_index": i,
            }

        return result
    except Exception as e:
        logger.exception("Errore get_notifiche_settings: %s", e)
        return {}


def upsert_user_notifiche(
    user_id: int,
    nome: str,
    reminder_in: bool = True,
    orario_in: str = "08:00",
    reminder_out: bool = True,
    orario_out: str = "17:00",
) -> bool:
    try:
        sheet = get_sheet("Notifiche")
        rows = sheet.get_all_values()

        for row in rows[1:]:
            if row and row[0].strip() == str(user_id):
                return True

        sheet.append_row([
            str(user_id), nome,
            "TRUE" if reminder_in else "FALSE", orario_in,
            "TRUE" if reminder_out else "FALSE", orario_out,
        ])
        _invalidate_notifiche_cache()
        return True
    except Exception as e:
        logger.exception("Errore upsert_user_notifiche: %s", e)
        return False


def toggle_notifica(user_id: int, tipo: str) -> Optional[bool]:
    col = 3 if tipo == "in" else 5

    try:
        sheet = get_sheet("Notifiche")
        rows = sheet.get_all_values()

        for i, row in enumerate(rows[1:], start=2):
            if row and row[0].strip() == str(user_id):
                current = row[col - 1].strip().upper() == "TRUE"
                new_val = not current
                sheet.update_cell(i, col, "TRUE" if new_val else "FALSE")
                _invalidate_notifiche_cache()
                return new_val

        return None
    except Exception as e:
        logger.exception("Errore toggle_notifica: %s", e)
        return None


def set_orario_notifica(user_id: int, tipo: str, orario: str) -> bool:
    col = 4 if tipo == "in" else 6

    try:
        sheet = get_sheet("Notifiche")
        rows = sheet.get_all_values()

        for i, row in enumerate(rows[1:], start=2):
            if row and row[0].strip() == str(user_id):
                sheet.update_cell(i, col, orario)
                _invalidate_notifiche_cache()
                return True

        return False
    except Exception as e:
        logger.exception("Errore set_orario_notifica: %s", e)
        return False


# ============================================================
# Location utils
# ============================================================
def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371000
    dlat, dlon = radians(lat2 - lat1), radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    return R * 2 * atan2(sqrt(a), sqrt(1 - a))


def check_location(lat: float, lon: float) -> Optional[str]:
    work_locations = get_work_locations()
    for name, (wlat, wlon) in work_locations.items():
        if haversine(lat, lon, wlat, wlon) <= MAX_DISTANCE_METERS:
            return name
    return None


# ============================================================
# Keyboards
# ============================================================
main_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="🕓 Ingresso")],
        [KeyboardButton(text="🚪 Uscita")],
        [KeyboardButton(text="📝 Richiesta permessi")],
        [KeyboardButton(text="📄 Riepilogo")],
        [KeyboardButton(text="🔔 Mie Notifiche")],
        [KeyboardButton(text="📘 Istruzioni Bot")],
    ],
    resize_keyboard=True,
)

location_kb = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="📍 Invia posizione", request_location=True)]],
    resize_keyboard=True,
)


# ============================================================
# FSM States
# ============================================================
class RegistroForm(StatesGroup):
    waiting_ingresso_location = State()
    waiting_uscita_location = State()


class PermessiForm(StatesGroup):
    waiting_for_start = State()
    waiting_for_end = State()
    waiting_for_reason = State()


class AddZoneForm(StatesGroup):
    waiting_for_location = State()
    waiting_for_name = State()


class ZoneManagementForm(StatesGroup):
    waiting_for_new_name = State()


class NotificheForm(StatesGroup):
    waiting_for_orario = State()


# ============================================================
# Calendar builder
# ============================================================
def mese_nome(month: int) -> str:
    mesi = [
        "Gennaio", "Febbraio", "Marzo", "Aprile", "Maggio", "Giugno",
        "Luglio", "Agosto", "Settembre", "Ottobre", "Novembre", "Dicembre",
    ]
    return mesi[month - 1]


def build_calendar(year: int, month: int, phase: str):
    kb = InlineKeyboardBuilder()
    today = datetime.now(TIMEZONE)
    giorni = ["Lu", "Ma", "Me", "Gi", "Ve", "Sa", "Do"]

    kb.button(text=f"{mese_nome(month)} {year}", callback_data="ignore")
    for g in giorni:
        kb.button(text=g, callback_data="ignore")

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

    kb.button(text="◀️", callback_data=f"perm:{phase}:nav:{year}:{month}:prev")
    kb.button(text="▶️", callback_data=f"perm:{phase}:nav:{year}:{month}:next")
    kb.adjust(1, 7, *([7] * len(weeks)), 2)
    return kb.as_markup()


# ============================================================
# Zone UI helpers
# ============================================================
def _build_zones_markup(work_locations: Dict[str, Tuple[float, float]]):
    kb = InlineKeyboardBuilder()
    for zone_name in work_locations.keys():
        kb.button(text=f"📍 {zone_name}", callback_data=f"zone_select:{zone_name}")
    kb.button(text="➕ Aggiungi zona", callback_data="zone_add_new")
    kb.adjust(1)
    return kb.as_markup()


async def _show_zones_list(target) -> None:
    work_locations = await asyncio.to_thread(get_work_locations)
    text_vuoto = "❌ Nessuna zona trovata.\n\nAggiungi la tua prima zona di lavoro:"
    text_pieno = "📍 <b>Zone di lavoro disponibili:</b>\n\nSeleziona una zona per modificarla o rimuoverla, oppure aggiungi una nuova zona:"

    if isinstance(target, CallbackQuery):
        if not work_locations:
            kb = InlineKeyboardBuilder()
            kb.button(text="➕ Aggiungi prima zona", callback_data="zone_add_new")
            kb.adjust(1)
            await target.message.edit_text(text_vuoto, reply_markup=kb.as_markup())
        else:
            await target.message.edit_text(text_pieno, reply_markup=_build_zones_markup(work_locations))
    else:
        if not work_locations:
            kb = InlineKeyboardBuilder()
            kb.button(text="➕ Aggiungi prima zona", callback_data="zone_add_new")
            kb.adjust(1)
            await target.answer(text_vuoto, reply_markup=kb.as_markup())
        else:
            await target.answer(text_pieno, reply_markup=_build_zones_markup(work_locations))


# ============================================================
# Handlers base
# ============================================================
@dp.message(F.text == "/start")
async def start_handler(message: Message):
    await message.answer("Benvenuto! Scegli un'opzione:", reply_markup=main_kb)


@dp.message(F.text == "🕓 Ingresso")
async def ingresso_start(message: Message, state: FSMContext):
    await state.set_state(RegistroForm.waiting_ingresso_location)
    await message.answer("Invia la tua posizione per registrare l'ingresso:", reply_markup=location_kb)


@dp.message(RegistroForm.waiting_ingresso_location, F.location)
async def ingresso_location(message: Message, state: FSMContext):
    await state.clear()
    loc = message.location

    location_name = await asyncio.to_thread(check_location, loc.latitude, loc.longitude)

    if not location_name:
        await message.answer("❌ Non sei in un luogo autorizzato.", reply_markup=main_kb)
        return

    now_local = datetime.now(TIMEZONE).strftime("%H:%M")
    if await async_save_ingresso(message.from_user, now_local, location_name):
        await message.answer("✅ Ingresso registrato!", reply_markup=main_kb)
    else:
        await message.answer("❌ Ingresso già registrato per oggi.", reply_markup=main_kb)


@dp.message(F.text == "🚪 Uscita")
async def uscita_start(message: Message, state: FSMContext):
    await state.set_state(RegistroForm.waiting_uscita_location)
    await message.answer("Invia la tua posizione per registrare l'uscita:", reply_markup=location_kb)


@dp.message(RegistroForm.waiting_uscita_location, F.location)
async def uscita_location(message: Message, state: FSMContext):
    await state.clear()
    loc = message.location

    location_name = await asyncio.to_thread(check_location, loc.latitude, loc.longitude)

    if not location_name:
        await message.answer("❌ Non sei in un luogo autorizzato.", reply_markup=main_kb)
        return

    now_local = datetime.now(TIMEZONE).strftime("%H:%M")
    if await async_save_uscita(message.from_user, now_local, location_name):
        await message.answer("✅ Uscita registrata!", reply_markup=main_kb)
    else:
        await message.answer("❌ Nessun ingresso trovato per oggi.", reply_markup=main_kb)


# ============================================================
# Permessi
# ============================================================
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
        year, month, direction = int(parts[3]), int(parts[4]), parts[5]
        if direction == "prev":
            month, year = (12, year - 1) if month == 1 else (month - 1, year)
        else:
            month, year = (1, year + 1) if month == 12 else (month + 1, year)
        await cb.message.edit_reply_markup(reply_markup=build_calendar(year, month, phase))
        await cb.answer()
        return

    if kind == "day":
        year, month, day = int(parts[3]), int(parts[4]), int(parts[5])
        selected = f"{year}-{month:02d}-{day:02d}"

        if phase == "start":
            await state.update_data(start_date=selected)
            await state.set_state(PermessiForm.waiting_for_end)
            await cb.message.edit_text(
                f"📅 Inizio selezionato: <b>{selected}</b>\nSeleziona la data di fine:",
                reply_markup=build_calendar(year, month, "end"),
            )
        elif phase == "end":
            data = await state.get_data()
            start_date = data.get("start_date", "")
            if selected < start_date:
                await cb.answer("⚠️ La data di fine non può essere precedente all'inizio!", show_alert=True)
                return
            await state.update_data(end_date=selected)
            await state.set_state(PermessiForm.waiting_for_reason)
            await cb.message.edit_text(
                f"📅 Fine selezionata: <b>{selected}</b>\nOra scrivi il motivo del permesso:"
            )

        await cb.answer()


@dp.message(PermessiForm.waiting_for_reason)
async def permessi_reason(message: Message, state: FSMContext):
    data = await state.get_data()
    start_date = data.get("start_date", "")
    end_date = data.get("end_date", "")
    reason = message.text or ""
    await state.clear()

    if await async_save_permesso(message.from_user, start_date, end_date, reason):
        await message.answer("✅ Permesso registrato!", reply_markup=main_kb)
    else:
        await message.answer("❌ Errore nella registrazione del permesso.", reply_markup=main_kb)


# ============================================================
# Riepilogo
# ============================================================
def _build_year_keyboard() -> types.InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    current_year = datetime.now(TIMEZONE).year
    for y in range(current_year, current_year - 3, -1):
        label = f"🔵 {y}" if y == current_year else str(y)
        kb.button(text=label, callback_data=f"riepilogo:year:{y}")
    kb.adjust(3)
    return kb.as_markup()


def _build_month_keyboard(year: int) -> types.InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    nomi_mesi = ["Gen", "Feb", "Mar", "Apr", "Mag", "Giu", "Lug", "Ago", "Set", "Ott", "Nov", "Dic"]
    now = datetime.now(TIMEZONE)

    for i, nome in enumerate(nomi_mesi, start=1):
        is_current = year == now.year and i == now.month
        label = f"🔵 {nome}" if is_current else nome
        kb.button(text=label, callback_data=f"riepilogo:month:{year}:{i}")

    kb.button(text="🔙 Cambia anno", callback_data="riepilogo:back_year")
    kb.adjust(3, 3, 3, 3, 1)
    return kb.as_markup()


@dp.message(F.text == "📄 Riepilogo")
async def riepilogo_handler(message: Message):
    await message.answer(
        "📅 <b>Seleziona l'anno</b> per cui vuoi vedere il riepilogo presenze:",
        reply_markup=_build_year_keyboard(),
    )


@dp.callback_query(F.data == "riepilogo:back_year")
async def riepilogo_back_year(cb: CallbackQuery):
    await cb.message.edit_text(
        "📅 <b>Seleziona l'anno</b> per cui vuoi vedere il riepilogo presenze:",
        reply_markup=_build_year_keyboard(),
    )
    await cb.answer()


@dp.callback_query(F.data.startswith("riepilogo:year:"))
async def riepilogo_year_handler(cb: CallbackQuery):
    year = int(cb.data.split(":")[2])
    await cb.message.edit_text(
        f"📅 Anno selezionato: <b>{year}</b>\n\nOra seleziona il <b>mese</b>:",
        reply_markup=_build_month_keyboard(year),
    )
    await cb.answer()


@dp.callback_query(F.data.startswith("riepilogo:month:"))
async def riepilogo_month_handler(cb: CallbackQuery):
    parts = cb.data.split(":")
    year, month = int(parts[2]), int(parts[3])
    nome_mese = mese_nome(month)

    await cb.answer(f"⏳ Carico {nome_mese} {year}…")
    riepilogo = await get_riepilogo(cb.from_user, year, month)

    if not riepilogo:
        await cb.message.edit_text(
            f"❌ Nessuna registrazione trovata per <b>{nome_mese} {year}</b>.",
            reply_markup=_build_month_keyboard(year),
        )
        return

    csv_bytes = riepilogo.getvalue().encode("utf-8")
    filename = f"riepilogo_{year}_{month:02d}.csv"
    input_file = BufferedInputFile(csv_bytes, filename=filename)

    try:
        await cb.message.edit_text(f"✅ Riepilogo <b>{nome_mese} {year}</b> pronto, lo invio…")
        await bot.send_document(
            chat_id=cb.message.chat.id,
            document=input_file,
            caption=f"📄 Presenze <b>{nome_mese} {year}</b>",
        )
    except Exception as e:
        logger.exception("Errore invio riepilogo: %s", e)
        await cb.message.answer(
            "❌ Errore nell'invio del riepilogo. Contatta l'amministratore.",
            reply_markup=main_kb,
        )


# ============================================================
# Istruzioni
# ============================================================
@dp.message(F.text == "📘 Istruzioni Bot")
async def istruzioni_handler(message: Message):
    istruzioni_text = (
        "<b>📖 Guida al Bot Presenze</b>\n\n"
 
        "<b>▶️ Avvio</b>\n"
        "Invia /start per aprire il menu principale con questi tasti:\n"
        "🕓 Ingresso  🚪 Uscita  📝 Permessi  📄 Riepilogo\n\n"
 
        "<b>🕓 Registrazione ingresso</b>\n"
        "1. Premi <b>Ingresso</b>\n"
        "2. Tocca il bottone 📍 <b>Invia posizione</b>\n"
        "3. Il bot verifica che tu sia in una sede autorizzata e salva ora e luogo.\n"
        "⚠️ Puoi registrare un solo ingresso al giorno.\n\n"
 
        "<b>🚪 Registrazione uscita</b>\n"
        "1. Premi <b>Uscita</b> e invia la posizione come sopra.\n"
        "2. Il bot aggiorna il tuo registro con l'orario di uscita.\n"
        "⚠️ È necessario aver già registrato l'ingresso nella stessa giornata.\n\n"
 
        "<b>📝 Richiesta permessi</b>\n"
        "1. Premi <b>Richiesta permessi</b>\n"
        "2. Seleziona la <b>data di inizio</b> dal calendario (🔵 = oggi)\n"
        "3. Seleziona la <b>data di fine</b>\n"
        "4. Scrivi il <b>motivo</b> (es. ferie, malattia, permesso)\n"
        "La richiesta viene salvata nel foglio Permessi su Google Sheets.\n\n"
 
        "<b>📄 Riepilogo presenze</b>\n"
        "1. Premi <b>Riepilogo</b>\n"
        "2. Scegli l'<b>anno</b>\n"
        "3. Scegli il <b>mese</b>\n"
        "Riceverai un file CSV con tutti i tuoi ingressi e uscite di quel mese.\n\n"
 
        "<b>⚙️ Comando /mienotifiche</b>\n"
        "Gestisci le tue notifiche personali:\n"
        "• Attiva o disattiva il reminder di ingresso\n"
        "• Attiva o disattiva il reminder di uscita\n"
        "• Imposta l'orario desiderato per ciascuno (formato HH:MM)\n\n"
 
        "<b>📍 Geolocalizzazione e privacy</b>\n"
        "Il bot <b>NON traccia mai</b> la posizione in automatico.\n"
        "La posizione viene usata solo quando la invii tu manualmente.\n"
        "Dati registrati: data/ora · nome e ID Telegram · sede riconosciuta.\n"
        "Nessun tracciamento in background, nessuna condivisione con terzi.\n\n"
 
        "<b>🛡️ GDPR (UE 2016/679)</b>\n"
        "Trasparenza · Minimizzazione · Limitazione temporale · Sicurezza\n"
        "I dati sono accessibili solo ai responsabili autorizzati.\n\n"
 
        "<b>📧 Assistenza</b>\n"
        "sserviceitalia@gmail.com – Shust Dmytro (3298333622)"
    )
    await message.answer(istruzioni_text, reply_markup=main_kb)


# ============================================================
# Gestione Zone admin
# ============================================================
@dp.message(F.text == "/addzone")
async def addzone_start(message: Message, state: FSMContext):
    if message.from_user.id not in ADMINS:
        await message.answer("❌ Non hai i permessi per aggiungere zone.")
        return
    await state.set_state(AddZoneForm.waiting_for_location)
    await message.answer("📍 Invia la posizione della nuova zona di lavoro:", reply_markup=location_kb)


@dp.message(AddZoneForm.waiting_for_location, F.location)
async def addzone_location(message: Message, state: FSMContext):
    await state.update_data(lat=message.location.latitude, lon=message.location.longitude)
    await state.set_state(AddZoneForm.waiting_for_name)
    await message.answer(
        "✏️ Inserisci il nome della nuova zona (o scrivi <b>Annulla</b> per abortire):",
        reply_markup=main_kb,
    )


@dp.message(AddZoneForm.waiting_for_name)
async def addzone_name(message: Message, state: FSMContext):
    if (message.text or "").strip().lower() == "annulla":
        await state.clear()
        await message.answer("❌ Operazione annullata.", reply_markup=main_kb)
        return

    data = await state.get_data()
    lat, lon = data.get("lat"), data.get("lon")
    name = (message.text or "").strip()

    if lat is None or lon is None or not name:
        await message.answer("❌ Dati mancanti. Riprova con /addzone.", reply_markup=main_kb)
        await state.clear()
        return

    if await asyncio.to_thread(save_new_zone, name, lat, lon):
        kb = InlineKeyboardBuilder()
        kb.button(text="📋 Vedi tutte le zone", callback_data="zone_back")
        kb.adjust(1)
        await message.answer(
            f"✅ Zona <b>{name}</b> aggiunta!\n📍 ({lat:.6f}, {lon:.6f})",
            reply_markup=kb.as_markup(),
        )
    else:
        await message.answer("❌ Errore durante il salvataggio della zona.", reply_markup=main_kb)

    await state.clear()


@dp.message(F.text == "/listzones")
async def listzones_handler(message: Message):
    if message.from_user.id not in ADMINS:
        await message.answer("❌ Non hai i permessi per visualizzare le zone.")
        return
    try:
        await _show_zones_list(message)
    except Exception as e:
        logger.exception("Errore listzones: %s", e)
        await message.answer("❌ Errore nel caricamento delle zone.", reply_markup=main_kb)


@dp.callback_query(F.data.startswith("zone_select:"))
async def zone_select_handler(cb: CallbackQuery):
    zone_name = cb.data.split(":", 1)[1]
    kb = InlineKeyboardBuilder()
    kb.button(text="✏️ Modifica nome", callback_data=f"zone_edit:{zone_name}")
    kb.button(text="🗑️ Rimuovi zona", callback_data=f"zone_delete:{zone_name}")
    kb.button(text="🔙 Indietro", callback_data="zone_back")
    kb.adjust(1)
    await cb.message.edit_text(
        f"📍 <b>Zona selezionata:</b> {zone_name}\n\nCosa vuoi fare?",
        reply_markup=kb.as_markup(),
    )
    await cb.answer()


@dp.callback_query(F.data == "zone_add_new")
async def zone_add_new_handler(cb: CallbackQuery, state: FSMContext):
    await state.set_state(AddZoneForm.waiting_for_location)
    await cb.message.edit_text("📍 <b>Aggiungi nuova zona</b>\n\nUsa il bottone qui sotto per inviare la posizione:")
    await bot.send_message(cb.message.chat.id, "Invia la posizione:", reply_markup=location_kb)
    await cb.answer()


@dp.callback_query(F.data == "zone_back")
async def zone_back_handler(cb: CallbackQuery):
    try:
        await _show_zones_list(cb)
    except Exception as e:
        logger.exception("Errore zone_back: %s", e)
        await cb.message.edit_text("❌ Errore nel caricamento delle zone.")
    await cb.answer()


@dp.callback_query(F.data.startswith("zone_edit:"))
async def zone_edit_handler(cb: CallbackQuery, state: FSMContext):
    zone_name = cb.data.split(":", 1)[1]
    await state.update_data(editing_zone=zone_name)
    await state.set_state(ZoneManagementForm.waiting_for_new_name)
    await cb.message.edit_text(
        f"✏️ <b>Modifica zona:</b> {zone_name}\n\nInserisci il nuovo nome (o scrivi <b>Annulla</b>):"
    )
    await cb.answer()


@dp.callback_query(F.data.startswith("zone_delete:"))
async def zone_delete_handler(cb: CallbackQuery):
    zone_name = cb.data.split(":", 1)[1]
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Conferma rimozione", callback_data=f"zone_confirm_delete:{zone_name}")
    kb.button(text="❌ Annulla", callback_data=f"zone_select:{zone_name}")
    kb.adjust(1)
    await cb.message.edit_text(
        f"🗑️ <b>Conferma rimozione</b>\n\nSei sicuro di voler rimuovere <b>{zone_name}</b>?",
        reply_markup=kb.as_markup(),
    )
    await cb.answer()


@dp.callback_query(F.data.startswith("zone_confirm_delete:"))
async def zone_confirm_delete_handler(cb: CallbackQuery):
    zone_name = cb.data.split(":", 1)[1]
    if await asyncio.to_thread(delete_zone, zone_name):
        kb = InlineKeyboardBuilder()
        kb.button(text="🔙 Torna alla lista", callback_data="zone_back")
        kb.adjust(1)
        await cb.message.edit_text(
            f"✅ Zona <b>{zone_name}</b> rimossa con successo!",
            reply_markup=kb.as_markup(),
        )
    else:
        await cb.message.edit_text(f"❌ Errore nella rimozione della zona <b>{zone_name}</b>.")
    await cb.answer()


@dp.message(ZoneManagementForm.waiting_for_new_name)
async def zone_new_name_handler(message: Message, state: FSMContext):
    if (message.text or "").strip().lower() == "annulla":
        await state.clear()
        await message.answer("❌ Operazione annullata.", reply_markup=main_kb)
        return

    data = await state.get_data()
    old_name = data.get("editing_zone")
    new_name = (message.text or "").strip()

    if not old_name or not new_name:
        await message.answer("❌ Dati mancanti. Riprova con /listzones.", reply_markup=main_kb)
        await state.clear()
        return

    if await asyncio.to_thread(update_zone_name, old_name, new_name):
        await message.answer(
            f"✅ Zona rinominata!\n<b>Prima:</b> {old_name}\n<b>Dopo:</b> {new_name}",
            reply_markup=main_kb,
        )
    else:
        await message.answer(f"❌ Errore nella modifica della zona <b>{old_name}</b>.", reply_markup=main_kb)

    await state.clear()


# ============================================================
# Scheduler / Reminders
# ============================================================
async def send_reminder(user_id: int, text: str) -> None:
    try:
        await bot.send_message(user_id, text)
        logger.info("Reminder inviato a %s", user_id)
    except Exception as e:
        logger.error("Errore invio reminder a %s: %s", user_id, e)


_notifiche_cache: Dict[int, dict] = {}
_notifiche_cache_time: Optional[datetime] = None
_NOTIFICHE_TTL = 300


def _invalidate_notifiche_cache() -> None:
    global _notifiche_cache, _notifiche_cache_time
    _notifiche_cache = {}
    _notifiche_cache_time = None


async def _get_notifiche_cached() -> Dict[int, dict]:
    global _notifiche_cache, _notifiche_cache_time

    now = datetime.now(TIMEZONE)
    if (
        _notifiche_cache_time is not None
        and (now - _notifiche_cache_time).total_seconds() < _NOTIFICHE_TTL
    ):
        return _notifiche_cache

    _notifiche_cache = await asyncio.to_thread(get_notifiche_settings)
    _notifiche_cache_time = now
    return _notifiche_cache


async def scheduler_loop() -> None:
    logger.info("Scheduler loop avviato")

    try:
        while True:
            try:
                now = datetime.now(TIMEZONE)

                if now.weekday() < 5:
                    hhmm = now.strftime("%H:%M")
                    today = now.strftime("%d.%m.%Y")
                    today_date = now.date()

                    settings = await _get_notifiche_cached()

                    needs_ingresso = [
                        (uid, cfg) for uid, cfg in settings.items()
                        if cfg["reminder_ingresso"]
                        and cfg["orario_ingresso"] == hhmm
                        and _sent_ingresso_today.get(uid) != today_date
                    ]

                    needs_uscita = [
                        (uid, cfg) for uid, cfg in settings.items()
                        if cfg["reminder_uscita"]
                        and cfg["orario_uscita"] == hhmm
                        and _sent_uscita_today.get(uid) != today_date
                    ]

                    if needs_ingresso or needs_uscita:
                        sheet_reg = await asyncio.to_thread(get_sheet, "Registro")
                        reg_rows = await asyncio.to_thread(sheet_reg.get_all_values)

                        entered_today = {
                            row[1] for row in reg_rows[1:]
                            if len(row) > 2 and row[0] == today and row[2]
                        }
                        exited_today = {
                            row[1] for row in reg_rows[1:]
                            if len(row) > 4 and row[0] == today and row[4]
                        }

                        for uid, cfg in needs_ingresso:
                            has_entered = any(s.endswith(f"| {uid}") for s in entered_today)
                            if not has_entered:
                                await send_reminder(uid, f"🔔 Ciao {cfg['nome']}, ricorda di registrare l'ingresso!")
                            _sent_ingresso_today[uid] = today_date

                        for uid, cfg in needs_uscita:
                            has_entered = any(s.endswith(f"| {uid}") for s in entered_today)
                            has_exited = any(s.endswith(f"| {uid}") for s in exited_today)
                            if has_entered and not has_exited:
                                await send_reminder(uid, f"🔔 Ciao {cfg['nome']}, ricorda di registrare l'uscita!")
                            _sent_uscita_today[uid] = today_date

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.exception("Errore nel scheduler loop: %s", e)

            await asyncio.sleep(30)
    except asyncio.CancelledError:
        logger.info("Scheduler loop terminato.")


# ============================================================
# Notifiche utente/admin
# ============================================================
def _build_notif_kb_user(uid: int, cfg: dict) -> types.InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    stato_in = "✅ Attivo" if cfg["reminder_ingresso"] else "❌ Disattivo"
    stato_out = "✅ Attivo" if cfg["reminder_uscita"] else "❌ Disattivo"
    kb.button(text=f"🕓 Reminder ingresso: {stato_in}", callback_data=f"notif:toggle_in:{uid}")
    kb.button(text=f"⏰ Orario ingresso: {cfg['orario_ingresso']}  ✏️", callback_data=f"notif:set_orario_in:{uid}")
    kb.button(text=f"🚪 Reminder uscita: {stato_out}", callback_data=f"notif:toggle_out:{uid}")
    kb.button(text=f"⏰ Orario uscita: {cfg['orario_uscita']}  ✏️", callback_data=f"notif:set_orario_out:{uid}")
    kb.adjust(1)
    return kb.as_markup()


def _build_notif_kb_admin(uid: int, cfg: dict) -> types.InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    stato_in = "✅ Attivo" if cfg["reminder_ingresso"] else "❌ Disattivo"
    stato_out = "✅ Attivo" if cfg["reminder_uscita"] else "❌ Disattivo"
    kb.button(text=f"🕓 Reminder ingresso: {stato_in}", callback_data=f"notif:toggle_in:{uid}")
    kb.button(text=f"⏰ Orario ingresso: {cfg['orario_ingresso']}  ✏️", callback_data=f"notif:set_orario_in:{uid}")
    kb.button(text=f"🚪 Reminder uscita: {stato_out}", callback_data=f"notif:toggle_out:{uid}")
    kb.button(text=f"⏰ Orario uscita: {cfg['orario_uscita']}  ✏️", callback_data=f"notif:set_orario_out:{uid}")
    kb.button(text="🔙 Torna alla lista", callback_data="notif:admin_list")
    kb.adjust(1)
    return kb.as_markup()


@dp.message(F.text.in_({"/mienotifiche", "🔔 Mie Notifiche"}))
async def mienotifiche_handler(message: Message):
    uid = message.from_user.id
    settings = await asyncio.to_thread(get_notifiche_settings)

    if uid not in settings:
        await message.answer(
            "⚠️ Non sei ancora registrato nel sistema notifiche.\n"
            "Registra almeno un ingresso per essere aggiunto automaticamente."
        )
        return

    cfg = settings[uid]
    await message.answer(
        "🔔 <b>Le tue notifiche</b>\n\nTocca un bottone per attivare/disattivare o cambiare l'orario:",
        reply_markup=_build_notif_kb_user(uid, cfg),
    )


@dp.message(F.text == "/notifiche")
async def notifiche_admin_handler(message: Message):
    if message.from_user.id not in ADMINS:
        await message.answer("❌ Non hai i permessi.")
        return

    settings = await asyncio.to_thread(get_notifiche_settings)
    if not settings:
        await message.answer("❌ Nessun utente nel foglio Notifiche.")
        return

    kb = InlineKeyboardBuilder()
    for uid, cfg in settings.items():
        stato = "✅" if (cfg["reminder_ingresso"] or cfg["reminder_uscita"]) else "❌"
        kb.button(text=f"{stato} {cfg['nome']}", callback_data=f"notif:admin_user:{uid}")
    kb.adjust(1)

    await message.answer("👥 <b>Gestione notifiche utenti</b>\n\nSeleziona un utente:", reply_markup=kb.as_markup())


@dp.callback_query(F.data.startswith("notif:admin_user:"))
async def notif_admin_user_handler(cb: CallbackQuery):
    if cb.from_user.id not in ADMINS:
        await cb.answer("❌ Non autorizzato.", show_alert=True)
        return

    uid = int(cb.data.split(":")[2])
    settings = await asyncio.to_thread(get_notifiche_settings)
    if uid not in settings:
        await cb.answer("Utente non trovato.", show_alert=True)
        return

    cfg = settings[uid]
    testo = (
        f"👤 <b>{cfg['nome']}</b>\n\n"
        f"🕓 Ingresso: {'✅ ON' if cfg['reminder_ingresso'] else '❌ OFF'} — {cfg['orario_ingresso']}\n"
        f"🚪 Uscita: {'✅ ON' if cfg['reminder_uscita'] else '❌ OFF'} — {cfg['orario_uscita']}"
    )
    await cb.message.edit_text(testo, reply_markup=_build_notif_kb_admin(uid, cfg))
    await cb.answer()


@dp.callback_query(F.data == "notif:admin_list")
async def notif_admin_list_handler(cb: CallbackQuery):
    if cb.from_user.id not in ADMINS:
        await cb.answer("❌ Non autorizzato.", show_alert=True)
        return

    settings = await asyncio.to_thread(get_notifiche_settings)
    kb = InlineKeyboardBuilder()
    for uid, cfg in settings.items():
        stato = "✅" if (cfg["reminder_ingresso"] or cfg["reminder_uscita"]) else "❌"
        kb.button(text=f"{stato} {cfg['nome']}", callback_data=f"notif:admin_user:{uid}")
    kb.adjust(1)

    await cb.message.edit_text("👥 <b>Gestione notifiche utenti</b>\n\nSeleziona un utente:", reply_markup=kb.as_markup())
    await cb.answer()


@dp.callback_query(F.data.startswith("notif:toggle_in:") | F.data.startswith("notif:toggle_out:"))
async def notif_toggle_handler(cb: CallbackQuery):
    parts = cb.data.split(":")
    tipo = "in" if parts[1] == "toggle_in" else "out"
    uid = int(parts[2])

    if cb.from_user.id != uid and cb.from_user.id not in ADMINS:
        await cb.answer("❌ Non autorizzato.", show_alert=True)
        return

    new_val = await asyncio.to_thread(toggle_notifica, uid, tipo)
    if new_val is None:
        await cb.answer("❌ Errore nel salvataggio.", show_alert=True)
        return

    tipo_str = "ingresso" if tipo == "in" else "uscita"
    stato = "✅ attivato" if new_val else "❌ disattivato"
    await cb.answer(f"Reminder {tipo_str} {stato}!")

    settings = await asyncio.to_thread(get_notifiche_settings)
    if uid not in settings:
        return

    cfg = settings[uid]
    is_admin_view = cb.from_user.id in ADMINS and cb.from_user.id != uid
    new_kb = _build_notif_kb_admin(uid, cfg) if is_admin_view else _build_notif_kb_user(uid, cfg)

    try:
        await cb.message.edit_reply_markup(reply_markup=new_kb)
    except Exception:
        pass


@dp.callback_query(F.data.startswith("notif:set_orario_in:") | F.data.startswith("notif:set_orario_out:"))
async def notif_set_orario_start(cb: CallbackQuery, state: FSMContext):
    parts = cb.data.split(":")
    tipo = "in" if parts[1] == "set_orario_in" else "out"
    uid = int(parts[2])

    if cb.from_user.id != uid and cb.from_user.id not in ADMINS:
        await cb.answer("❌ Non autorizzato.", show_alert=True)
        return

    await state.update_data(notif_uid=uid, notif_tipo=tipo)
    await state.set_state(NotificheForm.waiting_for_orario)

    tipo_str = "ingresso" if tipo == "in" else "uscita"
    await cb.message.edit_text(
        f"⏰ Inserisci il nuovo orario per il reminder di <b>{tipo_str}</b>\n\n"
        f"Formato: <code>HH:MM</code> — es. <code>08:30</code>"
    )
    await cb.answer()


@dp.message(NotificheForm.waiting_for_orario)
async def notif_set_orario_receive(message: Message, state: FSMContext):
    import re

    testo = (message.text or "").strip()
    if not re.match(r"^([01]\d|2[0-3]):[0-5]\d$", testo):
        await message.answer("❌ Formato non valido. Scrivi l'orario come <code>HH:MM</code>, es. <code>08:30</code>")
        return

    data = await state.get_data()
    uid = data["notif_uid"]
    tipo = data["notif_tipo"]
    await state.clear()

    ok = await asyncio.to_thread(set_orario_notifica, uid, tipo, testo)
    tipo_str = "ingresso" if tipo == "in" else "uscita"

    if ok:
        await message.answer(f"✅ Orario reminder <b>{tipo_str}</b> aggiornato a <b>{testo}</b>!", reply_markup=main_kb)
    else:
        await message.answer("❌ Errore nel salvataggio. L'utente potrebbe non essere nel foglio Notifiche.", reply_markup=main_kb)


@dp.message(F.text == "/remindtest")
async def remindtest_handler(message: Message):
    if message.from_user.id not in ADMINS:
        await message.answer("❌ Non hai i permessi per eseguire questo comando.")
        return

    await message.answer("⏳ Eseguo test scheduler…")
    settings = await asyncio.to_thread(get_notifiche_settings)
    count = 0

    for uid, cfg in settings.items():
        if cfg["reminder_ingresso"]:
            await send_reminder(uid, f"🔔 [TEST] Ciao {cfg['nome']}, reminder ingresso di prova!")
            count += 1
        if cfg["reminder_uscita"]:
            await send_reminder(uid, f"🔔 [TEST] Ciao {cfg['nome']}, reminder uscita di prova!")
            count += 1

    await message.answer(f"✅ Inviati {count} reminder di test.")


# ============================================================
# Status admin
# ============================================================
@dp.message(F.text == "/status")
async def status_handler(message: Message):
    if message.from_user.id not in ADMINS:
        await message.answer("❌ Non hai i permessi.")
        return

    lines = ["<b>🔍 Diagnostica Bot</b>\n"]
    lines.append(f"🔑 <b>Token:</b> {'✅ presente' if TOKEN else '❌ MANCANTE'}")
    lines.append(f"⚙️ <b>WEBHOOK_URL env:</b> {'✅ ' + WEBHOOK_URL if WEBHOOK_URL else '❌ MANCANTE'}")
    lines.append(f"🗂 <b>Sheet ID:</b> {'✅ presente' if SHEET_ID else '❌ MANCANTE'}")

    try:
        wh = await bot.get_webhook_info()
        if wh.url:
            lines.append(f"🌐 <b>Webhook URL:</b> ✅ {wh.url}")
            lines.append(f"   Pending updates: {wh.pending_update_count}")
            if wh.last_error_message:
                lines.append(f"   ⚠️ Ultimo errore Telegram: {wh.last_error_message}")
        else:
            lines.append("🌐 <b>Webhook URL:</b> ❌ NON impostato su Telegram")
    except Exception as e:
        lines.append(f"🌐 <b>Webhook:</b> ❌ errore lettura ({e})")

    try:
        await asyncio.to_thread(get_sheet, "Registro")
        lines.append("📊 <b>Google Sheets:</b> ✅ connesso")
    except Exception as e:
        lines.append(f"📊 <b>Google Sheets:</b> ❌ errore: {e}")

    if CREDENTIALS_JSON:
        lines.append("🔐 <b>Google Credentials:</b> ✅ da variabile JSON")
    elif CREDENTIALS_FILE:
        lines.append(f"🔐 <b>Google Credentials:</b> ✅ da file ({CREDENTIALS_FILE})")
    else:
        lines.append("🔐 <b>Google Credentials:</b> ❌ MANCANTI")

    await message.answer("\n".join(lines))


# ============================================================
# FastAPI + lifecycle
# ============================================================
scheduler_task: Optional[asyncio.Task] = None


async def on_startup() -> None:
    global scheduler_task

    await asyncio.to_thread(init_sheets)

    if WEBHOOK_URL:
        webhook_endpoint = f"{WEBHOOK_URL.rstrip('/')}/webhook"
        try:
            await bot.set_webhook(webhook_endpoint, drop_pending_updates=True)
            logger.info("Webhook impostato: %s", webhook_endpoint)
        except Exception as e:
            logger.error("Errore impostazione webhook: %s", e)
    else:
        logger.warning("WEBHOOK_URL non impostato: il webhook NON è stato registrato su Telegram.")

    scheduler_task = asyncio.create_task(scheduler_loop())
    logger.info("Startup completato.")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global scheduler_task

    await on_startup()
    try:
        yield
    finally:
        if scheduler_task:
            scheduler_task.cancel()
            try:
                await scheduler_task
            except asyncio.CancelledError:
                pass
        await bot.delete_webhook()
        await bot.session.close()
        logger.info("Shutdown completato.")


app = FastAPI(lifespan=lifespan)


async def process_update(update: types.Update) -> None:
    """
    Processa l'update fuori dalla richiesta HTTP.
    Così Telegram riceve subito 200 OK e non ritenta lo stesso update
    se Google Sheets è lento o un handler impiega troppo.
    """
    try:
        await dp.feed_update(bot=bot, update=update)
    except Exception as e:
        logger.exception("Errore processando update: %s", e)


@app.post("/webhook")
async def webhook(request: Request):
    """
    Webhook non bloccante.
    Non restituisce 500 a Telegram per errori interni, per evitare retry continui.
    """
    try:
        update = types.Update.model_validate(await request.json(), context={"bot": bot})
        asyncio.create_task(process_update(update))
        return {"ok": True}
    except Exception as e:
        logger.exception("Errore parsing webhook: %s", e)
        return {"ok": True}


@app.api_route("/", methods=["GET", "HEAD"])
async def health_check():
    return {"status": "running", "webhook_url": WEBHOOK_URL or "NON IMPOSTATO"}


@app.get("/debug")
async def debug_endpoint():
    result = {
        "bot_token_set": bool(TOKEN),
        "webhook_url_env": WEBHOOK_URL or "MANCANTE",
        "sheet_id_set": bool(SHEET_ID),
        "credentials_source": "json_env" if CREDENTIALS_JSON else ("file" if CREDENTIALS_FILE else "MANCANTE"),
    }

    try:
        wh = await bot.get_webhook_info()
        result["telegram_webhook_url"] = wh.url or "NON IMPOSTATO"
        result["telegram_pending_updates"] = wh.pending_update_count
        result["telegram_last_error"] = wh.last_error_message or "nessuno"
    except Exception as e:
        result["telegram_webhook_error"] = str(e)

    try:
        await asyncio.to_thread(get_sheet, "Registro")
        result["google_sheets"] = "ok"
    except Exception as e:
        result["google_sheets"] = f"ERRORE: {e}"

    return result


# ============================================================
# Main
# ============================================================
if __name__ == "__main__":
    import uvicorn

    logger.info("Avvio uvicorn FastAPI + webhook")
    uvicorn.run("bot:app", host="0.0.0.0", port=PORT, log_level="info")
