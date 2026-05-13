import os
import asyncio
import calendar
import json
import csv
import io
import logging
import threading
import re
from datetime import datetime, date
from math import radians, sin, cos, sqrt, atan2
from typing import Optional, Dict, Tuple, List

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

PRODUCTIVITY_SHEET_NAME = "Produttività"

if not TOKEN:
    raise RuntimeError("BOT_TOKEN non impostato nelle variabili d'ambiente.")

if not SHEET_ID:
    raise RuntimeError("GOOGLE_SHEETS_ID non impostato nelle variabili d'ambiente.")

ADMINS = {614102287}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher(storage=MemoryStorage())

_sheets_semaphore = asyncio.Semaphore(3)


async def sheets_call(fn, *args, timeout: float = 15.0):
    """
    Esegue fn in un thread separato con semaforo e timeout.
    Evita di bloccare l'event loop di aiogram durante le chiamate Google Sheets.
    """
    async with _sheets_semaphore:
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(fn, *args),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            logger.error("Timeout chiamata Sheets: %s", getattr(fn, "__name__", str(fn)))
            _reset_client()
            raise


_sent_ingresso_today: Dict[int, date] = {}
_sent_uscita_today: Dict[int, date] = {}

# ============================================================
# Google Sheets: client thread-local
# ============================================================
_thread_local = threading.local()

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def _build_creds():
    """Costruisce le credenziali Google."""
    if not (CREDENTIALS_JSON or CREDENTIALS_FILE):
        raise ValueError("Devi impostare GOOGLE_CREDENTIALS o GOOGLE_CREDENTIALS_FILE.")

    if CREDENTIALS_JSON:
        try:
            credentials_dict = json.loads(CREDENTIALS_JSON)
        except Exception as e:
            logger.exception("Errore parsing GOOGLE_CREDENTIALS: %s", e)
            raise

        if "private_key" in credentials_dict and isinstance(credentials_dict["private_key"], str):
            credentials_dict["private_key"] = credentials_dict["private_key"].replace("\\n", "\n")

        return Credentials.from_service_account_info(credentials_dict, scopes=SCOPES)

    return Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)


def _get_client() -> gspread.Client:
    """Restituisce il client gspread del thread corrente."""
    if not hasattr(_thread_local, "client") or _thread_local.client is None:
        _thread_local.client = gspread.authorize(_build_creds())
        logger.debug("Nuovo client gspread per thread %s", threading.current_thread().name)
    return _thread_local.client


def _reset_client():
    """Azzera il client del thread corrente dopo errore o timeout."""
    if hasattr(_thread_local, "client"):
        _thread_local.client = None


def get_sheet(sheet_name: str = "Registro") -> Worksheet:
    """Restituisce una worksheet esistente."""
    try:
        return _get_client().open_by_key(SHEET_ID).worksheet(sheet_name)
    except gspread.exceptions.APIError as e:
        if getattr(e, "response", None) is not None and e.response.status_code == 401:
            _reset_client()
            logger.warning("Token scaduto, client resettato.")
        logger.exception("Errore aprendo il foglio '%s': %s", sheet_name, e)
        raise
    except Exception as e:
        _reset_client()
        logger.exception("Errore aprendo il foglio '%s': %s", sheet_name, e)
        raise


def get_or_create_sheet(sheet_name: str, rows: int = 1000, cols: int = 20) -> Worksheet:
    """
    Restituisce una worksheet. Se non esiste, la crea automaticamente.
    """
    try:
        return get_sheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        spreadsheet = _get_client().open_by_key(SHEET_ID)
        logger.info("Creo nuovo foglio: %s", sheet_name)
        return spreadsheet.add_worksheet(title=sheet_name, rows=rows, cols=cols)


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
    """Legge ZoneLavoro da Sheets con cache TTL 5 minuti."""
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
        sheet = get_or_create_sheet("ZoneLavoro")
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
        sheet = get_or_create_sheet("ZoneLavoro")
        sheet.append_row([name, str(lat), str(lon)])
        _invalidate_locations_cache()
        return True
    except Exception as e:
        logger.exception("Errore salvataggio zona: %s", e)
        return False


def update_zone_name(old_name: str, new_name: str) -> bool:
    try:
        sheet = get_or_create_sheet("ZoneLavoro")
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
        sheet = get_or_create_sheet("ZoneLavoro")
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
# Google Sheets helpers: Registro, Permessi, Produttività
# ============================================================
async def async_save_ingresso(user: types.User, time_str: str, location_name: str) -> bool:
    return await sheets_call(_sync_save_ingresso, user, time_str, location_name)


def _sync_save_ingresso(user: types.User, time_str: str, location_name: str) -> bool:
    try:
        sheet = get_or_create_sheet("Registro")
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
    return await sheets_call(_sync_save_uscita, user, time_str, location_name)


def _sync_save_uscita(user: types.User, time_str: str, location_name: str) -> bool:
    try:
        sheet = get_or_create_sheet("Registro")
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
    return await sheets_call(_sync_save_permesso, user, start_date, end_date, reason)


def _sync_save_permesso(user: types.User, start_date: str, end_date: str, reason: str) -> bool:
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        if end_dt < start_dt:
            logger.warning("Data fine precedente alla data inizio.")
            return False

        sheet = get_or_create_sheet("Permessi")
        now_local = datetime.now(TIMEZONE)
        created = now_local.strftime("%d.%m.%Y %H:%M")
        user_id = f"{user.full_name} | {user.id}"
        sheet.append_row([created, user_id, start_date, end_date, reason])
        return True

    except Exception as e:
        logger.exception("Errore save_permesso: %s", e)
        return False


async def async_save_lavoro(user: types.User, numero_bus: str, tipo_lavoro: str, note: str) -> bool:
    return await sheets_call(_sync_save_lavoro, user, numero_bus, tipo_lavoro, note)


def _sync_save_lavoro(user: types.User, numero_bus: str, tipo_lavoro: str, note: str) -> bool:
    """Salva una registrazione lavoro nel foglio Produttività."""
    try:
        sheet = get_or_create_sheet(PRODUCTIVITY_SHEET_NAME)
        now_local = datetime.now(TIMEZONE)

        sheet.append_row([
            now_local.strftime("%d.%m.%Y"),
            now_local.strftime("%H:%M"),
            user.full_name,
            str(user.id),
            numero_bus,
            tipo_lavoro,
            note,
        ])
        return True

    except Exception as e:
        logger.exception("Errore save_lavoro: %s", e)
        return False


async def async_get_lavori_mese(user: types.User, year: int, month: int) -> List[dict]:
    return await sheets_call(_sync_get_lavori_mese, user, year, month)


def _sync_get_lavori_mese(user: types.User, year: int, month: int) -> List[dict]:
    """Legge le registrazioni lavoro dell'utente per mese."""
    try:
        sheet = get_or_create_sheet(PRODUCTIVITY_SHEET_NAME)
        rows = sheet.get_all_values()
        result: List[dict] = []
        month_filter = f"{month:02d}.{year}"
        user_id = str(user.id)

        for row in rows[1:]:
            if len(row) < 7:
                continue

            data = row[0].strip()
            telegram_id = row[3].strip()

            if telegram_id != user_id:
                continue

            if len(data) >= 10 and data[3:10] == month_filter:
                result.append({
                    "data": row[0],
                    "ora": row[1],
                    "utente": row[2],
                    "telegram_id": row[3],
                    "bus": row[4],
                    "tipo": row[5],
                    "note": row[6],
                })

        return result

    except Exception as e:
        logger.exception("Errore get_lavori_mese: %s", e)
        return []


async def async_get_lavori_giorno(user: types.User, selected_date: str) -> List[dict]:
    return await sheets_call(_sync_get_lavori_giorno, user, selected_date)


def _sync_get_lavori_giorno(user: types.User, selected_date: str) -> List[dict]:
    """Legge le registrazioni lavoro dell'utente per un giorno DD.MM.YYYY."""
    try:
        sheet = get_or_create_sheet(PRODUCTIVITY_SHEET_NAME)
        rows = sheet.get_all_values()
        result: List[dict] = []
        user_id = str(user.id)

        for row in rows[1:]:
            if len(row) < 7:
                continue

            if row[0].strip() == selected_date and row[3].strip() == user_id:
                result.append({
                    "data": row[0],
                    "ora": row[1],
                    "utente": row[2],
                    "telegram_id": row[3],
                    "bus": row[4],
                    "tipo": row[5],
                    "note": row[6],
                })

        return result

    except Exception as e:
        logger.exception("Errore get_lavori_giorno: %s", e)
        return []


async def get_riepilogo(user: types.User, year: int, month: int) -> Optional[io.StringIO]:
    return await sheets_call(_sync_get_riepilogo, user, year, month)


def _sync_get_riepilogo(user: types.User, year: int, month: int) -> Optional[io.StringIO]:
    """Restituisce CSV delle presenze dell'utente filtrato per anno e mese."""
    try:
        sheet = get_or_create_sheet("Registro")
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
    """Inizializza tutti i fogli necessari."""
    try:
        sheet_reg = get_or_create_sheet("Registro")
        if not sheet_reg.row_values(1):
            sheet_reg.append_row(["Data", "Utente", "Ingresso ora", "Posizione ingresso", "Uscita ora", "Posizione uscita"])

        sheet_perm = get_or_create_sheet("Permessi")
        if not sheet_perm.row_values(1):
            sheet_perm.append_row(["Data richiesta", "Utente", "Dal", "Al", "Motivo"])

        sheet_zone = get_or_create_sheet("ZoneLavoro")
        if not sheet_zone.row_values(1):
            sheet_zone.append_row(["Nome", "Latitudine", "Longitudine"])

        sheet_notif = get_or_create_sheet("Notifiche")
        if not sheet_notif.row_values(1):
            sheet_notif.append_row([
                "Telegram ID", "Nome",
                "Reminder Ingresso", "Orario Ingresso",
                "Reminder Uscita", "Orario Uscita",
            ])

        sheet_prod = get_or_create_sheet(PRODUCTIVITY_SHEET_NAME)
        if not sheet_prod.row_values(1):
            sheet_prod.append_row([
                "Data",
                "Ora",
                "Utente",
                "Telegram ID",
                "Numero bus",
                "Tipo lavoro",
                "Note",
            ])

        logger.info("Sheets inizializzati.")

    except Exception as e:
        logger.error("Errore init_sheets: %s", e)


# ============================================================
# Notifiche sheet helpers
# ============================================================
def get_notifiche_settings() -> Dict[int, dict]:
    try:
        sheet = get_or_create_sheet("Notifiche")
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
        sheet = get_or_create_sheet("Notifiche")
        rows = sheet.get_all_values()

        for row in rows[1:]:
            if row and row[0].strip() == str(user_id):
                return True

        sheet.append_row([
            str(user_id), nome,
            "TRUE" if reminder_in else "FALSE", orario_in,
            "TRUE" if reminder_out else "FALSE", orario_out,
        ])
        return True

    except Exception as e:
        logger.exception("Errore upsert_user_notifiche: %s", e)
        return False


def toggle_notifica(user_id: int, tipo: str) -> Optional[bool]:
    col = 3 if tipo == "in" else 5
    try:
        sheet = get_or_create_sheet("Notifiche")
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
        sheet = get_or_create_sheet("Notifiche")
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
        [KeyboardButton(text="🕓 Ingresso"), KeyboardButton(text="🚪 Uscita")],
        [KeyboardButton(text="🚌 Registra lavoro")],
        [KeyboardButton(text="📆 Calendario lavori")],
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


class LavoroForm(StatesGroup):
    waiting_for_bus = State()
    waiting_for_tipo = State()
    waiting_for_note = State()


# ============================================================
# Calendar builders
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


def build_lavori_calendar(year: int, month: int, lavori: List[dict]) -> types.InlineKeyboardMarkup:
    """Calendario mensile per le registrazioni lavoro."""
    kb = InlineKeyboardBuilder()
    today = datetime.now(TIMEZONE)
    giorni = ["Lu", "Ma", "Me", "Gi", "Ve", "Sa", "Do"]

    giorni_con_lavori = set()
    for item in lavori:
        try:
            dt = datetime.strptime(item["data"], "%d.%m.%Y")
            giorni_con_lavori.add(dt.day)
        except Exception:
            continue

    kb.button(text=f"📆 {mese_nome(month)} {year}", callback_data="ignore")

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
                is_today = day == today.day and month == today.month and year == today.year
                has_work = day in giorni_con_lavori

                if has_work:
                    text_day = f"🟢 {day}"
                elif is_today:
                    text_day = f"🔵 {day}"
                else:
                    text_day = str(day)

                kb.button(text=text_day, callback_data=f"workcal:day:{year}:{month}:{day}")

    kb.button(text="◀️ Mese prima", callback_data=f"workcal:nav:{year}:{month}:prev")
    kb.button(text="▶️ Mese dopo", callback_data=f"workcal:nav:{year}:{month}:next")
    kb.button(text="➕ Nuova registrazione", callback_data="work:new")

    kb.adjust(1, 7, *([7] * len(weeks)), 2, 1)
    return kb.as_markup()


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


def _build_tipo_lavoro_keyboard() -> types.InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="🔧 Installazione", callback_data="work:type:installazione")
    kb.button(text="🛠️ Manutenzione", callback_data="work:type:manutenzione")
    kb.button(text="❌ Annulla", callback_data="work:cancel")
    kb.adjust(1)
    return kb.as_markup()


def _build_note_keyboard() -> types.InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="⏭️ Salta note", callback_data="work:skip_notes")
    kb.button(text="❌ Annulla", callback_data="work:cancel")
    kb.adjust(1)
    return kb.as_markup()


def _build_lavori_after_save_keyboard() -> types.InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="📆 Vedi calendario lavori", callback_data="workcal:current")
    kb.button(text="➕ Registra un altro lavoro", callback_data="work:new")
    kb.adjust(1)
    return kb.as_markup()


# ============================================================
# Zone keyboard helpers
# ============================================================
def _build_zones_markup(work_locations: Dict[str, Tuple[float, float]]):
    kb = InlineKeyboardBuilder()
    for zone_name in work_locations.keys():
        kb.button(text=f"📍 {zone_name}", callback_data=f"zone_select:{zone_name}")
    kb.button(text="➕ Aggiungi zona", callback_data="zone_add_new")
    kb.adjust(1)
    return kb.as_markup()


async def _show_zones_list(target) -> None:
    work_locations = await sheets_call(get_work_locations)
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
# Handlers – Start / Ingresso / Uscita
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

    try:
        location_name = await sheets_call(check_location, loc.latitude, loc.longitude)
    except asyncio.TimeoutError:
        await message.answer("⚠️ Il server è lento, riprova tra qualche secondo.", reply_markup=main_kb)
        return

    if not location_name:
        await message.answer("❌ Non sei in un luogo autorizzato.", reply_markup=main_kb)
        return

    now_local = datetime.now(TIMEZONE).strftime("%H:%M")

    try:
        if await async_save_ingresso(message.from_user, now_local, location_name):
            await message.answer("✅ Ingresso registrato!", reply_markup=main_kb)
        else:
            await message.answer("❌ Ingresso già registrato per oggi.", reply_markup=main_kb)
    except asyncio.TimeoutError:
        await message.answer("⚠️ Timeout salvataggio, riprova tra qualche secondo.", reply_markup=main_kb)


@dp.message(F.text == "🚪 Uscita")
async def uscita_start(message: Message, state: FSMContext):
    await state.set_state(RegistroForm.waiting_uscita_location)
    await message.answer("Invia la tua posizione per registrare l'uscita:", reply_markup=location_kb)


@dp.message(RegistroForm.waiting_uscita_location, F.location)
async def uscita_location(message: Message, state: FSMContext):
    await state.clear()
    loc = message.location

    try:
        location_name = await sheets_call(check_location, loc.latitude, loc.longitude)
    except asyncio.TimeoutError:
        await message.answer("⚠️ Il server è lento, riprova tra qualche secondo.", reply_markup=main_kb)
        return

    if not location_name:
        await message.answer("❌ Non sei in un luogo autorizzato.", reply_markup=main_kb)
        return

    now_local = datetime.now(TIMEZONE).strftime("%H:%M")

    try:
        if await async_save_uscita(message.from_user, now_local, location_name):
            await message.answer("✅ Uscita registrata!", reply_markup=main_kb)
        else:
            await message.answer("❌ Nessun ingresso trovato per oggi.", reply_markup=main_kb)
    except asyncio.TimeoutError:
        await message.answer("⚠️ Timeout salvataggio, riprova tra qualche secondo.", reply_markup=main_kb)


# ============================================================
# Handlers – Registrazione lavoro / Produttività
# ============================================================
@dp.message(F.text == "🚌 Registra lavoro")
async def lavoro_start(message: Message, state: FSMContext):
    await state.clear()
    await state.set_state(LavoroForm.waiting_for_bus)
    await message.answer(
        "🚌 <b>Registrazione lavoro</b>\n\n"
        "Scrivi il <b>numero del bus</b> su cui hai lavorato.\n\n"
        "Esempio: <code>245</code>",
        reply_markup=main_kb,
    )


@dp.callback_query(F.data == "work:new")
async def lavoro_new_from_callback(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await state.set_state(LavoroForm.waiting_for_bus)
    await cb.message.edit_text(
        "🚌 <b>Nuova registrazione lavoro</b>\n\n"
        "Scrivi il <b>numero del bus</b> su cui hai lavorato.\n\n"
        "Esempio: <code>245</code>"
    )
    await cb.answer()


@dp.message(LavoroForm.waiting_for_bus)
async def lavoro_bus_receive(message: Message, state: FSMContext):
    numero_bus = (message.text or "").strip()

    if not numero_bus:
        await message.answer("❌ Numero bus non valido. Scrivi il numero del bus.")
        return

    if len(numero_bus) > 30:
        await message.answer("❌ Numero bus troppo lungo. Scrivi un valore più breve.")
        return

    await state.update_data(numero_bus=numero_bus)
    await state.set_state(LavoroForm.waiting_for_tipo)

    await message.answer(
        f"🚌 Bus selezionato: <b>{numero_bus}</b>\n\n"
        "Che tipo di lavoro hai svolto?",
        reply_markup=_build_tipo_lavoro_keyboard(),
    )


@dp.callback_query(F.data.startswith("work:type:"))
async def lavoro_tipo_receive(cb: CallbackQuery, state: FSMContext):
    tipo_raw = cb.data.split(":")[2]

    if tipo_raw == "installazione":
        tipo_lavoro = "Installazione"
        icon = "🔧"
    elif tipo_raw == "manutenzione":
        tipo_lavoro = "Manutenzione"
        icon = "🛠️"
    else:
        await cb.answer("Tipo lavoro non valido.", show_alert=True)
        return

    await state.update_data(tipo_lavoro=tipo_lavoro)
    await state.set_state(LavoroForm.waiting_for_note)

    data = await state.get_data()
    numero_bus = data.get("numero_bus", "")

    await cb.message.edit_text(
        "📝 <b>Note lavoro</b>\n\n"
        f"🚌 Bus: <b>{numero_bus}</b>\n"
        f"{icon} Tipo: <b>{tipo_lavoro}</b>\n\n"
        "Scrivi una nota, oppure premi <b>Salta note</b>.",
        reply_markup=_build_note_keyboard(),
    )
    await cb.answer()


@dp.message(LavoroForm.waiting_for_note)
async def lavoro_note_receive(message: Message, state: FSMContext):
    note = (message.text or "").strip()

    if len(note) > 500:
        await message.answer("❌ Nota troppo lunga. Massimo 500 caratteri.")
        return

    await _finalizza_registrazione_lavoro_message(message, state, note)


@dp.callback_query(F.data == "work:skip_notes")
async def lavoro_skip_notes(cb: CallbackQuery, state: FSMContext):
    await _finalizza_registrazione_lavoro_callback(cb, state, "")
    await cb.answer()


@dp.callback_query(F.data == "work:cancel")
async def lavoro_cancel(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.edit_text("❌ Registrazione lavoro annullata.")
    await cb.answer()


async def _finalizza_registrazione_lavoro_message(message: Message, state: FSMContext, note: str):
    data = await state.get_data()
    numero_bus = data.get("numero_bus")
    tipo_lavoro = data.get("tipo_lavoro")

    if not numero_bus or not tipo_lavoro:
        await state.clear()
        await message.answer("❌ Dati mancanti. Ricomincia da 🚌 Registra lavoro.", reply_markup=main_kb)
        return

    await state.clear()

    try:
        ok = await async_save_lavoro(message.from_user, numero_bus, tipo_lavoro, note)
    except asyncio.TimeoutError:
        await message.answer("⚠️ Timeout salvataggio. Riprova tra qualche secondo.", reply_markup=main_kb)
        return

    if ok:
        note_text = note if note else "Nessuna nota"
        await message.answer(
            "✅ <b>Lavoro registrato!</b>\n\n"
            f"🚌 Bus: <b>{numero_bus}</b>\n"
            f"📌 Tipo: <b>{tipo_lavoro}</b>\n"
            f"📝 Note: <i>{note_text}</i>",
            reply_markup=_build_lavori_after_save_keyboard(),
        )
    else:
        await message.answer("❌ Errore durante il salvataggio del lavoro.", reply_markup=main_kb)


async def _finalizza_registrazione_lavoro_callback(cb: CallbackQuery, state: FSMContext, note: str):
    data = await state.get_data()
    numero_bus = data.get("numero_bus")
    tipo_lavoro = data.get("tipo_lavoro")

    if not numero_bus or not tipo_lavoro:
        await state.clear()
        await cb.message.edit_text("❌ Dati mancanti. Ricomincia da 🚌 Registra lavoro.")
        return

    await state.clear()

    try:
        ok = await async_save_lavoro(cb.from_user, numero_bus, tipo_lavoro, note)
    except asyncio.TimeoutError:
        await cb.message.edit_text("⚠️ Timeout salvataggio. Riprova tra qualche secondo.")
        return

    if ok:
        await cb.message.edit_text(
            "✅ <b>Lavoro registrato!</b>\n\n"
            f"🚌 Bus: <b>{numero_bus}</b>\n"
            f"📌 Tipo: <b>{tipo_lavoro}</b>\n"
            "📝 Note: <i>Nessuna nota</i>",
            reply_markup=_build_lavori_after_save_keyboard(),
        )
    else:
        await cb.message.edit_text("❌ Errore durante il salvataggio del lavoro.")


# ============================================================
# Handlers – Calendario lavori
# ============================================================
@dp.message(F.text == "📆 Calendario lavori")
async def calendario_lavori_handler(message: Message):
    now = datetime.now(TIMEZONE)
    lavori = await async_get_lavori_mese(message.from_user, now.year, now.month)

    await message.answer(
        "📆 <b>Calendario lavori</b>\n\n"
        "Legenda:\n"
        "🟢 giorno con registrazioni\n"
        "🔵 oggi\n\n"
        "Tocca un giorno per vedere il dettaglio.",
        reply_markup=build_lavori_calendar(now.year, now.month, lavori),
    )


@dp.callback_query(F.data == "workcal:current")
async def calendario_lavori_current_handler(cb: CallbackQuery):
    now = datetime.now(TIMEZONE)
    lavori = await async_get_lavori_mese(cb.from_user, now.year, now.month)

    await cb.message.edit_text(
        "📆 <b>Calendario lavori</b>\n\n"
        "Legenda:\n"
        "🟢 giorno con registrazioni\n"
        "🔵 oggi\n\n"
        "Tocca un giorno per vedere il dettaglio.",
        reply_markup=build_lavori_calendar(now.year, now.month, lavori),
    )
    await cb.answer()


@dp.callback_query(F.data.startswith("workcal:nav:"))
async def calendario_lavori_nav_handler(cb: CallbackQuery):
    parts = cb.data.split(":")
    year = int(parts[2])
    month = int(parts[3])
    direction = parts[4]

    if direction == "prev":
        month, year = (12, year - 1) if month == 1 else (month - 1, year)
    else:
        month, year = (1, year + 1) if month == 12 else (month + 1, year)

    lavori = await async_get_lavori_mese(cb.from_user, year, month)

    await cb.message.edit_text(
        f"📆 <b>Calendario lavori — {mese_nome(month)} {year}</b>\n\n"
        "Legenda:\n"
        "🟢 giorno con registrazioni\n"
        "🔵 oggi\n\n"
        "Tocca un giorno per vedere il dettaglio.",
        reply_markup=build_lavori_calendar(year, month, lavori),
    )
    await cb.answer()


@dp.callback_query(F.data.startswith("workcal:day:"))
async def calendario_lavori_day_handler(cb: CallbackQuery):
    parts = cb.data.split(":")
    year = int(parts[2])
    month = int(parts[3])
    day = int(parts[4])

    selected_date = f"{day:02d}.{month:02d}.{year}"
    lavori = await async_get_lavori_giorno(cb.from_user, selected_date)

    kb = InlineKeyboardBuilder()
    kb.button(text="🔙 Torna al calendario", callback_data=f"workcal:back:{year}:{month}")
    kb.button(text="➕ Nuova registrazione", callback_data="work:new")
    kb.adjust(1)

    if not lavori:
        await cb.message.edit_text(
            f"📅 <b>{selected_date}</b>\n\n"
            "Nessuna registrazione lavoro per questo giorno.",
            reply_markup=kb.as_markup(),
        )
        await cb.answer()
        return

    lines = [
        f"📅 <b>{selected_date}</b>",
        "",
        f"Registrazioni trovate: <b>{len(lavori)}</b>",
        "",
    ]

    for index, item in enumerate(lavori, start=1):
        note = item["note"] if item["note"] else "Nessuna nota"
        lines.append(
            f"<b>{index}. {item['tipo']}</b>\n"
            f"🕒 Ora: <b>{item['ora']}</b>\n"
            f"🚌 Bus: <b>{item['bus']}</b>\n"
            f"📝 Note: <i>{note}</i>"
        )
        lines.append("")

    await cb.message.edit_text("\n".join(lines), reply_markup=kb.as_markup())
    await cb.answer()


@dp.callback_query(F.data.startswith("workcal:back:"))
async def calendario_lavori_back_handler(cb: CallbackQuery):
    parts = cb.data.split(":")
    year = int(parts[2])
    month = int(parts[3])
    lavori = await async_get_lavori_mese(cb.from_user, year, month)

    await cb.message.edit_text(
        f"📆 <b>Calendario lavori — {mese_nome(month)} {year}</b>\n\n"
        "Legenda:\n"
        "🟢 giorno con registrazioni\n"
        "🔵 oggi\n\n"
        "Tocca un giorno per vedere il dettaglio.",
        reply_markup=build_lavori_calendar(year, month, lavori),
    )
    await cb.answer()


# ============================================================
# Handlers – Permessi
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
# Handlers – Riepilogo
# ============================================================
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
# Handlers – Istruzioni
# ============================================================
@dp.message(F.text == "📘 Istruzioni Bot")
async def istruzioni_handler(message: Message):
    istruzioni_text = (
        "<b>📖 Guida al Bot Presenze e Produttività</b>\n\n"

        "<b>▶️ Avvio</b>\n"
        "Invia /start per aprire il menu principale.\n\n"

        "<b>🕓 Registrazione ingresso</b>\n"
        "1. Premi <b>Ingresso</b>\n"
        "2. Tocca il bottone 📍 <b>Invia posizione</b>\n"
        "3. Il bot verifica che tu sia in una sede autorizzata e salva ora e luogo.\n"
        "⚠️ Puoi registrare un solo ingresso al giorno.\n\n"

        "<b>🚪 Registrazione uscita</b>\n"
        "1. Premi <b>Uscita</b> e invia la posizione.\n"
        "2. Il bot aggiorna il registro con l'orario di uscita.\n"
        "⚠️ È necessario aver già registrato l'ingresso nella stessa giornata.\n\n"

        "<b>🚌 Registrazione lavoro</b>\n"
        "1. Premi <b>Registra lavoro</b>\n"
        "2. Scrivi il <b>numero del bus</b>\n"
        "3. Scegli <b>Installazione</b> oppure <b>Manutenzione</b>\n"
        "4. Scrivi una nota oppure premi <b>Salta note</b>\n"
        "La registrazione viene salvata nel foglio <b>Produttività</b>.\n\n"

        "<b>📆 Calendario lavori</b>\n"
        "Mostra le registrazioni lavoro giorno per giorno.\n"
        "🟢 indica i giorni con lavori registrati, 🔵 indica oggi.\n\n"

        "<b>📝 Richiesta permessi</b>\n"
        "1. Premi <b>Richiesta permessi</b>\n"
        "2. Seleziona data di inizio e data di fine\n"
        "3. Scrivi il motivo.\n\n"

        "<b>📄 Riepilogo presenze</b>\n"
        "1. Premi <b>Riepilogo</b>\n"
        "2. Scegli anno e mese\n"
        "Riceverai un file CSV.\n\n"

        "<b>🔔 Notifiche reminder</b>\n"
        "Puoi gestire promemoria ingresso e uscita con <b>Mie Notifiche</b>.\n\n"

        "<b>📍 Privacy posizione</b>\n"
        "Il bot non traccia la posizione in automatico. La usa solo quando la invii tu.\n\n"

        "<b>📧 Assistenza</b>\n"
        "sserviceitalia@gmail.com"
    )
    await message.answer(istruzioni_text, reply_markup=main_kb)


# ============================================================
# Handlers – Gestione Zone admin
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
        "✏️ Inserisci il nome della nuova zona oppure scrivi <b>Annulla</b>:",
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

    if await sheets_call(save_new_zone, name, lat, lon):
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
        f"✏️ <b>Modifica zona:</b> {zone_name}\n\nInserisci il nuovo nome oppure scrivi <b>Annulla</b>:"
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

    if await sheets_call(delete_zone, zone_name):
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

    if await sheets_call(update_zone_name, old_name, new_name):
        await message.answer(
            f"✅ Zona rinominata!\n<b>Prima:</b> {old_name}\n<b>Dopo:</b> {new_name}",
            reply_markup=main_kb,
        )
    else:
        await message.answer(f"❌ Errore nella modifica della zona <b>{old_name
