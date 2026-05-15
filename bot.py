import os
import asyncio
import calendar
import json
import csv
import io
import logging
import re
from datetime import datetime, date
from math import radians, sin, cos, sqrt, atan2
from typing import Optional, Dict, Tuple, List, Union

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
from fastapi.responses import JSONResponse
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

ADMINS = {614102287}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher(storage=MemoryStorage())

_sheets_semaphore = asyncio.Semaphore(3)


async def sheets_call(fn, *args, timeout: float = 15.0):
    async with _sheets_semaphore:
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(fn, *args),
                timeout=timeout
            )
        except asyncio.TimeoutError:
            logger.error("Timeout chiamata Sheets: %s", fn.__name__)
            _reset_client()
            raise


_sent_ingresso_today: Dict[int, date] = {}
_sent_uscita_today: Dict[int, date] = {}


# ============================================================
# Google Sheets: client thread-local (thread-safe)
# ============================================================
import threading
_thread_local = threading.local()

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def _build_creds():
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
    if not hasattr(_thread_local, "client") or _thread_local.client is None:
        _thread_local.client = gspread.authorize(_build_creds())
        logger.debug("Nuovo client gspread per thread %s", threading.current_thread().name)
    return _thread_local.client


def _reset_client():
    if hasattr(_thread_local, "client"):
        _thread_local.client = None


def get_sheet(sheet_name: str = "Registro") -> Worksheet:
    try:
        return _get_client().open_by_key(SHEET_ID).worksheet(sheet_name)
    except gspread.exceptions.APIError as e:
        if e.response.status_code == 401:
            _reset_client()
            logger.warning("Token scaduto, client resettato (thread %s).", threading.current_thread().name)
        logger.exception("Errore aprendo il foglio '%s': %s", sheet_name, e)
        raise
    except Exception as e:
        _reset_client()
        logger.exception("Errore aprendo il foglio '%s': %s", sheet_name, e)
        raise


# ============================================================
# Caching work_locations con TTL (5 minuti)
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
# Google Sheets helpers – Registro presenze
# ============================================================

async def async_save_ingresso(user: types.User, time_str: str, location_name: str) -> bool:
    return await sheets_call(_sync_save_ingresso, user, time_str, location_name)

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
    return await sheets_call(_sync_save_uscita, user, time_str, location_name)

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
                    'range': f'{col_e}:{col_f}',
                    'values': [[time_str, location_name]]
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
    return await sheets_call(_sync_get_riepilogo, user, year, month)

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


# ============================================================
# Google Sheets helpers – Produttività
# ============================================================

async def async_save_lavoro(
    user: types.User,
    numero_bus: str,
    tipo: str,
    note: str,
) -> bool:
    return await sheets_call(_sync_save_lavoro, user, numero_bus, tipo, note)


def _sync_save_lavoro(
    user: types.User,
    numero_bus: str,
    tipo: str,
    note: str,
) -> bool:
    """
    Salva una riga nel foglio 'Produttività'.
    Colonne: Data | Ora | Utente | N° Bus | Tipo Lavoro | Note
    """
    try:
        sheet = get_sheet("Produttività")
        now_local = datetime.now(TIMEZONE)
        today = now_local.strftime("%d.%m.%Y")
        ora = now_local.strftime("%H:%M")
        user_id = f"{user.full_name} | {user.id}"
        sheet.append_row([today, ora, user_id, numero_bus, tipo, note])
        return True
    except Exception as e:
        logger.exception("Errore save_lavoro: %s", e)
        return False


def _sync_get_lavori_mese(user_id_str: str, year: int, month: int) -> List[dict]:
    """
    Legge il foglio Produttività e restituisce tutte le righe dell'utente
    per l'anno/mese specificati, come lista di dizionari.
    """
    try:
        sheet = get_sheet("Produttività")
        rows = sheet.get_all_values()
        month_filter = f"{month:02d}.{year}"
        result = []
        for row in rows[1:]:
            if len(row) < 5:
                continue
            if row[2] != user_id_str:
                continue
            if len(row[0]) < 7 or row[0][3:10] != month_filter:
                continue
            result.append({
                "data":  row[0],
                "ora":   row[1] if len(row) > 1 else "",
                "bus":   row[3] if len(row) > 3 else "",
                "tipo":  row[4] if len(row) > 4 else "",
                "note":  row[5] if len(row) > 5 else "",
            })
        return result
    except Exception as e:
        logger.exception("Errore get_lavori_mese: %s", e)
        return []


def _sync_get_lavori_giorno(user_id_str: str, giorno: str) -> List[dict]:
    """
    Legge il foglio Produttività e restituisce tutte le righe dell'utente
    per il giorno specificato (formato DD.MM.YYYY).
    """
    try:
        sheet = get_sheet("Produttività")
        rows = sheet.get_all_values()
        result = []
        for row in rows[1:]:
            if len(row) < 5:
                continue
            if row[0] != giorno or row[2] != user_id_str:
                continue
            result.append({
                "data":  row[0],
                "ora":   row[1] if len(row) > 1 else "",
                "bus":   row[3] if len(row) > 3 else "",
                "tipo":  row[4] if len(row) > 4 else "",
                "note":  row[5] if len(row) > 5 else "",
            })
        return result
    except Exception as e:
        logger.exception("Errore get_lavori_giorno: %s", e)
        return []


async def async_get_lavori_mese(user_id_str: str, year: int, month: int) -> List[dict]:
    return await sheets_call(_sync_get_lavori_mese, user_id_str, year, month)


async def async_get_lavori_giorno(user_id_str: str, giorno: str) -> List[dict]:
    return await sheets_call(_sync_get_lavori_giorno, user_id_str, giorno)


# ============================================================
# Google Sheets helpers – Appunti
# ============================================================
# Struttura foglio "Appunti":
# Col A: ID (numero progressivo)  B: Telegram ID  C: Testo appunto  D: Data creazione

def _sync_get_appunti(user_id: int) -> List[dict]:
    """Restituisce tutti gli appunti dell'utente come lista di dict."""
    try:
        sheet = get_sheet("Appunti")
        rows = sheet.get_all_values()
        result = []
        for i, row in enumerate(rows[1:], start=2):
            if len(row) < 3:
                continue
            if row[1].strip() != str(user_id):
                continue
            result.append({
                "row": i,
                "id": row[0].strip(),
                "testo": row[2].strip(),
                "data": row[3].strip() if len(row) > 3 else "",
            })
        return result
    except Exception as e:
        logger.exception("Errore get_appunti: %s", e)
        return []


def _sync_add_appunto(user_id: int, testo: str) -> bool:
    """Aggiunge un nuovo appunto nel foglio Appunti."""
    try:
        sheet = get_sheet("Appunti")
        rows = sheet.get_all_values()
        # Calcola il prossimo ID progressivo per questo utente
        ids_utente = [
            int(r[0]) for r in rows[1:]
            if len(r) >= 2 and r[1].strip() == str(user_id)
            and r[0].strip().isdigit()
        ]
        next_id = (max(ids_utente) + 1) if ids_utente else 1
        now_local = datetime.now(TIMEZONE).strftime("%d.%m.%Y %H:%M")
        sheet.append_row([str(next_id), str(user_id), testo, now_local])
        return True
    except Exception as e:
        logger.exception("Errore add_appunto: %s", e)
        return False


def _sync_delete_appunto(user_id: int, row_index: int) -> bool:
    """Elimina la riga specificata dal foglio Appunti (verifica che appartenga all'utente)."""
    try:
        sheet = get_sheet("Appunti")
        row = sheet.row_values(row_index)
        if len(row) < 2 or row[1].strip() != str(user_id):
            return False
        sheet.delete_rows(row_index)
        return True
    except Exception as e:
        logger.exception("Errore delete_appunto: %s", e)
        return False


def _sync_edit_appunto(user_id: int, row_index: int, nuovo_testo: str) -> bool:
    """Modifica il testo di un appunto esistente."""
    try:
        sheet = get_sheet("Appunti")
        row = sheet.row_values(row_index)
        if len(row) < 2 or row[1].strip() != str(user_id):
            return False
        sheet.update_cell(row_index, 3, nuovo_testo)
        return True
    except Exception as e:
        logger.exception("Errore edit_appunto: %s", e)
        return False


async def async_get_appunti(user_id: int) -> List[dict]:
    return await sheets_call(_sync_get_appunti, user_id)

async def async_add_appunto(user_id: int, testo: str) -> bool:
    return await sheets_call(_sync_add_appunto, user_id, testo)

async def async_delete_appunto(user_id: int, row_index: int) -> bool:
    return await sheets_call(_sync_delete_appunto, user_id, row_index)

async def async_edit_appunto(user_id: int, row_index: int, nuovo_testo: str) -> bool:
    return await sheets_call(_sync_edit_appunto, user_id, row_index, nuovo_testo)


def _setup_appunti_formatting(sheet_app: Worksheet) -> None:
    """Formattazione intestazione foglio Appunti."""
    try:
        sid = sheet_app.id
        sheet_app.format("A1:D1", {
            "backgroundColor": {"red": 0.18, "green": 0.44, "blue": 0.31},
            "textFormat": {
                "bold": True,
                "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0},
                "fontSize": 11,
            },
            "horizontalAlignment": "CENTER",
        })
        sheet_app.spreadsheet.batch_update({"requests": [
            {
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": sid,
                        "gridProperties": {"frozenRowCount": 1},
                    },
                    "fields": "gridProperties.frozenRowCount",
                }
            },
            *[
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": sid,
                            "dimension": "COLUMNS",
                            "startIndex": i,
                            "endIndex": i + 1,
                        },
                        "properties": {"pixelSize": size},
                        "fields": "pixelSize",
                    }
                }
                for i, size in enumerate([50, 120, 400, 140])
            ],
        ]})
    except Exception as e:
        logger.warning("Formattazione Appunti non applicata: %s", e)


def _setup_produttivita_formatting(sheet_prod: Worksheet) -> None:
    """
    Applica formattazione completa al foglio Produttività:
    - Intestazione colorata, freeze, filtro
    - Formattazione condizionale alternata per data
    - Larghezze colonne ottimizzate
    """
    try:
        sid = sheet_prod.id
        spreadsheet = sheet_prod.spreadsheet

        # Intestazione
        sheet_prod.format("A1:F1", {
            "backgroundColor": {"red": 0.13, "green": 0.27, "blue": 0.53},
            "textFormat": {
                "bold": True,
                "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0},
                "fontSize": 11,
            },
            "horizontalAlignment": "CENTER",
            "verticalAlignment": "MIDDLE",
        })

        requests = [
            # Freeze riga 1
            {
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": sid,
                        "gridProperties": {"frozenRowCount": 1},
                    },
                    "fields": "gridProperties.frozenRowCount",
                }
            },
            # Filtro automatico
            {
                "setBasicFilter": {
                    "filter": {
                        "range": {
                            "sheetId": sid,
                            "startRowIndex": 0,
                            "startColumnIndex": 0,
                            "endColumnIndex": 6,
                        }
                    }
                }
            },
            # Formattazione condizionale righe pari (per data alternata)
            {
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [{
                            "sheetId": sid,
                            "startRowIndex": 1,
                            "startColumnIndex": 0,
                            "endColumnIndex": 6,
                        }],
                        "booleanRule": {
                            "condition": {
                                "type": "CUSTOM_FORMULA",
                                "values": [{"userEnteredValue": "=ISEVEN(COUNTIF($A$2:$A2,$A2))"}],
                            },
                            "format": {
                                "backgroundColor": {"red": 0.88, "green": 0.93, "blue": 0.98}
                            },
                        },
                    },
                    "index": 0,
                }
            },
            # Larghezze colonne: A=90, B=70, C=200, D=80, E=130, F=300
            *[
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": sid,
                            "dimension": "COLUMNS",
                            "startIndex": i,
                            "endIndex": i + 1,
                        },
                        "properties": {"pixelSize": size},
                        "fields": "pixelSize",
                    }
                }
                for i, size in enumerate([90, 70, 200, 80, 130, 300])
            ],
            # Altezza riga intestazione
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sid,
                        "dimension": "ROWS",
                        "startIndex": 0,
                        "endIndex": 1,
                    },
                    "properties": {"pixelSize": 36},
                    "fields": "pixelSize",
                }
            },
        ]

        spreadsheet.batch_update({"requests": requests})
        logger.info("Formattazione Produttività applicata.")
    except Exception as e:
        logger.warning("Formattazione Produttività non applicata (non bloccante): %s", e)


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
            pass
        try:
            sheet_notif = get_sheet("Notifiche")
            if not sheet_notif.row_values(1):
                sheet_notif.append_row([
                    "Telegram ID", "Nome",
                    "Reminder Ingresso", "Orario Ingresso",
                    "Reminder Uscita", "Orario Uscita"
                ])
        except Exception:
            pass
        # ── Produttività ──────────────────────────────────────
        try:
            sheet_prod = get_sheet("Produttività")
            if not sheet_prod.row_values(1):
                sheet_prod.append_row(
                    ["Data", "Ora", "Utente", "N° Bus", "Tipo Lavoro", "Note"]
                )
                _setup_produttivita_formatting(sheet_prod)
        except Exception as e:
            logger.warning("Produttività sheet init warning: %s", e)

        # ── Appunti ───────────────────────────────────────────
        try:
            sheet_app = get_sheet("Appunti")
            if not sheet_app.row_values(1):
                sheet_app.append_row(["ID", "Telegram ID", "Testo", "Data creazione"])
                _setup_appunti_formatting(sheet_app)
        except Exception as e:
            logger.warning("Appunti sheet init warning: %s", e)

        logger.info("Sheets inizializzati.")
    except Exception as e:
        logger.error("Errore init_sheets: %s", e)


# ============================================================
# Notifiche sheet helpers
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


def upsert_user_notifiche(user_id: int, nome: str,
                           reminder_in: bool = True, orario_in: str = "08:00",
                           reminder_out: bool = True, orario_out: str = "17:00") -> bool:
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
        [KeyboardButton(text="🔧 Registra Lavoro")],
        [KeyboardButton(text="📆 Calendario Lavori")],
        [KeyboardButton(text="📋 I miei Appunti")],
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

class LavoroForm(StatesGroup):
    waiting_for_bus = State()
    waiting_for_tipo = State()
    waiting_for_note = State()

class AppuntiForm(StatesGroup):
    waiting_for_testo = State()       # testo del nuovo appunto
    waiting_for_edit_testo = State()  # testo modificato di un appunto esistente

class NotificheForm(StatesGroup):
    waiting_for_orario = State()


# ============================================================
# Calendar builder (permessi)
# ============================================================
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

    weeks = calendar.monthcalendar(year, month)
    while len(weeks) < 6:
        weeks.append([0] * 7)

    for week in weeks:
        for day in week:
            if day == 0:
                kb.button(text=" ", callback_data="ignore")
            else:
                text_day = (
                    f"🔵{day}"
                    if (day == today.day and month == today.month and year == today.year)
                    else str(day)
                )
                kb.button(text=text_day, callback_data=f"perm:{phase}:day:{year}:{month}:{day}")

    kb.button(text="◀️", callback_data=f"perm:{phase}:nav:{year}:{month}:prev")
    kb.button(text="▶️", callback_data=f"perm:{phase}:nav:{year}:{month}:next")
    kb.adjust(1, 7, *([7] * len(weeks)), 2)
    return kb.as_markup()


# ============================================================
# Calendario Lavori builder
# ============================================================
def build_lavori_calendar(
    year: int,
    month: int,
    giorni_con_lavori: set,
) -> types.InlineKeyboardMarkup:
    """
    Costruisce il calendario mensile per i lavori.
    I giorni con registrazioni sono evidenziati con ✅.
    I giorni vuoti mostrano solo il numero.
    """
    kb = InlineKeyboardBuilder()
    today = datetime.now(TIMEZONE)
    nomi_giorni = ["Lu", "Ma", "Me", "Gi", "Ve", "Sa", "Do"]

    # Riga titolo mese/anno
    kb.button(
        text=f"📆 {mese_nome(month)} {year}",
        callback_data="cal_lavori:ignore"
    )
    # Nomi giorni settimana
    for g in nomi_giorni:
        kb.button(text=g, callback_data="cal_lavori:ignore")

    weeks = calendar.monthcalendar(year, month)
    while len(weeks) < 6:
        weeks.append([0] * 7)

    for week in weeks:
        for day in week:
            if day == 0:
                kb.button(text=" ", callback_data="cal_lavori:ignore")
            else:
                giorno_str = f"{day:02d}.{month:02d}.{year}"
                ha_lavori = giorno_str in giorni_con_lavori
                is_today = (
                    day == today.day
                    and month == today.month
                    and year == today.year
                )
                if ha_lavori and is_today:
                    label = f"✅{day}◉"
                elif ha_lavori:
                    label = f"✅{day}"
                elif is_today:
                    label = f"🔵{day}"
                else:
                    label = str(day)

                kb.button(
                    text=label,
                    callback_data=f"cal_lavori:day:{year}:{month}:{day}"
                )

    # Navigazione mese
    kb.button(text="◀️ Mese prec.", callback_data=f"cal_lavori:nav:{year}:{month}:prev")
    kb.button(text="Mese succ. ▶️", callback_data=f"cal_lavori:nav:{year}:{month}:next")
    kb.adjust(1, 7, *([7] * len(weeks)), 2)
    return kb.as_markup()


# ============================================================
# Helper – zones list
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
# Handlers – Ingresso / Uscita
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
    nomi_mesi = [
        "Gen", "Feb", "Mar", "Apr", "Mag", "Giu",
        "Lug", "Ago", "Set", "Ott", "Nov", "Dic",
    ]
    now = datetime.now(TIMEZONE)
    for i, nome in enumerate(nomi_mesi, start=1):
        is_current = (year == now.year and i == now.month)
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
        await cb.message.answer("❌ Errore nell'invio del riepilogo.", reply_markup=main_kb)


# ============================================================
# Handlers – Registra Lavoro (Produttività)
# ============================================================

def _build_tipo_lavoro_kb() -> types.InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="🔩 Installazione", callback_data="lavoro_tipo:Installazione")
    kb.button(text="🔧 Manutenzione",  callback_data="lavoro_tipo:Manutenzione")
    kb.adjust(2)
    return kb.as_markup()


def _build_note_kb(appunti: List[dict]) -> types.InlineKeyboardMarkup:
    """
    Costruisce la keyboard per la selezione/inserimento note.
    Mostra gli appunti salvati dell'utente come bottoni veloci (max 8),
    più i tasti per scrivere una nota libera, saltare e gestire gli appunti.
    """
    kb = InlineKeyboardBuilder()
    # Bottoni veloci per ogni appunto salvato (max 8 per non esplodere la UI)
    for app in appunti[:8]:
        testo_breve = app["testo"][:40] + ("…" if len(app["testo"]) > 40 else "")
        kb.button(
            text=f"💬 {testo_breve}",
            callback_data=f"lavoro_note:appunto:{app['row']}"
        )
    kb.button(text="✏️ Scrivi nota libera", callback_data="lavoro_note:libera")
    kb.button(text="⏭️ Salta (nessuna nota)", callback_data="lavoro_note:skip")
    kb.adjust(1)
    return kb.as_markup()


@dp.message(F.text == "🔧 Registra Lavoro")
async def lavoro_start(message: Message, state: FSMContext):
    await state.set_state(LavoroForm.waiting_for_bus)
    await message.answer(
        "🚌 <b>Registrazione Lavoro</b>\n\n"
        "Inserisci il <b>numero del bus</b> su cui hai lavorato:",
        reply_markup=types.ReplyKeyboardRemove(),
    )


@dp.message(LavoroForm.waiting_for_bus)
async def lavoro_bus(message: Message, state: FSMContext):
    numero_bus = (message.text or "").strip()
    if not numero_bus:
        await message.answer("⚠️ Inserisci un numero di bus valido:")
        return
    await state.update_data(numero_bus=numero_bus)
    await state.set_state(LavoroForm.waiting_for_tipo)
    await message.answer(
        f"✅ Bus: <b>{numero_bus}</b>\n\n"
        "Seleziona il <b>tipo di lavoro</b> svolto:",
        reply_markup=_build_tipo_lavoro_kb(),
    )


@dp.callback_query(F.data.startswith("lavoro_tipo:"))
async def lavoro_tipo(cb: CallbackQuery, state: FSMContext):
    current = await state.get_state()
    if current != LavoroForm.waiting_for_tipo:
        await cb.answer()
        return
    tipo = cb.data.split(":", 1)[1]
    await state.update_data(tipo=tipo)
    await state.set_state(LavoroForm.waiting_for_note)

    # Carica gli appunti salvati per mostrare i bottoni veloci
    try:
        appunti = await async_get_appunti(cb.from_user.id)
    except asyncio.TimeoutError:
        appunti = []

    if appunti:
        testo_kb = (
            f"✅ Tipo: <b>{tipo}</b>\n\n"
            "📝 <b>Note:</b> seleziona un appunto salvato, scrivi una nota libera, oppure salta:"
        )
    else:
        testo_kb = (
            f"✅ Tipo: <b>{tipo}</b>\n\n"
            "📝 Vuoi aggiungere delle <b>note</b>?\n"
            "Scrivile qui sotto, oppure premi il bottone per saltare:"
        )

    await cb.message.edit_text(testo_kb, reply_markup=_build_note_kb(appunti))
    await cb.answer()


@dp.callback_query(F.data.startswith("lavoro_note:appunto:"))
async def lavoro_note_da_appunto(cb: CallbackQuery, state: FSMContext):
    """L'utente ha selezionato un appunto salvato come nota veloce."""
    current = await state.get_state()
    if current != LavoroForm.waiting_for_note:
        await cb.answer()
        return
    row_index = int(cb.data.split(":", 2)[2])
    try:
        appunti = await async_get_appunti(cb.from_user.id)
    except asyncio.TimeoutError:
        await cb.answer("⚠️ Timeout, riprova.", show_alert=True)
        return
    appunto = next((a for a in appunti if a["row"] == row_index), None)
    if not appunto:
        await cb.answer("⚠️ Appunto non trovato.", show_alert=True)
        return
    await cb.answer()
    await _salva_lavoro(cb, state, appunto["testo"], user=cb.from_user)


@dp.callback_query(F.data == "lavoro_note:libera")
async def lavoro_note_libera(cb: CallbackQuery, state: FSMContext):
    """L'utente vuole scrivere una nota libera: modifica il messaggio e attendi testo."""
    current = await state.get_state()
    if current != LavoroForm.waiting_for_note:
        await cb.answer()
        return
    # Salva message_id per poterlo editare dopo che l'utente scrive la nota
    await state.update_data(inline_msg_id=cb.message.message_id)
    await cb.message.edit_text(
        "✏️ <b>Scrivi la nota</b> da aggiungere al lavoro:\n\n"
        "<i>Digita il testo qui sotto e invia.</i>"
    )
    await cb.answer()


@dp.message(LavoroForm.waiting_for_note)
async def lavoro_note_testo(message: Message, state: FSMContext):
    """L'utente ha scritto la nota libera."""
    note = (message.text or "").strip()
    if not note:
        await message.answer("⚠️ La nota non può essere vuota. Scrivi qualcosa oppure premi Salta.")
        return
    await _salva_lavoro(message, state, note, user=message.from_user)


@dp.callback_query(F.data == "lavoro_note:skip")
async def lavoro_note_skip(cb: CallbackQuery, state: FSMContext):
    """L'utente salta la nota."""
    current = await state.get_state()
    if current != LavoroForm.waiting_for_note:
        await cb.answer()
        return
    await cb.answer()
    await _salva_lavoro(cb, state, "", user=cb.from_user)


async def _salva_lavoro(
    trigger: Union[CallbackQuery, Message],
    state: FSMContext,
    note: str,
    user: types.User,
) -> None:
    """
    Salva il lavoro su Sheets e mostra il riepilogo con la main_kb.

    Strategia output a seconda del trigger:
    - CallbackQuery  → edit_text sul messaggio inline (feedback immediato,
                       nessun bottone residuo) + send_message per riepilogo + main_kb
    - Message        → il messaggio inline precedente (nota libera) è già stato editato
                       da lavoro_note_libera; qui basta send_message per riepilogo + main_kb
    """
    data = await state.get_data()
    numero_bus = data.get("numero_bus", "")
    tipo = data.get("tipo", "")
    chat_id = (
        trigger.message.chat.id
        if isinstance(trigger, CallbackQuery)
        else trigger.chat.id
    )
    await state.clear()

    # Feedback immediato: rimuovi i bottoni inline dal messaggio precedente
    if isinstance(trigger, CallbackQuery):
        try:
            await trigger.message.edit_text("⏳ Salvataggio in corso…")
        except Exception:
            pass

    try:
        ok = await async_save_lavoro(user, numero_bus, tipo, note)
    except asyncio.TimeoutError:
        await bot.send_message(
            chat_id,
            "⚠️ Timeout salvataggio, riprova tra qualche secondo.",
            reply_markup=main_kb,
        )
        return

    if ok:
        riepilogo = (
            "✅ <b>Lavoro registrato!</b>\n\n"
            f"🚌 Bus: <b>{numero_bus}</b>\n"
            f"🔧 Tipo: <b>{tipo}</b>\n"
            f"📝 Note: {note if note else '—'}"
        )
        if note:
            # Proponi di salvare la nota come appunto futuro
            kb_salva = InlineKeyboardBuilder()
            kb_salva.button(
                text="💾 Salva questa nota negli appunti",
                callback_data=f"appunto_salva_rapido:{note[:200]}",
            )
            kb_salva.adjust(1)
            await bot.send_message(chat_id, riepilogo, reply_markup=kb_salva.as_markup())
        else:
            await bot.send_message(chat_id, riepilogo)
        # Messaggio separato con la main_kb: garantisce che la tastiera riappaia sempre
        await bot.send_message(chat_id, "Scegli un'opzione:", reply_markup=main_kb)
    else:
        await bot.send_message(
            chat_id,
            "❌ Errore durante il salvataggio. Riprova.",
            reply_markup=main_kb,
        )


@dp.callback_query(F.data.startswith("appunto_salva_rapido:"))
async def appunto_salva_rapido_handler(cb: CallbackQuery):
    """Salva la nota appena usata come nuovo appunto direttamente dalla conferma lavoro."""
    testo = cb.data.split(":", 1)[1]
    try:
        ok = await async_add_appunto(cb.from_user.id, testo)
    except asyncio.TimeoutError:
        await cb.answer("⚠️ Timeout, riprova.", show_alert=True)
        return
    if ok:
        await cb.answer("✅ Nota salvata negli appunti!", show_alert=False)
        await cb.message.edit_reply_markup(reply_markup=None)
    else:
        await cb.answer("❌ Errore nel salvataggio.", show_alert=True)


# ============================================================
# Handlers – Appunti (gestione completa)
# ============================================================

def _build_appunti_lista_kb(appunti: List[dict]) -> types.InlineKeyboardMarkup:
    """Keyboard elenco appunti: ogni appunto ha bottone per visualizzarlo/modificarlo."""
    kb = InlineKeyboardBuilder()
    for app in appunti:
        testo_breve = app["testo"][:35] + ("…" if len(app["testo"]) > 35 else "")
        kb.button(
            text=f"📌 {testo_breve}",
            callback_data=f"appunto:view:{app['row']}"
        )
    kb.button(text="➕ Nuovo appunto", callback_data="appunto:new")
    kb.adjust(1)
    return kb.as_markup()


def _build_appunto_detail_kb(row_index: int) -> types.InlineKeyboardMarkup:
    """Keyboard dettaglio singolo appunto: modifica, elimina, torna alla lista."""
    kb = InlineKeyboardBuilder()
    kb.button(text="✏️ Modifica", callback_data=f"appunto:edit:{row_index}")
    kb.button(text="🗑️ Elimina", callback_data=f"appunto:delete:{row_index}")
    kb.button(text="🔙 Torna agli appunti", callback_data="appunto:list")
    kb.adjust(2, 1)
    return kb.as_markup()


@dp.message(F.text == "📋 I miei Appunti")
async def appunti_handler(message: Message):
    """Mostra la lista degli appunti salvati dell'utente."""
    try:
        appunti = await async_get_appunti(message.from_user.id)
    except asyncio.TimeoutError:
        await message.answer("⚠️ Il server è lento, riprova tra qualche secondo.", reply_markup=main_kb)
        return

    if not appunti:
        kb = InlineKeyboardBuilder()
        kb.button(text="➕ Crea il tuo primo appunto", callback_data="appunto:new")
        kb.adjust(1)
        await message.answer(
            "📋 <b>I miei Appunti</b>\n\n"
            "Non hai ancora nessun appunto salvato.\n"
            "Gli appunti sono <b>note veloci</b> riutilizzabili durante la registrazione dei lavori.",
            reply_markup=kb.as_markup(),
        )
    else:
        await message.answer(
            f"📋 <b>I miei Appunti</b> ({len(appunti)} salvati)\n\n"
            "Tocca un appunto per vederlo, modificarlo o eliminarlo.\n"
            "Puoi usarli come <b>note veloci</b> durante la registrazione lavori.",
            reply_markup=_build_appunti_lista_kb(appunti),
        )


@dp.callback_query(F.data == "appunto:list")
async def appunto_list_handler(cb: CallbackQuery):
    """Torna alla lista appunti (usato dal dettaglio)."""
    try:
        appunti = await async_get_appunti(cb.from_user.id)
    except asyncio.TimeoutError:
        await cb.answer("⚠️ Timeout, riprova.", show_alert=True)
        return

    if not appunti:
        kb = InlineKeyboardBuilder()
        kb.button(text="➕ Crea il tuo primo appunto", callback_data="appunto:new")
        kb.adjust(1)
        await cb.message.edit_text(
            "📋 <b>I miei Appunti</b>\n\nNessun appunto salvato.",
            reply_markup=kb.as_markup(),
        )
    else:
        await cb.message.edit_text(
            f"📋 <b>I miei Appunti</b> ({len(appunti)} salvati)\n\n"
            "Tocca un appunto per vederlo, modificarlo o eliminarlo.",
            reply_markup=_build_appunti_lista_kb(appunti),
        )
    await cb.answer()


@dp.callback_query(F.data == "appunto:new")
async def appunto_new_handler(cb: CallbackQuery, state: FSMContext):
    """Avvia la creazione di un nuovo appunto."""
    await state.set_state(AppuntiForm.waiting_for_testo)
    await cb.message.edit_text(
        "➕ <b>Nuovo Appunto</b>\n\n"
        "Scrivi il testo dell'appunto.\n"
        "<i>Esempio: «Sostituzione display anteriore», «Controllo freni», «Pulizia motore»…</i>"
    )
    await cb.answer()


@dp.message(AppuntiForm.waiting_for_testo)
async def appunto_testo_handler(message: Message, state: FSMContext):
    """Riceve il testo del nuovo appunto e lo salva."""
    testo = (message.text or "").strip()
    if not testo:
        await message.answer("⚠️ Il testo non può essere vuoto. Scrivi qualcosa:")
        return
    if len(testo) > 300:
        await message.answer("⚠️ Testo troppo lungo (max 300 caratteri). Riprova:")
        return
    await state.clear()
    try:
        ok = await async_add_appunto(message.from_user.id, testo)
    except asyncio.TimeoutError:
        await message.answer("⚠️ Timeout salvataggio, riprova.", reply_markup=main_kb)
        return
    if ok:
        kb = InlineKeyboardBuilder()
        kb.button(text="📋 Vedi tutti gli appunti", callback_data="appunto:list")
        kb.button(text="➕ Aggiungi un altro", callback_data="appunto:new")
        kb.adjust(1)
        await message.answer(
            f"✅ <b>Appunto salvato!</b>\n\n<i>«{testo}»</i>",
            reply_markup=kb.as_markup(),
        )
        await message.answer("Scegli un'opzione:", reply_markup=main_kb)
    else:
        await message.answer("❌ Errore nel salvataggio. Riprova.", reply_markup=main_kb)


@dp.callback_query(F.data.startswith("appunto:view:"))
async def appunto_view_handler(cb: CallbackQuery):
    """Mostra il dettaglio di un singolo appunto con opzioni modifica/elimina."""
    row_index = int(cb.data.split(":")[2])
    try:
        appunti = await async_get_appunti(cb.from_user.id)
    except asyncio.TimeoutError:
        await cb.answer("⚠️ Timeout, riprova.", show_alert=True)
        return
    appunto = next((a for a in appunti if a["row"] == row_index), None)
    if not appunto:
        await cb.answer("⚠️ Appunto non trovato (potrebbe essere stato eliminato).", show_alert=True)
        return
    data_str = f"\n<i>Creato il {appunto['data']}</i>" if appunto["data"] else ""
    await cb.message.edit_text(
        f"📌 <b>Appunto</b>\n\n{appunto['testo']}{data_str}",
        reply_markup=_build_appunto_detail_kb(row_index),
    )
    await cb.answer()


@dp.callback_query(F.data.startswith("appunto:edit:"))
async def appunto_edit_start(cb: CallbackQuery, state: FSMContext):
    """Avvia la modifica di un appunto."""
    row_index = int(cb.data.split(":")[2])
    # Recupera il testo attuale
    try:
        appunti = await async_get_appunti(cb.from_user.id)
    except asyncio.TimeoutError:
        await cb.answer("⚠️ Timeout, riprova.", show_alert=True)
        return
    appunto = next((a for a in appunti if a["row"] == row_index), None)
    if not appunto:
        await cb.answer("⚠️ Appunto non trovato.", show_alert=True)
        return
    await state.update_data(editing_appunto_row=row_index)
    await state.set_state(AppuntiForm.waiting_for_edit_testo)
    await cb.message.edit_text(
        f"✏️ <b>Modifica Appunto</b>\n\n"
        f"Testo attuale:\n<i>«{appunto['testo']}»</i>\n\n"
        "Scrivi il nuovo testo (o scrivi <b>Annulla</b> per tornare indietro):"
    )
    await cb.answer()


@dp.message(AppuntiForm.waiting_for_edit_testo)
async def appunto_edit_testo_handler(message: Message, state: FSMContext):
    """Riceve il nuovo testo e aggiorna l'appunto."""
    if (message.text or "").strip().lower() == "annulla":
        await state.clear()
        await message.answer("❌ Modifica annullata.", reply_markup=main_kb)
        return
    nuovo_testo = (message.text or "").strip()
    if not nuovo_testo:
        await message.answer("⚠️ Il testo non può essere vuoto. Scrivi qualcosa:")
        return
    if len(nuovo_testo) > 300:
        await message.answer("⚠️ Testo troppo lungo (max 300 caratteri). Riprova:")
        return
    data = await state.get_data()
    row_index = data.get("editing_appunto_row")
    await state.clear()
    try:
        ok = await async_edit_appunto(message.from_user.id, row_index, nuovo_testo)
    except asyncio.TimeoutError:
        await message.answer("⚠️ Timeout, riprova.", reply_markup=main_kb)
        return
    if ok:
        kb = InlineKeyboardBuilder()
        kb.button(text="📋 Vedi tutti gli appunti", callback_data="appunto:list")
        kb.adjust(1)
        await message.answer(
            f"✅ <b>Appunto aggiornato!</b>\n\n<i>«{nuovo_testo}»</i>",
            reply_markup=kb.as_markup(),
        )
        await message.answer("Scegli un'opzione:", reply_markup=main_kb)
    else:
        await message.answer("❌ Errore nell'aggiornamento. Riprova.", reply_markup=main_kb)


@dp.callback_query(F.data.startswith("appunto:delete:"))
async def appunto_delete_confirm(cb: CallbackQuery):
    """Chiede conferma prima di eliminare l'appunto."""
    row_index = int(cb.data.split(":")[2])
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Sì, elimina", callback_data=f"appunto:confirm_delete:{row_index}")
    kb.button(text="❌ Annulla", callback_data=f"appunto:view:{row_index}")
    kb.adjust(2)
    await cb.message.edit_text(
        "🗑️ <b>Conferma eliminazione</b>\n\nSei sicuro di voler eliminare questo appunto?\nNon sarà più disponibile come nota veloce.",
        reply_markup=kb.as_markup(),
    )
    await cb.answer()


@dp.callback_query(F.data.startswith("appunto:confirm_delete:"))
async def appunto_confirm_delete_handler(cb: CallbackQuery):
    """Esegue la cancellazione dell'appunto."""
    row_index = int(cb.data.split(":")[2])
    try:
        ok = await async_delete_appunto(cb.from_user.id, row_index)
    except asyncio.TimeoutError:
        await cb.answer("⚠️ Timeout, riprova.", show_alert=True)
        return
    if ok:
        # Ricarica la lista aggiornata
        try:
            appunti = await async_get_appunti(cb.from_user.id)
        except asyncio.TimeoutError:
            appunti = []
        if appunti:
            await cb.message.edit_text(
                f"✅ Appunto eliminato.\n\n📋 <b>I miei Appunti</b> ({len(appunti)} rimasti)",
                reply_markup=_build_appunti_lista_kb(appunti),
            )
        else:
            kb = InlineKeyboardBuilder()
            kb.button(text="➕ Crea il tuo primo appunto", callback_data="appunto:new")
            kb.adjust(1)
            await cb.message.edit_text(
                "✅ Appunto eliminato.\n\n📋 Non hai altri appunti salvati.",
                reply_markup=kb.as_markup(),
            )
    else:
        await cb.answer("❌ Errore nell'eliminazione.", show_alert=True)
    await cb.answer()


# ============================================================
# Handlers – Calendario Lavori
# ============================================================

@dp.message(F.text == "📆 Calendario Lavori")
async def calendario_lavori_start(message: Message):
    """
    Mostra il calendario del mese corrente con i giorni evidenziati
    dove l'utente ha registrato dei lavori.
    """
    now = datetime.now(TIMEZONE)
    year, month = now.year, now.month
    user_id_str = f"{message.from_user.full_name} | {message.from_user.id}"

    await message.answer("⏳ Carico il calendario…", reply_markup=main_kb)

    try:
        lavori = await async_get_lavori_mese(user_id_str, year, month)
    except asyncio.TimeoutError:
        await message.answer("⚠️ Il server è lento, riprova tra qualche secondo.", reply_markup=main_kb)
        return

    giorni_con_lavori = {r["data"] for r in lavori}
    totale = len(lavori)

    testo = (
        f"📆 <b>Calendario Lavori — {mese_nome(month)} {year}</b>\n\n"
        f"✅ = giorno con registrazioni  🔵 = oggi\n\n"
        f"Lavori registrati questo mese: <b>{totale}</b>\n"
        "Tocca un giorno evidenziato per vedere il dettaglio."
    )

    await message.answer(
        testo,
        reply_markup=build_lavori_calendar(year, month, giorni_con_lavori),
    )


@dp.callback_query(F.data.startswith("cal_lavori:"))
async def calendario_lavori_handler(cb: CallbackQuery):
    parts = cb.data.split(":")
    action = parts[1]

    if action == "ignore":
        await cb.answer()
        return

    user_id_str = f"{cb.from_user.full_name} | {cb.from_user.id}"

    # ── Navigazione mese ──────────────────────────────────
    if action == "nav":
        year, month, direction = int(parts[2]), int(parts[3]), parts[4]
        if direction == "prev":
            month, year = (12, year - 1) if month == 1 else (month - 1, year)
        else:
            month, year = (1, year + 1) if month == 12 else (month + 1, year)

        await cb.answer(f"⏳ Carico {mese_nome(month)} {year}…")

        try:
            lavori = await async_get_lavori_mese(user_id_str, year, month)
        except asyncio.TimeoutError:
            await cb.answer("⚠️ Timeout, riprova.", show_alert=True)
            return

        giorni_con_lavori = {r["data"] for r in lavori}
        totale = len(lavori)

        testo = (
            f"📆 <b>Calendario Lavori — {mese_nome(month)} {year}</b>\n\n"
            f"✅ = giorno con registrazioni  🔵 = oggi\n\n"
            f"Lavori registrati questo mese: <b>{totale}</b>\n"
            "Tocca un giorno evidenziato per vedere il dettaglio."
        )

        await cb.message.edit_text(
            testo,
            reply_markup=build_lavori_calendar(year, month, giorni_con_lavori),
        )
        return

    # ── Dettaglio giorno ──────────────────────────────────
    if action == "day":
        year, month, day = int(parts[2]), int(parts[3]), int(parts[4])
        giorno_str = f"{day:02d}.{month:02d}.{year}"

        await cb.answer(f"⏳ Carico {giorno_str}…")

        try:
            lavori_giorno = await async_get_lavori_giorno(user_id_str, giorno_str)
        except asyncio.TimeoutError:
            await cb.answer("⚠️ Timeout, riprova.", show_alert=True)
            return

        if not lavori_giorno:
            await cb.answer(
                f"Nessun lavoro registrato il {giorno_str}.",
                show_alert=True
            )
            return

        # Costruisce il testo dettaglio
        righe = [f"📋 <b>Lavori del {giorno_str}</b>\n"]
        for i, lav in enumerate(lavori_giorno, start=1):
            tipo_emoji = "🔩" if lav["tipo"] == "Installazione" else "🔧"
            righe.append(
                f"<b>{i}.</b> {tipo_emoji} <b>{lav['tipo']}</b> — Bus <b>{lav['bus']}</b>\n"
                f"   🕐 {lav['ora']}"
                + (f"\n   📝 {lav['note']}" if lav['note'] else "")
            )

        righe.append(f"\n<i>Totale: {len(lavori_giorno)} lavoro/i</i>")

        # Bottone per tornare al calendario
        kb = InlineKeyboardBuilder()
        kb.button(
            text=f"🔙 Torna a {mese_nome(month)} {year}",
            callback_data=f"cal_lavori:back:{year}:{month}"
        )
        kb.adjust(1)

        await cb.message.edit_text(
            "\n".join(righe),
            reply_markup=kb.as_markup(),
        )
        return

    # ── Torna al calendario (da dettaglio giorno) ─────────
    if action == "back":
        year, month = int(parts[2]), int(parts[3])

        await cb.answer(f"⏳ Carico {mese_nome(month)} {year}…")

        try:
            lavori = await async_get_lavori_mese(user_id_str, year, month)
        except asyncio.TimeoutError:
            await cb.answer("⚠️ Timeout, riprova.", show_alert=True)
            return

        giorni_con_lavori = {r["data"] for r in lavori}
        totale = len(lavori)

        testo = (
            f"📆 <b>Calendario Lavori — {mese_nome(month)} {year}</b>\n\n"
            f"✅ = giorno con registrazioni  🔵 = oggi\n\n"
            f"Lavori registrati questo mese: <b>{totale}</b>\n"
            "Tocca un giorno evidenziato per vedere il dettaglio."
        )

        await cb.message.edit_text(
            testo,
            reply_markup=build_lavori_calendar(year, month, giorni_con_lavori),
        )
        return

    await cb.answer()


# ============================================================
# Handlers – Istruzioni
# ============================================================
@dp.message(F.text == "📘 Istruzioni Bot")
async def istruzioni_handler(message: Message):
    istruzioni_text = (
        "<b>📖 Guida al Bot Presenze</b>\n\n"

        "<b>▶️ Avvio</b>\n"
        "Invia /start per aprire il menu principale.\n\n"

        "<b>🕓 Registrazione ingresso</b>\n"
        "1. Premi <b>Ingresso</b>\n"
        "2. Tocca il bottone 📍 <b>Invia posizione</b>\n"
        "3. Il bot verifica che tu sia in una sede autorizzata e salva ora e luogo.\n"
        "⚠️ Puoi registrare un solo ingresso al giorno.\n\n"

        "<b>🚪 Registrazione uscita</b>\n"
        "1. Premi <b>Uscita</b> e invia la posizione come sopra.\n"
        "2. Il bot aggiorna il tuo registro con l'orario di uscita.\n"
        "⚠️ È necessario aver già registrato l'ingresso nella stessa giornata.\n\n"

        "<b>🔧 Registra Lavoro</b>\n"
        "1. Premi <b>Registra Lavoro</b>\n"
        "2. Inserisci il <b>numero del bus</b>\n"
        "3. Seleziona il tipo: <b>Installazione</b> o <b>Manutenzione</b>\n"
        "4. Scrivi eventuali note oppure premi <b>Salta</b>\n"
        "Il lavoro viene salvato nel foglio Produttività con data e ora.\n\n"

        "<b>📆 Calendario Lavori</b>\n"
        "Mostra il calendario mensile dei tuoi lavori.\n"
        "✅ = giorno con registrazioni  🔵 = oggi\n"
        "Tocca un giorno evidenziato per vedere il dettaglio completo.\n"
        "Naviga tra i mesi con ◀️ e ▶️.\n\n"

        "<b>📝 Richiesta permessi</b>\n"
        "1. Premi <b>Richiesta permessi</b>\n"
        "2. Seleziona data inizio e fine dal calendario\n"
        "3. Scrivi il motivo\n"
        "La richiesta viene salvata nel foglio Permessi.\n\n"

        "<b>📄 Riepilogo presenze</b>\n"
        "Scegli anno e mese: riceverai un CSV con ingressi e uscite.\n\n"

        "<b>🔔 Notifiche reminder</b>\n"
        "Attiva/disattiva e configura gli orari dei reminder con /mienotifiche\n"
        "I reminder non vengono inviati sabato e domenica.\n\n"

        "<b>📍 Privacy</b>\n"
        "Il bot NON traccia la posizione in automatico.\n"
        "La posizione viene usata solo quando la invii manualmente.\n\n"

        "<b>📧 Assistenza</b>\n"
        "sserviceitalia@gmail.com – Shust Dmytro (3298333622)"
    )
    await message.answer(istruzioni_text, reply_markup=main_kb)


# ============================================================
# Handlers – Gestione Zone (admin)
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
    if message.text.strip().lower() == "annulla":
        await state.clear()
        await message.answer("❌ Operazione annullata.", reply_markup=main_kb)
        return
    data = await state.get_data()
    lat, lon = data.get("lat"), data.get("lon")
    name = message.text.strip()
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
        f"🗑️ <b>Conferma rimozione</b>\n\nSei sicuro di voler rimuovere <b>{zone_name}</b>?\nQuesta azione non può essere annullata.",
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
    if message.text.strip().lower() == "annulla":
        await state.clear()
        await message.answer("❌ Operazione annullata.", reply_markup=main_kb)
        return
    data = await state.get_data()
    old_name = data.get("editing_zone")
    new_name = message.text.strip()
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
    _notifiche_cache = await sheets_call(get_notifiche_settings)
    _notifiche_cache_time = now
    return _notifiche_cache


async def scheduler_loop() -> None:
    logger.info("Scheduler loop avviato (controllo ogni 30s, per-utente)")
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
                        sheet_reg = await sheets_call(get_sheet, "Registro")
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
                                await send_reminder(
                                    uid,
                                    f"🔔 Ciao {cfg['nome']}, ricorda di registrare l'ingresso!"
                                )
                            _sent_ingresso_today[uid] = today_date

                        for uid, cfg in needs_uscita:
                            has_entered = any(s.endswith(f"| {uid}") for s in entered_today)
                            has_exited = any(s.endswith(f"| {uid}") for s in exited_today)
                            if has_entered and not has_exited:
                                await send_reminder(
                                    uid,
                                    f"🔔 Ciao {cfg['nome']}, ricorda di registrare l'uscita!"
                                )
                            _sent_uscita_today[uid] = today_date

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.exception("Errore nel scheduler loop: %s", e)

            await asyncio.sleep(30)
    except asyncio.CancelledError:
        logger.info("Scheduler loop terminato.")


# ============================================================
# Handlers – /mienotifiche
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


@dp.message(F.text.in_({"/mienotifiche", "🔔 Mie Notifiche"}))
async def mienotifiche_handler(message: Message):
    uid = message.from_user.id
    settings = await sheets_call(get_notifiche_settings)
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


# ============================================================
# Handlers – /notifiche (admin)
# ============================================================
@dp.message(F.text == "/notifiche")
async def notifiche_admin_handler(message: Message):
    if message.from_user.id not in ADMINS:
        await message.answer("❌ Non hai i permessi.")
        return
    settings = await sheets_call(get_notifiche_settings)
    if not settings:
        await message.answer("❌ Nessun utente nel foglio Notifiche.")
        return
    kb = InlineKeyboardBuilder()
    for uid, cfg in settings.items():
        stato = "✅" if (cfg["reminder_ingresso"] or cfg["reminder_uscita"]) else "❌"
        kb.button(text=f"{stato} {cfg['nome']}", callback_data=f"notif:admin_user:{uid}")
    kb.adjust(1)
    await message.answer(
        "👥 <b>Gestione notifiche utenti</b>\n\nSeleziona un utente:",
        reply_markup=kb.as_markup(),
    )


@dp.callback_query(F.data.startswith("notif:admin_user:"))
async def notif_admin_user_handler(cb: CallbackQuery):
    if cb.from_user.id not in ADMINS:
        await cb.answer("❌ Non autorizzato.", show_alert=True)
        return
    uid = int(cb.data.split(":")[2])
    settings = await sheets_call(get_notifiche_settings)
    if uid not in settings:
        await cb.answer("Utente non trovato.", show_alert=True)
        return
    cfg = settings[uid]
    nome = cfg['nome']
    in_stato = '✅ ON' if cfg['reminder_ingresso'] else '❌ OFF'
    out_stato = '✅ ON' if cfg['reminder_uscita'] else '❌ OFF'
    testo = (
        f"👤 <b>{nome}</b>\n\n"
        f"🕓 Ingresso: {in_stato} — {cfg['orario_ingresso']}\n"
        f"🚪 Uscita: {out_stato} — {cfg['orario_uscita']}"
    )
    await cb.message.edit_text(testo, reply_markup=_build_notif_kb_admin(uid, cfg))
    await cb.answer()


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


@dp.callback_query(F.data == "notif:admin_list")
async def notif_admin_list_handler(cb: CallbackQuery):
    if cb.from_user.id not in ADMINS:
        await cb.answer("❌ Non autorizzato.", show_alert=True)
        return
    settings = await sheets_call(get_notifiche_settings)
    kb = InlineKeyboardBuilder()
    for uid, cfg in settings.items():
        stato = "✅" if (cfg["reminder_ingresso"] or cfg["reminder_uscita"]) else "❌"
        kb.button(text=f"{stato} {cfg['nome']}", callback_data=f"notif:admin_user:{uid}")
    kb.adjust(1)
    await cb.message.edit_text(
        "👥 <b>Gestione notifiche utenti</b>\n\nSeleziona un utente:",
        reply_markup=kb.as_markup(),
    )
    await cb.answer()


@dp.callback_query(F.data.startswith("notif:toggle_in:") | F.data.startswith("notif:toggle_out:"))
async def notif_toggle_handler(cb: CallbackQuery):
    parts = cb.data.split(":")
    tipo = "in" if parts[1] == "toggle_in" else "out"
    uid = int(parts[2])

    if cb.from_user.id != uid and cb.from_user.id not in ADMINS:
        await cb.answer("❌ Non autorizzato.", show_alert=True)
        return

    new_val = await sheets_call(toggle_notifica, uid, tipo)
    if new_val is None:
        await cb.answer("❌ Errore nel salvataggio.", show_alert=True)
        return

    stato = "✅ attivato" if new_val else "❌ disattivato"
    tipo_str = "ingresso" if tipo == "in" else "uscita"
    await cb.answer(f"Reminder {tipo_str} {stato}!")

    settings = await sheets_call(get_notifiche_settings)
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
    msg = f"⏰ Inserisci il nuovo orario per il reminder di <b>{tipo_str}</b>\n\nFormato: <code>HH:MM</code> — es. <code>08:30</code>"
    await cb.message.edit_text(msg)
    await cb.answer()


@dp.message(NotificheForm.waiting_for_orario)
async def notif_set_orario_receive(message: Message, state: FSMContext):
    testo = (message.text or "").strip()
    if not re.match(r"^([01]\d|2[0-3]):[0-5]\d$", testo):
        await message.answer("❌ Formato non valido. Scrivi l'orario come <code>HH:MM</code>, es. <code>08:30</code>")
        return

    data = await state.get_data()
    uid = data["notif_uid"]
    tipo = data["notif_tipo"]
    await state.clear()

    ok = await sheets_call(set_orario_notifica, uid, tipo, testo)
    tipo_str = "ingresso" if tipo == "in" else "uscita"
    if ok:
        await message.answer(
            f"✅ Orario reminder <b>{tipo_str}</b> aggiornato a <b>{testo}</b>!",
            reply_markup=main_kb,
        )
    else:
        await message.answer(
            "❌ Errore nel salvataggio. L'utente potrebbe non essere nel foglio Notifiche.",
            reply_markup=main_kb,
        )


@dp.message(F.text == "/remindtest")
async def remindtest_handler(message: Message):
    if message.from_user.id not in ADMINS:
        await message.answer("❌ Non hai i permessi per eseguire questo comando.")
        return
    await message.answer("⏳ Eseguo test scheduler…")
    settings = await sheets_call(get_notifiche_settings)
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
# Fallback handler
# ============================================================
@dp.message()
async def fallback_handler(message: Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state is not None:
        await state.clear()
        await message.answer(
            "⚠️ Operazione annullata. Scegli un'opzione dal menu:",
            reply_markup=main_kb
        )
    else:
        await message.answer(
            "Non ho capito. Usa i bottoni del menu oppure /start per ricominciare.",
            reply_markup=main_kb
        )


# ============================================================
# Comando /status (solo admin)
# ============================================================
@dp.message(F.text == "/status")
async def status_handler(message: Message):
    if message.from_user.id not in ADMINS:
        await message.answer("❌ Non hai i permessi.")
        return

    lines = ["<b>🔍 Diagnostica Bot</b>\n"]
    lines.append(f"🔑 <b>Token:</b> {'✅ presente' if TOKEN else '❌ MANCANTE'}")

    try:
        wh = await bot.get_webhook_info()
        if wh.url:
            lines.append(f"🌐 <b>Webhook URL:</b> ✅ {wh.url}")
            lines.append(f"   Pending updates: {wh.pending_update_count}")
            if wh.last_error_message:
                lines.append(f"   ⚠️ Ultimo errore Telegram: {wh.last_error_message}")
        else:
            lines.append("🌐 <b>Webhook URL:</b> ❌ NON impostato su Telegram!")
    except Exception as e:
        lines.append(f"🌐 <b>Webhook:</b> ❌ errore lettura ({e})")

    lines.append(f"⚙️ <b>WEBHOOK_URL env:</b> {'✅ ' + WEBHOOK_URL if WEBHOOK_URL else '❌ MANCANTE'}")

    try:
        await sheets_call(get_sheet, "Registro")
        lines.append("📊 <b>Google Sheets:</b> ✅ connesso")
    except Exception as e:
        lines.append(f"📊 <b>Google Sheets:</b> ❌ errore: {e}")

    if CREDENTIALS_JSON:
        lines.append("🔐 <b>Google Credentials:</b> ✅ da variabile JSON")
    elif CREDENTIALS_FILE:
        lines.append(f"🔐 <b>Google Credentials:</b> ✅ da file ({CREDENTIALS_FILE})")
    else:
        lines.append("🔐 <b>Google Credentials:</b> ❌ MANCANTI")

    lines.append(f"🗂 <b>Sheet ID:</b> {'✅ presente' if SHEET_ID else '❌ MANCANTE'}")

    await message.answer("\n".join(lines))


# ============================================================
# FastAPI + lifecycle
# ============================================================
def _handle_task_exception(loop, context):
    exc = context.get("exception")
    msg = context.get("message", "Errore sconosciuto nel task")
    if exc:
        logger.exception("Eccezione non gestita in task asyncio: %s", msg, exc_info=exc)
    else:
        logger.error("Errore in task asyncio: %s", msg)


async def on_startup() -> None:
    global _sheets_semaphore
    _sheets_semaphore = asyncio.Semaphore(3)

    loop = asyncio.get_event_loop()
    loop.set_exception_handler(_handle_task_exception)

    try:
        await sheets_call(init_sheets)
    except Exception as e:
        logger.error("Init Sheets fallito (bot parte comunque): %s", e)

    if WEBHOOK_URL:
        webhook_endpoint = f"{WEBHOOK_URL.rstrip('/')}/webhook"
        try:
            await bot.set_webhook(webhook_endpoint, drop_pending_updates=True)
            logger.info("Webhook impostato: %s", webhook_endpoint)
        except Exception as e:
            logger.error("Errore impostazione webhook: %s", e)
    else:
        logger.warning(
            "WEBHOOK_URL non impostato: il webhook NON è stato registrato su Telegram."
        )

    asyncio.create_task(scheduler_loop())
    logger.info("Startup completato.")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await on_startup()
    yield
    logger.info("Shutdown completato.")


app = FastAPI(lifespan=lifespan)


async def _process_update(update: types.Update) -> None:
    try:
        await dp.feed_update(bot=bot, update=update)
    except Exception as e:
        logger.exception("Errore processando update: %s", e)


@app.post("/webhook")
async def webhook(request: Request):
    try:
        update = types.Update.model_validate(await request.json(), context={"bot": bot})
        asyncio.create_task(_process_update(update))
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
        "credentials_source": (
            "json_env" if CREDENTIALS_JSON
            else ("file" if CREDENTIALS_FILE else "MANCANTE")
        ),
    }
    try:
        wh = await bot.get_webhook_info()
        result["telegram_webhook_url"] = wh.url or "NON IMPOSTATO"
        result["telegram_pending_updates"] = wh.pending_update_count
        result["telegram_last_error"] = wh.last_error_message or "nessuno"
    except Exception as e:
        result["telegram_webhook_error"] = str(e)

    try:
        await sheets_call(get_sheet, "Registro")
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
