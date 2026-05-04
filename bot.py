import json
import asyncio
from pathlib import Path
import aiohttp
import html

import asyncio
import base64
from concurrent.futures import ThreadPoolExecutor

import cv2
import numpy as np
from pylibdmtx.pylibdmtx import decode as dm_decode
from pyzbar.pyzbar import decode as bar_decode

from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton
from aiogram.filters import CommandStart

# ================= CONFIG =================

import os
BOT_TOKEN  = os.environ.get("BOT_TOKEN", "8389107035:AAGC6OG1Nvp-HhpfRBhluwPmNNHgzFs5dwM")
DATA_FILE  = Path("tokens.json")
USAGE_FILE = Path("usage.json")

# ---- Whitelist по username (без @, регистр не важен) ----
WHITELIST_USERNAMES: set[str] = {
    "azim_gws", "Smartup_Asadullo"
    # добавь сюда других: "username2", "username3"
}

DAILY_LIMIT = 5  # запросов в сутки для не-whitelist юзеров

KEYBOARD = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📦 Проверить статус маркировки")],
        [KeyboardButton(text="🔄 Обновить токен")],
        [KeyboardButton(text="ℹ️ Как пользоваться ботом")]
    ],
    resize_keyboard=True
)

USER_STATE: dict[int, str] = {}
EXECUTOR = ThreadPoolExecutor(max_workers=4)

# ================= СПРАВОЧНИКИ =================

STATUS_RU = {
    "RECEIVED":    "Получен 📩",
    "APPLIED":     "Нанесён 🏷",
    "INTRODUCED":  "В обороте 🟢",
    "WITHDRAWN":   "Выведен из оборота ⭕️",
    "WRITTEN_OFF": "Списан 🪓",
}

EXT_STATUS_RU = {
    "CUSTOMS_ISSUED":      "Выпущен таможней",
    "CUSTOMS_ACCEPTED":    "На контроле таможни",
    "INSERT_INTO_ACC":     "Включён в АИК",
    "ACC_AGGREGATED":      "АИК сформирован",
    "ACC_DISAGGREGATED":   "АИК расформирован",
    "WAIT_SHIPMENT":       "Ожидает приёмки",
    "WAITING_FOR_IMPORT":  "Ожидает подтверждения импорта",
    "CONNECTED":           "Есть связанный код",
}

PACKAGE_TYPE_RU = {
    "UNIT":    "Потребительская (штучная)",
    "GROUP":   "Групповая",
    "SET":     "Набор",
    "BOX_LV_1": "Транспортная (1-й уровень)",
    "BOX_LV_2": "Транспортная (2-й уровень)",
    "ACC":     "Агрегированный код (АИК)",
}

RELEASE_METHOD_RU = {
    "PRODUCTION":  "Местное производство",
    "IMPORT":      "Импорт",
    "CIRCULATION": "Товар в обороте",
}

EMISSION_TYPE_RU = {
    "PRIMARY":   "Первичная маркировка",
    "REMAINS":   "Маркировка остатков",
    "COMISSION": "Комиссионная торговля",
    "REMARK":    "Перемаркировка",
    "EXTERNAL":  "Признание КМ",
    "SHIPPING":  "Транспортировка",
    "CUSTOMS":   "Таможенное оформление",
}

PRODUCT_GROUP_RU = {
    3:  "Табачная продукция",
    11: "Алкогольная продукция",
    15: "Пиво и пивные напитки",
    18: "Бытовая техника",
    7:  "Лекарственные средства",
    10: "Изделия медицинского назначения",
    13: "Вода и напитки",
    33: "Масложировая продукция",
    17: "Биологически активные добавки",
    19: "Антисептики",
    53: "Удобрения и средства защиты растений",
}

WITHDRAWAL_REASON_RU = {
    "DEFECT":           "Ошибки при маркировке",
    "SAMPLES":          "Образцы",
    "OTHER":            "Другое",
    "PRODUCTION_USE":   "Собственные нужды",
    "EXPIRATION":       "Истёк срок годности",
    "CONFISCATION":     "Конфискация",
    "PRODUCT_RECALL":   "Отзыв с рынка",
    "COMPLAINTS":       "Рекламации",
    "LOSS":             "Утрата",
    "DESTRUCTION":      "Уничтожение/Утилизация",
    "RETAIL":           "Розничная продажа",
    "EXPORT":           "Экспорт",
    "RETURN":           "Возврат от покупателя",
    "DISTANCE":         "Дистанционная продажа",
    "RECEIPT_SALE":     "Продажа по чеку",
}

RETURN_REASON_RU = {
    "RETAIL_RETURN":           "Возврат при рознице",
    "RECEIPT_RETURN":          "Возврат по чеку",
    "PRODUCTION_USE_RETURN":   "Возврат для производства",
    "OWN_USE_RETURN":          "Возврат для собственных нужд",
    "NOT_FOR_SALE_RETURN":     "Возврат от покупателя",
    "RECEIPT_RETURN_HORECA":   "Возврат по чеку HoReCa",
}

def tr_status(v):      return STATUS_RU.get(v, v) if v else "—"
def tr_ext(v):         return EXT_STATUS_RU.get(v, v) if v else None
def tr_pkg(v):         return PACKAGE_TYPE_RU.get(v, v) if v else "—"
def tr_release(v):     return RELEASE_METHOD_RU.get(v, v) if v else "—"
def tr_emission(v):    return EMISSION_TYPE_RU.get(v, v) if v else "—"
def tr_group(v):       return PRODUCT_GROUP_RU.get(v, str(v)) if v else "—"
def tr_withdrawal(v):  return WITHDRAWAL_REASON_RU.get(v, v) if v else None
def tr_return(v):      return RETURN_REASON_RU.get(v, v) if v else None

# ================= STORAGE =================

def load_tokens():
    if DATA_FILE.exists():
        return json.loads(DATA_FILE.read_text(encoding="utf-8"))
    return {}

def save_tokens(data):
    DATA_FILE.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

def get_user_token(user_id):
    return load_tokens().get(str(user_id))

def set_user_token(user_id, token):
    data = load_tokens()
    data[str(user_id)] = token.strip()
    save_tokens(data)

# ================= ACCESS CONTROL =================

def is_whitelisted(username: str | None) -> bool:
    if not username:
        return False
    return username.lower() in WHITELIST_USERNAMES

def load_usage() -> dict:
    if USAGE_FILE.exists():
        return json.loads(USAGE_FILE.read_text(encoding="utf-8"))
    return {}

def save_usage(data: dict):
    USAGE_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

def check_and_increment(user_id: int, username: str | None) -> bool:
    """
    True  → запрос разрешён
    False → лимит исчерпан или нет доступа
    Whitelist юзеры всегда True.
    """
    if is_whitelisted(username):
        return True

    from datetime import date
    today = str(date.today())
    data  = load_usage()
    key   = str(user_id)
    entry = data.get(key, {"date": today, "count": 0})

    if entry["date"] != today:
        entry = {"date": today, "count": 0}

    if entry["count"] >= DAILY_LIMIT:
        return False

    entry["count"] += 1
    data[key] = entry
    save_usage(data)
    return True

def remaining_today(user_id: int) -> int:
    from datetime import date
    today = str(date.today())
    data  = load_usage()
    entry = data.get(str(user_id), {"date": today, "count": 0})
    if entry["date"] != today:
        return DAILY_LIMIT
    return max(0, DAILY_LIMIT - entry["count"])

LIMIT_MSG = (
    "⛔ <b>Лимит запросов исчерпан</b>\n\n"
    f"Бесплатный план позволяет <b>{DAILY_LIMIT} проверок в сутки</b>.\n"
    "Для неограниченного доступа оформите подписку. 💳\n\n"
    "Контакт для оформления подписки: @azim_gws"
    )

NO_ACCESS_MSG = (
    "🔒 <b>Доступ закрыт</b>\n\n"
    "У вашего аккаунта нет доступа к боту.\n"
    "Для подключения оформите подписку. 💳"
    "Контакт для оформления подписки: @azim_gws"
)

# ================= UTILS =================

def clean_km(code: str) -> str:
    GS  = chr(29)
    RS  = chr(30)
    EOT = chr(4)
    code = code.replace(RS, "").replace(EOT, "").strip()
    if GS in code:
        code = code.split(GS)[0]
    else:
        if len(code) > 31:
            code = code[:31]
    return code.strip()


def looks_like_km(text: str) -> bool:
    t = text.strip()
    return len(t) >= 20 and t.startswith("01") and "21" in t and " " not in t

def format_date(date_str):
    if not date_str:
        return "—"
    try:
        return date_str.split("T")[0]
    except:
        return date_str

# ================= PARSER =================

def parse_xtrace_response(data):

    # ===== ОШИБКА ТОКЕНА =====
    if isinstance(data, dict) and data.get("code") == "access-denied":
        return {"type": "token_error"}

    # ===== КМ МОЙ (private API вернул results) =====
    if isinstance(data, dict) and "results" in data:

        if not data["results"]:
            return {"type": "not_found"}

        item = data["results"][0]

        code_data    = item.get("codeData", {})
        package_data = item.get("packageData", {})
        marking_data = item.get("markingData", {})
        issuer_info  = marking_data.get("issuerInfo", {})
        contractor   = marking_data.get("contractorInfo", {})
        turnover     = item.get("turnoverData", {})
        owner_info   = turnover.get("ownerInfo", {})
        product_data = item.get("productData", {})
        customs      = turnover.get("customsDeclaration", {})

        pkg_type     = package_data.get("packageType", "")
        children     = package_data.get("children", [])
        actually_packed = package_data.get("actuallyPacked", 0)

        # --- ТРАНСПОРТНАЯ УПАКОВКА BOX ---
        if pkg_type in ("BOX_LV_1", "BOX_LV_2"):
            agg_groups = package_data.get("aggregateProductGroups", [])
            agg_cats   = package_data.get("aggregateCategories", [])
            units_total = sum(g.get("unitsNumber", 0) for g in agg_groups)

            return {
                "type": "my_box",
                "code":           code_data.get("code"),
                "status":         code_data.get("status"),
                "extendedStatus": code_data.get("extendedStatus"),
                "packageType":    pkg_type,
                "parentCode":     package_data.get("parentCode"),
                "emptyPackage":   package_data.get("emptyPackage"),
                "childrenCount":  actually_packed or len(children),
                "unitsTotal":     units_total,
                "mixed":          package_data.get("mixedProductGroups") or package_data.get("mixedCategories"),
                "productGroupId": agg_groups[0].get("productGroupId") if agg_groups else None,
                "issuerTin":      issuer_info.get("issuerTin"),
                "issuerName":     issuer_info.get("issuerName", {}).get("ru"),
                "ownerTin":       owner_info.get("ownerTin"),
                "ownerName":      owner_info.get("ownerName", {}).get("ru"),
                "releaseMethod":  turnover.get("originalReleaseMethod"),
                "emissionType":   marking_data.get("emissionType"),
                "emissionDate":   format_date(marking_data.get("emissionDate")),
                "issueDate":      format_date(marking_data.get("issueDate")),
                "withdrawalDate": format_date(turnover.get("withdrawalDate")),
                "withdrawalReason": turnover.get("withdrawalReason"),
                "customs":        customs if customs else None,
            }

        # --- ГРУППОВАЯ УПАКОВКА ---
        elif pkg_type == "GROUP":
            agg_groups  = package_data.get("aggregateProductGroups", [])
            units_total = sum(g.get("unitsNumber", 0) for g in agg_groups)

            return {
                "type": "my_group",
                "code":           code_data.get("code"),
                "status":         code_data.get("status"),
                "extendedStatus": code_data.get("extendedStatus"),
                "packageType":    pkg_type,
                "parentCode":     package_data.get("parentCode"),
                "childrenCount":  actually_packed or len(children),
                "unitsTotal":     units_total,
                "mixed":          package_data.get("mixedProductGroups") or package_data.get("mixedCategories"),
                "productGroupId": agg_groups[0].get("productGroupId") if agg_groups else product_data.get("productGroupId"),
                "gtin":           product_data.get("gtin"),
                "expirationDate": format_date(product_data.get("expirationDate")),
                "productionDate": format_date(product_data.get("productionDate")),
                "productSeries":  product_data.get("productSeries"),
                "issuerTin":      issuer_info.get("issuerTin"),
                "issuerName":     issuer_info.get("issuerName", {}).get("ru"),
                "ownerTin":       owner_info.get("ownerTin"),
                "ownerName":      owner_info.get("ownerName", {}).get("ru"),
                "releaseMethod":  turnover.get("originalReleaseMethod"),
                "emissionType":   marking_data.get("emissionType"),
                "emissionDate":   format_date(marking_data.get("emissionDate")),
                "issueDate":      format_date(marking_data.get("issueDate")),
            }

        # --- ШТУЧНАЯ УПАКОВКА UNIT ---
        else:
            contractor_name = contractor.get("contractorName", {}).get("ru") if contractor else None
            contractor_tin  = contractor.get("contractorTin") if contractor else None

            return {
                "type": "my_unit",
                "code":             code_data.get("code"),
                "status":           code_data.get("status"),
                "extendedStatus":   code_data.get("extendedStatus"),
                "template":         code_data.get("template"),
                "packageType":      pkg_type,
                "parentCode":       package_data.get("parentCode"),
                "gtin":             product_data.get("gtin"),
                "productGroupId":   product_data.get("productGroupId"),
                "categoryId":       product_data.get("categoryId"),
                "productSeries":    product_data.get("productSeries"),
                "productionDate":   format_date(product_data.get("productionDate")),
                "expirationDate":   format_date(product_data.get("expirationDate")),
                "manufacturerCountry": product_data.get("manufacturerCountry", "").upper() or None,
                "issuerTin":        issuer_info.get("issuerTin"),
                "issuerName":       issuer_info.get("issuerName", {}).get("ru"),
                "contractorName":   contractor_name,
                "contractorTin":    contractor_tin,
                "ownerTin":         owner_info.get("ownerTin"),
                "ownerName":        owner_info.get("ownerName", {}).get("ru"),
                "releaseMethod":    turnover.get("originalReleaseMethod"),
                "emissionType":     marking_data.get("emissionType"),
                "emissionDate":     format_date(marking_data.get("emissionDate")),
                "utilisationDate":  format_date(marking_data.get("utilisationDate")),
                "validationDate":   format_date(marking_data.get("validationDate")),
                "paymentDate":      format_date(marking_data.get("paymentDate")),
                "withdrawalDate":   format_date(turnover.get("withdrawalDate")),
                "withdrawalReason": turnover.get("withdrawalReason"),
                "returnDate":       format_date(turnover.get("returnDate")),
                "returnReason":     turnover.get("returnReason"),
                "partialQuantity":  turnover.get("partialQuantity"),
                "customs":          customs if customs else None,
            }

    # ===== КМ НЕ МОЙ (public API вернул list) =====
    if isinstance(data, list):

        if not data:
            return {"type": "not_found"}

        item = data[0]
        pkg_type = item.get("packageType", "")

        if pkg_type in ("BOX_LV_1", "BOX_LV_2", "GROUP"):
            agg_groups = item.get("aggregateProductGroups", [])
            units_total = sum(g.get("unitsNumber", 0) for g in agg_groups)

            return {
                "type": "foreign_box" if pkg_type in ("BOX_LV_1", "BOX_LV_2") else "foreign_group",
                "code":           item.get("code"),
                "packageType":    pkg_type,
                "status":         item.get("status"),
                "extendedStatus": item.get("extendedStatus"),
                "issuerTin":      item.get("issuerShortInfo", {}).get("issuerTin"),
                "issuerName":     item.get("issuerShortInfo", {}).get("issuerName", {}).get("ru"),
                "unitsNumber":    units_total,
                "mixed":          item.get("mixedProductGroups") or item.get("mixedCategories"),
                "expirationDate": format_date(item.get("expirationDate")),
                "productionDate": format_date(item.get("productionDate")),
            }

        else:
            return {
                "type": "foreign_unit",
                "code":             item.get("code"),
                "status":           item.get("status"),
                "extendedStatus":   item.get("extendedStatus"),
                "packageType":      pkg_type,
                "gtin":             item.get("gtin"),
                "productGroupId":   item.get("productGroupId"),
                "issuerTin":        item.get("issuerShortInfo", {}).get("issuerTin"),
                "issuerName":       item.get("issuerShortInfo", {}).get("issuerName", {}).get("ru"),
                "expirationDate":   format_date(item.get("expirationDate")),
                "productionDate":   format_date(item.get("productionDate")),
                "productSeries":    item.get("productSeries"),
                "manufacturerCountry": item.get("manufacturerCountry", "").upper() or None,
            }

    return {"type": "unknown"}

# ================= API =================

async def check_marking(token, km):

    url = "https://xtrace.aslbelgisi.uz/public/api/cod/private/codes"

    headers = {
        "Content-Type": "application/json;charset=UTF-8",
        "Authorization": f"Bearer {token}"
    }

    payload = {"codes": [km]}

    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, json=payload) as resp:
            data = await resp.json()
            return parse_xtrace_response(data)

# ================= ROUTER =================

router = Router()

@router.message(CommandStart())
async def start(message: Message):
    USER_STATE[message.from_user.id] = "awaiting_token"
    await message.answer("Введите токен Asl Belgisi:")

@router.message(F.text)
async def handle_text(message: Message):

    user_id  = message.from_user.id
    username = message.from_user.username  # может быть None
    text     = message.text.strip()
    state    = USER_STATE.get(user_id)

    # --- /start и служебные кнопки — без ограничений ---
    if state == "awaiting_token":
        set_user_token(user_id, text)
        USER_STATE.pop(user_id, None)
        await message.answer("Токен сохранён", reply_markup=KEYBOARD)
        return

    if text == "🔄 Обновить токен":
        USER_STATE[user_id] = "awaiting_token"
        await message.answer("Введите новый токен:")
        return

    if text == "📦 Проверить статус маркировки":
        if not get_user_token(user_id):
            await message.answer("Сначала введите токен через /start")
            return
        USER_STATE[user_id] = "awaiting_km"
        await message.answer("Введите KM:")
        return

    # --- Проверка KM — здесь применяем access control ---
    if state == "awaiting_km":
        USER_STATE.pop(user_id, None)

        # Нет username → нет доступа
        if not username:
            await message.answer(NO_ACCESS_MSG, parse_mode="HTML")
            return

        # Не в whitelist и нет подписки
        if not is_whitelisted(username) and remaining_today(user_id) == 0:
            await message.answer(LIMIT_MSG, parse_mode="HTML")
            return

        check_and_increment(user_id, username)
        token    = get_user_token(user_id)
        km_clean = clean_km(text)
        result   = await check_marking(token, km_clean)
        await send_result(message, result)
        return

    if looks_like_km(text):
        token = get_user_token(user_id)
        if not token:
            await message.answer("Сначала введите токен через /start")
            return

        if not username:
            await message.answer(NO_ACCESS_MSG, parse_mode="HTML")
            return

        if not is_whitelisted(username) and remaining_today(user_id) == 0:
            await message.answer(LIMIT_MSG, parse_mode="HTML")
            return

        check_and_increment(user_id, username)
        km_clean = clean_km(text)
        result   = await check_marking(token, km_clean)
        await send_result(message, result)

# ================= SEND RESULT =================

def e(value) -> str:
    return html.escape(str(value)) if value is not None else "—"

def row(label: str, value, transform=None) -> str | None:
    """Возвращает строку 'Метка: Значение' или None если значение пустое."""
    if value is None or value == "" or value == "—":
        return None
    v = transform(value) if transform else value
    if not v or v == "—":
        return None
    return f"<b>{label}:</b> {e(v)}"

def build_message(lines: list) -> str:
    """Собирает сообщение из непустых строк."""
    return "\n".join(l for l in lines if l is not None)


async def send_result(message: Message, result: dict):

    t = result["type"]

    # ===== ОШИБКИ =====

    if t == "token_error":
        await message.answer(
            "❌ <b>Ошибка авторизации</b>\n\n"
            "Токен не активен или введён неправильно.\n"
            "Обновите через кнопку: 🔄 Обновить токен",
            parse_mode="HTML"
        )
        return

    if t == "not_found":
        await message.answer(
            "❌ <b>Код маркировки не найден</b>\n\n"
            "Проверьте правильность кода и попробуйте снова.",
            parse_mode="HTML"
        )
        return

    if t == "unknown":
        await message.answer("⚠️ Неизвестный ответ от сервера")
        return

    # ===== МОЯ ШТУЧНАЯ =====

    if t == "my_unit":
        r = result
        ext = tr_ext(r.get("extendedStatus"))
        customs = r.get("customs")

        lines = [
            "📦 <b>Штучная маркировка</b>  <i>(ваш товар ✅)</i>\n",
            f"<b>Код:</b>\n<code>{e(r['code'])}</code>\n",
            row("Статус",          r.get("status"),         tr_status),
            row("Расш. статус",    ext) if ext else None,
            row("Тип упаковки",    r.get("packageType"),    tr_pkg),
            row("GTIN",            r.get("gtin")),
            row("Товарная группа", r.get("productGroupId"), tr_group),
            row("Серия/партия",    r.get("productSeries")),
            row("Страна произв.",  r.get("manufacturerCountry")),
            "",
            row("Родительская упаковка", r.get("parentCode")) and
                f"<b>Родительская упаковка:</b>\n<code>{e(r.get('parentCode'))}</code>",
            "",
            "<b>📅 Даты</b>",
            row("Дата произв.",    r.get("productionDate")),
            row("Срок годности",   r.get("expirationDate")),
            row("Дата эмиссии",    r.get("emissionDate")),
            row("Дата нанесения",  r.get("utilisationDate")),
            row("Дата валидации",  r.get("validationDate")),
            row("Дата оплаты",     r.get("paymentDate")),
            "",
            "<b>🏭 Эмитент</b>",
            f"{e(r.get('issuerName'))}\nИНН: {e(r.get('issuerTin'))}",
            row("Цель маркировки", r.get("emissionType"),   tr_emission),
            row("Способ ввода",    r.get("releaseMethod"),  tr_release),
        ]

        # СП (contractorInfo) — если есть
        if r.get("contractorName") or r.get("contractorTin"):
            lines += [
                "",
                "<b>🏢 Сервис-провайдер (СП)</b>",
                f"{e(r.get('contractorName'))}\nИНН: {e(r.get('contractorTin'))}",
            ]

        lines += [
            "",
            "<b>👤 Текущий владелец</b>",
            f"{e(r.get('ownerName'))}\nИНН: {e(r.get('ownerTin'))}",
        ]

        # Вывод из оборота
        if r.get("withdrawalDate") and r["withdrawalDate"] != "—":
            lines += [
                "",
                "<b>⛔ Вывод из оборота</b>",
                row("Дата",    r.get("withdrawalDate")),
                row("Причина", r.get("withdrawalReason"), tr_withdrawal),
                row("Остаток", r.get("partialQuantity")),
            ]

        # Возврат в оборот
        if r.get("returnDate") and r["returnDate"] != "—":
            lines += [
                "",
                "<b>🔄 Возврат в оборот</b>",
                row("Дата",    r.get("returnDate")),
                row("Причина", r.get("returnReason"), tr_return),
            ]

        # Таможня
        if customs:
            lines += [
                "",
                "<b>🛃 Таможенная декларация</b>",
                row("Номер ГТД",  customs.get("number")),
                row("Дата",       format_date(customs.get("date"))),
                row("Код органа", customs.get("authorityCode")),
            ]

        await message.answer(build_message(lines), parse_mode="HTML")

    # ===== МОЯ ГРУППОВАЯ =====

    elif t == "my_group":
        r = result
        ext = tr_ext(r.get("extendedStatus"))

        lines = [
            "📦 <b>Групповая упаковка</b>  <i>(ваш товар ✅)</i>\n",
            f"<b>Код:</b>\n<code>{e(r['code'])}</code>\n",
            row("Статус",           r.get("status"),         tr_status),
            row("Расш. статус",     ext) if ext else None,
            row("Тип упаковки",     r.get("packageType"),    tr_pkg),
            row("GTIN",             r.get("gtin")),
            row("Товарная группа",  r.get("productGroupId"), tr_group),
            row("Вложено упаковок", r.get("childrenCount")),
            row("Всего единиц",     r.get("unitsTotal")),
            "⚠️ <b>Разнородный состав</b>" if r.get("mixed") else None,
            "",
            row("Родительская упаковка", r.get("parentCode")) and
                f"<b>Родительская упаковка:</b>\n<code>{e(r.get('parentCode'))}</code>",
            "",
            "<b>📅 Даты</b>",
            row("Дата произв.",  r.get("productionDate")),
            row("Срок годности", r.get("expirationDate")),
            row("Серия/партия",  r.get("productSeries")),
            row("Дата эмиссии",  r.get("emissionDate")),
            "",
            "<b>🏭 Производитель</b>",
            f"{e(r.get('issuerName'))}\nИНН: {e(r.get('issuerTin'))}",
            row("Цель маркировки", r.get("emissionType"),   tr_emission),
            row("Способ ввода",    r.get("releaseMethod"),  tr_release),
            "",
            "<b>👤 Текущий владелец</b>",
            f"{e(r.get('ownerName'))}\nИНН: {e(r.get('ownerTin'))}",
        ]

        await message.answer(build_message(lines), parse_mode="HTML")

    # ===== МОЯ ТРАНСПОРТНАЯ =====

    elif t == "my_box":
        r = result
        ext = tr_ext(r.get("extendedStatus"))
        customs = r.get("customs")

        lines = [
            "🗃 <b>Транспортная упаковка</b>  <i>(ваш товар ✅)</i>\n",
            f"<b>Код:</b>\n<code>{e(r['code'])}</code>\n",
            row("Статус",          r.get("status"),         tr_status),
            row("Расш. статус",    ext) if ext else None,
            row("Тип упаковки",    r.get("packageType"),    tr_pkg),
            row("Товарная группа", r.get("productGroupId"), tr_group),
            row("Пустая",          "Да" if r.get("emptyPackage") else None),
            row("Вложено упаковок",r.get("childrenCount")),
            row("Всего единиц",    r.get("unitsTotal")),
            "⚠️ <b>Разнородный состав</b>" if r.get("mixed") else None,
            "",
            row("Родительская упаковка", r.get("parentCode")) and
                f"<b>Родительская упаковка:</b>\n<code>{e(r.get('parentCode'))}</code>",
            "",
            "<b>📅 Даты</b>",
            row("Дата эмиссии",  r.get("emissionDate")),
            row("Дата выдачи",   r.get("issueDate")),
            "",
            "<b>🏭 Производитель</b>",
            f"{e(r.get('issuerName'))}\nИНН: {e(r.get('issuerTin'))}",
            row("Цель маркировки", r.get("emissionType"),  tr_emission),
            row("Способ ввода",    r.get("releaseMethod"), tr_release),
            "",
            "<b>👤 Текущий владелец</b>",
            f"{e(r.get('ownerName'))}\nИНН: {e(r.get('ownerTin'))}",
        ]

        if r.get("withdrawalDate") and r["withdrawalDate"] != "—":
            lines += [
                "",
                "<b>⛔ Вывод из оборота</b>",
                row("Дата",    r.get("withdrawalDate")),
                row("Причина", r.get("withdrawalReason"), tr_withdrawal),
            ]

        if customs:
            lines += [
                "",
                "<b>🛃 Таможенная декларация</b>",
                row("Номер ГТД",  customs.get("number")),
                row("Дата",       format_date(customs.get("date"))),
                row("Код органа", customs.get("authorityCode")),
            ]

        await message.answer(build_message(lines), parse_mode="HTML")

    # ===== ЧУЖАЯ ШТУЧНАЯ =====

    elif t == "foreign_unit":
        r = result
        ext = tr_ext(r.get("extendedStatus"))

        lines = [
            "⚠️ <b>Штучная маркировка</b>  <i>(не ваш товар ❌)</i>\n",
            f"<b>Код:</b>\n<code>{e(r['code'])}</code>\n",
            row("Статус",         r.get("status"),         tr_status),
            row("Расш. статус",   ext) if ext else None,
            row("Тип упаковки",   r.get("packageType"),    tr_pkg),
            row("GTIN",           r.get("gtin")),
            row("Товарная группа",r.get("productGroupId"), tr_group),
            row("Серия/партия",   r.get("productSeries")),
            row("Страна произв.", r.get("manufacturerCountry")),
            "",
            "<b>📅 Даты</b>",
            row("Дата произв.",  r.get("productionDate")),
            row("Срок годности", r.get("expirationDate")),
            "",
            "<b>🏭 Производитель</b>",
            f"{e(r.get('issuerName'))}\nИНН: {e(r.get('issuerTin'))}",
        ]

        await message.answer(build_message(lines), parse_mode="HTML")

    # ===== ЧУЖАЯ ГРУППОВАЯ / ТРАНСПОРТНАЯ =====

    elif t in ("foreign_group", "foreign_box"):
        r = result
        ext = tr_ext(r.get("extendedStatus"))
        icon = "🗃" if t == "foreign_box" else "📦"
        label = "Транспортная упаковка" if t == "foreign_box" else "Групповая упаковка"

        lines = [
            f"⚠️ <b>{icon} {label}</b>  <i>(не ваш товар ❌)</i>\n",
            f"<b>Код:</b>\n<code>{e(r['code'])}</code>\n",
            row("Статус",          r.get("status"),      tr_status),
            row("Расш. статус",    ext) if ext else None,
            row("Тип упаковки",    r.get("packageType"), tr_pkg),
            row("Всего единиц",    r.get("unitsNumber")),
            "⚠️ <b>Разнородный состав</b>" if r.get("mixed") else None,
            "",
            "<b>📅 Даты</b>",
            row("Дата произв.",  r.get("productionDate")),
            row("Срок годности", r.get("expirationDate")),
            "",
            "<b>🏭 Производитель</b>",
            f"{e(r.get('issuerName'))}\nИНН: {e(r.get('issuerTin'))}",
        ]

        await message.answer(build_message(lines), parse_mode="HTML")


# ================= IMAGE RECOGNITION =================

def _decode_local_sync(path: str) -> str | None:
    import os

    img = cv2.imread(path)
    if img is None:
        return None

    h, w = img.shape[:2]

    def try_decode(image):
        devnull = os.open(os.devnull, os.O_WRONLY)
        old_stderr = os.dup(2)
        os.dup2(devnull, 2)
        os.close(devnull)
        try:
            found = dm_decode(image, timeout=500)
            if found:
                return clean_km(found[0].data.decode())
            found = bar_decode(image)
            if found:
                return clean_km(found[0].data.decode())
        finally:
            os.dup2(old_stderr, 2)
            os.close(old_stderr)
        return None

    if max(h, w) > 1600:
        scale = 1600 / max(h, w)
        img = cv2.resize(img, (int(w * scale), int(h * scale)))
        h, w = img.shape[:2]

    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    _, otsu = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)

    attempts = [
        ("orig",     img),
        ("gray",     gray),
        ("2x",       cv2.resize(gray, (w*2, h*2), interpolation=cv2.INTER_CUBIC)),
        ("otsu",     otsu),
        ("inv_otsu", cv2.bitwise_not(otsu)),
        ("adapt",    cv2.adaptiveThreshold(gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 11, 2)),
        ("clahe",    cv2.createCLAHE(2.0, (8, 8)).apply(gray)),
        ("sharp",    cv2.filter2D(gray, -1, np.array([[0, -1, 0], [-1, 5, -1], [0, -1, 0]]))),
        ("3x",       cv2.resize(gray, (w*3, h*3), interpolation=cv2.INTER_CUBIC)),
        ("otsu_2x",  cv2.resize(otsu, (w*2, h*2), interpolation=cv2.INTER_NEAREST)),
    ]

    for label, im in attempts:
        r = try_decode(im)
        if r:
            print(f"[decode] success with: {label}")
            return r

    for angle in [-5, 5, 90, -10, 10, 180, 270]:
        rot_map = {90: cv2.ROTATE_90_CLOCKWISE, 180: cv2.ROTATE_180, 270: cv2.ROTATE_90_COUNTERCLOCKWISE}
        if angle in rot_map:
            rotated = cv2.rotate(gray, rot_map[angle])
        else:
            M = cv2.getRotationMatrix2D((w//2, h//2), angle, 1.0)
            rotated = cv2.warpAffine(gray, M, (w, h), flags=cv2.INTER_CUBIC, borderMode=cv2.BORDER_REPLICATE)
        r = try_decode(rotated)
        if r:
            print(f"[decode] success with rotation: {angle}")
            return r

    return None


async def detect_km_from_image(path: str) -> str | None:
    loop = asyncio.get_event_loop()
    try:
        result = await asyncio.wait_for(
            loop.run_in_executor(EXECUTOR, _decode_local_sync, path),
            timeout=15.0
        )
        return result
    except asyncio.TimeoutError:
        return None


@router.message(F.photo)
async def handle_photo(message: Message, bot: Bot):

    user_id  = message.from_user.id
    username = message.from_user.username

    if not get_user_token(user_id):
        await message.answer("Сначала введите токен через /start")
        return

    if not username:
        await message.answer(NO_ACCESS_MSG, parse_mode="HTML")
        return

    if not is_whitelisted(username) and remaining_today(user_id) == 0:
        await message.answer(LIMIT_MSG, parse_mode="HTML")
        return

    await message.answer("🔍 Распознаю код...")

    photo     = message.photo[-1]
    file      = await bot.get_file(photo.file_id)
    file_path = f"tmp_{photo.file_id}.jpg"
    await bot.download_file(file.file_path, file_path)

    km = await detect_km_from_image(file_path)

    try:
        import os
        os.remove(file_path)
    except:
        pass

    if not km:
        await message.answer(
            "❌ Не удалось распознать код.\n"
            "Попробуйте сделать фото ближе и без размытия."
        )
        return

    await message.answer(
        f"📷 <b>Код распознан</b>\n\n<code>{e(km)}</code>",
        parse_mode="HTML"
    )

    token  = get_user_token(user_id)
    check_and_increment(user_id, username)
    result = await check_marking(token, km)
    await send_result(message, result)


# ================= MAIN =================

from aiohttp import web

async def health(request):
    return web.Response(text="OK")

async def main():
    bot = Bot(BOT_TOKEN)
    dp  = Dispatcher()
    dp.include_router(router)

    app = web.Application()
    app.router.add_get("/", health)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()

    print(f"Bot started, health server on port {port}")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
