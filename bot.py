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
BOT_TOKEN = os.environ.get("BOT_TOKEN", "8389107035:AAGC6OG1Nvp-HhpfRBhluwPmNNHgzFs5dwM")
DATA_FILE = Path("tokens.json")

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

# ================= UTILS =================

def clean_km(code: str) -> str:
    """
    Нормализует GS1 DataMatrix код. Убирает крипто-хвост 93...

    Структура GS1 маркировки:
      01 + GTIN(14) + 21 + serial(13) = 31 символ (чистый КМ)
      + GS(chr29) + 93 + crypto(4)    = крипто-хвост

    Если GS присутствует → делим по GS, берём первую часть.
    Если GS нет → берём первые 31 символ.
    """
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
    """True если текст похож на код маркировки GS1."""
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

    # ===== TOKEN INVALID =====
    if isinstance(data, dict) and data.get("code") == "access-denied":
        return {"type": "token_error"}

    # ===== KM МОЙ =====
    if isinstance(data, dict) and "results" in data:

        if not data["results"]:
            return {"type": "not_found"}

        item = data["results"][0]

        code_data = item.get("codeData", {})
        package_data = item.get("packageData", {})
        marking_data = item.get("markingData", {})
        issuer_info = marking_data.get("issuerInfo", {})
        turnover_data = item.get("turnoverData", {})
        owner_info = turnover_data.get("ownerInfo", {})
        product_data = item.get("productData", {})

        package_type = package_data.get("packageType")

        if package_type == "GROUP":

            children_count = package_data.get("actuallyPacked") or len(package_data.get("children", []))

            return {
                "type": "my_group",
                "code": code_data.get("code"),
                "status": code_data.get("status"),
                "packageType": package_type,
                "parentCode": package_data.get("parentCode"),
                "childrenCount": children_count,
                "issuerTin": issuer_info.get("issuerTin"),
                "issuerName": issuer_info.get("issuerName", {}).get("ru"),
                "ownerTin": owner_info.get("ownerTin"),
                "ownerName": owner_info.get("ownerName", {}).get("ru"),
            }

        else:

            return {
                "type": "my_unit",
                "code": code_data.get("code"),
                "status": code_data.get("status"),
                "packageType": package_type,
                "parentCode": package_data.get("parentCode"),
                "issuerTin": issuer_info.get("issuerTin"),
                "issuerName": issuer_info.get("issuerName", {}).get("ru"),
                "ownerTin": owner_info.get("ownerTin"),
                "ownerName": owner_info.get("ownerName", {}).get("ru"),
                "expirationDate": format_date(product_data.get("expirationDate")),
            }

    # ===== KM НЕ МОЙ =====
    if isinstance(data, list):

        if not data:
            return {"type": "not_found"}

        item = data[0]

        package_type = item.get("packageType")

        if package_type == "GROUP":

            units = 0

            if item.get("aggregateProductGroups"):
                units = item["aggregateProductGroups"][0].get("unitsNumber", 0)

            return {
                "type": "foreign_group",
                "code": item.get("code"),
                "packageType": package_type,
                "status": item.get("status"),
                "issuerTin": item.get("issuerShortInfo", {}).get("issuerTin"),
                "issuerName": item.get("issuerShortInfo", {}).get("issuerName", {}).get("ru"),
                "unitsNumber": units,
                "expirationDate": format_date(item.get("expirationDate")),
            }

        else:

            return {
                "type": "foreign_unit",
                "code": item.get("code"),
                "issuerTin": item.get("issuerShortInfo", {}).get("issuerTin"),
                "issuerName": item.get("issuerShortInfo", {}).get("issuerName", {}).get("ru"),
                "expirationDate": format_date(item.get("expirationDate")),
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

    await message.answer(
        "Введите токен Asl Belgisi:"
    )

@router.message(F.text)
async def handle_text(message: Message):

    user_id = message.from_user.id
    text = message.text.strip()

    state = USER_STATE.get(user_id)

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

    if state == "awaiting_km":

        USER_STATE.pop(user_id, None)

        token = get_user_token(user_id)
        km_clean = clean_km(text)

        result = await check_marking(token, km_clean)
        await send_result(message, result)
        return

    # Авто-определение KM без нажатия кнопки
    if looks_like_km(text):
        token = get_user_token(user_id)
        if not token:
            await message.answer("Сначала введите токен через /start")
            return
        km_clean = clean_km(text)
        result = await check_marking(token, km_clean)
        await send_result(message, result)


# ================= SEND RESULT =================

def e(value) -> str:
    """Экранирует строку для безопасной вставки в HTML Telegram."""
    return html.escape(str(value)) if value is not None else "—"


async def send_result(message: Message, result: dict):
    """Отправляет результат проверки KM пользователю."""

    if result["type"] == "token_error":
        await message.answer(
            "❌ <b>Ошибка авторизации</b>\n\n"
            "Токен Asl Belgisi не активен или введён неправильно.\n\n"
            "Обновите токен через кнопку:\n"
            "🔄 Обновить токен",
            parse_mode="HTML"
        )

    elif result["type"] == "not_found":
        await message.answer(
            "❌ <b>Код маркировки не найден</b>\n\n"
            "Проверьте правильность кода и попробуйте снова.",
            parse_mode="HTML"
        )

    elif result["type"] == "my_unit":
        await message.answer(
            "<b>📦 Штучная маркировка</b>\n\n"
            f"<b>Код:</b>\n<code>{e(result['code'])}</code>\n\n"
            f"<b>Статус:</b> {e(result['status'])}\n"
            f"<b>Тип упаковки:</b> {e(result['packageType'])}\n"
            f"<b>Родительская упаковка:</b>\n<code>{e(result['parentCode'])}</code>\n\n"
            f"<b>Производитель</b>\n{e(result['issuerName'])}\nИНН: {e(result['issuerTin'])}\n\n"
            f"<b>Текущий владелец</b>\n{e(result['ownerName'])}\nИНН: {e(result['ownerTin'])}\n\n"
            f"<b>Срок годности:</b> {e(result['expirationDate'])}",
            parse_mode="HTML"
        )

    elif result["type"] == "my_group":
        await message.answer(
            "<b>📦 Групповая упаковка</b>\n\n"
            f"<b>Код коробки:</b>\n<code>{e(result['code'])}</code>\n\n"
            f"<b>Статус:</b> {e(result['status'])}\n"
            f"<b>Тип:</b> {e(result['packageType'])}\n"
            f"<b>Родитель:</b>\n<code>{e(result['parentCode'])}</code>\n\n"
            f"<b>Количество внутри:</b> {e(result['childrenCount'])} шт\n\n"
            f"<b>Производитель</b>\n{e(result['issuerName'])}\nИНН: {e(result['issuerTin'])}\n\n"
            f"<b>Владелец</b>\n{e(result['ownerName'])}\nИНН: {e(result['ownerTin'])}",
            parse_mode="HTML"
        )

    elif result["type"] == "foreign_unit":
        await message.answer(
            "⚠️ <b>Этот код маркировки не принадлежит вам</b>\n\n"
            f"<b>Код:</b>\n<code>{e(result['code'])}</code>\n\n"
            f"<b>Производитель</b>\n{e(result['issuerName'])}\nИНН: {e(result['issuerTin'])}\n\n"
            f"<b>Срок годности:</b> {e(result['expirationDate'])}",
            parse_mode="HTML"
        )

    elif result["type"] == "foreign_group":
        await message.answer(
            "⚠️ <b>Этот код маркировки не принадлежит вам</b>\n\n"
            f"<b>Код:</b>\n<code>{e(result['code'])}</code>\n\n"
            f"<b>Тип упаковки:</b> {e(result['packageType'])}\n"
            f"<b>Статус:</b> {e(result['status'])}\n\n"
            f"<b>Производитель</b>\n{e(result['issuerName'])}\nИНН: {e(result['issuerTin'])}\n\n"
            f"<b>Количество единиц:</b> {e(result['unitsNumber'])}\n"
            f"<b>Срок годности:</b> {e(result['expirationDate'])}",
            parse_mode="HTML"
        )

    else:
        await message.answer("⚠️ Неизвестный ответ от сервера")


# ================= IMAGE RECOGNITION =================

def _decode_local_sync(path: str) -> str | None:
    """Синхронное декодирование — запускается в потоке."""
    import os, sys

    img = cv2.imread(path)
    if img is None:
        return None

    h, w = img.shape[:2]

    def try_decode(image):
        # Подавляем stderr-варнинги от zbar (pdf417 assertion)
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

    # Уменьшить если очень большое (> 1600 по длинной стороне)
    if max(h, w) > 1600:
        scale = 1600 / max(h, w)
        img = cv2.resize(img, (int(w * scale), int(h * scale)))
        h, w = img.shape[:2]

    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    _, otsu = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)

    # Пробуем все варианты — сначала 2x (работает на крупных кодах в скриншотах)
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

    # Повороты
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
    """Async обёртка — не блокирует event loop, таймаут 15 сек."""
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

    user_id = message.from_user.id

    if not get_user_token(user_id):
        await message.answer("Сначала введите токен через /start")
        return

    await message.answer("🔍 Распознаю код...")

    photo = message.photo[-1]
    file = await bot.get_file(photo.file_id)
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

    token = get_user_token(user_id)
    result = await check_marking(token, km)
    await send_result(message, result)



# ================= MAIN =================

async def main():

    bot = Bot(BOT_TOKEN)
    dp = Dispatcher()

    dp.include_router(router)

    print("Bot started")

    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
