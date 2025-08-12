import os
import io
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Any, List, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential

from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("popmart-bot")

BASE_URL = os.getenv("BASE_URL", "https://popmartstt.com").rstrip("/")
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
ADMINS = [x.strip() for x in os.getenv("ADMINS", "").split(",") if x.strip()]
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))
MAX_WORKERS_CAP = int(os.getenv("MAX_WORKERS", "10"))

# 2Captcha
TWO_CAPTCHA_API_KEY = os.getenv("TWO_CAPTCHA_API_KEY", "").strip()
USE_2CAPTCHA = os.getenv("USE_2CAPTCHA", "0").strip() == "1"
CAPTCHA_SOFT_TIMEOUT = int(os.getenv("CAPTCHA_SOFT_TIMEOUT", "120"))
CAPTCHA_POLL_INTERVAL = int(os.getenv("CAPTCHA_POLL_INTERVAL", "5"))
CAPTCHA_MAX_TRIES = int(os.getenv("CAPTCHA_MAX_TRIES", "4"))

# Pending for manual captcha
PENDING_CAPTCHAS: Dict[str, Dict[str, Any]] = {}
PENDING_LOCK = threading.Lock()


class PopmartClient:
    def __init__(self, base_url: str, timeout: int = 30):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0"
        })
        self.timeout = timeout

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10), reraise=True)
    def get_main_page(self) -> str:
        url = f"{self.base_url}/popmart"
        r = self.session.get(url, timeout=self.timeout)
        r.raise_for_status()
        return r.text

    def map_sales_date_to_id(self, html: str, target_date: str) -> Optional[str]:
        soup = BeautifulSoup(html, "html.parser")
        sel = soup.find("select", {"id": "slNgayBanHang"})
        if not sel:
            return None
        for opt in sel.find_all("option"):
            if (opt.text or '').strip() == target_date:
                return (opt.get("value") or '').strip()
        return None

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10), reraise=True)
    def load_sessions_for_day(self, id_ngay: str) -> List[Dict[str, str]]:
        r = self.session.get(f"{self.base_url}/Ajax.aspx",
                             params={"Action": "LoadPhien", "idNgayBanHang": id_ngay},
                             timeout=self.timeout)
        r.raise_for_status()
        html = r.text.split("||@@||")[0]
        soup = BeautifulSoup(html, "html.parser")
        return [{"value": (opt.get("value") or '').strip(),
                 "label": (opt.text or '').strip()} for opt in soup.find_all("option")]

    def fetch_captcha(self) -> Optional[str]:
        r = self.session.get(f"{self.base_url}/Ajax.aspx",
                             params={"Action": "LoadCaptcha"},
                             timeout=self.timeout)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        img = soup.find("img")
        if img and img.get("src"):
            src = img["src"]
            if not src.startswith("http"):
                src = f"{self.base_url}/{src.lstrip('./')}"
            return src
        return None

    def download_image(self, url: str) -> bytes:
        r = self.session.get(url, timeout=self.timeout)
        r.raise_for_status()
        return r.content

    def submit_registration(self, payload: Dict[str, str]) -> str:
        r = self.session.get(f"{self.base_url}/Ajax.aspx",
                             params=payload, timeout=self.timeout)
        r.raise_for_status()
        return r.text.strip()


def extract_all_sales_dates(html: str) -> List[str]:
    out = []
    soup = BeautifulSoup(html, "html.parser")
    sel = soup.find("select", {"id": "slNgayBanHang"})
    if not sel:
        return out
    for opt in sel.find_all("option"):
        txt = (opt.text or "").strip()
        val = (opt.get("value") or "").strip()
        if txt and val:
            out.append(txt)
    return out


def solve_captcha_via_2captcha(image_bytes: bytes) -> Optional[str]:
    if not TWO_CAPTCHA_API_KEY:
        return None
    try:
        import time, base64
        b64 = base64.b64encode(image_bytes).decode()
        r = requests.post("https://2captcha.com/in.php",
                          data={"key": TWO_CAPTCHA_API_KEY, "method": "base64", "body": b64, "json": 1},
                          timeout=REQUEST_TIMEOUT)
        rid = r.json().get("request")
        if not rid:
            return None
        end_time = time.time() + CAPTCHA_SOFT_TIMEOUT
        while time.time() < end_time:
            time.sleep(CAPTCHA_POLL_INTERVAL)
            pr = requests.get("https://2captcha.com/res.php",
                              params={"key": TWO_CAPTCHA_API_KEY, "action": "get", "id": rid, "json": 1},
                              timeout=REQUEST_TIMEOUT)
            jr = pr.json()
            if jr.get("status") == 1:
                return jr.get("request")
        return None
    except Exception as e:
        log.warning(f"2Captcha error: {e}")
        return None


def is_admin(uid: int) -> bool:
    return not ADMINS or str(uid) in ADMINS


def build_payload(id_ngay: str, id_phien: str, row: Dict[str, Any], captcha_text: str) -> Dict[str, str]:
    return {
        "Action": "DangKyThamDu",
        "idNgayBanHang": id_ngay,
        "idPhien": id_phien,
        "HoTen": str(row["FullName"]).strip(),
        "NgaySinh_Ngay": str(int(row["DOB_Day"])),
        "NgaySinh_Thang": str(int(row["DOB_Month"])),
        "NgaySinh_Nam": str(int(row["DOB_Year"])),
        "SoDienThoai": str(row["Phone"]).strip(),
        "Email": str(row["Email"]).strip(),
        "CCCD": str(row["IDNumber"]).strip(),
        "Captcha": captcha_text.strip(),
    }


async def start(update: Update, _: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    await update.message.reply_text(
        "Gửi file Excel (.xlsx) có các cột: FullName, DOB_Day, DOB_Month, DOB_Year, Phone, Email, IDNumber."
    )


async def handle_excel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    doc = update.message.document
    if not doc or not doc.file_name.lower().endswith(".xlsx"):
        await update.message.reply_text("Vui lòng gửi file .xlsx")
        return

    file = await doc.get_file()
    df = pd.read_excel(io.BytesIO(await file.download_as_bytearray()))
    for col in ["FullName", "DOB_Day", "DOB_Month", "DOB_Year", "Phone", "Email", "IDNumber"]:
        if col not in df.columns:
            await update.message.reply_text(f"Thiếu cột bắt buộc: {col}")
            return

    rows = df.to_dict(orient="records")
    client = PopmartClient(BASE_URL, REQUEST_TIMEOUT)
    all_days = extract_all_sales_dates(client.get_main_page())
    if not all_days:
        await update.message.reply_text("Không tìm thấy Sales Dates trên form.")
        return

    # Gán round-robin
    assignments = [(day, rows[i % len(rows)]) for i, day in enumerate(all_days)]
    unique_days = list(dict.fromkeys(all_days))
    max_workers = min(len(unique_days), MAX_WORKERS_CAP)
    await update.message.reply_text(f"Tìm thấy {len(unique_days)} ngày. Chạy {max_workers} luồng.")

    buckets: Dict[str, List[Dict[str, Any]]] = {d: [] for d in unique_days}
    for day, row in assignments:
        buckets[day].append(row)

    async def process_day(day: str, tasks: List[Dict[str, Any]]):
        try:
            id_ngay = client.map_sales_date_to_id(client.get_main_page(), day)
            if not id_ngay:
                await update.message.reply_text(f"[{day}] Không tìm thấy idNgàyBanHang.")
                return
            sessions = client.load_sessions_for_day(id_ngay)
            if not sessions:
                await update.message.reply_text(f"[{day}] Không có phiên để đăng ký.")
                return
            target_session = sessions[0]  # Luôn chọn phiên đầu tiên

            for row in tasks:
                attempt = 0
                while attempt < CAPTCHA_MAX_TRIES:
                    attempt += 1
                    img_url = client.fetch_captcha()
                    if not img_url:
                        await update.message.reply_text(f"[{day}] Không lấy được captcha.")
                        break
                    img_bytes = client.download_image(img_url)
                    captcha_answer = solve_captcha_via_2captcha(img_bytes) if USE_2CAPTCHA else None
                    if not captcha_answer:
                        await update.message.reply_photo(img_bytes, caption=f"[{day}] Nhập captcha:")
                        return
                    result = client.submit_registration(build_payload(id_ngay, target_session["value"], row, captcha_answer))
                    if "!!!True|~~|" in result:
                        await update.message.reply_text(f"✅ {day} — Thành công sau {attempt} lần thử.")
                        break
                    elif "Captcha" in result:
                        continue
                    else:
                        await update.message.reply_text(f"[{day}] Thất bại: {result[:100]}")
                        break
        except Exception as e:
            await update.message.reply_text(f"[{day}] Lỗi: {e}")

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for d in unique_days:
            ex.submit(lambda day=d: context.application.create_task(process_day(day, buckets[day])))


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    chat_id = update.effective_chat.id
    text = update.message.text.strip()
    key = next((k for k in PENDING_CAPTCHAS if k.startswith(f"{chat_id}:")), None)
    if not key:
        return
    data = PENDING_CAPTCHAS.pop(key, None)
    if not data:
        return
    client = data["client"]
    payload = build_payload(data["id_ngay"], data["id_phien"], data["row"], text)
    try:
        result = client.submit_registration(payload)
        if "!!!True|~~|" in result:
            await update.message.reply_text("✅ Thành công.")
        else:
            await update.message.reply_text(f"❌ Thất bại: {result[:100]}")
    except Exception as e:
        await update.message.reply_text(f"Lỗi: {e}")


def main():
    if not BOT_TOKEN:
        raise SystemExit("Missing TELEGRAM_BOT_TOKEN")
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_excel))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    log.info("Bot started.")
    app.run_polling(close_loop=False)


if __name__ == "__main__":
    main()
