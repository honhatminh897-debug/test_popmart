
import os
import io
import re
import json
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

# 2Captcha (optional)
TWO_CAPTCHA_API_KEY = os.getenv("TWO_CAPTCHA_API_KEY", "").strip()
USE_2CAPTCHA = os.getenv("USE_2CAPTCHA", "0").strip() == "1"
CAPTCHA_SOFT_TIMEOUT = int(os.getenv("CAPTCHA_SOFT_TIMEOUT", "120"))  # seconds
CAPTCHA_POLL_INTERVAL = int(os.getenv("CAPTCHA_POLL_INTERVAL", "5"))  # seconds

# Pending captchas for manual fallback
PENDING_CAPTCHAS: Dict[str, Dict[str, Any]] = {}
PENDING_LOCK = threading.Lock()


class PopmartClient:
    def __init__(self, base_url: str, timeout: int = 30):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
        })
        self.timeout = timeout

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10), reraise=True)
    def get_main_page(self) -> str:
        url = f"{self.base_url}/popmart"
        r = self.session.get(url, timeout=self.timeout)
        r.raise_for_status()
        return r.text

    def map_sales_date_to_id(self, html: str, target_date_ddmmyyyy: str) -> Optional[str]:
        soup = BeautifulSoup(html, "html.parser")
        sel = soup.find("select", {"id": "slNgayBanHang"})
        if not sel:
            return None
        for opt in sel.find_all("option"):
            txt = (opt.text or '').strip()
            val = (opt.get("value") or '').strip()
            if txt == target_date_ddmmyyyy and val:
                return val
        return None

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10), reraise=True)
    def load_sessions_for_day(self, id_ngay: str) -> List[Dict[str, str]]:
        url = f"{self.base_url}/Ajax.aspx"
        r = self.session.get(url, params={"Action": "LoadPhien", "idNgayBanHang": id_ngay}, timeout=self.timeout)
        r.raise_for_status()
        raw = r.text.strip()
        parts = raw.split("||@@||")
        options_html = parts[0] if parts else ""
        soup = BeautifulSoup(options_html, "html.parser")
        out = []
        for opt in soup.find_all("option"):
            out.append({"value": (opt.get("value") or '').strip(), "label": (opt.text or '').strip()})
        return out

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10), reraise=True)
    def fetch_captcha_image_and_key(self) -> Optional[Dict[str, str]]:
        url = f"{self.base_url}/Ajax.aspx"
        r = self.session.get(url, params={"Action": "LoadCaptcha"}, timeout=self.timeout)
        r.raise_for_status()
        html = r.text.strip()
        img_url = None
        soup = BeautifulSoup(html, "html.parser")
        img = soup.find("img")
        if img and img.get("src"):
            src = img.get("src")
            if src.startswith("http"):
                img_url = src
            else:
                img_url = f"{self.base_url}/{src.lstrip('./')}"
        return {"html": html, "img_url": img_url}

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10), reraise=True)
    def download_image(self, url: str) -> bytes:
        r = self.session.get(url, timeout=self.timeout)
        r.raise_for_status()
        return r.content

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10), reraise=True)
    def submit_registration(self, payload: Dict[str, str]) -> str:
        url = f"{self.base_url}/Ajax.aspx"
        r = self.session.get(url, params=payload, timeout=self.timeout)
        r.raise_for_status()
        return r.text.strip()



def extract_all_sales_dates(html: str) -> List[str]:
    """Return all visible SalesDate labels (dd/mm/yyyy) from the select#slNgayBanHang, in document order."""
    out: List[str] = []
    soup = BeautifulSoup(html, "html.parser")
    sel = soup.find("select", {"id": "slNgayBanHang"})
    if not sel:
        return out
    for opt in sel.find_all("option"):
        txt = (opt.text or "").strip()
        val = (opt.get("value") or "").strip()
        if txt and val:  # skip placeholder
            out.append(txt)
    return out
def solve_captcha_via_2captcha(image_bytes: bytes) -> Optional[str]:
    """Send image to 2Captcha (method=base64) and poll for result."""
    if not TWO_CAPTCHA_API_KEY:
        return None
    try:
        import time, base64
        b64 = base64.b64encode(image_bytes).decode("ascii")
        in_url = "https://2captcha.com/in.php"
        data = {
            "key": TWO_CAPTCHA_API_KEY,
            "method": "base64",
            "body": b64,
            "json": 1,
        }
        r = requests.post(in_url, data=data, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        j = r.json()
        if j.get("status") != 1 or "request" not in j:
            return None
        captcha_id = j["request"]
        res_url = "https://2captcha.com/res.php"
        deadline = time.time() + CAPTCHA_SOFT_TIMEOUT
        while time.time() < deadline:
            time.sleep(CAPTCHA_POLL_INTERVAL)
            pr = requests.get(res_url, params={
                "key": TWO_CAPTCHA_API_KEY,
                "action": "get",
                "id": captcha_id,
                "json": 1,
            }, timeout=REQUEST_TIMEOUT)
            pr.raise_for_status()
            pj = pr.json()
            if pj.get("status") == 1 and "request" in pj:
                return str(pj["request"]).strip()
        return None
    except Exception as e:
        log.warning(f"2Captcha solve error: {e}")
        return None


def is_admin(user_id: int) -> bool:
    if not ADMINS:
        return True
    return str(user_id) in ADMINS


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    await update.message.reply_text(
        "Gửi 1 file Excel (.xlsx) có các cột: FullName, DOB_Day, DOB_Month, DOB_Year, Phone, Email, IDNumber, SalesDate (dd/mm/yyyy), optional SessionName."
    )


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


async def handle_excel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    doc = update.message.document
    if not doc or not doc.file_name.lower().endswith(".xlsx"):
        await update.message.reply_text("Vui lòng gửi file .xlsx")
        return

    file = await doc.get_file()
    raw = await file.download_as_bytearray()
    df = pd.read_excel(io.BytesIO(raw))
    required_cols = ["FullName", "DOB_Day", "DOB_Month", "DOB_Year", "Phone", "Email", "IDNumber"]
    for c in required_cols:
        if c not in df.columns:
            await update.message.reply_text(f"Thiếu cột bắt buộc: {c}")
            return


    rows = df.to_dict(orient="records")
    # Scrape all available dates from the form
    client = PopmartClient(BASE_URL, REQUEST_TIMEOUT)
    main_html = client.get_main_page()
    all_days = extract_all_sales_dates(main_html)
    if not all_days:
        await update.message.reply_text("Không tìm thấy Sales Dates trên form.")
        return

    # Assign 1 row per day in round-robin fashion
    if not rows:
        await update.message.reply_text("Excel không có dữ liệu hàng để điền.")
        return
    assignments = []  # list of tuples (day, row_dict)
    for i, day in enumerate(all_days):
        row = rows[i % len(rows)].copy()
        row["__row_idx"] = (i % len(rows))
        assignments.append((day, row))

    # Plan max workers = number of days (cap by MAX_WORKERS_CAP)
    unique_days = list(dict.fromkeys(all_days))  # preserve order
    max_workers = min(len(unique_days), MAX_WORKERS_CAP if MAX_WORKERS_CAP > 0 else len(unique_days))
    await update.message.reply_text(f"Tìm thấy {len(unique_days)} ngày trên form. Sẽ chạy tối đa {max_workers} luồng (mỗi ngày 1 luồng). Gán dữ liệu theo round-robin từ Excel ({len(rows)} dòng).")

        client = PopmartClient(BASE_URL, REQUEST_TIMEOUT)    client = PopmartClient(BASE_URL, REQUEST_TIMEOUT)


    buckets: Dict[str, List[Dict[str, Any]]] = {d: [] for d in unique_days}
    for (day, row) in assignments:
        buckets[day].append(row)


    async def process_day(day: str, tasks: List[Dict[str, Any]]):

    async def process_day(day: str, tasks: List[Dict[str, Any]]):
        try:
            main_html = client.get_main_page()
            id_ngay = client.map_sales_date_to_id(main_html, day)
            if not id_ngay:
                await update.message.reply_text(f"[{day}] Không tìm thấy idNgàyBanHang trên trang.")
                return
            sessions = client.load_sessions_for_day(id_ngay)
            if not sessions:
                await update.message.reply_text(f"[{day}] Không có phiên để đăng ký.")
                return


            for row in tasks:
                target_session = None
                if "SessionName" in row and str(row["SessionName"]).strip():
                    name = str(row["SessionName"]).strip()
                    for s in sessions:
                        if s["label"] == name:
                            target_session = s
                            break
                if not target_session:
                    target_session = sessions[0]

                max_tries = int(os.getenv("CAPTCHA_MAX_TRIES", "4"))
                attempt = 0
                success = False
                last_msg = ""

                while attempt < max_tries and not success:
                    attempt += 1
                    try:
                        # always reload fresh captcha each attempt
                        cap = client.fetch_captcha_image_and_key()
                        if not cap or not cap.get("img_url"):
                            last_msg = "Không lấy được captcha."
                            break
                        img_bytes = client.download_image(cap["img_url"])

                        captcha_answer = None
                        if USE_2CAPTCHA:
                            captcha_answer = solve_captcha_via_2captcha(img_bytes)

                        if not captcha_answer and USE_2CAPTCHA:
                            # If using 2Captcha and didn't get an answer, retry by loop
                            last_msg = "2Captcha không trả lời."
                            continue

                        if USE_2CAPTCHA and captcha_answer:
                            payload = build_payload(id_ngay, target_session["value"], row, captcha_answer)
                            result = client.submit_registration({**payload})
                            if "!!!True|~~|" in result:
                                await update.message.reply_text(f"✅ Dòng {row['__row_idx'] + 1} — Đăng ký thành công ngày {day} (thử {attempt}/{max_tries}).")
                                success = True
                                break
                            else:
                                # If server mentions Captcha, treat as captcha failure and retry
                                if "Captcha" in result or "captcha" in result.lower():
                                    last_msg = f"Server báo lỗi Captcha (thử {attempt}/{max_tries})."
                                    continue
                                else:
                                    # Not a captcha issue — don't loop further to avoid spamming
                                    last_msg = f"Không thành công: {result[:200]}"
                                    break
                        else:
                            # Not using 2Captcha: fall back to manual ask once then break loop
                            key = f"{update.effective_chat.id}:{row['__row_idx']}"
                            with PENDING_LOCK:
                                PENDING_CAPTCHAS[key] = {
                                    "client": client,
                                    "id_ngay": id_ngay,
                                    "id_phien": target_session["value"],
                                    "row": row,
                                }
                            await update.message.reply_photo(photo=img_bytes, caption=f"[{day}] Dòng {row['__row_idx'] + 1}: Vui lòng trả lời tin nhắn này bằng **mã captcha**.", parse_mode="MarkdownV2")
                            last_msg = "Chuyển sang nhập tay."
                            break

                    except Exception as e:
                        last_msg = f"Lỗi attempt {attempt}: {e}"
                        continue

                if not success and USE_2CAPTCHA:
                    await update.message.reply_text(f"⏭️ Dòng {row['__row_idx'] + 1} — bỏ qua sau {max_tries} lần thử. {last_msg}")
        except Exception as e:
                        await update.message.reply_text(f"❌ Lỗi gửi đăng ký (2Captcha) cho dòng {row['__row_idx'] + 1}: {e}. Chuyển sang nhập tay.")

                # Fallback to manual
                key = f"{update.effective_chat.id}:{row['__row_idx']}"
                with PENDING_LOCK:
                    PENDING_CAPTCHAS[key] = {
                        "client": client,
                        "id_ngay": id_ngay,
                        "id_phien": target_session["value"],
                        "row": row,
                    }
                await update.message.reply_photo(photo=img_bytes, caption=f"[{day}] Dòng {row['__row_idx'] + 1}: Vui lòng trả lời tin nhắn này bằng **mã captcha**.", parse_mode="MarkdownV2")

        except Exception as e:
            await update.message.reply_text(f"[{day}] Lỗi: {e}")

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = [ex.submit(lambda d=day: (context.application.create_task(process_day(d, buckets[d])))) for day in unique_days]
        for f in futs:
            _ = f.result()

    await update.message.reply_text("Đã xử lý các dòng. Nếu có ảnh captcha còn lại, hãy trả lời bằng mã tương ứng.")

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    if not update.message or not update.message.text:
        return
    chat_id = update.effective_chat.id
    text = update.message.text.strip()
    key = None
    with PENDING_LOCK:
        for k in list(PENDING_CAPTCHAS.keys()):
            if k.startswith(f"{chat_id}:"):
                key = k
                break
    if not key:
        return

    data = None
    with PENDING_LOCK:
        data = PENDING_CAPTCHAS.pop(key, None)
    if not data:
        await update.message.reply_text("Không tìm thấy tác vụ captcha tương ứng.")
        return

    client: PopmartClient = data["client"]
    id_ngay = data["id_ngay"]
    id_phien = data["id_phien"]
    row = data["row"]
    payload = build_payload(id_ngay, id_phien, row, text)

    try:
        result = client.submit_registration({**payload})
        if "!!!True|~~|" in result:
            await update.message.reply_text(f"✅ Dòng {row['__row_idx'] + 1} — Đăng ký thành công cho ngày {day}.")
        else:
            if "Captcha" in result:
                await update.message.reply_text(f"❌ Dòng {row['__row_idx'] + 1} — Sai captcha hoặc hết hạn. Dùng /retry {row['__row_idx']} để thử lại.")
            else:
                await update.message.reply_text(f"⚠️ Dòng {row['__row_idx'] + 1} — Không thành công: {result[:500]}")
    except Exception as e:
        await update.message.reply_text(f"❌ Lỗi gửi đăng ký cho dòng {row['__row_idx'] + 1}: {e}")


async def retry_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    args = context.args
    if not args:
        await update.message.reply_text("Dùng: /retry <row_index>")
        return
    try:
        int(args[0])
    except:
        await update.message.reply_text("row_index phải là số.")
        return
    await update.message.reply_text("Hãy gửi lại file Excel để khởi tạo phiên làm việc mới rồi nhập captcha lại cho dòng này.")

def main():
    if not BOT_TOKEN or not BASE_URL:
        raise SystemExit("Missing TELEGRAM_BOT_TOKEN or BASE_URL env vars.")
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("retry", retry_cmd))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_excel))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    log.info("Bot started.")
    app.run_polling(close_loop=False)

if __name__ == "__main__":
    main()
