import os
import io
import asyncio
import logging
import threading
from typing import Dict, Any, List, Optional
from urllib.parse import urlsplit, urlunsplit

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential

from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("popmart-bot")

# ===== Config =====
# BASE_URL có thể là root (vd https://your-app) hoặc đã kèm /popmart (vd https://your-app/popmart)
BASE_URL = os.getenv("BASE_URL", "https://clone-popmart-production.up.railway.app").rstrip("/")
POP_PAGE_PATH = os.getenv("POP_PAGE_PATH", "/popmart").strip() or "/popmart"
AJAX_PATH = os.getenv("AJAX_PATH", "/Ajax.aspx").strip() or "/Ajax.aspx"

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
ADMINS = [x.strip() for x in os.getenv("ADMINS", "").split(",") if x.strip()]
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))
MAX_WORKERS_CAP = int(os.getenv("MAX_WORKERS", "10"))

# 2Captcha (optional)
TWO_CAPTCHA_API_KEY = os.getenv("TWO_CAPTCHA_API_KEY", "").strip()
USE_2CAPTCHA = os.getenv("USE_2CAPTCHA", "0").strip() == "1"
CAPTCHA_SOFT_TIMEOUT = int(os.getenv("CAPTCHA_SOFT_TIMEOUT", "120"))
CAPTCHA_POLL_INTERVAL = int(os.getenv("CAPTCHA_POLL_INTERVAL", "5"))
CAPTCHA_MAX_TRIES = int(os.getenv("CAPTCHA_MAX_TRIES", "4"))

# Anti-dup day registry
ACTIVE_DAYS = set()
COMPLETED_DAYS = set()
ACTIVE_LOCK = threading.Lock()

# Pending manual captcha (if not using 2Captcha)
PENDING_CAPTCHAS: Dict[str, Dict[str, Any]] = {}
PENDING_LOCK = threading.Lock()


def _normalize_endpoints(base_url: str, pop_path: str, ajax_path: str):
    """
    Từ BASE_URL (root hoặc đã kèm /popmart) => tính:
      - page_url: URL trang form (…/popmart)
      - ajax_url: URL Ajax ở "root" (…/Ajax.aspx)
      - ajax_alt_url: Ajax fallback nằm cùng thư mục với page (…/popmart/Ajax.aspx)
    """
    sp = urlsplit(base_url)
    base_path = sp.path.rstrip("/")

    pop_path = "/" + pop_path.lstrip("/")
    ajax_path = "/" + ajax_path.lstrip("/")

    ends_with_pop = base_path.endswith(pop_path)
    # path của page
    page_path = base_path if ends_with_pop else (base_path + pop_path)
    if not page_path.startswith("/"):
        page_path = "/" + page_path

    # root_path (thư mục cha của page)
    root_path = base_path[:-len(pop_path)] if ends_with_pop else base_path
    if not root_path:
        root_path = "/"
    if not root_path.startswith("/"):
        root_path = "/" + root_path

    page_url = urlunsplit((sp.scheme, sp.netloc, page_path, "", ""))
    ajax_url = urlunsplit((sp.scheme, sp.netloc, (root_path.rstrip("/") + ajax_path), "", ""))
    page_dir = page_path.rsplit("/", 1)[0] or "/"
    ajax_alt_url = urlunsplit((sp.scheme, sp.netloc, (page_dir + ajax_path), "", ""))

    return page_url, ajax_url, ajax_alt_url


class PopmartClient:
    def __init__(self, base_url: str, pop_path: str, ajax_path: str, timeout: int = 30):
        self.base_url = base_url
        self.page_url, self.ajax_url, self.ajax_alt_url = _normalize_endpoints(base_url, pop_path, ajax_path)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
        })
        self.timeout = timeout
        log.info(f"[ENDPOINTS] page_url={self.page_url} | ajax_url={self.ajax_url} | ajax_alt_url={self.ajax_alt_url}")

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10), reraise=True)
    def get_main_page(self) -> str:
        r = self.session.get(self.page_url, timeout=self.timeout)
        r.raise_for_status()
        return r.text

    def _ajax_get(self, params: Dict[str, str]) -> requests.Response:
        r = self.session.get(self.ajax_url, params=params, timeout=self.timeout, allow_redirects=True)
        if r.status_code == 404:
            r2 = self.session.get(self.ajax_alt_url, params=params, timeout=self.timeout, allow_redirects=True)
            r2.raise_for_status()
            return r2
        r.raise_for_status()
        return r

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10), reraise=True)
    def load_sessions_for_day(self, id_ngay: str) -> List[Dict[str, str]]:
        r = self._ajax_get({"Action": "LoadPhien", "idNgayBanHang": id_ngay})
        html = r.text.split("||@@||")[0]
        soup = BeautifulSoup(html, "html.parser")
        return [{"value": (opt.get("value") or "").strip(), "label": (opt.text or "").strip()}
                for opt in soup.find_all("option")]

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10), reraise=True)
    def fetch_captcha_image_url(self) -> Optional[str]:
        r = self._ajax_get({"Action": "LoadCaptcha"})
        soup = BeautifulSoup(r.text, "html.parser")
        img = soup.find("img")
        if img and img.get("src"):
            src = img["src"].strip()
            if src.startswith("http"):
                return src
            root = self.ajax_url.rsplit("/", 1)[0]
            return f"{root}/{src.lstrip('./')}"
        return None

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10), reraise=True)
    def download_image(self, url: str) -> bytes:
        r = self.session.get(url, timeout=self.timeout)
        r.raise_for_status()
        return r.content

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10), reraise=True)
    def submit_registration(self, payload: Dict[str, str]) -> str:
        r = self._ajax_get(payload)
        return r.text.strip()

    def map_sales_date_to_id(self, html: str, target_date: str) -> Optional[str]:
        soup = BeautifulSoup(html, "html.parser")
        sel = soup.find("select", {"id": "slNgayBanHang"})
        if not sel:
            return None
        for opt in sel.find_all("option"):
            if (opt.text or "").strip() == target_date:
                return (opt.get("value") or "").strip()
        return None


def extract_all_sales_dates(html: str) -> List[str]:
    out: List[str] = []
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
        b64 = base64.b64encode(image_bytes).decode("ascii")
        r = requests.post("https://2captcha.com/in.php",
                          data={"key": TWO_CAPTCHA_API_KEY, "method": "base64", "body": b64, "json": 1},
                          timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        j = r.json()
        if j.get("status") != 1 or "request" not in j:
            return None
        rid = j["request"]
        end_time = time.time() + CAPTCHA_SOFT_TIMEOUT
        while time.time() < end_time:
            time.sleep(CAPTCHA_POLL_INTERVAL)
            pr = requests.get("https://2captcha.com/res.php",
                              params={"key": TWO_CAPTCHA_API_KEY, "action": "get", "id": rid, "json": 1},
                              timeout=REQUEST_TIMEOUT)
            pr.raise_for_status()
            jr = pr.json()
            if jr.get("status") == 1:
                return str(jr.get("request", "")).strip()
        return None
    except Exception as e:
        log.warning(f"2Captcha error: {e}")
        return None


def is_session_full(text: str) -> bool:
    t = (text or "").lower()
    keys = [
        "đã hết số lượng đăng ký phiên này",
        "het so luong dang ky phien nay",
        "this session is full",
        "session is full",
    ]
    return any(k in t for k in keys)


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
        "Gửi file Excel (.xlsx) cột: FullName, DOB_Day, DOB_Month, DOB_Year, Phone, Email, IDNumber.\n"
        "Bot tự lấy mọi Sales Dates & chọn session đầu tiên. Mỗi ngày chạy 1 luồng và xử lý toàn bộ các dòng."
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
    required = ["FullName", "DOB_Day", "DOB_Month", "DOB_Year", "Phone", "Email", "IDNumber"]
    for c in required:
        if c not in df.columns:
            await update.message.reply_text(f"Thiếu cột bắt buộc: {c}")
            return

    rows = df.to_dict(orient="records")
    for idx, r in enumerate(rows):
        r["__row_idx"] = idx

    client = PopmartClient(BASE_URL, POP_PAGE_PATH, AJAX_PATH, REQUEST_TIMEOUT)

    # Sales dates
    main_html = await asyncio.to_thread(client.get_main_page)
    all_days = extract_all_sales_dates(main_html)
    if not all_days:
        await update.message.reply_text("Không tìm thấy Sales Dates trên form.")
        return

    unique_days = list(dict.fromkeys(all_days))
    # Anti-dup scheduling
    days_to_run = []
    with ACTIVE_LOCK:
        for d in unique_days:
            if d in ACTIVE_DAYS or d in COMPLETED_DAYS:
                continue
            ACTIVE_DAYS.add(d)
            days_to_run.append(d)

    if not days_to_run:
        await update.message.reply_text("Không có ngày nào mới để chạy (đã chạy trước đó).")
        return

    max_workers = min(len(days_to_run), MAX_WORKERS_CAP if MAX_WORKERS_CAP > 0 else len(days_to_run))
    await update.message.reply_text(f"Tìm thấy {len(days_to_run)} ngày. Sẽ tạo {max_workers} task (mỗi ngày 1 task).")

    # Each day -> all rows
    buckets: Dict[str, List[Dict[str, Any]]] = {d: list(rows) for d in days_to_run}

    async def process_day(day: str, tasks: List[Dict[str, Any]]):
        try:
            html = await asyncio.to_thread(client.get_main_page)
            id_ngay = client.map_sales_date_to_id(html, day)
            if not id_ngay:
                await update.message.reply_text(f"[{day}] Không tìm thấy idNgàyBanHang.")
                return

            sessions = await asyncio.to_thread(client.load_sessions_for_day, id_ngay)
            if not sessions:
                await update.message.reply_text(f"[{day}] Không có phiên để đăng ký. Bỏ qua.")
                return

            target_session = sessions[0]  # always pick first

            for row in tasks:
                attempt = 0
                success = False
                last_msg = ""

                while attempt < CAPTCHA_MAX_TRIES and not success:
                    attempt += 1
                    try:
                        img_url = await asyncio.to_thread(client.fetch_captcha_image_url)
                        if not img_url:
                            last_msg = "Không lấy được captcha."
                            break
                        img_bytes = await asyncio.to_thread(client.download_image, img_url)

                        captcha_answer = await asyncio.to_thread(solve_captcha_via_2captcha, img_bytes) if USE_2CAPTCHA else None
                        if not captcha_answer and USE_2CAPTCHA:
                            last_msg = "2Captcha không trả lời."
                            continue

                        if USE_2CAPTCHA and captcha_answer:
                            result = await asyncio.to_thread(
                                client.submit_registration,
                                build_payload(id_ngay, target_session["value"], row, captcha_answer)
                            )
                            if "!!!True|~~|" in result:
                                await update.message.reply_text(
                                    f"✅ [{day}] Dòng {row['__row_idx'] + 1} — Thành công sau {attempt}/{CAPTCHA_MAX_TRIES}."
                                )
                                success = True
                                break
                            elif is_session_full(result):
                                await update.message.reply_text(
                                    f"⛔ [{day}] Phiên đã hết lượt. Kết thúc xử lý ngày này."
                                )
                                return  # stop processing remaining rows for this day
                            elif "captcha" in result.lower():
                                last_msg = f"Sai captcha (thử {attempt}/{CAPTCHA_MAX_TRIES})."
                                continue
                            else:
                                last_msg = f"Không thành công: {result[:200]}"
                                break
                        else:
                            # manual mode
                            key = f"{update.effective_chat.id}:{day}:{row['__row_idx']}"
                            with PENDING_LOCK:
                                PENDING_CAPTCHAS[key] = {
                                    "client": client,
                                    "id_ngay": id_ngay,
                                    "id_phien": target_session["value"],
                                    "row": row,
                                }
                            await update.message.reply_photo(
                                photo=img_bytes,
                                caption=f"[{day}] Dòng {row['__row_idx'] + 1}: Vui lòng trả lời tin nhắn này bằng **mã captcha**.",
                                parse_mode="MarkdownV2",
                            )
                            last_msg = "Chuyển sang nhập tay."
                            break
                    except Exception as e:
                        last_msg = f"Lỗi attempt {attempt}: {e}"
                        continue

                if not success and USE_2CAPTCHA:
                    await update.message.reply_text(
                        f"⏭️ [{day}] Dòng {row['__row_idx'] + 1} — Bỏ qua sau {CAPTCHA_MAX_TRIES} lần thử. {last_msg}"
                    )

        except Exception as e:
            await update.message.reply_text(f"[{day}] Lỗi: {e}")
        finally:
            with ACTIVE_LOCK:
                ACTIVE_DAYS.discard(day)
                COMPLETED_DAYS.add(day)

    # Tạo task asyncio cho mỗi ngày (KHÔNG dùng ThreadPoolExecutor)
    tasks = []
    for d in days_to_run:
        tasks.append(context.application.create_task(process_day(d, buckets[d])))

    await update.message.reply_text("Đã khởi chạy các task theo ngày. Bot sẽ báo kết quả khi có.")
    # Không await gather để không block handler; nếu muốn đợi xong thì:
    # await asyncio.gather(*tasks, return_exceptions=True)


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Manual captcha answer when not using 2Captcha."""
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

    try:
        result = await asyncio.to_thread(
            client.submit_registration,
            build_payload(id_ngay, id_phien, row, text)
        )
        if "!!!True|~~|" in result:
            await update.message.reply_text("✅ Thành công.")
        elif is_session_full(result):
            await update.message.reply_text("⛔ Phiên đã hết lượt. Kết thúc xử lý ngày này.")
        elif "captcha" in result.lower():
            await update.message.reply_text("❌ Sai captcha. Dùng /start và gửi lại file để thử lại.")
        else:
            await update.message.reply_text(f"⚠️ Không thành công: {result[:200]}")
    except Exception as e:
        await update.message.reply_text(f"❌ Lỗi: {e}")


async def on_error(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log.exception("Unhandled exception", exc_info=context.error)
    try:
        if update and update.effective_message:
            await update.effective_message.reply_text(f"Lỗi nội bộ: {context.error}")
    except Exception:
        pass


def main():
    if not BOT_TOKEN:
        raise SystemExit("Missing TELEGRAM_BOT_TOKEN")
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_excel))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_error_handler(on_error)
    log.info("Bot started.")
    app.run_polling(close_loop=False)


if __name__ == "__main__":
    main()
