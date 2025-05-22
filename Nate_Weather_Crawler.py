import os
import time
import threading
from flask import Flask
from dotenv import load_dotenv
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import firebase_admin
from firebase_admin import credentials, db

# ───────────── 설정 ─────────────
AREA_NAME = "cheonan_asan"
AREA_CODE = "11C20302"
COMBINED_KEY = f"{AREA_NAME}({AREA_CODE})"

load_dotenv()
FIREBASE_DB_URL = os.getenv("FIREBASE_DB_URL")

cred = credentials.Certificate("firebase_service_account.json")
if not firebase_admin._apps:
    firebase_admin.initialize_app(cred, {
        "databaseURL": FIREBASE_DB_URL
    })

# ───────────── Flask Keep-Alive ─────────────
app = Flask(__name__)

@app.route("/")
def ping():
    return "OK"

# ───────────── 크롤링 로직 ─────────────
def get_10min_aligned_key():
    now = datetime.now()
    aligned_minute = (now.minute // 10) * 10
    aligned_time = now.replace(minute=aligned_minute, second=0, microsecond=0)
    return aligned_time.strftime("%Y-%m-%d %H:%M:%S")

def is_time_to_crawl():
    now = datetime.now()
    return now.minute % 10 == 0 and now.second == 0

def fetch_weather():
    try:
        res = requests.get(
            f"https://news.nate.com/weather?areaCode={AREA_CODE}",
            headers={"User-Agent": "Mozilla/5.0"}
        )
        res.encoding = "euc-kr"
        soup = BeautifulSoup(res.text, "html.parser")

        temp_el = soup.select_one("#contentsWraper .temperature p.celsius")
        humid_el = soup.select_one("#contentsWraper .humidity em")
        rain_el = soup.select_one("#contentsWraper .rainfall em")
        wind_el = soup.select_one("#contentsWraper .wind em")

        if not all([temp_el, humid_el, rain_el, wind_el]):
            print("❌ 태그 못 찾음")
            return None

        wind_text = wind_el.get_text(strip=True)
        try:
            wind_dir, wind_speed = wind_text.split(maxsplit=1)
        except ValueError:
            wind_dir, wind_speed = wind_text, ""

        return {
            "온도": temp_el.get_text(strip=True),
            "습도": humid_el.get_text(strip=True),
            "강수": rain_el.get_text(strip=True),
            "풍향": wind_dir,
            "풍속": wind_speed
        }

    except Exception as e:
        print("❌ 크롤링 실패:", e)
        return None

def weather_loop():
    while True:
        if is_time_to_crawl():
            print(f"🕒 {datetime.now()} - 크롤링 시작")
            data = fetch_weather()
            if data:
                try:
                    ref = db.reference(f"/weather/{COMBINED_KEY}")
                    ref.child(get_10min_aligned_key()).set(data)
                    print("✅ 저장 완료")
                except Exception as e:
                    print("❌ Firebase 저장 실패:", e)
            else:
                print("⛔ 데이터 없음")
            time.sleep(55)
        else:
            time.sleep(1)

# ───────────── Flask 실행 + 백그라운드 크롤링 ─────────────
if __name__ == "__main__":
    # 크롤링 루프를 백그라운드로 실행
    threading.Thread(target=weather_loop, daemon=True).start()

    # Flask는 메인 스레드에서 실행해야 Render가 포트 감지 가능
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
