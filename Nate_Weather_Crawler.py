import requests
from bs4 import BeautifulSoup
import firebase_admin
from firebase_admin import credentials, db
from dotenv import load_dotenv
import os
import time
from datetime import datetime
import threading
import socket

AREA_NAME = "cheonan_asan"
AREA_CODE = "11C20302"
COMBINED_KEY = f"{AREA_NAME}({AREA_CODE})"

# .env에서 Firebase URL 로드
load_dotenv()
FIREBASE_DB_URL = os.getenv("FIREBASE_DB_URL")

# Firebase 초기화
try:
    cred = credentials.Certificate("firebase_service_account.json")
    if not firebase_admin._apps:
        firebase_admin.initialize_app(cred, {
            "databaseURL": FIREBASE_DB_URL
        })
    print("✅ Firebase 초기화 성공")
except Exception as e:
    print("❌ Firebase 초기화 실패:", e)
    exit(1)

# 정확히 10분 단위 시각에 해당하는 키 생성
def get_10min_aligned_key():
    now = datetime.now()
    aligned_minute = (now.minute // 10) * 10
    aligned_time = now.replace(minute=aligned_minute, second=0, microsecond=0)
    return aligned_time.strftime("%Y-%m-%d %H:%M:%S")

# 현재 시각이 정확히 10분 단위인지 확인
def is_time_to_crawl():
    now = datetime.now()
    return now.minute % 10 == 0 and now.second == 0

# 날씨 데이터 크롤링
def fetch_weather():
    url = f"https://news.nate.com/weather?areaCode={AREA_CODE}"
    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    try:
        res = requests.get(url, headers=headers)
        res.encoding = "euc-kr"
        soup = BeautifulSoup(res.text, "html.parser")

        # 필요한 요소 선택
        temp_el = soup.select_one(
            "#contentsWraper > div.weather_main_today_wrap > div.weather_today > div.today_wrap > div > div.right_today > div.temperature > p.celsius"
        )
        humid_el = soup.select_one(
            "#contentsWraper > div.weather_main_today_wrap > div.weather_today > div.today_wrap > div > div.right_today > div.hrw_area > p.humidity > em"
        )
        rain_el = soup.select_one(
            "#contentsWraper > div.weather_main_today_wrap > div.weather_today > div.today_wrap > div > div.right_today > div.hrw_area > p.rainfall > em"
        )
        wind_el = soup.select_one(
            "#contentsWraper > div.weather_main_today_wrap > div.weather_today > div.today_wrap > div > div.right_today > div.hrw_area > p.wind > em"
        )

        # 요소가 다 존재하는지 확인
        if not all([temp_el, humid_el, rain_el, wind_el]):
            print("❌ 일부 데이터 태그를 찾을 수 없습니다.")
            return None

        # 풍향과 풍속 분리
        wind_text = wind_el.get_text(strip=True)
        try:
            wind_dir, wind_speed = wind_text.split(maxsplit=1)  # "북서", "2.5 m/s"
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
        print("❌ 크롤링 중 오류 발생:", e)
        return None

# ─────────────────────────────
# Render가 꺼지지 않도록 포트 바인딩
# ─────────────────────────────
def keep_alive():
    port = int(os.environ.get("PORT", 10000))
    s = socket.socket()
    s.bind(("0.0.0.0", port))
    s.listen(1)
    print(f"🟢 Keep-alive socket bound to port {port}")
    while True:
        conn, _ = s.accept()
        conn.close()

# 백그라운드 스레드로 keep-alive 실행
threading.Thread(target=keep_alive, daemon=True).start()

# 메인 루프
while True:
    if is_time_to_crawl():
        print(f"🕒 {datetime.now()} - 정확한 시각 도달, 크롤링 시작")
        data = fetch_weather()
        if data:
            try:
                timestamp_key = get_10min_aligned_key()
                ref = db.reference(f"/weather/{COMBINED_KEY}")
                ref.child(timestamp_key).set(data)
                print(f"✅ Firebase 저장 성공: {timestamp_key}")
            except Exception as e:
                print("❌ Firebase 저장 실패:", e)
        else:
            print("⛔ 유효한 데이터를 가져오지 못했습니다.")

        # 중복 저장 방지: 약 55초 쉬기
        time.sleep(55)
    else:
        # 1초 간격으로 폴링
        time.sleep(1)