import requests
from bs4 import BeautifulSoup
import firebase_admin
from firebase_admin import credentials, db
from dotenv import load_dotenv
import os
import time
from datetime import datetime

# ───── 상수 정의 ─────
AREA_NAME = "cheonan_asan"
AREA_CODE = "11C20302"
COMBINED_KEY = f"{AREA_NAME}({AREA_CODE})"
# ────────────────────

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

# 현재 시각이 정확히 10분 단위 인지 확인
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

        temp_el = soup.select_one(
            "#contentsWraper > div.weather_main_today_wrap > div.weather_today > div.today_wrap > div > div.right_today > div.temperature > p.celsius"
        )
        humid_el = soup.select_one(
            "#contentsWraper > div.weather_main_today_wrap > div.weather_today > div.today_wrap > div > div.right_today > div.hrw_area > p.humidity > em"
        )
        rain_el = soup.select_one(
            "#contentsWraper > div.weather_main_today_wrap > div.weather_today > div.today_wrap > div > div.right_today > div.hrw_area > p.rainfall > em"
        )

        if not temp_el or not humid_el or not rain_el:
            print("❌ 일부 데이터 태그를 찾을 수 없습니다.")
            return None

        return {
            "temperature": temp_el.get_text(strip=True),
            "humidity": humid_el.get_text(strip=True),
            "precipitation": rain_el.get_text(strip=True)
        }

    except Exception as e:
        print("❌ 크롤링 중 오류 발생:", e)
        return None

# 메인 루프
while True:
    if is_time_to_crawl():
        print(f"🕒 {datetime.now()} - 정확한 시각 도달, 크롤링 시작")
        data = fetch_weather()
        if data:
            try:
                timestamp_key = get_10min_aligned_key()
                ref = db.reference(f"/weather/{COMBINED_KEY}")
                ref.child(timestamp_key).set({
                    "온도": data["temperature"],
                    "습도": data["humidity"],
                    "강수": data["precipitation"]
                })
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
