import requests
import pandas as pd
import os
import subprocess
from datetime import datetime
import pytz

# ============================================================
# CẤU HÌNH
# ============================================================
API_KEY = "453116d36315e347ce5232925acc96ae"
CITY_ID = 1566083          # ID của TP. Hồ Chí Minh trên OpenWeatherMap
BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

print(f"Gọi API thời tiết hiện tại cho CITY_ID={CITY_ID}")

# ============================================================
# GỌI API
# ============================================================
params = {
    "id"    : CITY_ID,
    "appid" : API_KEY,
    "units" : "metric",   # °C; dùng "imperial" cho °F
    "lang"  : "en",       # nhãn mô tả bằng tiếng Anh
}

response = requests.get(BASE_URL, params=params)
print(f"\nStatus code: {response.status_code}")

if response.status_code == 200:
    data = response.json()
    
    # Thiết lập Timezone HCM (GMT+7)
    hcm_tz = pytz.timezone('Asia/Ho_Chi_Minh')
    
    city_id = data.get("id")
    
    # Tính toán Timestamp theo giờ địa phương (HCM)
    dt_utc = datetime.fromtimestamp(data["dt"], tz=pytz.UTC)
    dt_local = dt_utc.astimezone(hcm_tz)
    
    # LÀM TRÒN THỜI GIAN VỀ GIỜ GẦN NHẤT (Nearest Hour)
    # Ví dụ: 10:14:00 -> 10:00:00 | 09:48:00 -> 10:00:00
    if dt_local.minute >= 30:
        dt_local = dt_local.replace(hour=(dt_local.hour + 1) % 24, minute=0, second=0, microsecond=0)
    else:
        dt_local = dt_local.replace(minute=0, second=0, microsecond=0)
        
    dt_local_str = dt_local.strftime('%Y-%m-%d %H:%M:%S')
    
    sunrise_utc = datetime.fromtimestamp(data["sys"]["sunrise"], tz=pytz.UTC)
    sunrise_local = sunrise_utc.astimezone(hcm_tz).strftime('%Y-%m-%d %H:%M:%S')
    
    sunset_utc = datetime.fromtimestamp(data["sys"]["sunset"], tz=pytz.UTC)
    sunset_local = sunset_utc.astimezone(hcm_tz).strftime('%Y-%m-%d %H:%M:%S')

    temp = data["main"]["temp"]
    humidity = data["main"]["humidity"]
    description = data["weather"][0]["description"]
    wind_speed = data["wind"]["speed"]
    wind_direction = data["wind"].get("deg")
    cloud_rate = data.get("clouds", {}).get("all")
    rain_1h = data.get("rain", {}).get("1h", 0.0)
    
    # Tạo DataFrame
    df = pd.DataFrame([{
        "CityID": city_id,
        "DateTime": dt_local_str, # Dùng biến đã làm tròn
        "Temperature": temp,
        "Humidity": humidity,
        "Description": description,
        "WindSpeed": wind_speed,
        "WindDirection": wind_direction,
        "CloudRate": cloud_rate,
        "RainOneHour": rain_1h,
        "SunRise": sunrise_local,
        "SunSet": sunset_local
    }])

    print(df.to_string(index=False))
    
    # Tạo tên file kèm timestamp hiện tại của HCM lúc chạy script
    current_time_hcm = datetime.now(hcm_tz)
    current_time_str = current_time_hcm.strftime("%Y%m%d_%H%M%S")
    file_name = f"raw_actual_{current_time_str}.parquet"

    # 1. Lưu file local: weather_forecast_project/data/raw_actual_YYYYMMDD_HHMMSS.parquet
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_dir = os.path.dirname(script_dir)
    local_data_dir = os.path.join(project_dir, "data")
    os.makedirs(local_data_dir, exist_ok=True)
    
    local_file_path = os.path.join(local_data_dir, file_name)
    df.to_parquet(local_file_path, index=False)
    print(f"\n[+] Đã xuất file local tại: {local_file_path}")
    
    # 2. Lưu file lên HDFS (tạm thời comment vì chỉ lưu local)
    # hdfs_dir = "/weather_data"
    # hdfs_file_path = f"{hdfs_dir}/{file_name}"
    # 
    # try:
    #     # Tạo thư mục trên HDFS nếu chưa có
    #     subprocess.run(["hdfs", "dfs", "-mkdir", "-p", hdfs_dir], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    #     
    #     # Đưa file từ local lên HDFS
    #     result = subprocess.run(["hdfs", "dfs", "-put", local_file_path, hdfs_file_path], capture_output=True, text=True)
    #     if result.returncode == 0:
    #         print(f"[+] Đã upload thành công lên HDFS tại: {hdfs_file_path}")
    #     else:
    #         print(f"[-] Lỗi khi upload lên HDFS:\n{result.stderr}")
    # except FileNotFoundError:
    #     print("[-] Lỗi: Không tìm thấy lệnh 'hdfs'. Vui lòng kiểm tra lại môi trường Hadoop.")
else:
    print("[-] Lỗi khi gọi API:")
    print(response.json())
    import sys
    sys.exit(1)