import requests
import pandas as pd
import os
import subprocess
from datetime import datetime
import pytz

# ============================================================
# CẤU HÌNH API DỰ BÁO
# ============================================================
API_KEY = "453116d36315e347ce5232925acc96ae"
CITY_ID = 1566083          # ID của TP. Hồ Chí Minh trên OpenWeatherMap
BASE_URL = "https://api.openweathermap.org/data/2.5/forecast"

# ============================================================
# GỌI API & XỬ LÝ TIMEZONE HCM
# ============================================================
hcm_tz = pytz.timezone('Asia/Ho_Chi_Minh')
current_time_hcm = datetime.now(hcm_tz)
forecast_created_time = current_time_hcm.strftime('%Y-%m-%d %H:%M:%S')

print(f"Gọi API lấy 40 mốc DỰ BÁO tương lai cho CITY_ID={CITY_ID} lúc {forecast_created_time}")

params = {
    "id"    : CITY_ID,
    "appid" : API_KEY,
    "units" : "metric",   # °C
    "lang"  : "en",       # Tiếng Anh
}

response = requests.get(BASE_URL, params=params)

if response.status_code == 200:
    data = response.json()
    city_id = data["city"]["id"]
    forecast_list = data["list"]
    
    records = []
    
    # Lấy ra chuỗi "Ngày hiện tại" chuẩn giờ HCM (VD: '2026-03-08')
    current_date_str = current_time_hcm.strftime('%Y-%m-%d')
    
    # Duyệt qua danh sách 40 mốc thời gian dự báo (mỗi 3 tiếng)
    for item in forecast_list:
        # Giờ dự báo mục tiêu (Target Time)
        dt_utc = datetime.fromtimestamp(item["dt"], tz=pytz.UTC)
        target_local_full = dt_utc.astimezone(hcm_tz)
        target_local_str = target_local_full.strftime('%Y-%m-%d %H:%M:%S')
        target_date_str = target_local_full.strftime('%Y-%m-%d')
        
        # Chỉ lấy những mốc thời gian thuộc về Ngày Hôm Nay
        if target_date_str == current_date_str:
            temp = item["main"]["temp"]
            humidity = item["main"]["humidity"]
            description = item["weather"][0]["description"]
            wind_speed = item["wind"]["speed"]
            wind_direction = item["wind"].get("deg")
            cloud_rate = item.get("clouds", {}).get("all")
            # API Forecast thường trả lượng mưa trong 3h thay vì 1h
            rain_3h = item.get("rain", {}).get("3h", 0.0) 
            
            records.append({
                "CityID": city_id,
                "Forecast_Created_Time": forecast_created_time, # Lúc tôi đưa ra lời tiên tri này
                "Target_Time": target_local_str,                # Đích đến của lời tiên tri
                "Temperature": temp,
                "Humidity": humidity,
                "Description": description,
                "WindSpeed": wind_speed,
                "WindDirection": wind_direction,
                "CloudRate": cloud_rate,
                "RainThreeHour": rain_3h
            })
        
    # Tạo DataFrame từ list dictionary
    df = pd.DataFrame(records)
    
    print(f"\n[+] Số mốc dự báo thu thập được: {len(df)}")
    print("Mẫu 3 dòng đầu (mốc gần nhất 3h, 6h, 9h tới):")
    print(df.head(3).to_string(index=False))
    
    # 1. Lưu file local:
    current_time_str = current_time_hcm.strftime("%Y%m%d_%H%M%S")
    file_name = f"raw_forecast_{current_time_str}.parquet"
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_dir = os.path.dirname(script_dir)
    local_data_dir = os.path.join(project_dir, "data")
    os.makedirs(local_data_dir, exist_ok=True)
    
    local_file_path = os.path.join(local_data_dir, file_name)
    df.to_parquet(local_file_path, index=False)
    print(f"\n[+] Đã xuất file local tại: {local_file_path}")
    
    # 2. Lưu file lên HDFS (tạm thời comment lại vì chỉ lưu local)
    # hdfs_dir = "/weather_data"
    # hdfs_file_path = f"{hdfs_dir}/{file_name}"
    # 
    # try:
    #     subprocess.run(["hdfs", "dfs", "-mkdir", "-p", hdfs_dir], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    #     result = subprocess.run(["hdfs", "dfs", "-put", local_file_path, hdfs_file_path], capture_output=True, text=True)
    #     if result.returncode == 0:
    #         print(f"[+] Đã upload thành công lên HDFS tại: {hdfs_file_path}")
    #     else:
    #         print(f"[-] Lỗi khi upload lên HDFS:\n{result.stderr}")
    # except FileNotFoundError:
    #     print("[-] Lỗi: Không tìm thấy lệnh 'hdfs'. Vui lòng kiểm tra lại môi trường Hadoop.")

else:
    print(f"[-] Lỗi khi gọi API: {response.status_code}")
    print(response.json())
    import sys
    sys.exit(1)

