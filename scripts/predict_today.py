"""
predict_today.py
=================================================================================
Mục đích : Dùng model Linear Regression đã train để dự đoán nhiệt độ thực tế
           từ file raw_forecast của ngày HÔM NAY (dữ liệu tương lai, chưa có Actual).

Pipeline:
  1. Đọc raw_forecast của hôm nay  (data/data/raw_forecast_YYYYMMDD_*.parquet)
  2. Tạo features (Lead_Time_Hours, HourOfDay, Month, Hour_Sin, Hour_Cos,
                  IsDaylight, Forecast_Description_Index)  — giống transform.py
  3. Train lại model trên toàn bộ ml_ready_dataset mới nhất
  4. Dự đoán nhiệt độ thực tế cho từng mốc thời gian hôm nay
  5. In kết quả + lưu ra CSV
=================================================================================
"""

import pandas as pd
import numpy as np
import glob
import os
import math
from datetime import datetime
import pytz
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler, LabelEncoder
import warnings
warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────
# CẤU HÌNH
# ─────────────────────────────────────────────
hcm_tz      = pytz.timezone("Asia/Ho_Chi_Minh")
TODAY_STR   = datetime.now(hcm_tz).strftime("%Y%m%d")      # VD: "20260406"
DATA_DIR    = "data/data"
RAW_PREDICT_DIR = "data/raw_predict_data"
RESULT_DIR  = "data/result_predict_today"

# Sunrise / Sunset gần đúng cho TP.HCM (thay đổi ít trong năm)
SUNRISE_HOUR = 6    # ~ 6:00 AM
SUNSET_HOUR  = 18   # ~ 6:00 PM

FEATURE_COLS = [
    "Forecast_Temperature",
    "Forecast_Humidity",
    "Forecast_WindSpeed",
    "Forecast_WindDirection",
    "Forecast_CloudRate",
    "Forecast_RainThreeHour",
    "Lead_Time_Hours",
    "HourOfDay",
    "Month",
    "Hour_Sin",
    "Hour_Cos",
    "IsDaylight",
    "Forecast_Description_Index",
]
TARGET_COL = "Actual_Temperature"

print("=" * 65)
print("  PREDICT TODAY - Dự đoán Nhiệt Độ Thực Tế TP.HCM")
print(f"  Ngày: {TODAY_STR}")
print("=" * 65)

# ─────────────────────────────────────────────
# BƯỚC 1: ĐỌC FILE RAW_FORECAST HÔM NAY TỪ RAW_PREDICT_DIR
# ─────────────────────────────────────────────
pattern = os.path.join(RAW_PREDICT_DIR, f"raw_forecast_{TODAY_STR}_*.parquet")
files   = sorted(glob.glob(pattern))

if not files:
    print(f"\n[!] Không tìm thấy file forecast cho ngày {TODAY_STR}")
    print(f"    Pattern tìm kiếm: {pattern}")
    print("    → Thử dùng file forecast mới nhất có sẵn...")
    all_files = sorted(glob.glob(os.path.join(RAW_PREDICT_DIR, "raw_forecast_*.parquet")))
    if not all_files:
        raise FileNotFoundError(f"Không có file raw_forecast nào trong {RAW_PREDICT_DIR}")
    files = [all_files[-1]]   # Lấy file mới nhất
    print(f"    → Sử dụng: {files[0]}")

print(f"\n[1] Đọc {len(files)} file forecast: {[os.path.basename(f) for f in files]}")
df_raw = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
print(f"    → Shape: {df_raw.shape}")
print(f"    → Columns: {list(df_raw.columns)}")

# ─────────────────────────────────────────────
# BƯỚC 2: TẠO FEATURES (giống transform.py)
# ─────────────────────────────────────────────
print("\n[2] Tạo features từ dữ liệu forecast...")

df = df_raw.copy()

# Đổi tên cột cho khớp với ml_ready_dataset
rename_map = {
    "Temperature"   : "Forecast_Temperature",
    "Humidity"      : "Forecast_Humidity",
    "WindSpeed"     : "Forecast_WindSpeed",
    "WindDirection" : "Forecast_WindDirection",
    "CloudRate"     : "Forecast_CloudRate",
    "RainThreeHour" : "Forecast_RainThreeHour",
    "Description"   : "Forecast_Description",
}
df.rename(columns=rename_map, inplace=True)

# Parse thời gian
df["Target_Time"]          = pd.to_datetime(df["Target_Time"])
df["Forecast_Created_Time"] = pd.to_datetime(df["Forecast_Created_Time"])

# Chỉ lấy mốc thời gian tương lai (hoặc hôm nay)
today = datetime.now(hcm_tz).date()
df["Target_Date"] = df["Target_Time"].dt.date
df = df[df["Target_Date"] >= today].copy()
print(f"    → Số mốc thời gian từ hôm nay trở đi: {len(df)}")

# Lead_Time_Hours: Khoảng cách từ lúc tạo forecast đến Target_Time (giờ)
df["Lead_Time_Hours"] = (
    (df["Target_Time"] - df["Forecast_Created_Time"])
    .dt.total_seconds() / 3600
).round(1)

# Time features
df["HourOfDay"] = df["Target_Time"].dt.hour
df["Month"]     = df["Target_Time"].dt.month

# Cyclic encoding
df["Hour_Sin"] = (df["HourOfDay"] * 2.0 * math.pi / 24.0).apply(math.sin).round(4)
df["Hour_Cos"] = (df["HourOfDay"] * 2.0 * math.pi / 24.0).apply(math.cos).round(4)

# IsDaylight (dựa trên giờ đơn giản — không cần SunRise/SunSet thực tế)
df["IsDaylight"] = ((df["HourOfDay"] >= SUNRISE_HOUR) & (df["HourOfDay"] < SUNSET_HOUR)).astype(int)

# Forecast_Description_Index: Label Encode (dùng LabelEncoder để nhất quán với training)
# — Sẽ được fit lại từ training data bên dưới.
# Tạm thời để dạng string, sẽ encode sau khi có training data.

print(f"    → Lead_Time_Hours range: {df['Lead_Time_Hours'].min():.1f}h → {df['Lead_Time_Hours'].max():.1f}h")

# ─────────────────────────────────────────────
# BƯỚC 3: TRAIN MODEL TRÊN DỮ LIỆU MỚI NHẤT
# ─────────────────────────────────────────────
print("\n[3] Tải ml_ready_dataset mới nhất để train model...")

ml_files = sorted(glob.glob(os.path.join(DATA_DIR, "ml_ready_dataset_*.parquet")))
if not ml_files:
    raise FileNotFoundError("Không tìm thấy file ml_ready_dataset trong data/data/")

latest_ml = ml_files[-1]
print(f"    → Dùng: {os.path.basename(latest_ml)}")
df_train = pd.read_parquet(latest_ml)
print(f"    → Training samples: {len(df_train)}")

# Encode Forecast_Description trong training data
le = LabelEncoder()
df_train["Forecast_Description_Index"] = le.fit_transform(
    df_train["Forecast_Description"].astype(str)
)

# Encode Forecast_Description trong prediction data (dùng cùng LabelEncoder)
df["Forecast_Description_str"] = df["Forecast_Description"].astype(str)
# Xử lý các nhãn chưa thấy trong training → gán giá trị trung bình
known_classes = set(le.classes_)
df["Forecast_Description_Index"] = df["Forecast_Description_str"].apply(
    lambda x: le.transform([x])[0] if x in known_classes
    else df_train["Forecast_Description_Index"].median()
)

# Chuẩn bị X, y cho training
df_model = df_train[FEATURE_COLS + [TARGET_COL]].dropna()
X_train  = df_model[FEATURE_COLS].values
y_train  = df_model[TARGET_COL].values

# Train model + scaler trên toàn bộ data (không split — dùng toàn bộ để predict tốt nhất)
scaler = StandardScaler()
X_train_sc = scaler.fit_transform(X_train)

model = LinearRegression()
model.fit(X_train_sc, y_train)
print(f"    → Model trained! (R² trên full data: {model.score(X_train_sc, y_train):.4f})")

# ─────────────────────────────────────────────
# BƯỚC 4: DỰ ĐOÁN
# ─────────────────────────────────────────────
print("\n[4] Dự đoán nhiệt độ thực tế...")

# Kiểm tra các cột cần thiết
missing = [c for c in FEATURE_COLS if c not in df.columns]
if missing:
    raise ValueError(f"Thiếu các cột: {missing}")

X_pred    = df[FEATURE_COLS].fillna(0).values
X_pred_sc = scaler.transform(X_pred)
y_pred    = model.predict(X_pred_sc)

df["Predicted_Actual_Temperature"] = np.round(y_pred, 2)
df["Forecast_Error_Expected"]      = np.round(y_pred - df["Forecast_Temperature"], 2)

# ─────────────────────────────────────────────
# BƯỚC 5: HIỂN THỊ KẾT QUẢ
# ─────────────────────────────────────────────
display_cols = [
    "Target_Time",
    "Forecast_Temperature",
    "Predicted_Actual_Temperature",
    "Forecast_Error_Expected",
    "Forecast_Description",
    "Lead_Time_Hours",
    "HourOfDay",
    "IsDaylight",
]

df_result = df[display_cols].sort_values("Target_Time").reset_index(drop=True)

print("\n" + "=" * 65)
print("  KẾT QUẢ DỰ ĐOÁN NHIỆT ĐỘ THỰC TẾ")
print("=" * 65)
print(df_result.to_string(index=True))

print("\n" + "─" * 65)
print("  THỐNG KÊ TÓM TẮT:")
print(f"  Tổng số mốc thời gian dự đoán : {len(df_result)}")
print(f"  Nhiệt độ dự đoán thấp nhất    : {df_result['Predicted_Actual_Temperature'].min():.2f}°C")
print(f"  Nhiệt độ dự đoán cao nhất     : {df_result['Predicted_Actual_Temperature'].max():.2f}°C")
print(f"  Nhiệt độ dự đoán trung bình   : {df_result['Predicted_Actual_Temperature'].mean():.2f}°C")
print(f"  Sai số kỳ vọng TB (pred-forecast): {df_result['Forecast_Error_Expected'].mean():.2f}°C")
print("─" * 65)

# ─────────────────────────────────────────────
# BƯỚC 6: LƯU KẾT QUẢ VÀO RESULT_DIR
# ─────────────────────────────────────────────
os.makedirs(RESULT_DIR, exist_ok=True)
output_path = os.path.join(RESULT_DIR, f"prediction_today_{TODAY_STR}.csv")
df_result.to_csv(output_path, index=False, encoding="utf-8-sig")
print(f"\n[✓] Đã lưu kết quả: {output_path}")

print("\n" + "=" * 65)
print("  HOÀN TẤT!")
print("=" * 65)
