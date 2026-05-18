import os
import glob
import numpy as np
import pandas as pd
from datetime import datetime

def extract_data(actual_path_pattern, forecast_path_pattern):
    """Đọc dữ liệu thực tế và dữ liệu dự báo từ Local và sửa lỗi ép kiểu"""
    def read_and_union(path_pattern):
        files = glob.glob(path_pattern)
        if not files:
            return pd.DataFrame()

        dfs = []
        for file in files:
            try:
                df_temp = pd.read_parquet(file)
                # Ép các cột dễ bị sai lệch về float để đồng nhất schema
                cols_to_cast = ["Temperature", "WindSpeed", "RainOneHour", "RainThreeHour"]
                for c in cols_to_cast:
                    if c in df_temp.columns:
                        df_temp[c] = df_temp[c].astype(float)
                dfs.append(df_temp)
            except Exception as e:
                print(f"[-] Lỗi khi đọc file {file}: {e}")
                
        if not dfs:
            return pd.DataFrame()
            
        # Hợp nhất các DataFrame tương tự unionByName trong Spark
        df_merged = pd.concat(dfs, ignore_index=True, sort=False)
        return df_merged

    print(f"[*] Đang đọc và hợp nhất Actual Data từ: {actual_path_pattern}")
    df_actual = read_and_union(actual_path_pattern)
    
    print(f"[*] Đang đọc và hợp nhất Forecast Data từ: {forecast_path_pattern}")
    df_forecast = read_and_union(forecast_path_pattern)
    
    return df_actual, df_forecast

def clean_data(df_actual, df_forecast):
    """Xóa trùng lặp và xử lý các giá trị rỗng"""
    if df_actual.empty or df_forecast.empty:
        return df_actual, df_forecast

    # 1. Bảng Thực tế
    df_actual = df_actual.drop_duplicates(subset=["CityID", "DateTime"])
    df_actual["RainOneHour"] = df_actual["RainOneHour"].fillna(0.0)
    df_actual["CloudRate"] = df_actual["CloudRate"].fillna(0.0)
    df_actual = df_actual.dropna(subset=["Temperature", "Humidity"])
    
    # 2. Bảng Dự báo
    df_forecast = df_forecast.drop_duplicates(subset=["CityID", "Target_Time", "Forecast_Created_Time"])
    df_forecast["RainThreeHour"] = df_forecast["RainThreeHour"].fillna(0.0)
    df_forecast["CloudRate"] = df_forecast["CloudRate"].fillna(0.0)
    df_forecast = df_forecast.dropna(subset=["Temperature", "Humidity"])
    
    return df_actual, df_forecast

def join_and_calculate_error(df_actual, df_forecast):
    """
    Nối bảng Dự Báo và bảng Thực Tế dựa trên Thời gian mục tiêu.
    Sau đó tính Target_Error.
    """
    if df_actual.empty or df_forecast.empty:
        return pd.DataFrame()

    # Đổi tên các cột của Actual để phân biệt khi Join
    # Gắn thêm tiền tố "Actual_" cho các cột thực tế
    actual_rename = {c: f"Actual_{c}" for c in df_actual.columns if c not in ["CityID", "DateTime"]}
    df_actual = df_actual.rename(columns=actual_rename)
            
    # Đổi tên các cột của Forecast để phân biệt
    forecast_rename = {c: f"Forecast_{c}" for c in df_forecast.columns if c not in ["CityID", "Target_Time", "Forecast_Created_Time"]}
    df_forecast = df_forecast.rename(columns=forecast_rename)
            
    # Inner Join: Lấy những điểm giao nhau giữa lúc Dự Báo và lúc Xảy ra Thực tế
    df_joined = pd.merge(
        df_forecast,
        df_actual,
        left_on=["Target_Time", "CityID"],
        right_on=["DateTime", "CityID"],
        how="inner"
    )
    
    # Tính Cột Mục Tiêu để Model Linear Regression dự đoán (Y)
    # Sai Số = Nhiệt độ Thực tế - Nhiệt độ Dự báo
    df_joined["Target_Error"] = (df_joined["Actual_Temperature"] - df_joined["Forecast_Temperature"]).round(2)
    
    # Tính cột Lead_Time_Hours: Lời dự báo được đưa ra trước bao lâu?
    target_ts = pd.to_datetime(df_joined["Target_Time"])
    created_ts = pd.to_datetime(df_joined["Forecast_Created_Time"])
    df_joined["Lead_Time_Hours"] = ((target_ts - created_ts).dt.total_seconds() / 3600.0).round(1)
    
    # Dọn dẹp cột DateTime trùng lắp
    df_joined = df_joined.drop(columns=["DateTime"])
    
    return df_joined

def create_features(df):
    """Tạo features Time-series từ cột Target_Time (Thời điểm xảy ra sự kiện)"""
    if df.empty:
        return df

    target_ts = pd.to_datetime(df["Target_Time"])
    sunrise_ts = pd.to_datetime(df["Actual_SunRise"])
    sunset_ts = pd.to_datetime(df["Actual_SunSet"])

    # Features Giờ và Tháng
    df["HourOfDay"] = target_ts.dt.hour
    df["Month"] = target_ts.dt.month
    
    # Cyclic encoding cho Giờ
    df["Hour_Sin"] = (np.sin(df["HourOfDay"] * (2.0 * np.pi / 24.0))).round(4)
    df["Hour_Cos"] = (np.cos(df["HourOfDay"] * (2.0 * np.pi / 24.0))).round(4)

    # Is_Daylight: Lúc đó là ban ngày hay ban đêm
    df["IsDaylight"] = ((target_ts >= sunrise_ts) & (target_ts <= sunset_ts)).astype(int)
    
    return df

def feature_encoding(df):
    """Mã hoá các biến hạng mục của Dữ liệu Dự báo"""
    if df.empty:
        return df
        
    # Sắp xếp các giá trị theo tần suất xuất hiện giảm dần để mô phỏng tương tự StringIndexer của PySpark
    freq = df["Forecast_Description"].value_counts()
    mapping = {val: float(i) for i, val in enumerate(freq.index)}
    df["Forecast_Description_Index"] = df["Forecast_Description"].map(mapping).astype(float)
    
    return df

def load_data(df, local_data_dir):
    """Lưu Dataset phục vụ Machine Learning ra Parquet"""
    current_time_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"ml_ready_dataset_{current_time_str}.parquet"
    local_final_path = os.path.join(local_data_dir, file_name)
    
    df.to_parquet(local_final_path, index=False)
    print(f"\n[+] Đã xuất file Local (Machine Learning Ready Dataset) tại: {local_final_path}")

if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_dir = os.path.dirname(script_dir)
    # Nguyên bản sử dụng thư mục data trực tiếp
    LOCAL_DATA_DIR = os.path.join(project_dir, "data")
    os.makedirs(LOCAL_DATA_DIR, exist_ok=True)
    
    # Sử dụng Pattern cho file local
    LOCAL_ACTUAL_PATTERN = os.path.join(LOCAL_DATA_DIR, "raw_actual_*.parquet")
    LOCAL_FORECAST_PATTERN = os.path.join(LOCAL_DATA_DIR, "raw_forecast_*.parquet")
    
    print("="*50)
    print("BẮT ĐẦU CHẠY PANDAS PIPELINE ĐỂ TẠO DATASET MACHINE LEARNING...")
    
    try:
        # Bước 1: Đọc cả 2 rổ dữ liệu từ local
        df_act, df_fore = extract_data(LOCAL_ACTUAL_PATTERN, LOCAL_FORECAST_PATTERN)
        
        if df_act.empty or df_fore.empty:
            print("[-] Không tìm thấy dữ liệu Actual hoặc Forecast để xử lý.")
        else:
            # Bước 2: Cleanup riêng biệt
            df_act_clean, df_fore_clean = clean_data(df_act, df_fore)
            
            # Bước 3: Nối bảng tạo label (Y) là Target_Error
            df_joined = join_and_calculate_error(df_act_clean, df_fore_clean)
            
            # Bước 4: Tạo Feature time-series (X)
            df_features = create_features(df_joined)
            
            # Bước 5: Mã hóa
            df_final = feature_encoding(df_features)
            
            print("\n[+] Cấu trúc dữ liệu cuối cùng:")
            print(df_final.info())
            
            print(f"\n[+] Tổng số mẫu hợp lệ để Train Model: {len(df_final)} dòng\n")
            if not df_final.empty:
                print(df_final[["Target_Time", "Forecast_Temperature", "Actual_Temperature", "Target_Error", "Lead_Time_Hours"]].head(5))
            
                # Bước 6: Load ra Parquet 
                load_data(df_final, LOCAL_DATA_DIR)
            
    except Exception as e:
        print(f"[-] Lỗi trong quá trình chạy Pandas Pipeline: {e}")
