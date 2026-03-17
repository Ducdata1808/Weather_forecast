import os
import subprocess
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, hour, sin, cos, round, lit, when, unix_timestamp, monotonically_increasing_id
from pyspark.ml.feature import StringIndexer, OneHotEncoder
import shutil

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"

def get_spark_session():
    return SparkSession.builder \
        .appName("WeatherForecastTransformation") \
        .master("local[2]") \
        .config("spark.driver.memory", "512m") \
        .config("spark.executor.memory", "512m") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.driver.maxResultSize", "256m") \
        .config("spark.ui.enabled", "false") \
        .config("spark.sql.parquet.enableVectorizedReader", "false") \
        .config("spark.sql.parquet.mergeSchema", "true") \
        .getOrCreate()

def extract_data(spark, hdfs_actual_path, hdfs_forecast_path):
    """Đọc dữ liệu thực tế và dữ liệu dự báo từ HDFS"""
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
    
    # Định nghĩa lược đồ chuẩn cho Actual Data
    actual_schema = StructType([
        StructField("CityID", LongType(), True),
        StructField("DateTime", StringType(), True),
        StructField("Temperature", DoubleType(), True),
        StructField("Humidity", LongType(), True),
        StructField("Description", StringType(), True),
        StructField("WindSpeed", DoubleType(), True),
        StructField("WindDirection", LongType(), True),
        StructField("CloudRate", LongType(), True),
        StructField("RainOneHour", DoubleType(), True), # Ép kiểu Double
        StructField("SunRise", StringType(), True),
        StructField("SunSet", StringType(), True)
    ])
    
    # Định nghĩa lược đồ chuẩn cho Forecast Data
    forecast_schema = StructType([
        StructField("CityID", LongType(), True),
        StructField("Target_Time", StringType(), True),
        StructField("Forecast_Created_Time", StringType(), True),
        StructField("Temperature", DoubleType(), True),
        StructField("Humidity", LongType(), True),
        StructField("Description", StringType(), True),
        StructField("WindSpeed", DoubleType(), True),
        StructField("WindDirection", LongType(), True),
        StructField("CloudRate", LongType(), True),
        StructField("RainThreeHour", DoubleType(), True) # Ép kiểu Double
    ])

    print(f"[*] Đang đọc Actual Data từ: {hdfs_actual_path}")
    df_actual = spark.read.schema(actual_schema).parquet(hdfs_actual_path)
    
    print(f"[*] Đang đọc Forecast Data từ: {hdfs_forecast_path}")
    df_forecast = spark.read.schema(forecast_schema).parquet(hdfs_forecast_path)
    
    return df_actual, df_forecast

def clean_data(df_actual, df_forecast):
    """Xóa trùng lặp và xử lý các giá trị rỗng"""
    # 1. Bảng Thực tế
    df_actual = df_actual.dropDuplicates(["CityID", "DateTime"])
    df_actual = df_actual.fillna(0.0, subset=["RainOneHour"])
    df_actual = df_actual.fillna(0, subset=["CloudRate"])
    df_actual = df_actual.dropna(subset=["Temperature", "Humidity"])
    
    # 2. Bảng Dự báo
    df_forecast = df_forecast.dropDuplicates(["CityID", "Target_Time", "Forecast_Created_Time"])
    df_forecast = df_forecast.fillna(0.0, subset=["RainThreeHour"])
    df_forecast = df_forecast.fillna(0, subset=["CloudRate"])
    df_forecast = df_forecast.dropna(subset=["Temperature", "Humidity"])
    
    return df_actual, df_forecast

def join_and_calculate_error(df_actual, df_forecast):
    """
    Nối bảng Dự Báo và bảng Thực Tế dựa trên Thời gian mục tiêu.
    Sau đó tính Target_Error.
    """
    # Đổi tên các cột của Actual để phân biệt khi Join
    # Gắn thêm tiền tố "Actual_" cho các cột thực tế
    actual_columns = df_actual.columns
    for c in actual_columns:
        if c not in ["CityID", "DateTime"]:
            df_actual = df_actual.withColumnRenamed(c, f"Actual_{c}")
            
    # Đổi tên các cột của Forecast để phân biệt
    forecast_columns = df_forecast.columns
    for c in forecast_columns:
        if c not in ["CityID", "Target_Time", "Forecast_Created_Time"]:
            df_forecast = df_forecast.withColumnRenamed(c, f"Forecast_{c}")
            
    # Inner Join: Lấy những điểm giao nhau giữa lúc Dự Báo và lúc Xảy ra Thực tế
    # Match: Forecast.Target_Time == Actual.DateTime AND Forecast.CityID == Actual.CityID
    df_joined = df_forecast.join(
        df_actual,
        (df_forecast["Target_Time"] == df_actual["DateTime"]) & 
        (df_forecast["CityID"] == df_actual["CityID"]),
        "inner"
    )
    
    # Tính Cột Mục Tiêu để Model Linear Regression dự đoán (Y)
    # Sai Số = Nhiệt độ Thực tế - Nhiệt độ Dự báo
    df_joined = df_joined.withColumn("Target_Error", round(col("Actual_Temperature") - col("Forecast_Temperature"), 2))
    
    # Tính cột Lead_Time_Hours: Lời dự báo được đưa ra trước bao lâu?
    # Convert string sang timestamp để trừ
    df_joined = df_joined.withColumn("Target_Time_ts", unix_timestamp("Target_Time", "yyyy-MM-dd HH:mm:ss"))
    df_joined = df_joined.withColumn("Created_Time_ts", unix_timestamp("Forecast_Created_Time", "yyyy-MM-dd HH:mm:ss"))
    df_joined = df_joined.withColumn("Lead_Time_Hours", round((col("Target_Time_ts") - col("Created_Time_ts")) / 3600, 1))
    
    # Có thể drop bớt các cột dư thừa sau khi đã tính xong
    df_joined = df_joined.drop("Target_Time_ts", "Created_Time_ts", "DateTime")
    
    return df_joined

def create_features(df):
    """Tạo features Time-series từ cột Target_Time (Thời điểm xảy ra sự kiện)"""
    import math
    
    df = df.withColumn("Target_Time_ts", unix_timestamp("Target_Time", "yyyy-MM-dd HH:mm:ss").cast("timestamp"))
    df = df.withColumn("SunRise_ts", unix_timestamp("Actual_SunRise", "yyyy-MM-dd HH:mm:ss").cast("timestamp"))
    df = df.withColumn("SunSet_ts", unix_timestamp("Actual_SunSet", "yyyy-MM-dd HH:mm:ss").cast("timestamp"))

    # Features Giờ và Tháng
    df = df.withColumn("HourOfDay", hour("Target_Time_ts"))
    df = df.withColumn("Month", month("Target_Time_ts"))
    
    # Cyclic encoding cho Giờ
    df = df.withColumn("Hour_Sin", round(sin(col("HourOfDay") * lit(2.0 * math.pi / 24.0)), 4))
    df = df.withColumn("Hour_Cos", round(cos(col("HourOfDay") * lit(2.0 * math.pi / 24.0)), 4))

    # Is_Daylight: Lúc đó là ban ngày hay ban đêm
    df = df.withColumn("IsDaylight", 
                       when((col("Target_Time_ts") >= col("SunRise_ts")) & 
                            (col("Target_Time_ts") <= col("SunSet_ts")), 1).otherwise(0))
                            
    # Dọn dẹp lại các cột tạm
    df = df.drop("Target_Time_ts", "SunRise_ts", "SunSet_ts")
    
    return df

def feature_encoding(df):
    """Mã hoá One-Hot các biến hạng mục của Dữ liệu Dự báo (Features X đầu vào)"""
    # Ta dùng Forecast_Description vì khi dự đoán tương lai, ta chỉ biết Forecast_Description chứ không biết Actual_Description
    indexer = StringIndexer(inputCol="Forecast_Description", outputCol="Forecast_Description_Index", handleInvalid="keep")
    model_idx = indexer.fit(df)
    df = model_idx.transform(df)
    
    encoder = OneHotEncoder(inputCols=["Forecast_Description_Index"], outputCols=["Forecast_Description_Vec"])
    model_enc = encoder.fit(df)
    df = model_enc.transform(df)
    
    return df

def load_data(df, hdfs_output_dir, local_data_dir):
    """Lưu Dataset phục vụ Machine Learning ra Parquet"""
    current_time_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"ml_ready_dataset_{current_time_str}.parquet"
    
    local_tmp_path = os.path.join(local_data_dir, "tmp_spark_output")
    local_final_path = os.path.join(local_data_dir, file_name)
    
    df.coalesce(1).write.mode("overwrite").parquet(local_tmp_path)
    
    for f in os.listdir(local_tmp_path):
        if f.startswith("part-") and f.endswith(".parquet"):
            os.rename(os.path.join(local_tmp_path, f), local_final_path)
    
    shutil.rmtree(local_tmp_path)
    print(f"\n[+] Đã xuất file Local (Machine Learning Ready Dataset) tại: {local_final_path}")
    
    # Upload lên HDFS (tạm thời comment lại)
    # hdfs_file_path = f"{hdfs_output_dir}/{file_name}"
    # subprocess.run(["hdfs", "dfs", "-mkdir", "-p", hdfs_output_dir], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    # 
    # result = subprocess.run(["hdfs", "dfs", "-put", local_final_path, hdfs_file_path], capture_output=True, text=True)
    # if result.returncode == 0:
    #     print(f"[+] Đã upload thành công lên HDFS tại: {hdfs_file_path}")
    # else:
    #     print(f"[-] Lỗi khi upload lên HDFS:\n{result.stderr}")

if __name__ == "__main__":
    # Cấu hình đường dẫn tuyệt đối HDFS (đã comment vì chạy local)
    # HDFS_ACTUAL_PATTERN = "hdfs://localhost:9000/weather_data/raw_actual_*.parquet"
    # HDFS_FORECAST_PATTERN = "hdfs://localhost:9000/weather_data/raw_forecast_*.parquet"
    # HDFS_OUTPUT_DIR = "/weather_data" 
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_dir = os.path.dirname(script_dir)
    LOCAL_DATA_DIR = os.path.join(project_dir, "data")
    os.makedirs(LOCAL_DATA_DIR, exist_ok=True)
    
    # Sử dụng Pattern cho file local
    LOCAL_ACTUAL_PATTERN = os.path.join(LOCAL_DATA_DIR, "raw_actual_*.parquet")
    LOCAL_FORECAST_PATTERN = os.path.join(LOCAL_DATA_DIR, "raw_forecast_*.parquet")
    
    print("="*50)
    print("BẮT ĐẦU CHẠY SPARK ĐỂ TẠO DATASET MACHINE LEARNING...")
    
    spark = get_spark_session()
    
    try:
        # Bước 1: Đọc cả 2 rổ dữ liệu từ local
        df_act, df_fore = extract_data(spark, LOCAL_ACTUAL_PATTERN, LOCAL_FORECAST_PATTERN)
        
        # Bước 2: Cleanup riêng biệt
        df_act_clean, df_fore_clean = clean_data(df_act, df_fore)
        
        # Bước 3: Nối bảng tạo lable (Y) là Target_Error
        df_joined = join_and_calculate_error(df_act_clean, df_fore_clean)
        
        # Bước 4: Tạo Feature time-series (X)
        df_features = create_features(df_joined)
        
        # Bước 5: Mã hóa Label (X)
        df_final = feature_encoding(df_features)
        
        df_final.printSchema()
        print(f"\n[+] Tổng số mẫu hợp lệ (có cả dự báo lẫn thực tế) để Train Model: {df_final.count()} dòng\n")
        df_final.select("Target_Time", "Forecast_Temperature", "Actual_Temperature", "Target_Error", "Lead_Time_Hours").show(5, truncate=False)
        
        # Bước 6: Load ra Parquet 
        load_data(df_final, None, LOCAL_DATA_DIR)
        
    except Exception as e:
        print(f"[-] Lỗi trong quá trình chạy Spark Pipeline: {e}")
        
    finally:
        spark.stop()
