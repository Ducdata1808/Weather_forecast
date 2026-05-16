"""
Linear Regression - Dự báo nhiệt độ thực tế từ dữ liệu forecast thời tiết HCM
=================================================================================
Target   : Actual_Temperature
Features : Forecast data + time features
Metrics  : MAE, MSE, RMSE, R², MAPE, Adjusted R²
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split, cross_val_score, KFold
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import (
    mean_absolute_error,
    mean_squared_error,
    r2_score,
)
import warnings
warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────
# 1. CẤU HÌNH
# ─────────────────────────────────────────────
DATA_PATH   = "data/data/ml_ready_dataset_20260404_165118.parquet"
TARGET_COL  = "Actual_Temperature"
TEST_SIZE   = 0.2
RANDOM_SEED = 42

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

# ─────────────────────────────────────────────
# 2. ĐỌC & TIỀN XỬ LÝ DỮ LIỆU
# ─────────────────────────────────────────────
print("=" * 60)
print("  LINEAR REGRESSION - DỰ BÁO NHIỆT ĐỘ TP.HCM")
print("=" * 60)

print(f"\n[1] Đang đọc file: {DATA_PATH}")
df = pd.read_parquet(DATA_PATH)
print(f"    → Shape gốc : {df.shape}")

# Chọn features & target
df_model = df[FEATURE_COLS + [TARGET_COL]].copy()
df_model.dropna(inplace=True)
print(f"    → Shape sau khi dropna: {df_model.shape}")

X = df_model[FEATURE_COLS].values
y = df_model[TARGET_COL].values

print(f"\n[2] Phân chia Train / Test  (test_size={TEST_SIZE})")
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=TEST_SIZE, random_state=RANDOM_SEED
)
print(f"    → Train: {X_train.shape[0]} mẫu | Test: {X_test.shape[0]} mẫu")

# Chuẩn hóa features
scaler = StandardScaler()
X_train_sc = scaler.fit_transform(X_train)
X_test_sc  = scaler.transform(X_test)

# ─────────────────────────────────────────────
# 3. HUẤN LUYỆN MÔ HÌNH
# ─────────────────────────────────────────────
print("\n[3] Huấn luyện Linear Regression ...")
model = LinearRegression()
model.fit(X_train_sc, y_train)
print("    → Hoàn thành!")

# ─────────────────────────────────────────────
# 4. DỰ ĐOÁN
# ─────────────────────────────────────────────
y_pred_train = model.predict(X_train_sc)
y_pred_test  = model.predict(X_test_sc)

# ─────────────────────────────────────────────
# 5. CHỈ SỐ ĐÁNH GIÁ
# ─────────────────────────────────────────────
def mape(y_true, y_pred):
    """Mean Absolute Percentage Error (tránh chia cho 0)"""
    mask = y_true != 0
    return np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100

def adjusted_r2(r2, n, p):
    """Adjusted R² = 1 - (1-R²)*(n-1)/(n-p-1)"""
    return 1 - (1 - r2) * (n - 1) / (n - p - 1)

def compute_metrics(y_true, y_pred, label=""):
    n = len(y_true)
    p = len(FEATURE_COLS)
    mae   = mean_absolute_error(y_true, y_pred)
    mse   = mean_squared_error(y_true, y_pred)
    rmse  = np.sqrt(mse)
    r2    = r2_score(y_true, y_pred)
    adj_r2 = adjusted_r2(r2, n, p)
    mape_ = mape(y_true, y_pred)
    return {
        "Tập dữ liệu"   : label,
        "MAE  (°C)"     : round(mae,   4),
        "MSE  (°C²)"    : round(mse,   4),
        "RMSE (°C)"     : round(rmse,  4),
        "R²"            : round(r2,    4),
        "Adj. R²"       : round(adj_r2, 4),
        "MAPE (%)"      : round(mape_, 4),
    }

metrics_train = compute_metrics(y_train, y_pred_train, "Train")
metrics_test  = compute_metrics(y_test,  y_pred_test,  "Test")

# Cross-validation R²
cv = KFold(n_splits=5, shuffle=True, random_state=RANDOM_SEED)
cv_scores = cross_val_score(model, scaler.transform(X), y, cv=cv, scoring="r2")

print("\n" + "=" * 60)
print("  [4] KẾT QUẢ ĐÁNH GIÁ MÔ HÌNH")
print("=" * 60)

metrics_df = pd.DataFrame([metrics_train, metrics_test])
metrics_df = metrics_df.set_index("Tập dữ liệu")
print(metrics_df.to_string())

print(f"\n  Cross-Validation R² (5-fold): {cv_scores.mean():.4f} ± {cv_scores.std():.4f}")
print(f"  Intercept (hệ số tự do)     : {model.intercept_:.4f}")

# Giải thích nhanh
print("\n  ─── Giải thích chỉ số ───────────────────────────────")
print(f"  MAE  = {metrics_test['MAE  (°C)']:.4f} °C  → Sai số tuyệt đối trung bình")
print(f"  RMSE = {metrics_test['RMSE (°C)']:.4f} °C  → Penalize sai số lớn hơn MAE")
print(f"  R²   = {metrics_test['R²']:.4f}      → Mô hình giải thích {metrics_test['R²']*100:.1f}% phương sai")
print(f"  MAPE = {metrics_test['MAPE (%)']:.4f} %   → Sai số % so với giá trị thực")

# ─────────────────────────────────────────────
# 6. HỆ SỐ FEATURE IMPORTANCE
# ─────────────────────────────────────────────
coef_df = pd.DataFrame({
    "Feature"    : FEATURE_COLS,
    "Coefficient": model.coef_,
    "Abs_Coef"   : np.abs(model.coef_),
}).sort_values("Abs_Coef", ascending=False).reset_index(drop=True)

print("\n" + "=" * 60)
print("  [5] HỆ SỐ HỒI QUY (Feature Importance)")
print("=" * 60)
print(coef_df[["Feature", "Coefficient"]].to_string(index=False))

# ─────────────────────────────────────────────
# 7. VISUALIZATION
# ─────────────────────────────────────────────
fig = plt.figure(figsize=(18, 14))
fig.suptitle(
    "Linear Regression - Dự báo Nhiệt Độ TP.HCM\n"
    f"(R² Test = {metrics_test['R²']:.4f} | RMSE = {metrics_test['RMSE (°C)']:.4f} °C | MAE = {metrics_test['MAE  (°C)']:.4f} °C)",
    fontsize=15, fontweight="bold", y=0.98
)

gs = gridspec.GridSpec(3, 3, figure=fig, hspace=0.45, wspace=0.38)

# ── Plot 1: Actual vs Predicted (Test) ──────────────────────
ax1 = fig.add_subplot(gs[0, :2])
ax1.scatter(y_test, y_pred_test, alpha=0.6, color="#4A90D9", edgecolors="white",
            linewidths=0.4, s=60, label="Test samples")
lim_min = min(y_test.min(), y_pred_test.min()) - 1
lim_max = max(y_test.max(), y_pred_test.max()) + 1
ax1.plot([lim_min, lim_max], [lim_min, lim_max], "r--", lw=2, label="Perfect fit (y=x)")
ax1.set_xlabel("Actual Temperature (°C)", fontsize=11)
ax1.set_ylabel("Predicted Temperature (°C)", fontsize=11)
ax1.set_title("Predicted vs Actual Temperature", fontsize=12, fontweight="bold")
ax1.legend(fontsize=9)
ax1.grid(alpha=0.3)

# ── Plot 2: Metrics Bar ──────────────────────────────────────
ax2 = fig.add_subplot(gs[0, 2])
metric_names  = ["MAE", "RMSE", "R²", "MAPE (%)"]
train_vals = [metrics_train["MAE  (°C)"], metrics_train["RMSE (°C)"],
              metrics_train["R²"],          metrics_train["MAPE (%)"]]
test_vals  = [metrics_test["MAE  (°C)"],  metrics_test["RMSE (°C)"],
              metrics_test["R²"],           metrics_test["MAPE (%)"]]
x_pos = np.arange(len(metric_names))
bars1 = ax2.bar(x_pos - 0.2, train_vals, 0.35, label="Train", color="#5CB85C", alpha=0.8)
bars2 = ax2.bar(x_pos + 0.2, test_vals,  0.35, label="Test",  color="#D9534F", alpha=0.8)
ax2.set_xticks(x_pos)
ax2.set_xticklabels(metric_names, fontsize=8)
ax2.set_title("Train vs Test Metrics", fontsize=12, fontweight="bold")
ax2.legend(fontsize=8)
ax2.grid(axis="y", alpha=0.3)
for bar in list(bars1) + list(bars2):
    ax2.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.01,
             f"{bar.get_height():.3f}", ha="center", va="bottom", fontsize=6.5)

# ── Plot 3: Residuals vs Predicted ──────────────────────────
ax3 = fig.add_subplot(gs[1, :2])
residuals = y_test - y_pred_test
ax3.scatter(y_pred_test, residuals, alpha=0.6, color="#9B59B6",
            edgecolors="white", linewidths=0.4, s=55)
ax3.axhline(y=0, color="red", linestyle="--", lw=2)
ax3.axhline(y= residuals.std(), color="orange", linestyle=":", lw=1.5, label=f"+1σ = {residuals.std():.2f}")
ax3.axhline(y=-residuals.std(), color="orange", linestyle=":", lw=1.5, label=f"-1σ")
ax3.set_xlabel("Predicted Temperature (°C)", fontsize=11)
ax3.set_ylabel("Residual (Actual - Predicted) °C", fontsize=11)
ax3.set_title("Residuals vs Predicted Values", fontsize=12, fontweight="bold")
ax3.legend(fontsize=9)
ax3.grid(alpha=0.3)

# ── Plot 4: Residual Distribution ───────────────────────────
ax4 = fig.add_subplot(gs[1, 2])
ax4.hist(residuals, bins=25, color="#E67E22", edgecolor="white",
         alpha=0.85, density=True)
# Đường chuẩn overlay
from scipy.stats import norm
mu, sigma = residuals.mean(), residuals.std()
x_norm = np.linspace(residuals.min(), residuals.max(), 200)
ax4.plot(x_norm, norm.pdf(x_norm, mu, sigma), "b-", lw=2, label="Normal dist.")
ax4.axvline(mu, color="red", linestyle="--", lw=1.5, label=f"mean={mu:.3f}")
ax4.set_xlabel("Residual (°C)", fontsize=11)
ax4.set_ylabel("Density", fontsize=11)
ax4.set_title("Residual Distribution", fontsize=12, fontweight="bold")
ax4.legend(fontsize=8)
ax4.grid(alpha=0.3)

# ── Plot 5: Feature Coefficients ─────────────────────────────
ax5 = fig.add_subplot(gs[2, :2])
colors = ["#E74C3C" if c < 0 else "#2ECC71" for c in coef_df["Coefficient"]]
bars = ax5.barh(coef_df["Feature"], coef_df["Coefficient"], color=colors, alpha=0.85, edgecolor="white")
ax5.axvline(x=0, color="black", lw=1)
ax5.set_xlabel("Coefficient (standardized)", fontsize=11)
ax5.set_title("Feature Coefficients (hệ số hồi quy)", fontsize=12, fontweight="bold")
ax5.grid(axis="x", alpha=0.3)
for bar, val in zip(bars, coef_df["Coefficient"]):
    ha = "left" if val >= 0 else "right"
    offset = 0.01 if val >= 0 else -0.01
    ax5.text(val + offset, bar.get_y() + bar.get_height() / 2,
             f"{val:.3f}", ha=ha, va="center", fontsize=8)

# ── Plot 6: CV Scores ────────────────────────────────────────
ax6 = fig.add_subplot(gs[2, 2])
fold_labels = [f"Fold {i+1}" for i in range(len(cv_scores))]
bar_colors = ["#3498DB" if s >= cv_scores.mean() else "#E74C3C" for s in cv_scores]
ax6.bar(fold_labels, cv_scores, color=bar_colors, alpha=0.85, edgecolor="white")
ax6.axhline(cv_scores.mean(), color="black", linestyle="--", lw=1.5,
            label=f"Mean = {cv_scores.mean():.4f}")
ax6.set_ylim(max(0, cv_scores.min() - 0.05), min(1.0, cv_scores.max() + 0.05))
ax6.set_ylabel("R² Score", fontsize=11)
ax6.set_title("5-Fold Cross Validation R²", fontsize=12, fontweight="bold")
ax6.legend(fontsize=9)
ax6.grid(axis="y", alpha=0.3)
for i, (bar, val) in enumerate(zip(ax6.patches, cv_scores)):
    ax6.text(bar.get_x() + bar.get_width() / 2, val + 0.002,
             f"{val:.3f}", ha="center", va="bottom", fontsize=8)

# Lưu hình
output_path = "data/linear_regression_results.png"
plt.savefig(output_path, dpi=150, bbox_inches="tight", facecolor="white")
print(f"\n[✓] Đã lưu biểu đồ: {output_path}")
plt.show()

print("\n" + "=" * 60)
print("  HOÀN TẤT!")
print("=" * 60)
