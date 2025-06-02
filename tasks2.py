# menghitung nilai rata-rata dan maksimum tiap polutan per stasiun.

from celery import Celery
import pandas as pd
import os
import traceback


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

os.environ["FORKED_BY_MULTIPROCESSING"] = '1'

user = "guest"
passw = "guest"
app = Celery(
    "celery_app",
    backend="rpc://",
    broker=f"amqp://{user}:{passw}@localhost:5672"
)


@app.task
def analyze_polutans(csv_path="ispu_dki_all.csv"):
    try:
        full_path = os.path.join(BASE_DIR, csv_path)
        df = pd.read_csv(full_path)

        # Kolom polutan yang akan dianalisis
        polutan_cols = ["pm25", "pm10", "so2", "co", "o3", "no2", "max"]

        # Pastikan data numerik
        df[polutan_cols] = df[polutan_cols].apply(pd.to_numeric, errors="coerce")

        # Drop data tanpa stasiun
        df = df[df["stasiun"].notna()]

        if df.empty:
            return "⚠️ Data kosong atau tidak valid."

        # Group by stasiun dan hitung rata-rata + maksimum
        summary = df.groupby("stasiun")[polutan_cols].agg(["mean", "max"]).round(2)

        # FLATTEN kolom multiindex: (polutan, agg) → polutan_agg
        summary.columns = ['_'.join(col) for col in summary.columns]
        summary = summary.reset_index()

        # Simpan ke CSV
        output_path = os.path.join(OUTPUT_DIR, "ispu_polutan_summary.csv")
        summary.to_csv(output_path, index=False)


        print(f"📁 Ringkasan polutan disimpan ke {output_path}")
        return f"✅ Polutan dianalisis untuk {len(summary)} stasiun."

    except Exception as e:
        error_msg = traceback.format_exc()
        print(f"❌ Gagal analisis polutan:\n{error_msg}")
        return f"❌ ERROR: {str(e)}"
