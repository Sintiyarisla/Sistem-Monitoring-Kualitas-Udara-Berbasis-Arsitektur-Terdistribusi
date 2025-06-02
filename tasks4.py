# Rata-rata Polutan Bulanan per Stasiun 

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
def rata_rata_bulanan(csv_path="ispu_dki_all.csv"):
    import pandas as pd
    import os
    import traceback

    try:
        full_path = os.path.join(BASE_DIR, csv_path)
        df = pd.read_csv(full_path)

        df = df[df["categori"].notna()]
        df = df[~df["categori"].str.upper().isin(["TIDAK ADA DATA"])]

        df["tanggal"] = pd.to_datetime(df["tanggal"])
        df["bulan"] = df["tanggal"].dt.to_period("M").astype(str)

        polutans = ["pm25", "pm10", "so2", "co", "o3", "no2"]

        avg_monthly = df.groupby(["stasiun", "bulan"])[polutans].mean().round(2).reset_index()

        output_path = os.path.join(OUTPUT_DIR, "ispu_rata_rata_bulanan.csv")
        avg_monthly.to_csv(output_path, index=False)

        print(f"📁 Rata-rata bulanan disimpan ke {output_path}")
        return f"✅ Rata-rata bulanan berhasil dihitung dan disimpan untuk {len(avg_monthly)} baris."

    except Exception as e:
        error_msg = traceback.format_exc()
        print(f"❌ Gagal hitung rata-rata bulanan:\n{error_msg}")
        return f"❌ ERROR: {str(e)}"
