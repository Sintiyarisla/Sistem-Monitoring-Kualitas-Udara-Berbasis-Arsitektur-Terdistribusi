# Hitung frekuensi polutan kritikal per stasiun

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
def frekuensi_polutan_kritikal(csv_path="ispu_dki_all.csv"):
    try:
        full_path = os.path.join(BASE_DIR, csv_path)
        df = pd.read_csv(full_path)

        df = df[df["stasiun"].notna()]
        df = df[df["critical"].notna()]

        # Hitung frekuensi kemunculan critical per stasiun
        freq = pd.crosstab(df["stasiun"], df["critical"]).reset_index()

        output_path = os.path.join(OUTPUT_DIR, "frekuensi_polutan_kritikal_per_stasiun.csv")
        freq.to_csv(output_path, index=False)

        print(f"📁 Frekuensi polutan kritikal disimpan ke {output_path}")
        return f"✅ Frekuensi polutan kritikal berhasil dihitung untuk {len(freq)} stasiun."

    except Exception as e:
        error_msg = traceback.format_exc()
        print(f"❌ Gagal hitung frekuensi polutan kritikal:\n{error_msg}")
        return f"❌ ERROR: {str(e)}"
