# Menghitung jumlah hari kualitas udara buruk

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
def analyze_ispu(csv_path="ispu_dki_all.csv"):
    try:
        full_path = os.path.join(BASE_DIR, csv_path)
        df = pd.read_csv(full_path)

        # Bersihkan data: drop jika kategori kosong atau "TIDAK ADA DATA"
        df = df[df["categori"].notna()]
        df = df[~df["categori"].str.upper().isin(["TIDAK ADA DATA"])]

        if df.empty:
            return "⚠️ Semua data kosong atau tidak valid."

        # Normalisasi huruf besar-kecil
        df["categori"] = df["categori"].str.upper()
        kategori_buruk = {"TIDAK SEHAT", "SANGAT TIDAK SEHAT", "BERBAHAYA"}

        summary = []

        for stasiun, group in df.groupby("stasiun"):
            buruk = group[group["categori"].isin(kategori_buruk)]

            summary.append({
                "stasiun": stasiun,
                "total_hari": len(group),
                "hari_kualitas_buruk": len(buruk),
                "persentase_buruk": round(len(buruk) / len(group) * 100, 2),
                "tanggal_buruk": ", ".join(buruk["tanggal"].tolist())
            })

        # Simpan ke CSV
        summary_df = pd.DataFrame(summary)
        output_path = os.path.join(OUTPUT_DIR, "ispu_summary.csv")
        summary_df.to_csv(output_path, index=False)

        print(f"📁 Ringkasan disimpan ke {output_path}")
        return f"✅ ISPU dianalisis: {len(summary)} stasiun."

    except Exception as e:
        error_msg = traceback.format_exc()
        print(f"❌ Gagal analisis ISPU:\n{error_msg}")
        return f"❌ ERROR: {str(e)}"
