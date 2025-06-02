# Task untuk mencari hari dengan polutan tertinggi per stasiun 


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
def cari_hari_polutan_tertinggi(csv_path="ispu_dki_all.csv"):
    try:
        full_path = os.path.join(BASE_DIR, csv_path)
        print(f"Load CSV from: {full_path}")
        df = pd.read_csv(full_path)

        df = df[df["categori"].notna()]
        df = df[~df["categori"].str.upper().isin(["TIDAK ADA DATA"])]

        polutans = ["pm25", "pm10", "so2", "co", "o3", "no2"]

        rows = []
        for stasiun, group in df.groupby("stasiun"):
            row = {"stasiun": stasiun}
            for polutan in polutans:
                if polutan not in group.columns:
                    print(f"⚠️ Kolom {polutan} tidak ada di data stasiun {stasiun}")
                    row[f"{polutan}_tanggal_max"] = None
                    row[f"{polutan}_nilai_max"] = None
                    continue

                max_value = group[polutan].max()
                if pd.isna(max_value):
                    row[f"{polutan}_tanggal_max"] = None
                    row[f"{polutan}_nilai_max"] = None
                else:
                    tanggal = group[group[polutan] == max_value]["tanggal"].values[0]
                    row[f"{polutan}_tanggal_max"] = tanggal
                    row[f"{polutan}_nilai_max"] = max_value

                print(f"{stasiun} - {polutan} max: {max_value}")

            rows.append(row)

        result_df = pd.DataFrame(rows)
        print(result_df.head())

        print(f"Output folder exists: {os.path.exists(OUTPUT_DIR)}")
        print(f"Output folder writable: {os.access(OUTPUT_DIR, os.W_OK)}")

        output_path = os.path.join(OUTPUT_DIR, "hari_polutan_tertinggi.csv")
        result_df.to_csv(output_path, index=False)

        print(f"📁 Hari polutan tertinggi disimpan ke {output_path}")
        return f"✅ Data hari polutan tertinggi berhasil disimpan untuk {len(result_df)} stasiun."

    except Exception as e:
        error_msg = traceback.format_exc()
        print(f"❌ Gagal cari hari polutan tertinggi:\n{error_msg}")
        return f"❌ ERROR: {str(e)}"
