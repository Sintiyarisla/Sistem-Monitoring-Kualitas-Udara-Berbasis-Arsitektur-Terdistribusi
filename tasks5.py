# Kategori Kualitas Udara Terbanyak per Stasiun

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
def kategori_terbanyak_per_stasiun(csv_path="ispu_dki_all.csv"):
    import pandas as pd
    import os
    import traceback

    try:
        full_path = os.path.join(BASE_DIR, csv_path)
        df = pd.read_csv(full_path)

        df = df[df["categori"].notna()]
        df = df[~df["categori"].str.upper().isin(["TIDAK ADA DATA"])]

        counts = df.groupby(["stasiun", "categori"]).size().reset_index(name="count")
        idx = counts.groupby("stasiun")["count"].idxmax()
        terbanyak = counts.loc[idx].reset_index(drop=True)

        output_path = os.path.join(OUTPUT_DIR, "ispu_kategori_terbanyak.csv")
        terbanyak.to_csv(output_path, index=False)

        print(f"📁 Kategori terbanyak per stasiun disimpan ke {output_path}")
        return f"✅ Kategori terbanyak per stasiun berhasil dihitung dan disimpan untuk {len(terbanyak)} stasiun."

    except Exception as e:
        error_msg = traceback.format_exc()
        print(f"❌ Gagal hitung kategori terbanyak:\n{error_msg}")
        return f"❌ ERROR: {str(e)}"
