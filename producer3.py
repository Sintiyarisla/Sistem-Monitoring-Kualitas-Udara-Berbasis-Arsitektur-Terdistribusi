from tasks3 import cari_hari_polutan_tertinggi

if __name__ == "__main__":
    print("🚀 Mengirim task analisis ISPU ke Celery worker...")
    result = cari_hari_polutan_tertinggi.delay("ispu_dki_all.csv")  

    print("⏳ Menunggu hasil...")
    print(result.get(timeout=30))  