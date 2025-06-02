from tasks5 import kategori_terbanyak_per_stasiun

if __name__ == "__main__":
    print("🚀 Mengirim task analisis ISPU ke Celery worker...")
    result = kategori_terbanyak_per_stasiun.delay("ispu_dki_all.csv")  

    print("⏳ Menunggu hasil...")
    print(result.get(timeout=30))  