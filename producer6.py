from tasks6 import frekuensi_polutan_kritikal

if __name__ == "__main__":
    print("🚀 Mengirim task analisis ISPU ke Celery worker...")
    result = frekuensi_polutan_kritikal.delay("ispu_dki_all.csv")  

    print("⏳ Menunggu hasil...")
    print(result.get(timeout=30))  