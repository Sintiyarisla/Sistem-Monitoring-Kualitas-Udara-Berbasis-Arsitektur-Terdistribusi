from tasks4 import rata_rata_bulanan

if __name__ == "__main__":
    print("🚀 Mengirim task analisis ISPU ke Celery worker...")
    result = rata_rata_bulanan.delay("ispu_dki_all.csv")  

    print("⏳ Menunggu hasil...")
    print(result.get(timeout=30))  