from tasks1 import analyze_ispu

if __name__ == "__main__":
    print("🚀 Mengirim task analisis ISPU ke Celery worker...")
    result = analyze_ispu.delay("ispu_dki_all.csv")  

    print("⏳ Menunggu hasil...")
    print(result.get(timeout=30))  