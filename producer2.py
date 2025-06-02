from tasks2 import analyze_polutans

if __name__ == "__main__":
    print("🚀 Mengirim task analisis polutan ke Celery...")
    result = analyze_polutans.delay("ispu_dki_all.csv")

    print("⏳ Menunggu hasil...")
    print(result.get(timeout=30))
