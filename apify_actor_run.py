from apify_client import ApifyClient
import json
import time
from datetime import datetime
import os
import threading
import threading

# Token
# Token
client = ApifyClient()

# Input cho Actor
# Input cho Actor
run_input = {
    "isDownloadVideo": False,
    "isDownloadVideoCover": False,
    "isDownloadVideo": False,
    "isDownloadVideoCover": False,
    "isUnlimited": False,
    "limit": 1000,
    "publishTime": "MONTH",
    "region": "JP",
    "region": "JP",
    "sortType": 0,
    "type": "TREND"
}

# Config
RESULT_DIR = "JP_result"
SLEEP_DURATION = 10
MAX_RUNS = None  # None = chạy vô hạn
STOP_FLAG = False  # flag để stop khi Ctrl+C


def fix_encoding(obj):
    """Sửa lỗi double-encoding Latin1 -> UTF-8 cho chuỗi"""
    if isinstance(obj, str):
        try:
            return obj.encode("latin1").decode("utf-8")
        except:
            return obj
    elif isinstance(obj, dict):
        return {k: fix_encoding(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [fix_encoding(v) for v in obj]
    else:
        return obj


def crawl_tiktok(thread_id):
# Config
RESULT_DIR = "JP_result"
SLEEP_DURATION = 10
MAX_RUNS = None  # None = chạy vô hạn
STOP_FLAG = False  # flag để stop khi Ctrl+C


def fix_encoding(obj):
    """Sửa lỗi double-encoding Latin1 -> UTF-8 cho chuỗi"""
    if isinstance(obj, str):
        try:
            return obj.encode("latin1").decode("utf-8")
        except:
            return obj
    elif isinstance(obj, dict):
        return {k: fix_encoding(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [fix_encoding(v) for v in obj]
    else:
        return obj


def crawl_tiktok(thread_id):
    try:
        print(f"[Thread {thread_id}] Starting crawl at {datetime.now()}")

        # gọi actor với timeout 30 phút
        run = client.actor("novi/fast-tiktok-api").call(
            run_input=run_input,
            timeout_secs=1800
        )

        # tạo thư mục lưu
        os.makedirs(RESULT_DIR, exist_ok=True)

        # đặt tên file theo timestamp và thread_id
        print(f"[Thread {thread_id}] Starting crawl at {datetime.now()}")

        # gọi actor với timeout 30 phút
        run = client.actor("novi/fast-tiktok-api").call(
            run_input=run_input,
            timeout_secs=1800
        )

        # tạo thư mục lưu
        os.makedirs(RESULT_DIR, exist_ok=True)

        # đặt tên file theo timestamp và thread_id
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{RESULT_DIR}/scan_results_thread{thread_id}_{timestamp}.json"
        filename = f"{RESULT_DIR}/scan_results_thread{thread_id}_{timestamp}.json"

        # ghi dữ liệu ra file JSON
        # ghi dữ liệu ra file JSON
        with open(filename, "w", encoding="utf-8") as f:
            f.write("[\n")
            first = True
            for item in client.dataset(run["defaultDatasetId"]).iterate_items():
                item = fix_encoding(item)
                if not first:
                    f.write(",\n")
                json.dump(item, f, ensure_ascii=False, indent=2)
                first = False
            f.write("\n]")
            f.write("[\n")
            first = True
            for item in client.dataset(run["defaultDatasetId"]).iterate_items():
                item = fix_encoding(item)
                if not first:
                    f.write(",\n")
                json.dump(item, f, ensure_ascii=False, indent=2)
                first = False
            f.write("\n]")

        print(f"✅ [Thread {thread_id}] Crawl completed. Saved results to {filename}")
        print(f"✅ [Thread {thread_id}] Crawl completed. Saved results to {filename}")
        return True

    except Exception as e:
        print(f"❌ [Thread {thread_id}] Error during crawl: {e}")
        print(f"❌ [Thread {thread_id}] Error during crawl: {e}")
        return False


def run_crawler_thread(thread_id):
    """Chạy crawler trong một thread riêng biệt"""
    global STOP_FLAG
    run_count = 0
    while not STOP_FLAG and (MAX_RUNS is None or run_count < MAX_RUNS):
        run_count += 1
        print(f"\n[Thread {thread_id}] --- Run {run_count} ---")
        crawl_tiktok(thread_id)
        print(f"[Thread {thread_id}] Sleeping {SLEEP_DURATION} seconds...\n")
        time.sleep(SLEEP_DURATION)


def main():
    global STOP_FLAG
    print("🚀 Starting 2 parallel TikTok crawlers...")

    # tạo 2 thread
    thread1 = threading.Thread(target=run_crawler_thread, args=(1,))
    thread2 = threading.Thread(target=run_crawler_thread, args=(2,))

    # khởi chạy
    thread1.start()
    thread2.start()

    print("✅ Both threads started successfully!")
    print("Press Ctrl+C to stop both crawlers...")

    try:
        while thread1.is_alive() or thread2.is_alive():
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 Stopping crawlers...")
        STOP_FLAG = True
        thread1.join()
        thread2.join()
        print("✅ All threads stopped.")


if __name__ == "__main__":
    main()