import os
import time
import threading
import subprocess
from datetime import datetime

# ==== CONFIG ====
RESULT_DIR = r"D:\Workspace\Python\Tiktok\tiktok_crawl\JP_result"
INPUT_FILE = r"D:\Workspace\Python\Tiktok\tiktok_crawl\actor_input.json"
MEMORY = 128            # RAM cho actor (MB), ví dụ: 128 / 512 / 2048
SLEEP_DURATION = 10     # nghỉ giữa 2 lần crawl
MAX_RUNS = None         # None = chạy vô hạn, hoặc đặt số lần tối đa
STOP_FLAG = False       # để stop khi Ctrl+C

# Nếu apify không nằm trong PATH, thay bằng đường dẫn tuyệt đối tới apify.cmd
APIFY_CLI = r"C:\Users\Do Tung\AppData\Roaming\npm\apify.cmd"  
# Ví dụ: APIFY_CLI = r"C:\Users\<username>\AppData\Roaming\npm\apify.cmd"


def crawl_tiktok(thread_id):
    """Chạy crawl TikTok bằng CLI và lưu ra file JSON"""
    try:
        print(f"[Thread {thread_id}] Starting crawl at {datetime.now()}")

        # tạo thư mục lưu nếu chưa có
        os.makedirs(RESULT_DIR, exist_ok=True)

        # đặt tên file theo timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{RESULT_DIR}\\scan_results_thread{thread_id}_{timestamp}.json"

        # lệnh CLI
        cmd = [
            APIFY_CLI, "call", "novi/fast-tiktok-api",
            f"--input-file={INPUT_FILE}",
            f"--memory={MEMORY}",
            "--output-dataset",
            "--silent"   # ẩn log, chỉ in ra dataset JSON
        ]

        # chạy và redirect output JSON vào file
        with open(filename, "w", encoding="utf-8") as f:
            subprocess.run(cmd, stdout=f, stderr=subprocess.STDOUT, check=True)

        print(f"✅ [Thread {thread_id}] Crawl completed. Saved results to {filename}")
        return True

    except Exception as e:
        print(f"❌ [Thread {thread_id}] Error during crawl: {e}")
        return False


def run_crawler_thread(thread_id):
    """Chạy crawler lặp vô hạn trong 1 thread"""
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
