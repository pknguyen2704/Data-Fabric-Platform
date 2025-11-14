# env_send.py
import argparse
import json
import random
import time
import numpy as np
from kafka import KafkaProducer
from datetime import datetime, timedelta

from Gen_data import (
    generate_environment_data,
    create_device_user_mapping,
    apply_noise
)

KAFKA_TOPIC = "home_sensor"
KAFKA_SERVER = "localhost:9092"


def convert_np(obj):
    """Convert numpy & datetime for JSON"""
    if isinstance(obj, np.generic):
        return obj.item()
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Cannot convert type: {type(obj)}")


producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v, default=convert_np).encode("utf-8")
)


# ==========================================================
# Gửi batch dữ liệu (1 timestamp) → có thể có noise + delay
# ==========================================================
def send_batch(devices, timestamp, dirty_rate, excluded_fields, delay=None):
    batch = generate_environment_data(devices, timestamp)
    count = 0

    for rec in batch:
        dirty_rec = apply_noise(rec, dirty_rate=dirty_rate, excluded_fields=excluded_fields)

        producer.send(KAFKA_TOPIC, value=dirty_rec)
        print("Sent:", dirty_rec)
        count += 1

        if delay:
            time.sleep(delay)

    producer.flush()
    return count


# ========================
# MODE 1: Gửi lịch sử
# ========================
def run_history_mode(devices, start_date, end_date, dirty_rate, excluded_fields):
    print(f"\n⏳ [HISTORY] Gửi dữ liệu từ {start_date.date()} đến {end_date.date()}...\n")

    timestamp = start_date
    interval = timedelta(minutes=5)
    count = 0

    while timestamp < end_date:
        count += send_batch(devices, timestamp, dirty_rate, excluded_fields, delay=None)
        timestamp += interval

    print(f"\n✅ Đã gửi tổng {count} bản ghi lịch sử.")


# ========================
# MODE 2: Gửi realtime
# ========================
def run_realtime_mode(devices, target_date, dirty_rate, excluded_fields, delay):
    print(f"\n⏳ [REALTIME] Giả lập ngày {target_date.date()}...\n")

    start = datetime(target_date.year, target_date.month, target_date.day, 0, 0)
    end = start + timedelta(days=1)
    interval = timedelta(minutes=5)
    timestamp = start
    count = 0

    while timestamp < end:
        count += send_batch(devices, timestamp, dirty_rate, excluded_fields, delay)
        timestamp += interval

    print(f"\n✅ Đã gửi realtime {count} bản ghi.")


# ====================================================
#                      MAIN CLI
# ====================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Home Sensor Data Sender")

    parser.add_argument("--num_devices", type=int, required=True)
    parser.add_argument("--num_users", type=int, required=True)

    parser.add_argument("--mode", choices=["history", "realtime"], required=True)

    # --- History Mode ---
    parser.add_argument("--start", type=str)
    parser.add_argument("--end", type=str)

    # --- Realtime Mode ---
    parser.add_argument("--date", type=str)
    parser.add_argument("--delay", type=float, default=1.0)

    # --- Noise Control ---
    parser.add_argument("--dirty_rate", type=float, default=0.0,
                        help="Tỉ lệ làm bẩn dữ liệu (0.0 → 1.0)")
    parser.add_argument("--exclude", type=str, default="",
                        help="Trường không làm bẩn: vd device_id,user_id")

    args = parser.parse_args()

    random.seed(1510)

    # parse excluded fields
    excluded_fields = [f.strip() for f in args.exclude.split(",") if f.strip() != ""]

    # mapping
    devices = create_device_user_mapping(args.num_devices, args.num_users)

    print("\n📌 Mapping Device ↔ User")
    for d in devices:
        print(f"  Device {d['device_id']} ↔ User {d['user_id']}")

    # mode xử lý
    if args.mode == "history":
        if not args.start or not args.end:
            raise ValueError("History mode cần --start và --end")
        start_date = datetime.strptime(args.start, "%Y-%m-%d")
        end_date = datetime.strptime(args.end, "%Y-%m-%d") + timedelta(days=1)
        run_history_mode(devices, start_date, end_date, args.dirty_rate, excluded_fields)

    elif args.mode == "realtime":
        if not args.date:
            raise ValueError("Realtime mode cần --date")
        target_date = datetime.strptime(args.date, "%Y-%m-%d")
        run_realtime_mode(devices, target_date, args.dirty_rate, excluded_fields, args.delay)

    producer.flush()
