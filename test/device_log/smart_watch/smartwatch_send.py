import argparse
import json
import random
import time
import numpy as np
from kafka import KafkaProducer
from datetime import datetime, timedelta

from smartwatch_gen import (
    create_device_user_mapping,
    generate_day_data,
    apply_noise
)

KAFKA_TOPIC = "smartwatch"
KAFKA_SERVER = "localhost:9092"


# ---------------------------
# Convert numpy / datetime
# ---------------------------
def convert_np(obj):
    if isinstance(obj, np.generic):
        return obj.item()
    if isinstance(obj, datetime):
        return int(obj.timestamp() * 1000)  # epoch millis
    raise TypeError(f"Cannot convert type: {type(obj)}")


producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v, default=convert_np).encode("utf-8")
)


# ---------------------------
# Send batch records
# ---------------------------
def send_batch(records, dirty_rate=0.0, excluded_fields=None, delay=None):
    count = 0
    for rec in records:
        dirty_rec = apply_noise(rec, dirty_rate=dirty_rate, excluded_fields=excluded_fields)
        producer.send(KAFKA_TOPIC, value=dirty_rec)
        print("SENT:", dirty_rec)
        count += 1
        if delay:
            time.sleep(delay)
    producer.flush()
    return count


# ---------------------------
# History mode (ngày A → B)
# ---------------------------
def run_history_mode(devices, start_date, end_date, interval_minutes, dirty_rate, excluded_fields):
    """
    Gửi dữ liệu "realtime" cho tất cả ngày trong khoảng start_date → end_date.
    """
    print(f"\n⏳ [HISTORY] Gửi dữ liệu từ {start_date.date()} đến {end_date.date()}...\n")
    count = 0
    current = start_date
    while current < end_date:
        realtime = generate_day_data(devices, current, interval_minutes=interval_minutes)
        count += send_batch(realtime, dirty_rate=dirty_rate, excluded_fields=excluded_fields, delay=None)
        current += timedelta(days=1)
    print(f"\n✅ Đã gửi tổng {count} bản ghi lịch sử.")


# ---------------------------
# Realtime mode (1 ngày)
# ---------------------------
def run_realtime_mode(devices, target_date, interval_minutes, dirty_rate, excluded_fields, delay):
    print(f"\n⏳ [REALTIME] Giả lập ngày {target_date.date()}...\n")
    realtime = generate_day_data(devices, target_date, interval_minutes=interval_minutes)
    count = send_batch(realtime, dirty_rate=dirty_rate, excluded_fields=excluded_fields, delay=delay)
    print(f"\n✅ Đã gửi realtime {count} bản ghi.")


# ---------------------------
# CLI
# ---------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Smartwatch Data Sender (History / Realtime)")

    parser.add_argument("--num_devices", type=int, required=True)
    parser.add_argument("--num_users", type=int, required=True)
    parser.add_argument("--mode", choices=["history", "realtime"], required=True)

    # History
    parser.add_argument("--start", type=str)
    parser.add_argument("--end", type=str)
    parser.add_argument("--interval", type=int, default=5, help="Phút / record")

    # Realtime
    parser.add_argument("--date", type=str)
    parser.add_argument("--delay", type=float, default=0.5)

    # Noise
    parser.add_argument("--dirty_rate", type=float, default=0.0)
    parser.add_argument("--exclude", type=str, default="")

    args = parser.parse_args()

    random.seed(1510)
    excluded_fields = [f.strip() for f in args.exclude.split(",") if f.strip() != ""]

    # Mapping device <-> user
    devices = create_device_user_mapping(args.num_devices, args.num_users)
    print("\n📌 Mapping Device ↔ User:")
    for d in devices:
        print(f"  Device {d['device_id']} ↔ User {d['user_id']}")

    if args.mode == "history":
        if not args.start or not args.end:
            raise ValueError("History mode cần --start và --end")
        start_date = datetime.strptime(args.start, "%Y-%m-%d")
        end_date = datetime.strptime(args.end, "%Y-%m-%d") + timedelta(days=1)
        run_history_mode(devices, start_date, end_date, args.interval, args.dirty_rate, excluded_fields)

    elif args.mode == "realtime":
        if not args.date:
            raise ValueError("Realtime mode cần --date")
        target_date = datetime.strptime(args.date, "%Y-%m-%d")
        run_realtime_mode(devices, target_date, args.interval, args.dirty_rate, excluded_fields, args.delay)

    producer.flush()
