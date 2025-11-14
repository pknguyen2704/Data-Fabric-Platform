# smartwatch_send.py
import argparse
import json
import random
import time
import numpy as np
from kafka import KafkaProducer
from datetime import datetime, timedelta

from smartwatch_gen import (
    create_device_user_mapping,
    generate_day_data
)

# ---------------------------
# Noise utilities (apply noise to record)
# ---------------------------
def apply_noise(record, dirty_rate=0.0, excluded_fields=None):
    """
    Apply noise to a record with probability dirty_rate.
    excluded_fields: list of field names not to corrupt.
    """
    if excluded_fields is None:
        excluded_fields = []

    # No noise applied
    if random.random() > dirty_rate:
        return record

    dirty = record.copy()
    noise_types = ["missing", "outlier", "wrong_type", "small_noise", "negative"]

    for field in list(dirty.keys()):
        if field in excluded_fields:
            continue

        # do not corrupt timestamp (keeps ordering and parsing sane)
        if field == "timestamp":
            continue

        typ = random.choice(noise_types)

        # Missing
        if typ == "missing":
            dirty[field] = None

        # Outlier
        elif typ == "outlier":
            if isinstance(dirty[field], (int, float)):
                dirty[field] = dirty[field] * random.uniform(5, 20)

        # Wrong type
        elif typ == "wrong_type":
            dirty[field] = "INVALID_DATA"

        # Small noise
        elif typ == "small_noise":
            if isinstance(dirty[field], (int, float)):
                dirty[field] = round(dirty[field] + random.uniform(-2, 2), 2)

        # Negative
        elif typ == "negative":
            if isinstance(dirty[field], (int, float)):
                dirty[field] = -abs(dirty[field])

    return dirty

# ---------------------------
# Kafka config & helper
# ---------------------------
KAFKA_TOPIC_DAILY = "smartwatch_daily"
KAFKA_TOPIC_REALTIME = "smartwatch_realtime"
KAFKA_SERVER = "localhost:9092"

def convert_np(obj):
    if isinstance(obj, np.generic):
        return obj.item()
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Cannot convert type: {type(obj)}")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v, default=convert_np).encode("utf-8")
)

# ---------------------------
# send helpers
# ---------------------------
def send_records(topic, records, dirty_rate, excluded_fields, delay_per_record=None):
    sent = 0
    for rec in records:
        payload = apply_noise(rec, dirty_rate=dirty_rate, excluded_fields=excluded_fields)
        producer.send(topic, value=payload)
        print(f"SENT -> {topic}: {payload}")
        sent += 1
        if delay_per_record:
            time.sleep(delay_per_record)
    producer.flush()
    return sent

# ---------------------------
# Mode logic
# ---------------------------
def run_history_mode(devices, start_date, end_date, interval_minutes, dirty_rate, excluded_fields):
    """
    Send historical data quickly (no per-record delay).
    start_date and end_date are datetime.date or datetime (end exclusive).
    """
    print(f"[HISTORY] Sending data from {start_date} to {end_date} (exclusive) ...")
    current = start_date
    total_daily = 0
    total_realtime = 0

    while current < end_date:
        # For each day, generate day data for the day current (i.e., summary refers to that day -> daily summary timestamp = current+1 at 00:00)
        daily, realtime = generate_day_data(devices, current, interval_minutes=interval_minutes)

        # send daily summaries (timestamp at next day 00:00)
        total_daily += send_records(KAFKA_TOPIC_DAILY, daily, dirty_rate, excluded_fields, delay_per_record=None)

        # send realtime records (fast, no delay)
        total_realtime += send_records(KAFKA_TOPIC_REALTIME, realtime, dirty_rate, excluded_fields, delay_per_record=None)

        current = current + timedelta(days=1)

    print(f"[HISTORY] Done. Sent {total_daily} daily and {total_realtime} realtime records.")


def run_realtime_mode(devices, target_date, interval_minutes, dirty_rate, excluded_fields, delay_per_record):
    """
    Simulate a single day's realtime sending. For target_date, realtime runs from 00:00 -> 23:59 of target_date.
    Daily summary should be sent at target_date + 1 at 00:00 (i.e., after day completes). For demo we will:
      - Option A: send realtime throughout the day with delay_per_record (real-time)
      - Option B: optionally send daily summary at the end (here we will send it after replaying day)
    """
    print(f"[REALTIME] Simulating realtime for {target_date} with interval {interval_minutes} minutes and delay {delay_per_record}s/record ...")

    # generate records for THIS day
    daily, realtime = generate_day_data(devices, target_date, interval_minutes=interval_minutes)

    # realtime is ordered by timestamp ascending; send with delay to mimic live stream
    print("[REALTIME] Sending realtime records (with delay)...")
    sent_rt = send_records(KAFKA_TOPIC_REALTIME, realtime, dirty_rate, excluded_fields, delay_per_record=delay_per_record)

    # after whole day simulated, send daily summaries (timestamp next day 00:00)
    print("[REALTIME] Sending daily summaries (end of day)...")
    sent_daily = send_records(KAFKA_TOPIC_DAILY, daily, dirty_rate, excluded_fields, delay_per_record=None)

    print(f"[REALTIME] Done. Sent {sent_rt} realtime and {sent_daily} daily records.")


# ---------------------------
# CLI
# ---------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Smartwatch data sender (history / realtime)")

    parser.add_argument("--num_devices", type=int, required=True)
    parser.add_argument("--num_users", type=int, required=True)
    parser.add_argument("--mode", choices=["history", "realtime"], required=True)

    # history params
    parser.add_argument("--start", type=str, help="Start date inclusive YYYY-MM-DD (for history)")
    parser.add_argument("--end", type=str, help="End date exclusive YYYY-MM-DD (for history)")

    # realtime params
    parser.add_argument("--date", type=str, help="Target date YYYY-MM-DD (for realtime)")
    parser.add_argument("--interval", type=int, default=5, help="Interval minutes for realtime (default 5). Use 1 for per-minute.")
    parser.add_argument("--delay", type=float, default=0.5, help="Delay seconds per record in realtime mode (use small for demo)")

    # noise control
    parser.add_argument("--dirty_rate", type=float, default=0.0, help="Rate of dirtying records (0.0-1.0)")
    parser.add_argument("--exclude", type=str, default="", help="Comma-separated fields not to dirty, e.g., device_id,user_id,timestamp")

    args = parser.parse_args()

    random.seed(1510)

    # parse excludes
    excluded_fields = [f.strip() for f in args.exclude.split(",") if f.strip() != ""]

    # create mapping
    devices = create_device_user_mapping(args.num_devices, args.num_users)
    print("Device <-> User mapping:")
    for d in devices:
        print(f"  Device {d['device_id']} <-> User {d['user_id']}")

    if args.mode == "history":
        if not args.start or not args.end:
            raise ValueError("History mode requires --start and --end")
        start_date = datetime.strptime(args.start, "%Y-%m-%d").date()
        end_date = datetime.strptime(args.end, "%Y-%m-%d").date()
        run_history_mode(devices, start_date, end_date, args.interval, args.dirty_rate, excluded_fields)

    elif args.mode == "realtime":
        if not args.date:
            raise ValueError("Realtime mode requires --date")
        target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
        run_realtime_mode(devices, target_date, args.interval, args.dirty_rate, excluded_fields, args.delay)

    producer.flush()
