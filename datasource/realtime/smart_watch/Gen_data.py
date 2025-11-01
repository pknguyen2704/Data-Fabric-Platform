# smartwatch_gen.py
import random
import numpy as np
from datetime import datetime, timedelta


# --- Mapping 1-1 ---
def create_device_user_mapping(num_devices, num_users):
    if num_devices != num_users:
        raise ValueError("❌ Số lượng thiết bị và người dùng phải BẰNG NHAU (mapping 1-1).")

    users = list(range(1, num_users + 1))
    random.shuffle(users)

    devices = []
    for device_id, user_id in zip(range(1, num_devices + 1), users):
        devices.append({
            "device_id": device_id,
            "user_id": user_id
        })

    return devices


# ✅ DAILY RECORD (05:00)
def generate_daily_record(device_id, user_id, timestamp):
    return {
        "device_id": device_id,
        "user_id": user_id,
        "timestamp": timestamp,

        "daily_steps": random.randint(3000, 15000),
        "SpO2": random.randint(92, 100),
        "sleep_hours": round(random.uniform(5.0, 9.0), 2),
        "deep_sleep_hours": round(random.uniform(1.0, 3.5), 2),
        "calories_burned": round(random.uniform(1500, 3500), 2),
        "standing_hours": round(random.uniform(4.0, 12.0), 2),
        "run_distance": round(random.uniform(0.5, 10.0), 2)
    }


# ✅ REALTIME RECORD (5 minutes)
def generate_realtime_record(device_id, user_id, timestamp):
    return {
        "device_id": device_id,
        "user_id": user_id,
        "timestamp": timestamp,
        "stress_level": random.randint(1, 3),
        "heart_rate": random.randint(55, 130),
    }


# ✅ Generate full-day data
def generate_day_data(devices, timestamp, interval_minutes=5):
    data_daily = []
    data_realtime = []

    # --- Daily data tại 05:00 ---
    daily_time = timestamp.replace(hour=5, minute=0, second=0, microsecond=0)

    for d in devices:
        data_daily.append(
            generate_daily_record(d["device_id"], d["user_id"], daily_time)
        )

    # --- Realtime data 5 phút một lần ---
    current_time = timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
    end_time = current_time.replace(hour=23, minute=59)

    delta = timedelta(minutes=interval_minutes)

    while current_time <= end_time:
        for d in devices:
            data_realtime.append(
                generate_realtime_record(d["device_id"], d["user_id"], current_time)
            )
        current_time += delta

    return data_daily, data_realtime
