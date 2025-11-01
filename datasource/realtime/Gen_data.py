import random
import numpy as np
from datetime import datetime


def create_device_user_mapping(num_devices, num_users):
    """
    Mapping 1-1 giữa device và user:
    - Một user chỉ có 1 device
    - Một device chỉ thuộc 1 user
    """

    if num_devices != num_users:
        raise ValueError("❌ Số lượng thiết bị và người dùng phải bằng nhau để đảm bảo mapping 1-1.")

    users = list(range(1, num_users + 1))
    random.shuffle(users)  # xáo trộn user để tránh trùng pattern

    devices = []
    for device_id, user_id in zip(range(1, num_devices + 1), users):
        devices.append({
            "device_id": device_id,
            "user_id": user_id
        })

    return devices


# --- Sinh record ---
def generate_environment_record(device_id, user_id, timestamp):
    return {
        "device_id": device_id,
        "user_id": user_id,
        "timestamp": timestamp,

        "temperature": round(random.uniform(20.0, 35.0), 2),
        "humidity": round(random.uniform(40.0, 85.0), 2),
        "air_quality_index": random.randint(1, 5),
    }


# --- Sinh data cho tất cả device ---
def generate_environment_data(devices, timestamp, null_prob=0.05):

    data = []

    for dev in devices:
        record = generate_environment_record(
            device_id=dev["device_id"],
            user_id=dev["user_id"],
            timestamp=timestamp
        )

        # Random trường NULL
        for field in ["temperature", "humidity", "air_quality_index"]:
            if random.random() < null_prob:
                record[field] = None

        data.append(record)

    return data
