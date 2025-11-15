# Gen_data.py
import random
import numpy as np
from datetime import datetime


# ==========================
# 1. Mapping device ↔ user
# ==========================
def create_device_user_mapping(num_devices, num_users):
    """
    Mapping 1-1 giữa device và user:
    - Một user chỉ có 1 device
    - Một device chỉ thuộc 1 user
    """
    if num_devices != num_users:
        raise ValueError("❌ Số lượng thiết bị và người dùng phải bằng nhau để đảm bảo mapping 1-1.")

    users = list(range(1, num_users + 1))
    random.shuffle(users)

    devices = []
    for device_id, user_id in zip(range(1, num_devices + 1), users):
        devices.append({
            "device_id": device_id,
            "user_id": user_id
        })

    return devices


# ==========================
# 2. Sinh record môi trường
# ==========================
def generate_environment_record(device_id, user_id, timestamp):
    return {
        "device_id": device_id,
        "user_id": user_id,
        "temperature": round(random.uniform(20.0, 35.0), 2),
        "humidity": round(random.uniform(40.0, 85.0), 2),
        "air_quality_index": random.randint(1, 5),
        "created_at": timestamp  # đặt cuối cùng, Iceberg sẽ đọc ok với định dạng sửa trên
    }


# ===========================================
# 3. Sinh data cho tất cả device tại 1 time
# ===========================================
def generate_environment_data(devices, timestamp):
    data = []
    for dev in devices:
        record = generate_environment_record(
            device_id=dev["device_id"],
            user_id=dev["user_id"],
            timestamp=timestamp
        )
        data.append(record)
    return data


# ==========================
# 4. Hàm làm bẩn dữ liệu
# ==========================
def apply_noise(record, dirty_rate=0.1, excluded_fields=None):
    """
    Làm bẩn 1 record theo dirty_rate.
    excluded_fields: list các field KHÔNG được làm bẩn.
    """
    if excluded_fields is None:
        excluded_fields = []

    # Không trúng tỉ lệ → không làm bẩn
    if random.random() > dirty_rate:
        return record

    dirty_record = record.copy()

    noise_types = ["missing", "outlier", "wrong_type", "small_noise", "negative"]

    for field in dirty_record.keys():

        if field in excluded_fields:
            continue

        if field == "created_at":
            continue

        noise = random.choice(noise_types)

        # 1) Missing
        if noise == "missing":
            dirty_record[field] = None

        # 2) Outlier giá trị
        elif noise == "outlier":
            if isinstance(dirty_record[field], (int, float)):
                dirty_record[field] = dirty_record[field] * random.uniform(5, 20)

        # 3) Sai kiểu dữ liệu
        elif noise == "wrong_type":
            # Chỉ áp dụng cho các cột kiểu string
            if isinstance(dirty_record[field], str):
                dirty_record[field] = "INVALID_DATA"
            # Nếu là số hoặc timestamp → bỏ qua
            else:
                continue

        # 4) Noise nhẹ
        elif noise == "small_noise":
            if isinstance(dirty_record[field], (int, float)):
                dirty_record[field] += random.uniform(-2, 2)

        # 5) Giá trị âm
        elif noise == "negative":
            if isinstance(dirty_record[field], (int, float)):
                dirty_record[field] *= -1

    return dirty_record
