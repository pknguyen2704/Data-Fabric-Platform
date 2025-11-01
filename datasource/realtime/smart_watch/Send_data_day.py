# smartwatch_send.py
import json
import random
import numpy as np
from kafka import KafkaProducer
from datetime import datetime, timedelta

from Gen_data import (
    create_device_user_mapping,
    generate_day_data
)

KAFKA_TOPIC_DAILY = "smartwatch_daily"
KAFKA_TOPIC_REALTIME = "smartwatch_realtime"
KAFKA_SERVER = "localhost:9092"

random.seed(1510)

def convert_np(obj):
    if isinstance(obj, np.generic):
        return obj.item()
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Không thể chuyển kiểu: {type(obj)}")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v, default=convert_np).encode("utf-8")
)

# --- INPUT ---
num_devices = int(input("Nhập số lượng thiết bị: "))
num_users = int(input("Nhập số lượng người dùng: "))
date_str = input("Nhập ngày cần sinh dữ liệu (yyyy-mm-dd): ")

try:
    target_date = datetime.strptime(date_str, "%Y-%m-%d")

    devices = create_device_user_mapping(num_devices, num_users)

    print("\n📌 Mapping User – Device (1–1):")
    for d in devices:
        print(f"  Device {d['device_id']} ↔ User {d['user_id']}")

    print("\n⏳ Đang sinh dữ liệu smartwatch...")

    data_daily, data_realtime = generate_day_data(devices, target_date)

    # ✅ Gửi daily
    for rec in data_daily:
        producer.send(KAFKA_TOPIC_DAILY, rec)
        print("DAILY →", rec)
    producer.flush()

    # ✅ Gửi realtime
    for rec in data_realtime:
        producer.send(KAFKA_TOPIC_REALTIME, rec)
        print("REALTIME →", rec)
    producer.flush()

    print(f"\n✅ Đã gửi {len(data_daily)} bản ghi daily và {len(data_realtime)} bản ghi realtime.")

except Exception as e:
    print("❌ Lỗi:", e)

finally:
    producer.flush()
