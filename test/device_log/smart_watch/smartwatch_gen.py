import random
from datetime import datetime, timedelta

# ----------------------------
# Mapping 1-1 device <-> user
# ----------------------------
def create_device_user_mapping(num_devices, num_users):
    if num_devices != num_users:
        raise ValueError("Số lượng thiết bị và người dùng phải bằng nhau (mapping 1-1).")
    users = list(range(1, num_users + 1))
    random.shuffle(users)
    devices = []
    for device_id, user_id in zip(range(1, num_devices + 1), users):
        devices.append({"device_id": device_id, "user_id": user_id})
    return devices

# ----------------------------
# Realtime record generator (cumulative)
# ----------------------------
def generate_realtime_record(device_id, user_id, timestamp, cumulative):
    """
    cumulative: dict chứa các giá trị tổng tới thời điểm timestamp
    """
    return {
        "device_id": device_id,
        "user_id": user_id,
        "created_at": timestamp,
        "daily_steps": int(cumulative["daily_steps"]),
        "SpO2": int(cumulative["SpO2"]),
        "sleep_hours": round(cumulative["sleep_hours"], 2),
        "deep_sleep_hours": round(cumulative["deep_sleep_hours"], 2),
        "calories_burned": round(cumulative["calories_burned"], 2),
        "standing_hours": round(cumulative["standing_hours"], 2),
        "run_distance": round(cumulative["run_distance"], 2),
        "stress_level": cumulative["stress_level"],
        "heart_rate": cumulative["heart_rate"]
    }

# ----------------------------
# Generate full-day realtime data
# ----------------------------
def generate_day_data(devices, day_date, interval_minutes=5):
    """
    Trả về danh sách realtime records (ordered) cho 1 ngày.
    cumulative fields được tính từ đầu ngày đến thời điểm hiện tại.
    """
    if isinstance(day_date, datetime):
        day = day_date.date()
    else:
        day = day_date

    # timeline
    start_dt = datetime(day.year, day.month, day.day, 0, 0, 0)
    end_dt = datetime(day.year, day.month, day.day, 23, 59, 0)
    delta = timedelta(minutes=interval_minutes)
    timestamps = []
    t = start_dt
    while t <= end_dt:
        timestamps.append(t)
        t += delta
    n_intervals = len(timestamps)

    all_records = []

    for d in devices:
        dev_id = d["device_id"]
        user_id = d["user_id"]

        # Tạo tổng cho cả ngày
        total_steps = random.randint(3000, 15000)
        total_SpO2 = random.randint(92, 100)
        total_sleep = round(random.uniform(4.5, 9.5), 2)
        total_deep_sleep = round(min(total_sleep, random.uniform(0.5, 4.0)), 2)
        total_calories = round(random.uniform(1400, 2000) + total_steps * 0.04, 2)
        total_standing = round(random.uniform(2.0, 14.0), 2)
        total_run = round(random.uniform(0.0, min(15.0, total_steps / 1000.0)), 2)

        # Chia increments theo timeline
        # tạo weights ngẫu nhiên, scale đến tổng
        weights = [random.random() for _ in range(n_intervals)]
        total_w = sum(weights)

        steps_incs = [round(w / total_w * total_steps) for w in weights]
        sleep_incs = [round(w / total_w * total_sleep, 2) for w in weights]
        deep_sleep_incs = [round(w / total_w * total_deep_sleep, 2) for w in weights]
        calories_incs = [round(w / total_w * total_calories, 2) for w in weights]
        standing_incs = [round(w / total_w * total_standing, 2) for w in weights]
        run_incs = [round(w / total_w * total_run, 2) for w in weights]

        cumulative = {
            "daily_steps": 0,
            "SpO2": total_SpO2,
            "sleep_hours": 0.0,
            "deep_sleep_hours": 0.0,
            "calories_burned": 0.0,
            "standing_hours": 0.0,
            "run_distance": 0.0,
            "stress_level": 1,
            "heart_rate": 70
        }

        for i, ts in enumerate(timestamps):
            cumulative["daily_steps"] += steps_incs[i]
            cumulative["sleep_hours"] += sleep_incs[i]
            cumulative["deep_sleep_hours"] += deep_sleep_incs[i]
            cumulative["calories_burned"] += calories_incs[i]
            cumulative["standing_hours"] += standing_incs[i]
            cumulative["run_distance"] += run_incs[i]
            cumulative["stress_level"] = random.randint(1, 3)
            cumulative["heart_rate"] = random.randint(50, 140)

            record = generate_realtime_record(dev_id, user_id, ts, cumulative)
            all_records.append(record)

        # Force đảm bảo cuối ngày = tổng cả ngày chính xác
        final = all_records[-1]
        final["daily_steps"] = total_steps
        final["SpO2"] = total_SpO2
        final["sleep_hours"] = total_sleep
        final["deep_sleep_hours"] = total_deep_sleep
        final["calories_burned"] = total_calories
        final["standing_hours"] = total_standing
        final["run_distance"] = total_run

    # sort theo timestamp, device_id
    all_records.sort(key=lambda r: (r["created_at"], r["device_id"]))
    return all_records

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
