# smartwatch_gen.py
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


# ------------------------------------------------
# Generate daily summary for a *completed* day
# Timestamp will be the NEXT day at 00:00 (i.e., when system sends summary)
# E.g., for day 2024-01-20 summary, timestamp = 2024-01-21T00:00:00
# ------------------------------------------------
def generate_daily_record(device_id, user_id, date_of_day):
    """
    date_of_day: datetime.date object representing the day being summarized
    Returns daily summary (timestamp will be date_of_day + 1 at 00:00)
    """
    # baseline random seeds for variability
    # daily steps typical 3000 - 15000
    daily_steps = random.randint(3000, 15000)

    # SpO2 typical 92 - 100
    SpO2 = random.randint(92, 100)

    # sleep hours and deep sleep
    sleep_hours = round(random.uniform(4.5, 9.5), 2)
    deep_sleep_hours = round(min(sleep_hours, random.uniform(0.5, 4.0)), 2)

    # calories: base + steps * factor (approx)
    calories_burned = round(random.uniform(1400, 2000) + daily_steps * 0.04, 2)

    standing_hours = round(random.uniform(2.0, 14.0), 2)
    run_distance = round(random.uniform(0.0, min(15.0, daily_steps/1000.0)), 2)  # km approx

    # timestamp set to next day at 00:00 (when device reports previous day)
    ts = datetime(year=date_of_day.year, month=date_of_day.month, day=date_of_day.day) + timedelta(days=1)

    return {
        "device_id": device_id,
        "user_id": user_id,
        "timestamp": ts,
        "daily_steps": daily_steps,
        "SpO2": SpO2,
        "sleep_hours": sleep_hours,
        "deep_sleep_hours": deep_sleep_hours,
        "calories_burned": calories_burned,
        "standing_hours": standing_hours,
        "run_distance": run_distance
    }


# ------------------------------------------------
# Realtime record: stress_level (1-3), heart_rate (bpm),
# and cumulative steps_so_far for that day (to ensure consistency)
# ------------------------------------------------
def generate_realtime_record(device_id, user_id, timestamp, steps_so_far):
    # stress level 1-3
    stress_level = random.randint(1, 3)

    # heart rate typical 50 - 140, can be noisy
    heart_rate = random.randint(50, 140)

    return {
        "device_id": device_id,
        "user_id": user_id,
        "timestamp": timestamp,
        "stress_level": stress_level,
        "heart_rate": heart_rate,
        # cumulative steps from midnight to this timestamp
        "steps_so_far": int(steps_so_far),
    }


# ------------------------------------------------
# Generate full-day data for all devices:
# - daily_summaries: one per device (timestamp = next day 00:00)
# - realtime_records: list of records through the day at given interval
# Implementation detail:
#   We first sample a target daily_steps for each device, then
#   simulate per-interval step increments that sum up to daily_steps.
# ------------------------------------------------
def generate_day_data(devices, day_date, interval_minutes=5):
    """
    devices: list of {"device_id","user_id"}
    day_date: datetime.date or datetime (we'll use its date part)
    interval_minutes: sampling interval for realtime (default 5)
    Returns: (daily_summaries, realtime_records)
      - daily_summaries: list of daily summary dicts (timestamp = day+1 at 00:00)
      - realtime_records: list of realtime dicts for all devices ordered by timestamp
    """
    # ensure day_date is date object
    if isinstance(day_date, datetime):
        day = day_date.date()
    else:
        day = day_date

    # generate a target daily summary for each device
    device_daily = {}
    for d in devices:
        # create daily summary record (will contain daily_steps)
        rec = generate_daily_record(d["device_id"], d["user_id"], day)
        device_daily[d["device_id"]] = rec

    # simulate realtime increments so that cumulative at end == daily_steps
    realtime = []
    # timeline from 00:00 to 23:59 inclusive
    start_dt = datetime(day.year, day.month, day.day, 0, 0, 0)
    # use end at 23:59 (so last interval's timestamp <= 23:59)
    end_dt = datetime(day.year, day.month, day.day, 23, 59, 0)
    delta = timedelta(minutes=interval_minutes)

    # For each device, create a schedule of increments across N intervals
    # We'll generate non-negative integer increments summing to daily_steps
    # Strategy: For each device, create N random weights then scale to daily_steps,
    # then round and adjust to ensure sum equals daily_steps (distribute remainder).
    timestamps = []
    t = start_dt
    while t <= end_dt:
        timestamps.append(t)
        t += delta

    n_intervals = len(timestamps)
    for d in devices:
        dev_id = d["device_id"]
        target_steps = device_daily[dev_id]["daily_steps"]

        if target_steps <= 0 or n_intervals == 0:
            increments = [0] * n_intervals
        else:
            # random positive weights
            weights = [random.random() for _ in range(n_intervals)]
            total_w = sum(weights)
            # convert to integer increments
            raw_incs = [max(0, int(round(w / total_w * target_steps))) for w in weights]

            # adjust remainder to exactly match target_steps
            current_sum = sum(raw_incs)
            diff = target_steps - current_sum
            idx = 0
            # distribute remainder (+/-)
            while diff != 0:
                if diff > 0:
                    raw_incs[idx % n_intervals] += 1
                    diff -= 1
                else:
                    # remove 1 from a non-zero slot if possible
                    j = idx % n_intervals
                    if raw_incs[j] > 0:
                        raw_incs[j] -= 1
                        diff += 1
                idx += 1
            increments = raw_incs

        # now build cumulative realtime records for this device
        cumulative = 0
        for ts, inc in zip(timestamps, increments):
            cumulative += inc
            realtime.append(
                generate_realtime_record(dev_id, d["user_id"], ts, cumulative)
            )

        # final check: cumulative must equal daily_steps
        if cumulative != device_daily[dev_id]["daily_steps"]:
            # as a safety: force match by adjusting last realtime
            # find last record for this device and set steps_so_far to daily_steps
            # (shouldn't normally happen due to algorithm)
            # We'll modify the last appended record for this dev
            for i in range(len(realtime)-1, -1, -1):
                if realtime[i]["device_id"] == dev_id:
                    realtime[i]["steps_so_far"] = device_daily[dev_id]["daily_steps"]
                    break

    # sort realtime by timestamp (interleaved devices)
    realtime.sort(key=lambda r: (r["timestamp"], r["device_id"]))

    # collect daily_summaries as list
    daily_summaries = [device_daily[d["device_id"]] for d in devices]

    return daily_summaries, realtime
