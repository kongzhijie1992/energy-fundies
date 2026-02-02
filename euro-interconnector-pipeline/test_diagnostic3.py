import pandas as pd
from datetime import datetime, timedelta

start = pd.Timestamp("2024-01-01", tz="UTC")
end = pd.Timestamp("2024-01-07", tz="UTC")

print("Method 1: date_range with freq='h'")
ts1 = pd.date_range(start, end, freq="h")
print(f"Generated: {len(ts1)} timestamps")

print("\nMethod 2: date_range with freq='H' (capital H)")
ts2 = pd.date_range(start, end, freq="H")
print(f"Generated: {len(ts2)} timestamps")

print("\nMethod 3: date_range with periods")
expected_hours = int((end - start).total_seconds() / 3600) + 1
ts3 = pd.date_range(start, periods=expected_hours, freq="H")
print(f"Generated: {len(ts3)} timestamps")
print(f"Last timestamp: {ts3[-1]}")

print("\nMethod 4: Manual calculation")
hours_diff = (end - start).total_seconds() / 3600
print(f"Hours between start and end: {hours_diff}")
print(f"Expected timestamps (inclusive): {int(hours_diff) + 1}")
