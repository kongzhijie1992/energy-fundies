import pandas as pd
from fundie.ftr.core.time import parse_datetime_utc

start = parse_datetime_utc("2024-01-01")
end = parse_datetime_utc("2024-01-07")

print(f"Start: {start}")
print(f"End: {end}")

timestamps = pd.date_range(start, end, freq="h", tz="UTC")
print(f"\nGenerated {len(timestamps)} timestamps")
print(f"Expected: {7*24+1} timestamps")
print(f"\nFirst: {timestamps[0]}")
print(f"Last: {timestamps[-1]}")

# Check what 7 days should be
print(f"\n7 days * 24 hours = {7*24} hours")
print(f"Plus 1 for inclusive end = {7*24+1} hours")
