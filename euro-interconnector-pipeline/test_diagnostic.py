from fundie.ftr.data.synthetic import generate_zone_prices
import pandas as pd

prices = generate_zone_prices('FR', '2024-01-01', '2024-01-07', seed=42)
print(f'Length: {len(prices)}')
print(f'Expected: {7*24+1}')
print(f'Start: {prices["timestamp_utc"].min()}')
print(f'End: {prices["timestamp_utc"].max()}')
print(f'\nFirst few rows:')
print(prices.head())
print(f'\nLast few rows:')
print(prices.tail())
