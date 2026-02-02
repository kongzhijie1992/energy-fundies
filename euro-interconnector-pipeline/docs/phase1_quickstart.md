# FTR Pricing Tool - Phase 1 Quickstart Guide

Get started with the FTR (Financial Transmission Rights) pricing tool in minutes.

## Prerequisites

- Python 3.11 or higher
- Poetry (for dependency management)
- Git (optional, for cloning)

## Installation

### 1. Navigate to the Project

```bash
cd euro-interconnector-pipeline
```

### 2. Install Dependencies

```bash
poetry install
```

This will install all required packages including:
- `pandas`, `numpy` - Data manipulation
- `streamlit` - Web interface
- `matplotlib` - Visualization
- `pydantic` - Data validation
- `pyyaml` - Configuration

### 3. Verify Installation

```bash
poetry run pytest tests/ftr/test_synthetic_data.py -v
```

All tests should pass ✅

## Quick Start

### Option 1: Web Interface (Recommended)

Launch the Streamlit web application:

```bash
poetry run streamlit run dashboard/ftr_app.py
```

Your browser will open automatically at `http://localhost:8501`

**Using the Web Interface:**

1. **Configure Settings** (sidebar):
   - Adjust number of scenarios (default: 500)
   - Set block length for bootstrap (default: 7 days)
   - Modify price volatility (default: 15%)

2. **Build Your Contract**:
   - Select **Source Zone** (e.g., FR)
   - Select **Sink Zone** (e.g., DE_LU)
   - Choose **Start Date** and **End Date**
   - Enter **MW Capacity** (e.g., 10 MW)

3. **Price Contract**:
   - Click the **"💰 Price Contract"** button
   - View results instantly

4. **Review Results**:
   - Contract value (expected payoff)
   - Risk metrics (standard deviation, percentiles)
   - Payoff distribution chart
   - Detailed contract information

5. **Export**:
   - Download results as CSV or JSON

### Option 2: Command Line Interface

Price a single contract:

```bash
poetry run fundie ftr price \
  --prices data/synthetic_prices.csv \
  --source FR \
  --sink DE_LU \
  --start 2024-02-01 \
  --end 2024-03-01 \
  --mw 10
```

**First, generate synthetic price data:**

```python
from fundie.ftr.data.io import read_synthetic_prices, save_prices
from pathlib import Path

# Generate prices for FR and DE_LU
prices = read_synthetic_prices(
    zones=["FR", "DE_LU"],
    start_date="2023-01-01",
    end_date="2024-12-31",
    volatility=0.15,
    seed=42,
)

# Save to file
save_prices(prices, Path("data/synthetic_prices.csv"))
```

### Option 3: Python API

```python
import pandas as pd
from fundie.ftr.core.types import ContractSpec
from fundie.ftr.pricing.engine import price_contract
from fundie.ftr.config.settings import FTRSettings
from fundie.ftr.data.io import read_synthetic_prices

# Generate synthetic prices
prices_df = read_synthetic_prices(
    zones=["FR", "DE_LU"],
    start_date="2023-01-01",
    end_date="2024-12-31",
    seed=42,
)

# Define contract
spec = ContractSpec.from_dict({
    "source": "FR",
    "sink": "DE_LU",
    "start_utc": "2024-02-01T00:00:00Z",
    "end_utc": "2024-03-01T00:00:00Z",
    "mw": 10.0,
    "contract_type": "obligation",
})

# Price contract
settings = FTRSettings(n_scenarios=500, seed=42)
result = price_contract(spec, prices_df, curve=None, settings=settings)

# View results
print(f"Contract Value: €{result.price:,.0f}")
print(f"Std Dev: €{result.stdev_payoff:,.0f}")
print(f"5th Percentile: €{result.p5_payoff:,.0f}")
print(f"95th Percentile: €{result.p95_payoff:,.0f}")
```

## Understanding the Results

### Key Metrics

- **Contract Value**: Expected payoff (mean of all scenarios)
  - This is the fair value of the FTR contract
  - Represents average profit/loss across all simulated scenarios

- **Standard Deviation**: Risk measure
  - Higher = more volatile/risky contract
  - Lower = more stable/predictable payoff

- **5th Percentile**: Worst-case scenario
  - 95% of scenarios will be better than this
  - Useful for risk management

- **95th Percentile**: Best-case scenario
  - Only 5% of scenarios will be better than this
  - Represents upside potential

### Interpreting the Distribution Chart

The histogram shows the distribution of contract payoffs across all scenarios:
- **Peak**: Most likely payoff range
- **Spread**: Wider = higher risk, Narrower = lower risk
- **Skewness**: Asymmetry indicates directional bias

## Synthetic Price Data

Since historical day-ahead market prices are not available, the tool generates **synthetic price data** with realistic characteristics:

### Features

✅ **Seasonal Patterns**
- Winter premium (+20%): December-February
- Summer discount (-15%): June-August
- Shoulder months: Baseline

✅ **Hourly Patterns**
- Peak hours (8am-8pm): +30%
- Off-peak hours: -20%

✅ **Weekend Effects**
- Weekend discount: -10%

✅ **Volatility Clustering**
- GARCH(1,1)-inspired volatility
- Realistic price spikes (5% of hours)

✅ **Inter-Zonal Correlation**
- Correlated prices between connected zones
- Higher correlation for geographically close zones

### Calibration

Default base prices (EUR/MWh):
- FR: €50
- DE_LU: €45
- GB: €60
- ES: €48
- IT: €55
- NL: €47
- BE: €49

You can customize these in the code or via API parameters.

## Available Zones

The tool supports all European zones configured in `config/zones.yml`:

**Major Zones:**
- FR (France)
- DE_LU (Germany-Luxembourg)
- GB (Great Britain)
- ES (Spain)
- IT (Italy)
- NL (Netherlands)
- BE (Belgium)
- CH (Switzerland)
- AT (Austria)
- PL (Poland)

**Nordic Zones:**
- NO_1, NO_2, NO_3, NO_4, NO_5 (Norway)
- SE_1, SE_2, SE_3, SE_4 (Sweden)
- DK_1, DK_2 (Denmark)
- FI (Finland)

And many more! See `config/zones.yml` for the complete list.

## Troubleshooting

### Issue: "No zones found in configuration"

**Solution**: Ensure `config/zones.yml` exists and is properly formatted.

```bash
ls config/zones.yml
```

### Issue: "Module not found" errors

**Solution**: Reinstall dependencies:

```bash
poetry install
```

### Issue: Streamlit app won't start

**Solution**: Check if port 8501 is available:

```bash
poetry run streamlit run dashboard/ftr_app.py --server.port 8502
```

### Issue: Tests failing

**Solution**: Ensure you're in the correct directory:

```bash
cd euro-interconnector-pipeline
poetry run pytest tests/ftr/ -v
```

## Next Steps

### Phase 2: Enhanced Visualizations (Coming Soon)

- Interactive Plotly charts
- Historical spread analysis
- Portfolio management basics
- Excel export with formatting

### Phase 3: Advanced Analytics (Coming Soon)

- Portfolio optimization
- Mean-variance analysis
- Backtesting framework
- API endpoints

### Phase 4: Production Readiness (Coming Soon)

- Comprehensive documentation
- Performance optimization
- Deployment guides
- Example notebooks

## Configuration

### Pricing Model Settings

Edit settings via the web interface sidebar or programmatically:

```python
from fundie.ftr.config.settings import FTRSettings

settings = FTRSettings(
    n_scenarios=1000,        # More scenarios = more accurate
    block_length_days=7,     # Bootstrap block size
    seed=42,                 # For reproducibility
    missing_hour_policy="drop",  # How to handle missing data
)
```

### Synthetic Data Settings

Customize price generation:

```python
from fundie.ftr.data.io import read_synthetic_prices

prices = read_synthetic_prices(
    zones=["FR", "DE_LU"],
    start_date="2024-01-01",
    end_date="2024-12-31",
    base_prices={"FR": 55.0, "DE_LU": 48.0},  # Custom base prices
    volatility=0.20,  # Higher volatility
    seed=123,
)
```

## Support

For questions, issues, or feature requests:

1. Check the main [README.md](../README.md)
2. Review the [implementation plan](../docs/implementation_plan.md)
3. Run tests to verify setup: `poetry run pytest tests/ftr/ -v`

## Example Workflow

Here's a complete example workflow:

```python
from fundie.ftr.data.io import read_synthetic_prices
from fundie.ftr.core.types import ContractSpec
from fundie.ftr.pricing.engine import price_contract
from fundie.ftr.config.settings import FTRSettings

# 1. Generate synthetic prices
prices = read_synthetic_prices(
    zones=["FR", "GB"],
    start_date="2023-01-01",
    end_date="2024-12-31",
    volatility=0.15,
    seed=42,
)

# 2. Define contract
contract = ContractSpec.from_dict({
    "source": "FR",
    "sink": "GB",
    "start_utc": "2024-06-01T00:00:00Z",
    "end_utc": "2024-09-01T00:00:00Z",
    "mw": 50.0,
    "contract_type": "obligation",
})

# 3. Configure pricing
settings = FTRSettings(
    n_scenarios=1000,
    block_length_days=7,
    seed=42,
)

# 4. Price contract
result = price_contract(contract, prices, curve=None, settings=settings)

# 5. Display results
print(f"""
FTR Contract Pricing Results
============================
Route: {contract.source} → {contract.sink}
Capacity: {contract.mw} MW
Period: {contract.start_utc.date()} to {contract.end_utc.date()}

Contract Value: €{result.price:,.2f}
Standard Deviation: €{result.stdev_payoff:,.2f}
5th Percentile: €{result.p5_payoff:,.2f}
95th Percentile: €{result.p95_payoff:,.2f}

Scenarios: {result.n_scenarios}
Model: {result.model}
""")
```

---

**Ready to get started?** Launch the web app:

```bash
poetry run streamlit run dashboard/ftr_app.py
```

🎉 Happy pricing!
