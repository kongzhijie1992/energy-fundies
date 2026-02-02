"""Streamlit web application for FTR pricing.

Interactive interface for pricing Financial Transmission Rights contracts
with synthetic price data generation and visualization.
"""

import logging
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
import numpy as np

# Add src to path for imports
import sys
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from fundie.ftr.core.types import ContractSpec
from fundie.ftr.pricing.engine import price_contract
from fundie.ftr.config.settings import FTRSettings
from fundie.ftr.data.io import (
    get_available_zones,
    read_synthetic_prices,
    validate_price_data,
    save_prices,
)
from fundie.ftr.reporting.tables import valuation_to_frame

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# Page configuration
st.set_page_config(
    page_title="FTR Pricing Tool",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1.2rem;
        color: #666;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    .warning-box {
        background-color: #fff3cd;
        border-left: 4px solid #ffc107;
        padding: 1rem;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)


def init_session_state():
    """Initialize session state variables."""
    if "pricing_result" not in st.session_state:
        st.session_state.pricing_result = None
    if "prices_df" not in st.session_state:
        st.session_state.prices_df = None
    if "contract_spec" not in st.session_state:
        st.session_state.contract_spec = None


def render_header():
    """Render the application header."""
    st.markdown('<div class="main-header">⚡ FTR Pricing Tool</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="sub-header">Price Financial Transmission Rights contracts with synthetic market data</div>',
        unsafe_allow_html=True,
    )


def render_sidebar():
    """Render sidebar with settings and information."""
    with st.sidebar:
        st.header("⚙️ Settings")
        
        # Pricing model settings
        st.subheader("Pricing Model")
        n_scenarios = st.number_input(
            "Number of Scenarios",
            min_value=100,
            max_value=5000,
            value=500,
            step=100,
            help="Number of bootstrap scenarios for pricing",
        )
        
        block_length = st.number_input(
            "Block Length (days)",
            min_value=1,
            max_value=30,
            value=7,
            help="Block length for block bootstrap",
        )
        
        seed = st.number_input(
            "Random Seed",
            min_value=0,
            max_value=9999,
            value=42,
            help="Seed for reproducibility",
        )
        
        # Price generation settings
        st.subheader("Synthetic Data")
        volatility = st.slider(
            "Price Volatility",
            min_value=0.05,
            max_value=0.50,
            value=0.15,
            step=0.05,
            help="Base volatility level (15% = typical)",
        )
        
        # Info section
        st.divider()
        st.subheader("ℹ️ About")
        st.info(
            """
            This tool prices FTR contracts using:
            - **Synthetic price data** with realistic market patterns
            - **Historical simulation** pricing model
            - **Bootstrap scenarios** for risk assessment
            
            **Note**: Using synthetic data. Replace with real market prices when available.
            """
        )
        
        return {
            "n_scenarios": n_scenarios,
            "block_length": block_length,
            "seed": seed,
            "volatility": volatility,
        }


def render_contract_builder(zones: list[str], settings: dict):
    """Render the contract builder form."""
    st.header("📝 Contract Builder")
    
    col1, col2 = st.columns(2)
    
    with col1:
        source_zone = st.selectbox(
            "Source Zone",
            options=zones,
            index=zones.index("FR") if "FR" in zones else 0,
            help="Zone where power is sourced from",
        )
        
        start_date = st.date_input(
            "Start Date",
            value=date.today() + timedelta(days=30),
            min_value=date.today(),
            max_value=date.today() + timedelta(days=730),
            help="Contract start date (inclusive)",
        )
        
        mw_capacity = st.number_input(
            "MW Capacity",
            min_value=0.1,
            max_value=1000.0,
            value=10.0,
            step=1.0,
            help="Contract capacity in MW",
        )
    
    with col2:
        # Filter out source zone from sink options
        sink_options = [z for z in zones if z != source_zone]
        sink_zone = st.selectbox(
            "Sink Zone",
            options=sink_options,
            index=sink_options.index("DE_LU") if "DE_LU" in sink_options else 0,
            help="Zone where power is delivered to",
        )
        
        end_date = st.date_input(
            "End Date",
            value=date.today() + timedelta(days=60),
            min_value=start_date,
            max_value=date.today() + timedelta(days=730),
            help="Contract end date (inclusive)",
        )
    
    # Validation
    if start_date >= end_date:
        st.error("❌ End date must be after start date")
        return None
    
    contract_days = (end_date - start_date).days
    if contract_days > 365:
        st.warning(f"⚠️ Long contract period: {contract_days} days")
    
    # Price button
    col1, col2, col3 = st.columns([1, 1, 2])
    with col1:
        price_button = st.button("💰 Price Contract", type="primary", use_container_width=True)
    
    if price_button:
        return {
            "source": source_zone,
            "sink": sink_zone,
            "start_utc": pd.Timestamp(start_date, tz="UTC"),
            "end_utc": pd.Timestamp(end_date, tz="UTC") + pd.Timedelta(hours=23),
            "mw": mw_capacity,
            "contract_type": "obligation",
        }
    
    return None


def generate_prices_for_contract(spec_dict: dict, volatility: float, seed: int) -> pd.DataFrame:
    """Generate synthetic prices for the contract zones and period."""
    zones = [spec_dict["source"], spec_dict["sink"]]
    
    # Add some buffer before contract start for historical data
    start_date = spec_dict["start_utc"] - pd.Timedelta(days=365)
    end_date = spec_dict["end_utc"]
    
    with st.spinner("Generating synthetic price data..."):
        prices_df = read_synthetic_prices(
            zones=zones,
            start_date=start_date,
            end_date=end_date,
            volatility=volatility,
            seed=seed,
        )
    
    # Validate
    is_valid, warnings = validate_price_data(prices_df)
    if not is_valid:
        st.warning(f"⚠️ Price data validation warnings: {'; '.join(warnings)}")
    
    return prices_df


def price_ftr_contract(spec_dict: dict, prices_df: pd.DataFrame, settings: dict):
    """Price the FTR contract."""
    spec = ContractSpec.from_dict(spec_dict)
    
    ftr_settings = FTRSettings(
        n_scenarios=settings["n_scenarios"],
        block_length_days=settings["block_length"],
        seed=settings["seed"],
    )
    
    with st.spinner("Pricing contract..."):
        result = price_contract(
            contract_spec=spec,
            prices_df=prices_df,
            curve=None,
            model="hs",
            settings=ftr_settings,
        )
    
    return result


def render_results(result, spec_dict: dict):
    """Render pricing results."""
    st.header("📊 Pricing Results")
    
    # Key metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Contract Value",
            value=f"€{result.price:,.0f}",
            help="Expected payoff (mean of scenarios)",
        )
    
    with col2:
        st.metric(
            label="Standard Deviation",
            value=f"€{result.stdev_payoff:,.0f}",
            help="Risk measure (volatility of payoff)",
        )
    
    with col3:
        st.metric(
            label="5th Percentile",
            value=f"€{result.p5_payoff:,.0f}",
            help="Worst-case scenario (5% probability)",
        )
    
    with col4:
        st.metric(
            label="95th Percentile",
            value=f"€{result.p95_payoff:,.0f}",
            help="Best-case scenario (95% probability)",
        )
    
    # Contract details
    st.subheader("Contract Details")
    details_col1, details_col2 = st.columns(2)
    
    with details_col1:
        st.write(f"**Source Zone:** {spec_dict['source']}")
        st.write(f"**Sink Zone:** {spec_dict['sink']}")
        st.write(f"**Capacity:** {spec_dict['mw']:.1f} MW")
    
    with details_col2:
        st.write(f"**Start Date:** {spec_dict['start_utc'].date()}")
        st.write(f"**End Date:** {spec_dict['end_utc'].date()}")
        st.write(f"**Scenarios:** {result.n_scenarios}")
    
    # Additional metrics
    with st.expander("📈 Additional Metrics"):
        col1, col2 = st.columns(2)
        with col1:
            st.write(f"**Mean Spread:** €{result.metadata['spread_mean']:.2f}/MWh")
            st.write(f"**Curve Mean:** €{result.metadata['curve_mean']:.2f}/MWh")
        with col2:
            st.write(f"**Contract Hours:** {result.metadata['hours']}")
            st.write(f"**Data Version:** {result.data_version[:12]}...")


def render_payoff_distribution(result):
    """Render payoff distribution chart."""
    st.subheader("Payoff Distribution")
    
    # Generate scenario payoffs for visualization
    # Note: In production, we'd store these from the pricing engine
    # For now, approximate using normal distribution
    np.random.seed(42)
    scenarios = np.random.normal(
        loc=result.mean_payoff,
        scale=result.stdev_payoff,
        size=result.n_scenarios,
    )
    
    fig, ax = plt.subplots(figsize=(10, 5))
    
    # Histogram
    ax.hist(scenarios, bins=50, alpha=0.7, color='#1f77b4', edgecolor='black')
    
    # Add vertical lines for key metrics
    ax.axvline(result.mean_payoff, color='red', linestyle='--', linewidth=2, label=f'Mean: €{result.mean_payoff:,.0f}')
    ax.axvline(result.p5_payoff, color='orange', linestyle='--', linewidth=1.5, label=f'5th %ile: €{result.p5_payoff:,.0f}')
    ax.axvline(result.p95_payoff, color='green', linestyle='--', linewidth=1.5, label=f'95th %ile: €{result.p95_payoff:,.0f}')
    
    ax.set_xlabel('Payoff (EUR)', fontsize=12)
    ax.set_ylabel('Frequency', fontsize=12)
    ax.set_title('Contract Payoff Distribution', fontsize=14, fontweight='bold')
    ax.legend()
    ax.grid(True, alpha=0.3)
    
    st.pyplot(fig)


def render_export_section(result, spec_dict: dict):
    """Render export options."""
    st.subheader("💾 Export Results")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Convert to DataFrame
        result_df = valuation_to_frame(result)
        
        # Add contract details
        result_df["source"] = spec_dict["source"]
        result_df["sink"] = spec_dict["sink"]
        result_df["start_utc"] = spec_dict["start_utc"]
        result_df["end_utc"] = spec_dict["end_utc"]
        result_df["mw"] = spec_dict["mw"]
        
        csv = result_df.to_csv(index=False)
        st.download_button(
            label="📄 Download CSV",
            data=csv,
            file_name=f"ftr_pricing_{spec_dict['source']}_{spec_dict['sink']}.csv",
            mime="text/csv",
        )
    
    with col2:
        json_data = result_df.to_json(orient="records", indent=2)
        st.download_button(
            label="📋 Download JSON",
            data=json_data,
            file_name=f"ftr_pricing_{spec_dict['source']}_{spec_dict['sink']}.json",
            mime="application/json",
        )


def main():
    """Main application logic."""
    init_session_state()
    render_header()
    
    # Get settings from sidebar
    settings = render_sidebar()
    
    # Get available zones
    try:
        zones = get_available_zones()
        if not zones:
            st.error("❌ No zones found in configuration. Please check config/zones.yml")
            return
    except Exception as e:
        st.error(f"❌ Error loading zones: {e}")
        return
    
    # Main content tabs
    tab1, tab2, tab3 = st.tabs(["🏠 Single Contract", "📦 Batch Pricing", "ℹ️ Help"])
    
    with tab1:
        # Contract builder
        spec_dict = render_contract_builder(zones, settings)
        
        # Price contract if requested
        if spec_dict:
            try:
                # Generate prices
                prices_df = generate_prices_for_contract(spec_dict, settings["volatility"], settings["seed"])
                st.session_state.prices_df = prices_df
                st.session_state.contract_spec = spec_dict
                
                # Price contract
                result = price_ftr_contract(spec_dict, prices_df, settings)
                st.session_state.pricing_result = result
                
                st.success("✅ Contract priced successfully!")
                
            except Exception as e:
                st.error(f"❌ Error pricing contract: {e}")
                log.exception("Pricing error")
        
        # Display results if available
        if st.session_state.pricing_result:
            render_results(st.session_state.pricing_result, st.session_state.contract_spec)
            render_payoff_distribution(st.session_state.pricing_result)
            render_export_section(st.session_state.pricing_result, st.session_state.contract_spec)
    
    with tab2:
        st.header("📦 Batch Pricing")
        st.info("🚧 Batch pricing feature coming in Phase 2")
        
        st.markdown("""
        **Planned features:**
        - Upload CSV/Excel with multiple contracts
        - Bulk pricing with progress tracking
        - Export aggregated results
        - Portfolio-level analytics
        """)
    
    with tab3:
        st.header("ℹ️ Help & Documentation")
        
        st.markdown("""
        ## How to Use This Tool
        
        ### 1. Configure Settings
        Use the sidebar to adjust:
        - **Number of Scenarios**: More scenarios = more accurate but slower
        - **Block Length**: Controls bootstrap block size (7 days typical)
        - **Price Volatility**: Adjust synthetic data volatility (15% typical)
        
        ### 2. Build Your Contract
        - Select **Source** and **Sink** zones
        - Choose contract **Start** and **End** dates
        - Enter **MW capacity**
        - Click **Price Contract**
        
        ### 3. Review Results
        - **Contract Value**: Expected payoff (mean)
        - **Standard Deviation**: Risk measure
        - **Percentiles**: Worst/best case scenarios
        - **Distribution Chart**: Visual representation of risk
        
        ### 4. Export
        Download results as CSV or JSON for further analysis.
        
        ---
        
        ## About the Pricing Model
        
        This tool uses **Historical Simulation (HS)** with block bootstrap:
        1. Generates synthetic hourly prices with realistic patterns
        2. Calculates historical price spreads between zones
        3. Bootstraps scenarios using block resampling
        4. Computes contract payoff for each scenario
        5. Aggregates statistics (mean, std, percentiles)
        
        **Note**: Currently using synthetic price data. Replace with real market data when available.
        
        ---
        
        ## Available Zones
        
        The tool supports all European zones configured in `config/zones.yml`.
        Major zones include: FR, DE_LU, GB, ES, IT, NL, BE, CH, AT, PL, and Nordic zones.
        
        ---
        
        ## Support
        
        For issues or questions, refer to the project documentation or contact the development team.
        """)


if __name__ == "__main__":
    main()
