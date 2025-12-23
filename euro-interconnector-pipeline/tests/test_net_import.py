from __future__ import annotations

import pandas as pd

from eicflows.features import compute_net_import


def test_net_import_sums_inbound_minus_outbound() -> None:
    ts = pd.Timestamp("2024-01-01T00:00:00Z")
    flows = pd.DataFrame(
        [
            {"timestamp_utc": ts, "from_zone": "A", "to_zone": "B", "mw": 10.0},
            {"timestamp_utc": ts, "from_zone": "C", "to_zone": "B", "mw": 5.0},
            {"timestamp_utc": ts, "from_zone": "B", "to_zone": "D", "mw": 3.0},
        ]
    )
    out = compute_net_import(flows)

    b_val = out.loc[(out["zone"] == "B") & (out["timestamp_utc"] == ts), "net_import_mw"].iloc[0]
    a_val = out.loc[(out["zone"] == "A") & (out["timestamp_utc"] == ts), "net_import_mw"].iloc[0]
    d_val = out.loc[(out["zone"] == "D") & (out["timestamp_utc"] == ts), "net_import_mw"].iloc[0]

    assert float(b_val) == 12.0  # +10 +5 inbound, -3 outbound
    assert float(a_val) == -10.0
    assert float(d_val) == 3.0

