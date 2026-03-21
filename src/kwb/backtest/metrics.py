from __future__ import annotations

import pandas as pd


def summarize_trades(df: pd.DataFrame) -> dict[str, float]:
    if df.empty:
        return {"trades": 0, "total_pnl": 0.0, "avg_pnl": 0.0}
    return {
        "trades": float(len(df)),
        "total_pnl": float(df["pnl"].sum()),
        "avg_pnl": float(df["pnl"].mean()),
    }
