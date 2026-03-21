from __future__ import annotations


def forecast_anomaly(current_forecast_f: float, normal_tmax_f: float) -> float:
    return current_forecast_f - normal_tmax_f
