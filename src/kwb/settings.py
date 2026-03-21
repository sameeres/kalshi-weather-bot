from pathlib import Path
import os

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:  # pragma: no cover - optional dev convenience
    def load_dotenv() -> bool:
        return False

load_dotenv()

ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
STAGING_DIR = DATA_DIR / "staging"
MARTS_DIR = DATA_DIR / "marts"
CONFIG_DIR = ROOT / "configs"

KALSHI_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
NWS_BASE_URL = "https://api.weather.gov"

USER_AGENT = os.getenv(
    "NWS_USER_AGENT",
    "kalshi-weather-bot research contact@example.com",
)
