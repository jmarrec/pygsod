from pathlib import Path

# Global constants

# Where this py file is located.
GSOD_DIR = Path(__file__).resolve().parent

# Create support and weather_files directories if don't exist
SUPPORT_DIR = GSOD_DIR / "../support"
if not SUPPORT_DIR.exists():
    SUPPORT_DIR.mkdir(parents=True)

WEATHER_DIR = GSOD_DIR / "../weather_files"
if not WEATHER_DIR.exists():
    WEATHER_DIR.mkdir(parents=True)

RESULT_DIR = GSOD_DIR / "../results"
if not RESULT_DIR.exists():
    RESULT_DIR.mkdir(parents=True)

ISDHISTORY_PATH = SUPPORT_DIR / "isd-history.csv"
