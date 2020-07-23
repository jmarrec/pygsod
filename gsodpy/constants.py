import os

# Global constants

# Where this py file is located.
GSOD_DIR = os.path.dirname(os.path.realpath(__file__))

# Create support and weather_files directories if don't exist
SUPPORT_DIR = os.path.realpath(os.path.join(GSOD_DIR, '../support/'))
if not os.path.exists(SUPPORT_DIR):
    os.makedirs(SUPPORT_DIR)

WEATHER_DIR = os.path.realpath(os.path.join(GSOD_DIR, '../weather_files/'))
if not os.path.exists(WEATHER_DIR):
    os.makedirs(WEATHER_DIR)

RESULT_DIR = os.path.realpath(os.path.join(GSOD_DIR, '../results/'))
if not os.path.exists(RESULT_DIR):
    os.makedirs(RESULT_DIR)
