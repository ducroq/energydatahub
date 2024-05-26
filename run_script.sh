#!/bin/bash

# Path to your virtual environment
VENV_PATH="/home/pi/energyPriceScraper"

# Activate the virtual environment
source "$VENV_PATH/bin/activate"

# Execute your Python script
python /home/pi/energyPriceScraper/energyPriceScraper.py

# Deactivate the virtual environment
deactivate