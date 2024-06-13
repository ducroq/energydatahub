#!/bin/bash

# Path to your virtual environment
VENV_PATH="/home/pi/energyDataHub"

# Activate the virtual environment
source "$VENV_PATH/bin/activate"

# Execute your Python script
python /home/pi/energyDataHub/energyDataScraper.py

# Deactivate the virtual environment
deactivate