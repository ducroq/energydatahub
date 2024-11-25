#!/bin/bash

# Path to your virtual environment
VENV_PATH="/home/pi/energyDataHub"

# Activate the virtual environment
source "$VENV_PATH/bin/activate"

# Execute your Python script
python /home/pi/energyDataHub/local_data_fetcher.py

# Deactivate the virtual environment
deactivate