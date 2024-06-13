import json
from cerberus import Validator

schema = {
  "timestamp": {
    "type": "string"
  },
  "day-ahead-price": {
    "type": "float",
    "min": 0.0
  },
  "power_factor": {  # Example of adding validation for new data point
    "type": "float",
    "between": (0.0, 1.0)
  }
}

v = Validator(schema)

# Assuming you received JSON data in a string variable called 'data'
if v.validate(json.loads(data)):
  # Data is valid, process it further
  print(v.document)  # Access validated data as a dictionary
else:
  print("Error: Invalid JSON data")
  print(v.errors)  # Get detailed error messages
