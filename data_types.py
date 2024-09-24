import json
from datetime import datetime, timedelta
from typing import Dict, Any

def convert_value(value):
    if type(value) == int or type(value) == float:
        return value
    if value == '-':
        return None  # Using None instead of NaN for JSON compatibility
    elif value.lower() == 'none':
        return None
    try:
        return int(value)
    except ValueError:
        try:
            return float(value)
        except ValueError:
            return value  # Keep as string if it's not a number

class EnhancedDataSet:
    def __init__(self, metadata: Dict[str, Any], data: Dict[str, Any]):
        self.metadata = metadata
        self.metadata['data_type_version'] = "2.0"

        data_type = metadata['data_type']
        if data_type == 'energy_price':
             self.data = self.validate_energy_prices(data)
        # TODO: Add more validation rules, e.g. for other data types?

    def validate_energy_prices(self, data: Dict[str, Any]) -> Dict[str, Any]:
        validated_data = {timestamp: convert_value(value) for timestamp, value in data.items()}
        return validated_data    

    def __getitem__(self, key):
        if key in self.__dict__:
            return self.__dict__[key]
        raise KeyError(f"{key} not found in EnhancedDataSet")

    def __str__(self):
        return f"EnhancedDataSet(metadata={self.metadata}, data_points={len(self.data)})"

    def __repr__(self):
        return self.__str__()    

    def to_dict(self):
        return {
            'metadata': self.metadata,
            'data': self.data
        }

    def to_json(self):
        return json.dumps(self.to_dict(), indent=2, default=str)

if __name__ == "__main__":
    energy_prices = EnhancedDataSet(
        metadata={'data_type': 'energy_price',
                  'source': 'ENTSO-E Transparency Platform API v1.3',                  
                  'country': 'NL',
                  'units': 'EUR/MWh'},        
        data = {datetime.now().isoformat(): 50.5,
                (datetime.now() + timedelta(hours=1)).isoformat(): -5,
                (datetime.now() + timedelta(hours=2)).isoformat(): '-'},
    )

    print(energy_prices.to_json())