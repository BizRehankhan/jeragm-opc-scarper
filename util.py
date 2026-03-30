
import pandas as pd
from constant import ENERGY_TYPES



def to_tabular_format(data, energy_type):
    raw_data = data

    rows = []

    for record in raw_data:
        base = {
            "code": record.get("code"),
            "name": record.get("name"),
            "energy_type": energy_type
        }

        for item in record.get("values", []):
            row = base.copy()
            row.update(item)   # merge all fields
            rows.append(row)

    return pd.DataFrame(rows)



def get_all_energy_type(name=None):
    if name:
        return next(
            (item for item in ENERGY_TYPES if item["name"] == name.lower(
            ) or item["_name"] == name),
            None
        )
    return ENERGY_TYPES


