from util import get_all_energy_type, to_tabular_format
from cwarler import single_energy_record

if __name__ == "__main__":
    energy_type = get_all_energy_type("Coal")
    data = single_energy_record(energy_type['_id'])
    # #  Convert Data to tabular formate
    df = to_tabular_format(data['data'],energy_type["_name"])
    print(df.head(2))
