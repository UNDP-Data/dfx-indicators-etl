import configparser
import json


def read_dicts_from_file(file_name):
    with open(file_name, "r") as file:
        return json.load(file)


def dicts_to_ini(dicts_list):
    config = configparser.ConfigParser()
    section_names = set()

    for d in dicts_list:
        if not d:
            continue

        section, value = next(iter(d.items()))

        if value in section_names:
            raise ValueError(f"Duplicate section name found: {value}")

        section_names.add(value)
        config.add_section(value)

        for k, v in d.items():
            config.set(value, k, str(v))

    return config


# Read list of dictionaries from JSON file
file_name = "country_territory_groups.json"
list_of_dicts = read_dicts_from_file(file_name)

try:
    # Convert the list of dictionaries to a ConfigParser object
    config = dicts_to_ini(list_of_dicts)

    # Write the ConfigParser object to an INI file
    with open("country_territory_groups.cfg", "w", encoding="utf-8") as configfile:
        config.write(configfile)
except ValueError as e:
    print(e)
