import configparser
import csv
import json
import os

## TODO: Update Retrieval User Data/downloader_params automatically condtionally updated
## right now user must manually update these


def csv_to_cfg(input_csv, output_dir):
    with open(input_csv, "r") as csvfile:
        reader = csv.DictReader(csvfile)

        row_count = sum(1 for row in reader)
        print(f"Total rows in the CSV file: {row_count}")

        csvfile.seek(0)  # Reset the file pointer to the beginning
        next(reader)

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        file_count = 0

        for row in reader:
            config = configparser.ConfigParser(interpolation=None)

            config.add_section("source")
            config.add_section("downloader_params")

            for key, value in row.items():
                if key == "Retrieval User data" and value.strip() != "":
                    if "params=" in value:
                        request_params = {
                            "type": "params",
                            "value": value.split("params=")[1],
                        }
                        config.set("downloader_params", 'request_params', json.dumps(request_params))
                    if "data=" in value:
                        request_params = {
                            "type": "data",
                            "value": value.split("data=")[1],
                        }
                        config.set("downloader_params", 'request_params', json.dumps(request_params))
                    elif "headers=" in value:
                        request_params = {
                            "type": "headers",
                            "value": value.split("headers=")[1],
                        }
                        config.set("downloader_params", 'request_params', json.dumps(request_params))
                    elif "json=" in value:
                        request_params = {
                            "type": "json",
                            "value": value.split("json=")[1],
                        }
                        config.set("downloader_params", 'request_params', value)
                    else:
                        config.set("downloader_params", 'file', value)
                elif key == "Source ID":
                    config.set("source", "id", value)
                elif key == "Source URL":
                    config.set("source", "url", value)
                elif key == "Source Name":
                    config.set("source", "name", value)
                elif key == "Type":
                    config.set("source", "source_type", value)
                elif key == "SaveAs":
                    if value == '':
                        value = f"{row['Source ID'].upper()}.{row['Source URL'].split('.')[-1]}"
                    config.set("source", "save_As", value)
                    file_format = value.split(".")[-1]
                    config.set("source", "file_format", file_format)
                elif key == "Retrieval Notebook":
                    if value == "default":
                        config.set(
                            "source", "downloader_function", "default_http_downloader"
                        )
                    elif value == "RCC_DATA_retriever":
                        config.set("source", "downloader_function", "rcc_downloader")
                    elif value == "CPIA_retriever":
                        config.set("source", "downloader_function", "cpia_downloader")
                    elif value == "VDEM_retriever":
                        config.set("source", "downloader_function", "vdem_downloader")
                    elif value == "GET_retriever":
                        config.set("source", "downloader_function", "get_downloader")
                    elif value == "POST_retriever":
                        config.set("source", "downloader_function", "post_downloader")
                    elif value == "SIPRI_retriever":
                        config.set("source", "downloader_function", "sipri_downloader")
                    elif value == "COUNTRY_DATA_retriever":
                        config.set(
                            "source", "downloader_function", "country_downloader"
                        )
                    elif value == "NESTED_ZIP_retriever":
                        config.set(
                            "source", "downloader_function", "zip_content_downloader"
                        )
                    else:
                        if row["Type"] != "Manual" and value == "":
                            config.set("source", "downloader_function", "default_http_downloader")
                        else:
                            config.set("source", "downloader_function", value)
                elif key == "Frequency":
                    config.set("source", "frequency", value)
            config.set("source", "country_iso3_column", "")
            config.set("source", "country_name_column", "")
            config.set("source", "datetime_column", "")
            path = os.path.join(output_dir, row["Source ID"].lower())
            if not os.path.exists(path):
                os.makedirs(path)
            file_name = f"{path}/{row['Source ID'].lower()}.cfg"
            with open(file_name, "w", encoding="utf-8") as configfile:
                config.write(configfile)
                file_count += 1

    print(f"Total output files {file_count}, total rows {row_count}")


if __name__ == "__main__":
    input_csv = "source_list.csv"
    output_dir = "source_cfg_files"
    csv_to_cfg(input_csv, output_dir)
