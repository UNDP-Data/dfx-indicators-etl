import configparser
import os


def check_retrieval_user_data(config_dir):
    cfg_files = [f for f in os.listdir(config_dir) if f.endswith(".cfg")]
    allowed_keys = {"type", "url", "codes", "file", "request_params"}
    files_with_retrieval_user_data = []
    total_files = len(cfg_files)
    validated_file_count = 0

    for cfg_file in cfg_files:
        config = configparser.ConfigParser(interpolation=None)
        config.read(os.path.join(config_dir, cfg_file))

        if config.has_option("downloader_params", "retrieval user data"):
            files_with_retrieval_user_data.append(cfg_file)

        if config.has_section("downloader_params"):
            keys = set(config.options("downloader_params"))

            if not keys.issubset(allowed_keys):
                invalid_keys = keys - allowed_keys
                print(
                    f"Invalid keys found in the 'downloader_params' section of {cfg_file}: {', '.join(invalid_keys)}"
                )
            else:
                source_id = config.get("source", "id", fallback=None)
                print(f"Valid configuration in {cfg_file}. Source ID: {source_id}")
        else:
            print(f"Missing 'downloader_params' section in {cfg_file}")
    print(f"Total cfg files: {total_files}, validated: {validated_file_count}")
    return files_with_retrieval_user_data


if __name__ == "__main__":
    config_dir = "output_cfg_files"
    files_with_retrieval_user_data = check_retrieval_user_data(config_dir)

    if files_with_retrieval_user_data:
        print("Files with 'Retrieval User data' in the 'downloader_params' section:")
        for file in files_with_retrieval_user_data:
            print(file)
    else:
        print(
            "No files with 'Retrieval User data' found in the 'downloader_params' section."
        )
