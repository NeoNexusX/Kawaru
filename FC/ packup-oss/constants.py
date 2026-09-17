# Constants

from config import get_config

config = get_config()

# OSS_ZIP_STORAGE_ROOT = config.oss.oss_zip_storage_root
# OSS_PACKUP_ZIP_STORAGE_ROOT = config.oss.oss_packup_zip_storage_root
OSS_ZIP_STORAGE_ROOT = config["oss"]["oss_zip_storage_root"]
OSS_PACKUP_ZIP_STORAGE_ROOT = config["oss"]["oss_packup_zip_storage_root"]
OSS_MOUNTED_STORAGE_PATH = "/home/storage" # oss挂载到本地的路径
DOWNLOAD_EXPIRE_SECONDS = int(config["storage"]["download_zip_ttl_minutes"]) * 60