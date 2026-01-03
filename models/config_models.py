from pydantic import BaseModel


class ServerSettings(BaseModel):
    name: str
    port: int
    host: str
    log_level: str
    log_dir: str
    admin_email: str


# Database settings
class DatabaseSettings(BaseModel):
    name: str
    host: str
    port: int
    user_name: str
    password: str | int
    path: str
    thread: bool


# file storage model
class StorageSettings(BaseModel):
    root: str
    local_root: str
    max_file_size: int
    chunk_buffer_size: int
    allowed_storage_types: list[str]
    allowed_file_types: list[str]
    max_filename_length: int
    max_file_type_length: int
    max_downloads_per_user: int
    max_files_per_user: int
    preflight_ttl_minutes: int
    chunk_ttl_minutes: int

# user model 用户信息配置模型
class UserSettings(BaseModel):
    max_username_length: int
    max_useremail_length: int

# sample information model 样品信息配置模型
class SampleInfoSettings(BaseModel):
    max_experiment_type_length: int
    max_organism_length: int
    max_organism_part_length: int
    max_condition_length: int
    max_sample_growth_conditions_length: int
    max_sample_stabilisation_length: int
    max_tissue_modification_length: int
    max_maldi_matrix_length: int
    max_maldi_matrix_application_length: int
    max_solvent_length: int

class Config(BaseModel):
    server: ServerSettings
    database: DatabaseSettings
    storage: StorageSettings
    user: UserSettings
    sample_info: SampleInfoSettings