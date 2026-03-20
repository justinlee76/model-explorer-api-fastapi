from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict

class ModelStoreSettings(BaseModel):
    uri: str
    db: str

class Settings(BaseSettings):
    model_store: ModelStoreSettings
    path_prefix: str
    allow_origin: str

    model_config = SettingsConfigDict(env_file='.env', env_nested_delimiter='__')

settings = Settings() # type: ignore