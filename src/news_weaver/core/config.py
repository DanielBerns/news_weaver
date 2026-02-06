from pydantic import BaseSettings

class Settings(BaseSettings):
    app_name: str = "News Weaver"
    admin_email: str
    items_per_page: int = 50

    class Config:
        env_file = ".env"

settings = Settings()