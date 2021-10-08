import os
from pydantic import BaseSettings
from dotenv import load_dotenv

class Settings(BaseSettings):
    app_name: str
    admin_email: str
    cred:str
    class Config:
        env_file = ".env"