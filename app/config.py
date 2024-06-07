from pydantic import BaseModel
from pydantic.v1 import BaseSettings


class DbConfig(BaseModel):
    name: str
    user: str
    password: str
    host: str
    port: int


class Config(BaseModel):
    db: DbConfig = None


def load_config(path: str = ".env") -> Config:
    """
    Загружает конфигурацию из переменных окружения.

    Args:
        path (str): Путь к файлу окружения. По умолчанию ".env".

    return:
        Config: Загруженная конфигурация.
    """
    from pathlib import Path

    class Settings(BaseSettings):
        DB_NAME: str
        DB_USER: str
        DB_PASSWORD: str
        DB_HOST: str
        DB_PORT: int

        class Config:
            env_file = Path(path)
            env_file_encoding = "utf-8"

    settings = Settings()
    return Config(
        db=DbConfig(
            host=settings.DB_HOST,
            port=settings.DB_PORT,
            name=settings.DB_NAME,
            password=settings.DB_PASSWORD,
            user=settings.DB_USER,
        )
    )


config = load_config(
    "/home/pavel/PycharmProjects/parser_bulletin_on_trading_results/.env"
)
