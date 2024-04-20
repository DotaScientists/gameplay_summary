from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path

PROJECT_ROOT = Path(__file__).parents[2]

class Constants(BaseSettings):
    local_db_path: Path = PROJECT_ROOT / "data/main.db"
    HERO_INFO_PATH: Path = PROJECT_ROOT / Path('data/heroes.json')
    HERO_BENCHMARKS_PATH: Path = PROJECT_ROOT / Path('data/benchmarks.json')


class Settings(BaseSettings):
    MINUTE_INTERVAL: int = 10
    MAX_PLAYERS: int = 10

    BENCHMARK_PERCENTILE: int = 50

    PER_MINUTE_COLUMNS: list[str] = ["gold", "xp"]

    CLOUD_DATA_PATH: str = "gs://test_dota2_data/db/sqllite/main.db"
    CLOUD_JSONLINES_FOLDER: str = "gs://test_dota2_data/parsed_replays"
    PROJECT_NAME: str = "robust-doodad-416318"

    PARSER_SERVICE_URL: str
    PARSER_BATCH_SIZE: int = 2

    GROQ_TEMPERATURE: float = 0.5
    GROQ_MAX_TOKENS: int = 1000
    GROQ_API_KEY: str
    GROQ_MODEL: str = "mixtral-8x7b-32768"
    GROQ_DELAY: float = 8.0

    model_config = SettingsConfigDict(
        env_file=PROJECT_ROOT / "envs/.env",
        env_file_encoding="utf-8",
    )