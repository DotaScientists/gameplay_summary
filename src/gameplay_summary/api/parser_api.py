import requests
from gameplay_summary.entities import DownloadableMatch
from gameplay_summary.settings import Settings
from pydantic import BaseModel, Field
import tqdm

class ParserInput(BaseModel):
    items: list[DownloadableMatch]


class ParserConnector:
    def __init__(self, settings: Settings):
        self.settings = settings

    def parse_matches(self, matches: list[DownloadableMatch]):
        matches_range = list(range(0, len(matches), self.settings.PARSER_BATCH_SIZE))
        for i in tqdm.tqdm(matches_range):
            batch = matches[i:i + self.settings.PARSER_BATCH_SIZE]
            url = self.settings.PARSER_SERVICE_URL
            request_input_data = ParserInput(items=batch)
            response = requests.post(url, json=request_input_data.model_dump())
            response.raise_for_status()
