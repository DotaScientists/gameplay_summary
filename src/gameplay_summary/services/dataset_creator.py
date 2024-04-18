from gameplay_summary.settings import Settings, Constants, PROJECT_ROOT
from gameplay_summary.cloud.gcs_client import GCSConnector
from gameplay_summary.db.sqllite_client import SQLLiteDB
from gameplay_summary.api.parser_api import ParserConnector
from gameplay_summary.services.data_extractor.data_extractor import extract_data, CorruptedDataError
from gameplay_summary.services.prompt_generator.prompt_generator import PromptGenerator
from gameplay_summary.api.groq_api import GroqConnector, PromptOutput
import logging
import tqdm

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatasetCreator:
    def __init__(self, settings: Settings, constants: Constants):
        self.settings = settings
        self.constants = constants
        self.gcs_client = GCSConnector(settings.PROJECT_NAME)
        self.db_client = self._load_db()
        self.prompt_generator = PromptGenerator()
        self.groq = GroqConnector(settings)
        self.failed_matches = []

    def _load_db(self) -> SQLLiteDB:
        self.gcs_client.download(self.settings.CLOUD_DATA_PATH, self.constants.local_db_path)
        db_client = SQLLiteDB(self.settings, self.constants)
        db_client.setup_tables()
        return db_client

    def _get_jsonlines_path(self, match_id: int) -> str:
        return f"{self.settings.CLOUD_JSONLINES_FOLDER}/{match_id}.jsonlines"

    def _get_matches_converted_to_jsonlines(self) -> list[int]:
        converted_matches = []
        for file in self.gcs_client.get_all_files(self.settings.CLOUD_JSONLINES_FOLDER):
            match_id = int(file.split(".")[0])
            converted_matches.append(match_id)
        logger.info(f"Found {len(converted_matches)} total converted matches")
        converted_matches = [
            match_id for match_id in converted_matches if not self.db_client.is_match_in_dataset(match_id)
        ]
        logger.info(f"Found {len(converted_matches)} matches not in dataset")
        return sorted(converted_matches)

    def _process_jsonlines(
            self, match_ids: list[int]) -> list[PromptOutput] | None:
        local_temp_path = PROJECT_ROOT / "data/temp.jsonlines"
        for match_id in match_ids:
            cloud_path = self._get_jsonlines_path(match_id)
            self.gcs_client.download(cloud_path, local_temp_path)
            try:
                extracted_data = extract_data(
                    local_temp_path,
                    self.constants.HERO_INFO_PATH,
                    self.constants.HERO_BENCHMARKS_PATH,
                    self.settings
                )
                for slot, instruction_prompt, data_prompt in self.prompt_generator.generate_prompt(extracted_data):
                    output = self.groq.get_response(instruction_prompt, data_prompt)
                    output.slot = slot
                    output.match_id = match_id
                    yield output
            except CorruptedDataError as e:
                logger.error(f"Match {match_id} is corrupted: {e}")
                self.failed_matches.append(match_id)
                yield None

    def _upload_db(self, ):
        self.db_client.close()
        self.gcs_client.upload_file(
            self.constants.local_db_path, self.settings.CLOUD_DATA_PATH
        )

    def _generate_dataset(self, match_ids: list[int]):
        for prompt_output in tqdm.tqdm(self._process_jsonlines(match_ids), total=len(match_ids) * 10):
            if prompt_output is not None:
                self.db_client.insert_dataset(prompt_output)
        logger.info(f"Failed to process {len(self.failed_matches)} matches")


    def repair_data(self):
        dataset_matches = self.db_client.get_all_dataset_matches()
        clean_dataset_matches = self.db_client.get_clean_dataset_matches()
        corrupted_matches = set(dataset_matches).difference(clean_dataset_matches)
        if corrupted_matches:
            logger.info(f"Found {len(corrupted_matches)} corrupted dataset entities")
            for match_id in corrupted_matches:
                self.db_client.delete_dataset(match_id)
                self.db_client.set_match_in_dataset(match_id, 0)
        for match_id in clean_dataset_matches:
            self.db_client.set_match_in_dataset(match_id, 1)
        marked_matches = self.db_client.get_matches_marked_in_dataset()
        corrupted_matches = set(marked_matches).difference(clean_dataset_matches)
        if corrupted_matches:
            logger.info(f"Found {len(corrupted_matches)} corrupted matches")
            for match_id in corrupted_matches:
                self.db_client.set_match_in_dataset(match_id, 0)

    def create_dataset(self):
        # 1. Download db file from cloud
        # 2. Connect to db
        # 3. Get all matches that are not downloaded yet
        # 4. Send matches to parser service
        # 5. Load parsed jsonlines file from cloud
        # 6. Update matches table with is_parsed_jsonlines = 1
        # 7. Extract features from parsed jsonlines
        # 8. Create prompts
        # 9. Send prompts to Groq Mixtral service
        # 10. Save result to text file
        # 11. Upload text file to cloud
        # 12. Update matches table with is_in_dataset = 1
        logger.warning("We have a problem with opendota scrapper. It sets is_parsable to 0 for all matches")
        self.repair_data()
        new_matches = self.db_client.get_not_downloaded_matches()
        logger.info(f"Not downloaded matches: {len(new_matches)}")
        converted_matches = self._get_matches_converted_to_jsonlines()
        new_matches = [match for match in new_matches if match.match_id not in converted_matches]
        logger.info(f"Matches without jsonlines in cloud: {len(new_matches)}")
        parser_client = ParserConnector(self.settings)
        logger.info(f"Starting to parse matches")
        # parser_client.parse_matches(new_matches)
        converted_matches = self._get_matches_converted_to_jsonlines()
        self.db_client.set_matches_parsed(converted_matches)
        logger.info(f"Starting to generate dataset")
        self._generate_dataset(converted_matches)
        logger.info(f"Dataset generation finished")
        self._upload_db()
