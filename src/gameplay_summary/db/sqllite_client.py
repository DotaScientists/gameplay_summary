import sqlite3
from gameplay_summary.settings import Constants, Settings
from gameplay_summary.api.groq_api import PromptOutput
from gameplay_summary.entities import DownloadableMatch


class SQLLiteDB:

    def __init__(self, settings: Settings, constants: Constants):
        self.settings = settings
        self.constants = constants
        self.conn = sqlite3.connect(str(constants.local_db_path.absolute()))

    def setup_tables(self):
        sql = """
        CREATE TABLE IF NOT EXISTS dataset (
            match_id INTEGER,
            slot INTEGER,
            text_data TEXT,
            data_prompt TEXT,
            instruction_prompt TEXT,
            input_tokens INTEGER,
            output_tokens INTEGER,
            model TEXT,
            generation_time INTEGER,            
            PRIMARY KEY (match_id, slot)
        )
        """
        self.conn.execute(sql)
        self.conn.commit()

    def close(self):
        self.conn.close()

    def get_not_downloaded_matches(self) -> list[DownloadableMatch]:
        sql = "SELECT match_id, replay_salt, cluster FROM matches WHERE is_parsable = 1 and is_parsed_jsonlines = 0"
        cursor = self.conn.execute(sql)
        matches = []
        for row in cursor.fetchall():
            match = DownloadableMatch(match_id=row[0], replay_salt=row[1], cluster=row[2])
            matches.append(match)
        return matches

    def set_matches_parsed(self, match_ids: list[int]):
        matches_string = ",".join(map(str, match_ids))
        sql = f"UPDATE matches SET is_parsed_jsonlines = 1 WHERE match_id IN ({matches_string})"
        self.conn.execute(sql)
        self.conn.commit()

    def insert_dataset(self, prompt_output: PromptOutput):
        sql = """
        INSERT INTO dataset (
            match_id, slot, text_data, data_prompt, instruction_prompt, input_tokens, output_tokens, model, generation_time
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        self.conn.execute(sql, (
            prompt_output.match_id,
            prompt_output.slot,
            prompt_output.output,
            prompt_output.data_prompt,
            prompt_output.instruction_prompt,
            prompt_output.input_tokens,
            prompt_output.output_tokens,
            prompt_output.model,
            prompt_output.timestamp
        ))
        self.conn.commit()

    def set_match_in_dataset(self, match_id: int, value: int = 1):
        sql = f"UPDATE matches SET is_in_dataset = {value} WHERE match_id = {match_id}"
        self.conn.execute(sql)
        self.conn.commit()

    def is_match_in_dataset(self, match_id: int) -> bool:
        sql = f"SELECT is_in_dataset FROM matches WHERE match_id = {match_id}"
        cursor = self.conn.execute(sql)
        result = cursor.fetchone()
        if result is None:
            return False
        return bool(result[0])

    def get_all_dataset_matches(self)-> list[int]:
        sql = "SELECT DISTINCT match_id FROM dataset"
        cursor = self.conn.execute(sql)
        return [row[0] for row in cursor.fetchall()]

    def get_clean_dataset_matches(self) -> list[int]:
        sql = "SELECT match_id, count(slot) as slots_count from dataset group by match_id having slots_count = 10"
        cursor = self.conn.execute(sql)
        return [row[0] for row in cursor.fetchall()]

    def delete_dataset(self, match_id: int):
        sql = f"DELETE FROM dataset WHERE match_id = {match_id}"
        self.conn.execute(sql)
        self.conn.commit()

    def get_matches_marked_in_dataset(self):
        sql = "SELECT match_id FROM matches where is_in_dataset = 1"
        cursor = self.conn.execute(sql)
        return [row[0] for row in cursor.fetchall()]
