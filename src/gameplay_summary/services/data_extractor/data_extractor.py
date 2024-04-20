import pandas as pd
from pathlib import Path
import json
import os
from gameplay_summary.entities import HeroBenchmarks, Team, Benchmark
from gameplay_summary.services.data_extractor.post_processing import PostProcessor
from gameplay_summary.settings import Settings


REQUIRED_FIELDS = {"interval", "DOTA_COMBATLOG_DAMAGE", "DOTA_COMBATLOG_TEAM_BUILDING_KILL"}

def extract_winning_team(preprocessed_df: pd.DataFrame) -> Team:
    """
    Check if radiant won the game
    in the data with type 'DOTA_COMBATLOG_TEAM_BUILDING_KILL', last row's in column targetname
    corresponds to which fort was destroyed. If it is 'npc_dota_badguys_fort' then radiant won the game
    :param df: pandas dataframe
    :return: bool
    """
    building_destroyed_df = get_df_by_type(preprocessed_df, 'DOTA_COMBATLOG_TEAM_BUILDING_KILL')
    # get last row of the dataframe
    last_row = building_destroyed_df.iloc[-1]
    if last_row['targetname'] == 'npc_dota_badguys_fort':
        return Team.radiant
    return Team.dire


class CorruptedDataError(Exception):
    pass


def get_df_by_type(df: pd.DataFrame, type: str) -> pd.DataFrame:
    """
    Get a dataframe by type
    :param df: pandas dataframe
    :param type: type of the dataframe
    :return: pandas dataframe
    """
    return df[df['type'] == type].copy()

def split_data_by_player(df: pd.DataFrame, max_players: int) -> list[pd.DataFrame]:
    """
    Split the data by player
    :param df: pandas dataframe
    :return: list of pandas dataframes
    """
    slots_list = []
    for slot in range(0, max_players):
        slots_list.append(df.loc[df["slot"] == slot])
    return slots_list


def get_match_length(df: pd.DataFrame) -> int:
    """
    Get the length of the match
    :param df: pandas dataframe
    :return: int
    """
    temp = df[df['type'] == 'interval']
    return temp['time'].max() - temp['time'].min()


class DataExtractor:
    def __init__(self,hero_info_path: Path,
                 hero_benchmarks_path: Path,
                 settings: Settings
    ):
        self.heroes_info = json.load(hero_info_path.open())
        self.heroes_benchmarks = HeroBenchmarks(hero_benchmarks_path)
        self.settings = settings
        self.post_processor = PostProcessor(self.heroes_benchmarks, self.settings)

    def compute_final_stats(
            self,
            interval_df: pd.DataFrame, damage_df: pd.DataFrame
    ) -> dict:
        last_second = interval_df["time"].max()
        final_df = interval_df[interval_df["time"] == last_second]
        output_data = dict()

        for slot in range(0, self.settings.MAX_PLAYERS):
            df = final_df.loc[final_df["slot"] == slot].iloc[-1]
            hero_id = df["hero_id"].item()
            hero_name = self.heroes_info[str(int(hero_id))]['name']
            slot_combat_df = damage_df[damage_df["attackername"] == hero_name]
            total_kda = (df["kills"].item() + df["assists"].item()) / max(df["deaths"].item(), 1)
            output_data[slot] = {
                "Total gold": df["gold"].item(),
                "Total last hits": df["lh"].item(),
                "Total denies": df["denies"].item(),
                "Total xp": df["xp"].item(),
                "Total kills": df["kills"].item(),
                "Total deaths": df["deaths"].item(),
                "Total assists": df["assists"].item(),
                "Total KDA": round(total_kda, 1),
                "Total damage": slot_combat_df["damage_dealt_to_heroes"].sum(),
            }

        return output_data

    def preprocess_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Preprocess the data.
        Preprocessing includes:
            1. Remove raws where time < 0
            2. Compute the 'minute' column and convert it to datetime
            3. create multiple dataframes, one for config.COLUMN_TYPE_VALUES

        :param df: pandas dataframe
        :return: tuple of pandas dataframes
        """
        df = df[df['time'] >= 0].reset_index(drop=True)
        df["minute"] = df["time"] // 60
        df["block"] = df["minute"] // self.settings.MINUTE_INTERVAL
        df = df.sort_values(by=["time", "slot"])
        return df


    def add_hero_name(self, df: pd.DataFrame) -> pd.DataFrame:
        df['hero_name'] = df['hero_id'].apply(
            lambda x: self.heroes_info[str(int(x))]['name']
        )
        df["localized_hero_name"] = df["hero_id"].apply(
            lambda x: self.heroes_info[str(int(x))]['localized_name']
        )
        return df

    def aggregate_damage_data(self, df: pd.DataFrame) -> pd.DataFrame:
        # filter the rows where the attackername starts with 'npc_dota_hero_'
        df = df[df['attackername'].str.startswith('npc_dota_hero_')]
        # filter the rows where the targetname starts with 'npc_dota_hero_'
        df = df[df['targetname'].str.startswith('npc_dota_hero_')]

        df = df[(df["attackerhero"] == 1) & (df["targethero"] == 1)]

        df = df.groupby(by=[
            "block",
            'attackername',
        ], as_index=False).agg(
            damage_dealt_to_heroes=pd.NamedAgg(column="value", aggfunc="sum"),
            minute=pd.NamedAgg(column="minute", aggfunc="last"),
        )
        return df

    def aggregate_interval_data(
            self,
            df: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        process the interval data.
        """
        required_fields = {"time", "minute", "level", "hero_id", "teamfight_participation", "kills", "deaths", "assists"}
        required_fields.update(self.settings.PER_MINUTE_COLUMNS)
        required_fields.update({"denies", "lh", "slot"})
        if not required_fields.issubset(df.columns):
            raise CorruptedDataError(f"Required fields are missing in the interval data {required_fields - set(df.columns)}")

        group_by_df = df.groupby([
            "block",
            'slot'
        ], as_index=False)
        first_group_df = group_by_df.agg(
            time=pd.NamedAgg(column="time", aggfunc="last"),
            block_start=pd.NamedAgg(column="time", aggfunc="first"),
            minute=pd.NamedAgg(column="minute", aggfunc="last"),
            level=pd.NamedAgg(column="level", aggfunc="last"),
            hero_id=pd.NamedAgg(column="hero_id", aggfunc="last"),

            teamfight_participation=pd.NamedAgg(column="teamfight_participation", aggfunc="sum"),

            kills = pd.NamedAgg(column="kills", aggfunc="max"),
            deaths = pd.NamedAgg(column="deaths", aggfunc="max"),
            assists = pd.NamedAgg(column="assists", aggfunc="max"),
        )

        second_group_df = []
        for index, group_df in group_by_df:
            aggregated_data = {
                col: (group_df[col].max() - group_df[col].min())
                for col in self.settings.PER_MINUTE_COLUMNS
            }
            aggregated_data["denies"] = group_df["denies"].max() - group_df["denies"].min()
            aggregated_data["lh"] = group_df["lh"].max() - group_df["lh"].min()
            deaths = max(group_df["deaths"].max(), 1)
            aggregated_data["kda"] = (group_df["kills"].max() + group_df["assists"].max()) / deaths

            aggregated_data["minute"] = group_df["minute"].max()
            aggregated_data["slot"] = group_df["slot"].max()
            second_group_df.append(aggregated_data)
        second_group_df = pd.DataFrame(second_group_df)

        full_interval_df = first_group_df.merge(
            second_group_df,
            on=["minute", "slot"],
            how="left"
        )

        full_interval_df = self.add_hero_name(full_interval_df)
        full_interval_df = full_interval_df.fillna(0)
        return full_interval_df

    def normalize_per_minute_data(self, df: pd.DataFrame) -> pd.DataFrame:
        df["block_length"] = df["time"] - df["block_start"]
        df.rename(columns={"damage_dealt_to_heroes": "dpm"}, inplace=True)
        per_minute_columns = self.settings.PER_MINUTE_COLUMNS + ["dpm"]
        for column in per_minute_columns:
            df[column] = df[column] / df["block_length"] * 60
        df.drop(columns=["block_length"], inplace=True)
        return df

    def process_replay_data(
            self, df: pd.DataFrame,
    ) -> dict:
        """
        Processes the raw replay data and outputs the final json object.
        :param df:
        :param heroes_info:
        :param heroes_benchmarks:
        :return:
        """
        if not REQUIRED_FIELDS.issubset(df["type"].unique()):
            raise CorruptedDataError(f"Match data doesn't contain rows with types{REQUIRED_FIELDS - set(df['type'].unique())}")

        match_length = get_match_length(df)
        df = self.preprocess_data(df)
        winning_team = extract_winning_team(df)
        interval_df = get_df_by_type(df, 'interval')
        dota_combatlog_damage = get_df_by_type(df, 'DOTA_COMBATLOG_DAMAGE')

        processed_damage_df = self.aggregate_damage_data(
            dota_combatlog_damage
        )

        final_stats = self.compute_final_stats(
            interval_df, processed_damage_df
        )

        processed_interval_df = self.aggregate_interval_data(
            interval_df
        )

        combined_df = pd.merge(
            left=processed_interval_df,
            right=processed_damage_df[["block", "attackername", "damage_dealt_to_heroes"]],
            left_on=['block', 'hero_name'],
            right_on=['block', 'attackername'],
            how='left',
        )
        combined_df = self.normalize_per_minute_data(combined_df)

        slots_list = split_data_by_player(combined_df, self.settings.MAX_PLAYERS)
        post_process_data = {
            slot: self.post_processor.postprocess_data(
                data, winning_team, final_stats, match_length, slot
            )
            for slot, data in enumerate(slots_list)
        }
        return post_process_data

    def extract_data(self, input_file_path: Path):
        df = pd.read_json(input_file_path, lines=True)

        processed_replay_data = self.process_replay_data(df)

        return processed_replay_data
