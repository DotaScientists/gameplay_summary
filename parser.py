import pandas as pd
from pathlib import Path
import json
import os
from enum import Enum
import dataclasses


@dataclasses.dataclass
class Benchmark:
    gold_per_min: float
    xp_per_min: float
    kills_per_min: float
    last_hits_per_min: float
    hero_damage_per_min: float
    hero_healing_per_min: float
    tower_damage: float


class HeroBenchmarks:
    def __init__(self, input_file_path: Path):
        benchmarks_json = json.loads(input_file_path.read_text())
        self.benchmarks = dict()
        for hero_id, benchmark_values in benchmarks_json.items():
            stats_names = list(benchmark_values.keys())
            percentiles = [item for item in benchmark_values[stats_names[0]].keys()]
            percentile_benchmarks = dict()
            for percentile in percentiles:
                percentile_benchmarks[percentile] = Benchmark(
                    **{
                        stat_name: benchmark_values[stat_name][percentile]
                        for stat_name in stats_names
                    }
                )
            self.benchmarks[int(hero_id)] = percentile_benchmarks

    def get_benchmark(self, hero_id: int, percentile: str) -> Benchmark:
        return self.benchmarks[hero_id][percentile]


class Team(Enum):
    radiant = "radiant"
    dire = "dire"


class Config:
    MINUTE_INTERVAL: int = 5
    MAX_PLAYERS: int = 10

    BENCHMARK_PERCENTILE: str = "50"


def agg_per_min(col: pd.Series) -> float:
    """
    Aggregate the data per minute
    :param col: pandas series
    :return:
    """
    return (col.max() - col.min()) / Config.MINUTE_INTERVAL


INTERVAL_AGGREGATE: dict[str: str] = {
    "time": "last",
    "level": "last",
    "gold": [agg_per_min, "max"],
    "lh": [agg_per_min, "max"],
    "xp": [agg_per_min, "max"],
    "kills": [agg_per_min, "max"],
    "deaths": [agg_per_min, "max"],
    "assists": [agg_per_min, "max"],
    "denies": [agg_per_min, "max"],
    "hero_id": "last",
    "teamfight_participation": "sum",
}


def extract_winning_team(df: pd.DataFrame) -> Team:
    """
    Check if radiant won the game
    in the data with type 'DOTA_COMBATLOG_TEAM_BUILDING_KILL', last row's in column targetname
    corresponds to which fort was destroyed. If it is 'npc_dota_badguys_fort' then radiant won the game
    :param df: pandas dataframe
    :return: bool
    """
    # get last row of the dataframe
    last_row = df.iloc[-1]
    if last_row['targetname'] == 'npc_dota_badguys_fort':
        return Team.radiant
    return Team.dire


def json2df(path: Path) -> pd.DataFrame:
    """
    Read json file and return a pandas dataframe
    :param path: path to the json file
    :return: pandas dataframe with the json data
    """
    df = pd.read_json(path, lines=True)
    return df


def get_player_team(slot: int) -> Team:
    """
    Get the team of the player
    :param slot: slot of the player
    :return: team of the player
    """
    if slot < 5:
        return Team.radiant
    else:
        return Team.dire

def process_hero_name(raw_hero_name: str) -> str:
    hero_name = raw_hero_name.removeprefix("npc_dota_hero_")
    return hero_name

def get_df_by_type(df: pd.DataFrame, type: str) -> pd.DataFrame:
    """
    Get a dataframe by type
    :param df: pandas dataframe
    :param type: type of the dataframe
    :return: pandas dataframe
    """
    return df[df['type'] == type]


def preprocess_data(df: pd.DataFrame) -> pd.DataFrame:
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
    df['minute'] = pd.to_datetime(df['minute'], unit='m')
    return df


def split_data_by_player(df: pd.DataFrame) -> list[pd.DataFrame]:
    """
    Split the data by player
    :param df: pandas dataframe
    :return: list of pandas dataframes
    """
    slots_list = []
    for slot in range(0, Config.MAX_PLAYERS):
        slots_list.append(df.loc[df["slot_"] == slot])
    return slots_list


def process_interval_data(df: pd.DataFrame, heroes_info: dict) -> pd.DataFrame:
    """
    process the interval data.
    steps:
        1. Group by minute and slot
        2. Aggregate the data from CONFIG.INTERVAL_AGGREGATE
        3. Add the hero name to the interval data
    :param df: interval pandas dataframe
    :param heroes_info: heros info dictionary
    :return: pandas dataframe
    """
    df = df.groupby(by=[
        pd.Grouper(key='minute', freq=f'{Config.MINUTE_INTERVAL}T'),
        'slot'
    ]).agg(INTERVAL_AGGREGATE).reset_index()
    df.columns = ['_'.join(col).strip() for col in df.columns]

    df['hero_name'] = df['hero_id_last'].apply(
        # Conver the hero id at first to int, because in the data it is a float,
        # then convert it to string, because the heros_info keys are strings
        lambda x: heroes_info[str(int(x))]['name']
    )
    return df


def process_combatlog_damage_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate the combatlog damage data
    :param df: combatlog damage pandas dataframe
    :return: pandas dataframe
    """
    # filter the rows where the attackername starts with 'npc_dota_hero_'
    df = df[df['attackername'].str.startswith('npc_dota_hero_')]
    # filter the rows where the targetname starts with 'npc_dota_hero_'
    df = df[df['targetname'].str.startswith('npc_dota_hero_')]
    # group by  Config.MINUTE_INTERVAL and attackername
    df = df.groupby(by=[
        pd.Grouper(key='minute', freq=f'{Config.MINUTE_INTERVAL}T'),
        'attackername',
    ]).agg({"value": "sum"}).reset_index()
    # rename the value column to damage_dealt_to_heroes
    df = df.rename(columns={"value": "damage_dealt_to_heroes"})
    # Compute the damage per minute
    df['damage_dealt_to_heroes'] = df['damage_dealt_to_heroes'] / Config.MINUTE_INTERVAL
    return df


def _postprocess_common_data(df: pd.DataFrame, winning_team: Team) -> dict:
    slot = int(df['slot_'].unique()[0])
    hero_name = df['hero_name'].unique()[0]
    hero_name = process_hero_name(hero_name)
    player_data = {}
    player_data['team'] = get_player_team(slot).value
    player_data['win'] = player_data['team'] == winning_team.value
    player_data['hero'] = hero_name
    return player_data


def _postprocess_player_data(df: pd.DataFrame) -> dict:
    player_data = {}
    df = df.sort_values(by='minute')
    player_data['stats'] = [
        {
            "minute": row['minute'].minute + Config.MINUTE_INTERVAL,
            f"gold per minute": int(row['gold_agg_per_min']),
            f"last hits per minute": row['lh_agg_per_min'],
            f"denies per minute": row['denies_agg_per_min'],
            f"xp per minute": int(row['xp_agg_per_min']),
            f"kills per minute": row['kills_agg_per_min'],
            f"deaths per minute": row['deaths_agg_per_min'],
            f"assists per minute": row['assists_agg_per_min'],
            f"damage per minute": row[
                'damage_dealt_to_heroes_agg_per_min'] if 'damage_dealt_to_heroes_agg_per_min' in row else 0,
            "teamfight participation": int(row['teamfight_participation_sum']),
        } for _, row in df.iterrows()
    ]
    return player_data


def _postprocess_final_stats(df: pd.DataFrame) -> dict:
    player_data = dict()
    player_data['final stats'] = {
        "gold": int(df['gold_max'].max()),
        "last hits": int(df['lh_max'].max()),
        "denies": int(df['denies_max'].max()),
        "xp": int(df['xp_max'].max()),
        "kills": int(df['kills_max'].max()),
        "deaths": int(df['deaths_max'].max()),
        "assists": int(df['assists_max'].max()),
    }
    return player_data


def _postprocess_benchmarks(hero_id: int, heroes_benchmarks: HeroBenchmarks) -> dict:
    player_data = dict()
    benchmark = heroes_benchmarks.get_benchmark(hero_id, Config.BENCHMARK_PERCENTILE)
    player_data['benchmarks'] = {
        'gold_per_min': int(benchmark.gold_per_min),
        'xp_per_min': int(benchmark.xp_per_min),
        'kills_per_min': round(benchmark.kills_per_min, 1),
        'last_hits_per_min': round(benchmark.last_hits_per_min, 1),
        'damage_per_min': int(benchmark.hero_damage_per_min)
    }
    return player_data


def postprocess_data(df: pd.DataFrame, winning_team: Team, heroes_benchmarks: HeroBenchmarks) -> dict:
    """
    Postprocess the data per player.
    :param df: pandas dataframe
    :param winning_team: Team
    :param heroes_benchmarks: dict
    :return: dict
    """
    player_data = {}

    # extract the hero name from the first row
    hero_id = int(df['hero_id_last'].unique()[0])

    # add common data
    player_data.update(_postprocess_common_data(df, winning_team))
    # add the stats
    player_data.update(_postprocess_player_data(df))
    player_data.update(_postprocess_final_stats(df))
    # add the benchmarks
    player_data.update(_postprocess_benchmarks(hero_id, heroes_benchmarks))

    return player_data

def process_replay_data(df: pd.DataFrame, heroes_info: dict, heroes_benchmarks: HeroBenchmarks) -> dict:
    df = preprocess_data(df)
    interval = get_df_by_type(df, 'interval')
    dota_combatlog_damage = get_df_by_type(df, 'DOTA_COMBATLOG_DAMAGE')
    dota_combatlog_team_building_kill = get_df_by_type(df, 'DOTA_COMBATLOG_TEAM_BUILDING_KILL')

    winning_team = extract_winning_team(dota_combatlog_team_building_kill)
    interval = process_interval_data(interval, heroes_info)
    dota_combatlog_damage = process_combatlog_damage_data(dota_combatlog_damage)

    combined_df = pd.merge(
        left=interval,
        right=dota_combatlog_damage,
        left_on=['minute_', 'hero_name'],
        right_on=['minute', 'attackername'],
        how='outer'
    )
    slots_df = split_data_by_player(combined_df)
    post_process_data = {
        slot: postprocess_data(data, winning_team, heroes_benchmarks)
        for slot, data in enumerate(slots_df)
    }
    return post_process_data

def parse_data(
        input_file_path: Path,
        hero_info_path: Path,
        hero_benchmarks_path: Path,
        output_file_path: Path,

    ) -> None:
    """
    Parse the data
    :param input_file_path: path to the input file
    :param output_file_path: path to the output file
    :return: None
    """
    heroes_info = json.load(hero_info_path.open())
    heroes_benchmarks = HeroBenchmarks(hero_benchmarks_path)

    df = json2df(input_file_path)

    processed_replay_data = process_replay_data(df, heroes_info, heroes_benchmarks)

    match_id = os.path.basename(input_file_path).split('.')[0]
    parsed_gameplay = {match_id: processed_replay_data}
    json.dump(parsed_gameplay, output_file_path.open('w'), indent=4)


if __name__ == '__main__':
    HERO_INFO_PASS = Path('data/heroes.json')
    HERO_BENCHMARKS_PASS = Path('data/benchmarks.json')
    parse_data(
        Path('raw_replays/7623910241.jsonlines'),
        HERO_INFO_PASS,
        HERO_BENCHMARKS_PASS,
        Path('7623910241.json'),
    )
