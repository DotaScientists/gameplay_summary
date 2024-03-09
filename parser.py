import pandas as pd
from pathlib import Path
import json
import os


HERO_INFO_PASS = Path('data/heroes.json')
HERO_BENCHMARKS_PASS = Path('data/benchmarks.json')

class HeroBenchmark:
    gold: str = "gold_per_min"
    xp: str = "xp_per_min"
    kills: str = "kills_per_min"
    lh: str = "last_hits_per_min"
    damage_dealt_to_heroes: str = "hero_damage_per_min"
    hero_healing: str = "hero_healing_per_min"
    tower_damage: str = "tower_damage"


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


def is_radiant_win(df: pd.DataFrame) -> bool:
    """
    Check if radiant won the game
    in the data with type 'DOTA_COMBATLOG_TEAM_BUILDING_KILL', last row's in column targetname
    corresponds to which fort was destroyed. If it is 'npc_dota_badguys_fort' then radiant won the game
    :param df: pandas dataframe
    :return: bool
    """
    # get last row of the dataframe
    last_row = df.iloc[-1]
    return last_row['targetname'] == 'npc_dota_badguys_fort'


def json2df(path: Path) -> pd.DataFrame:
    """
    Read json file and return a pandas dataframe
    :param path: path to the json file
    :return: pandas dataframe with the json data
    """
    df = pd.read_json(path, lines=True)
    return df


def get_player_team(slot: int) -> str:
    """
    Get the team of the player
    :param slot: slot of the player
    :return: team of the player
    """
    return "radiant" if slot < 5 else "dire"


def get_df_by_type(df: pd.DataFrame, type: str) -> pd.DataFrame:
    """
    Get a dataframe by type
    :param df: pandas dataframe
    :param type: type of the dataframe
    :return: pandas dataframe
    """
    return df[df['type'] == type]


def preprocess_data(df: pd.DataFrame) -> tuple:
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
    interval, dota_combatlog_damage, dota_combatlog_team_building_kill = (
        get_df_by_type(df, 'interval'),
        get_df_by_type(df, 'DOTA_COMBATLOG_DAMAGE'),
        get_df_by_type(df, 'DOTA_COMBATLOG_TEAM_BUILDING_KILL'),
    )
    return interval, dota_combatlog_damage, dota_combatlog_team_building_kill


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
    df.columns = ['_'.join(col).strip() for col in df.columns.values]

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


def postprocess_data(df: pd.DataFrame, radiant_win: bool, heroes_benchmarks: dict) -> dict:
    """
    Postprocess the data per player.
    :param df: pandas dataframe
    :param radiant_win: bool
    :param heroes_benchmarks: dict
    :return: dict
    """
    player_data = {}
    slot = int(df['slot_'].unique()[0])
    # extract the hero name from the first row
    hero_name = df['hero_name'].unique()[0][len('npc_dota_hero_'):]
    hero_id = str(int(df['hero_id_last'].unique()[0]))

    # add common data
    player_data['team'] = get_player_team(slot)
    player_data['win'] = radiant_win if player_data['team'] == 'radiant' else not radiant_win
    player_data['hero'] = hero_name

    # add the stats
    df = df.sort_values(by='minute')
    player_data['stats'] = [
        {
            "minute": row['minute'].minute + Config.MINUTE_INTERVAL,
            f"gold_per_min": row['gold_agg_per_min'],
            f"lh_per_min": row['lh_agg_per_min'],
            f"denies_per_min": row['denies_agg_per_min'],
            f"xp_per_min": row['xp_agg_per_min'],
            f"kills_per_min": row['kills_agg_per_min'],
            f"deaths_per_min": row['deaths_agg_per_min'],
            f"assists_per_min": row['assists_agg_per_min'],
            f"damage_per_min": row['damage_dealt_to_heroes_agg_per_min'] if 'damage_dealt_to_heroes_agg_per_min' in row else 0,
            "teamfight_participation": row['teamfight_participation_sum'],
        } for _, row in df.iterrows()
    ]
    player_data['final_stats'] = {
        "gold": int(df['gold_max'].max()),
        "lh": int(df['lh_max'].max()),
        "denies": int(df['denies_max'].max()),
        "xp": int(df['xp_max'].max()),
        "kills": int(df['kills_max'].max()),
        "deaths": int(df['deaths_max'].max()),
        "assists": int(df['assists_max'].max()),
    }
    # add the benchmarks
    player_data['benchmarks'] = {
        'gold_per_min': heroes_benchmarks[hero_id][HeroBenchmark.gold][Config.BENCHMARK_PERCENTILE],
        'xp_per_min': heroes_benchmarks[hero_id][HeroBenchmark.xp][Config.BENCHMARK_PERCENTILE],
        'kills_per_min': heroes_benchmarks[hero_id][HeroBenchmark.kills][Config.BENCHMARK_PERCENTILE],
        'lh_per_min': heroes_benchmarks[hero_id][HeroBenchmark.lh][Config.BENCHMARK_PERCENTILE],
        'damage_per_min': heroes_benchmarks[hero_id][HeroBenchmark.damage_dealt_to_heroes][Config.BENCHMARK_PERCENTILE],
    }
    return player_data


def parse_data(input_file_path: Path, output_file_path: Path) -> None:
    """
    Parse the data
    :param input_file_path: path to the input file
    :param output_file_path: path to the output file
    :return: None
    """
    heroes_info = json.load(HERO_INFO_PASS.open())
    heroes_benchmarks = json.load(HERO_BENCHMARKS_PASS.open())

    df = json2df(input_file_path)
    interval, dota_combatlog_damage, dota_combatlog_team_building_kill = preprocess_data(df)
    radiant_win = is_radiant_win(dota_combatlog_team_building_kill)
    interval = process_interval_data(interval, heroes_info)
    dota_combatlog_damage = process_combatlog_damage_data(dota_combatlog_damage)
    combined_df = pd.merge(
        left=interval,
        right=dota_combatlog_damage,
        left_on=['minute_', 'hero_name'],
        right_on=['minute', 'attackername'],
        how='outer'
    )
    slots = split_data_by_player(combined_df)
    post_process_data = {slot: postprocess_data(data, radiant_win, heroes_benchmarks) for slot, data in enumerate(slots)}

    match_id = os.path.basename(input_file_path).split('.')[0]
    parsed_gameplay = {match_id: post_process_data}
    json.dump(parsed_gameplay, output_file_path.open('w'), indent=4)

if __name__ == '__main__':
    parse_data(
        Path('/Users/nktrn/Workspace/Dota2/data/parsed/7627350530.jsonlines'),
        Path('7627350530.json'),
    )

