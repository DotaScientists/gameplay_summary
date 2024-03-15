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

    HERO_INFO_PASS: Path = Path('data/heroes.json')
    HERO_BENCHMARKS_PASS: Path = Path('data/benchmarks.json')
    # PER_MINUTE_COLUMNS: list[str] = ["gold", "lh", "xp", "kills", "deaths", "assists", "denies"]
    PER_MINUTE_COLUMNS: list[str] = ["gold", "lh", "xp"]


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
    return df[df['type'] == type].copy()


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
    df = df[df['time'] > 0].reset_index(drop=True)
    df["minute"] = df["time"] // 60
    df = df.sort_values(by=["time", "slot"])
    # df['minute'] = pd.to_datetime(df['minute'], unit='m')
    return df


def split_data_by_player(df: pd.DataFrame) -> list[pd.DataFrame]:
    """
    Split the data by player
    :param df: pandas dataframe
    :return: list of pandas dataframes
    """
    slots_list = []
    for slot in range(0, Config.MAX_PLAYERS):
        slots_list.append(df.loc[df["slot"] == slot])
    return slots_list


def compute_final_stats(df: pd.DataFrame) -> dict:
    last_second = df["time"].max()
    final_df = df[df["time"] == last_second]
    output_data = dict()
    for slot in range(0, Config.MAX_PLAYERS):
        df = final_df.loc[final_df["slot"] == slot].tail(1)
        output_data[slot] = {
            "Total hold": df["gold"].item(),
            "Total last hits": df["lh"].item(),
            "Total denies": df["denies"].item(),
            "Total xp": df["xp"].item(),
            "Total kills": df["kills"].item(),
            "Total deaths": df["deaths"].item(),
            "Total assists": df["assists"].item(),
        }
    return output_data

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
    df["block"] = (df["time"] - 1 ) / 60 // Config.MINUTE_INTERVAL
    first_group_df = df.groupby(by=[
        "block",
        'slot'
    ], as_index=False).agg(
        time=pd.NamedAgg(column="time", aggfunc="last"),
        minute=pd.NamedAgg(column="minute", aggfunc="last"),
        level=pd.NamedAgg(column="level", aggfunc="last"),
        hero_id=pd.NamedAgg(column="hero_id", aggfunc="last"),
        teamfight_participation=pd.NamedAgg(column="teamfight_participation", aggfunc="sum"),

        kills = pd.NamedAgg(column="kills", aggfunc="max"),
        deaths = pd.NamedAgg(column="deaths", aggfunc="max"),
        assists = pd.NamedAgg(column="assists", aggfunc="max"),
        denies=pd.NamedAgg(column="denies", aggfunc="max"),
    ).reset_index()
    second_group_df = []

    for index, group_df in df.groupby(by=["block",'slot'], as_index=False):
        aggregated_data = {
            col: (group_df[col].max() - group_df[col].min()) / (group_df["time"].max() - group_df["time"].min()) * 60
            for col in Config.PER_MINUTE_COLUMNS
        }
        aggregated_data["minute"] = group_df["minute"].max()
        aggregated_data["slot"] = group_df["slot"].max()
        second_group_df.append(aggregated_data)
    second_group_df = pd.DataFrame(second_group_df)

    full_interval_df = first_group_df.merge(
        second_group_df,
        on=["minute", "slot"],
        how="left"
    )


    full_interval_df['hero_name'] = full_interval_df['hero_id'].apply(
        # Conver the hero id at first to int, because in the data it is a float,
        # then convert it to string, because the heros_info keys are strings
        lambda x: heroes_info[str(int(x))]['name']
    )
    return full_interval_df


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

    df = df[(df["attackerhero"] == 1) & (df["targethero"] == 1)]
    # group by  Config.MINUTE_INTERVAL and attackername
    df["block"] = (df["time"] - 1 ) / 60 // Config.MINUTE_INTERVAL

    df = df.groupby(by=[
        "block",
        'attackername',
    ], as_index=False).agg(
        damage_dealt_to_heroes=pd.NamedAgg(column="value", aggfunc="sum"),
        minute=pd.NamedAgg(column="minute", aggfunc="last"),
        time_max=pd.NamedAgg(column="time", aggfunc="max"),
        time_min=pd.NamedAgg(column="time", aggfunc="min"),
    )
    # Compute the damage per minute
    df['damage_dealt_to_heroes'] = df['damage_dealt_to_heroes'] / (df['time_max'] - df['time_min']).clip(lower=1) * 60
    df = df.drop(columns=["time_max", "time_min"])
    return df


def _postprocess_common_data(df: pd.DataFrame, winning_team: Team) -> dict:
    """
    Adds data that doesn't change during the game.
    :param df:
    :param winning_team:
    :return:
    """
    slot = int(df['slot'].unique()[0])
    hero_name = df['hero_name'].unique()[0]
    hero_name = process_hero_name(hero_name)
    player_data = {}
    player_data['team'] = get_player_team(slot).value
    player_data['win'] = player_data['team'] == winning_team.value
    player_data['hero'] = hero_name
    return player_data


def _postprocess_player_data(df: pd.DataFrame) -> dict:
    """
    Adds the stats for 5 minute intervals.
    :param df:
    :return:
    """
    player_data = {}
    df = df.sort_values(by='minute')

    player_data['stats'] = [
        {
            "minute": row['minute'],
            f"gold per minute": int(row['gold']),
            f"last hits per minute": round(row['lh'], 1),
            f"denies": round(row['denies']),
            f"xp per minute": int(row['xp']),
            f"kills": round(row['kills']),
            f"deaths": round(row['deaths']),
            f"assists": round(row['assists']),
            f"damage per minute":  int(row['damage_dealt_to_heroes']) if not pd.isna(row['damage_dealt_to_heroes']) else 0,
            "teamfight seconds": int(row['teamfight_participation']),
        } for _, row in df.iterrows()
    ]
    return player_data


def _postprocess_benchmarks(hero_id: int, heroes_benchmarks: HeroBenchmarks, match_length: int) -> dict:
    """
    Adds average stats for this hero.
    :param hero_id:
    :param heroes_benchmarks:
    :return:
    """
    player_data = dict()
    benchmark = heroes_benchmarks.get_benchmark(hero_id, Config.BENCHMARK_PERCENTILE)
    match_length = match_length / 60
    player_data['benchmarks'] = {
        'total gold': int(benchmark.gold_per_min * match_length),
        'total xp': int(benchmark.xp_per_min * match_length),
        'total kills': round(benchmark.kills_per_min * match_length),
        'total last hits': round(benchmark.last_hits_per_min * match_length),
        'damage_per_min': int(benchmark.hero_damage_per_min)
    }
    return player_data

def _postprocess_final_stats(final_stats: dict[str, float]) -> dict:
    output = {
        key: int(value)
        for key, value in final_stats.items()
    }
    return output


def postprocess_data(
        df: pd.DataFrame, winning_team: Team, heroes_benchmarks: HeroBenchmarks, final_stats: dict,
        match_length: int
    ) -> dict:
    """
    Postprocess the data per player.
    :param df: pandas dataframe
    :param winning_team: Team
    :param heroes_benchmarks: dict
    :return: dict
    """
    player_data = {}

    # extract the hero name from the first row
    hero_id = int(df['hero_id'].unique()[0])
    slot = df['slot'].unique()[0]

    # add common data
    player_data.update(_postprocess_common_data(df, winning_team))
    # add the stats
    player_data.update(_postprocess_player_data(df))
    player_data["final stats"] = _postprocess_final_stats(final_stats[slot])
    # add the benchmarks
    player_data.update(_postprocess_benchmarks(hero_id, heroes_benchmarks, match_length))

    return player_data

def get_match_length(df: pd.DataFrame) -> int:
    """
    Get the length of the match
    :param df: pandas dataframe
    :return: int
    """
    temp = df[df['type'] == 'interval']
    return temp['time'].max() - temp['time'].min()

def process_replay_data(df: pd.DataFrame, heroes_info: dict, heroes_benchmarks: HeroBenchmarks) -> dict:
    """
    Processes the raw replay data and outputs the final json object.
    :param df:
    :param heroes_info:
    :param heroes_benchmarks:
    :return:
    """
    match_length = get_match_length(df)
    df = preprocess_data(df)
    interval = get_df_by_type(df, 'interval')
    final_stats = compute_final_stats(interval)
    dota_combatlog_damage = get_df_by_type(df, 'DOTA_COMBATLOG_DAMAGE')
    dota_combatlog_team_building_kill = get_df_by_type(df, 'DOTA_COMBATLOG_TEAM_BUILDING_KILL')

    winning_team = extract_winning_team(dota_combatlog_team_building_kill)
    interval = process_interval_data(interval, heroes_info)
    dota_combatlog_damage = process_combatlog_damage_data(dota_combatlog_damage)

    combined_df = pd.merge(
        left=interval,
        right=dota_combatlog_damage[["block", "attackername", "damage_dealt_to_heroes"]],
        left_on=['block', 'hero_name'],
        right_on=['block', 'attackername'],
        how='left',
    )
    slots_df = split_data_by_player(combined_df)
    post_process_data = {
        slot: postprocess_data(data, winning_team, heroes_benchmarks, final_stats, match_length)
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
    project_root = Path().absolute()
    parse_data(
        Path('raw_replays/7623910241.jsonlines'),
        project_root / Config.HERO_INFO_PASS,
        project_root / Config.HERO_BENCHMARKS_PASS,
        Path('7623910241.json'),
    )
