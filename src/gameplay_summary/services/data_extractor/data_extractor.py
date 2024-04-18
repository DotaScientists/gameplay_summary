import pandas as pd
from pathlib import Path
import json
import os
from gameplay_summary.entities import HeroBenchmarks, Team, Benchmark
from gameplay_summary.services.data_extractor.post_processing import postprocess_data
from gameplay_summary.settings import Settings


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
    return df


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


def compute_final_stats(
        interval_df: pd.DataFrame, combat_df: pd.DataFrame, heroes_info: dict,
        max_players: int
    ) -> dict:
    last_second = interval_df["time"].max()
    final_df = interval_df[interval_df["time"] == last_second]
    output_data = dict()

    for slot in range(0, max_players):
        df = final_df.loc[final_df["slot"] == slot].tail(1)
        hero_id = df["hero_id"].item()
        hero_name = heroes_info[str(int(hero_id))]['name']
        slot_combat_df = combat_df[combat_df["attackername"] == hero_name]
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

def process_interval_data(
        df: pd.DataFrame, heroes_info: dict,
        minute_interval: int,
        per_minute_columns: list[str]
    ) -> pd.DataFrame:
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
    required_fields = {"time", "minute", "level", "hero_id", "teamfight_participation", "kills", "deaths", "assists"}
    required_fields.update(per_minute_columns)
    required_fields.update({"denies", "lh", "slot"})
    if not required_fields.issubset(df.columns):
        raise CorruptedDataError(f"Required fields are missing in the interval data {required_fields - set(df.columns)}")

    df["block"] = (df["time"] - 1 ) / 60 // minute_interval
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

    ).reset_index()
    second_group_df = []

    for index, group_df in df.groupby(by=["block",'slot'], as_index=False):
        aggregated_data = {
            col: (group_df[col].max() - group_df[col].min()) / (group_df["time"].max() - group_df["time"].min()) * 60
            for col in per_minute_columns
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


    full_interval_df['hero_name'] = full_interval_df['hero_id'].apply(
        lambda x: heroes_info[str(int(x))]['name']
    )
    full_interval_df["localized_hero_name"] = full_interval_df["hero_id"].apply(
        lambda x: heroes_info[str(int(x))]['localized_name']
    )
    full_interval_df = full_interval_df.fillna(0)


    return full_interval_df


def process_combatlog_damage_data(df: pd.DataFrame, minute_interval: int) -> pd.DataFrame:
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
    df["block"] = (df["time"] - 1 ) / 60 // minute_interval

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
    df['dpm'] = df['damage_dealt_to_heroes'] / (df['time_max'] - df['time_min']).clip(lower=1) * 60
    df = df.drop(columns=["time_max", "time_min"])
    return df




def get_match_length(df: pd.DataFrame) -> int:
    """
    Get the length of the match
    :param df: pandas dataframe
    :return: int
    """
    temp = df[df['type'] == 'interval']
    return temp['time'].max() - temp['time'].min()

def process_replay_data(
        df: pd.DataFrame, heroes_info: dict, heroes_benchmarks: HeroBenchmarks,
        settings: Settings
    ) -> dict:
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

    dota_combatlog_damage = get_df_by_type(df, 'DOTA_COMBATLOG_DAMAGE')
    dota_combatlog_team_building_kill = get_df_by_type(df, 'DOTA_COMBATLOG_TEAM_BUILDING_KILL')

    winning_team = extract_winning_team(dota_combatlog_team_building_kill)
    processed_interval = process_interval_data(interval, heroes_info, settings.MINUTE_INTERVAL, settings.PER_MINUTE_COLUMNS)
    processed_dota_combatlog_damage = process_combatlog_damage_data(dota_combatlog_damage, settings.MINUTE_INTERVAL)
    final_stats = compute_final_stats(interval, processed_dota_combatlog_damage, heroes_info, settings.MAX_PLAYERS)

    combined_df = pd.merge(
        left=processed_interval,
        right=processed_dota_combatlog_damage[["block", "attackername", "dpm"]],
        left_on=['block', 'hero_name'],
        right_on=['block', 'attackername'],
        how='left',
    )
    slots_df = split_data_by_player(combined_df, settings.MAX_PLAYERS)
    post_process_data = {
        slot: postprocess_data(
            data, winning_team, heroes_benchmarks, final_stats, match_length,
            settings.MINUTE_INTERVAL, settings.BENCHMARK_PERCENTILE
        )
        for slot, data in enumerate(slots_df)
    }
    return post_process_data

def extract_data(
        input_file_path: Path,
        hero_info_path: Path,
        hero_benchmarks_path: Path,
        settings: Settings
) -> dict:
    """
    Extract needed information from the raw replay data
    """
    heroes_info = json.load(hero_info_path.open())
    heroes_benchmarks = HeroBenchmarks(hero_benchmarks_path)

    df = pd.read_json(input_file_path, lines=True)

    processed_replay_data = process_replay_data(df, heroes_info, heroes_benchmarks, settings)

    return processed_replay_data

