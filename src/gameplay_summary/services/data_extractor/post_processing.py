import pandas as pd
from gameplay_summary.entities import Team, Benchmark, HeroBenchmarks

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


def _postprocess_common_data(df: pd.DataFrame, winning_team: Team, minute_interval: int) -> dict:
    """
    Adds data that doesn't change during the game.
    :param df:
    :param winning_team:
    :return:
    """
    slot = int(df['slot'].unique()[0])
    hero_name = df['localized_hero_name'].unique()[0]
    hero_name = process_hero_name(hero_name)
    player_data = {}
    player_data['team'] = get_player_team(slot).value
    player_data['win'] = player_data['team'] == winning_team.value
    player_data['hero'] = hero_name
    player_data["interval"] = minute_interval
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
            f"gold per minute": int(row['gold']) if row["gold"] else 0,
            f"last hits": int(row['lh']),
            f"denies": int(row['denies']),
            f"xp per minute": int(row['xp']),
            f"kills": int(row['kills']),
            f"deaths": int(row['deaths']),
            f"assists": int(row['assists']),
            f"KDA": round(row["kda"], 1),
            f"damage per minute":  int(row['dpm']) if not pd.isna(row['dpm']) else 0,
            "teamfight seconds": int(row['teamfight_participation']),
        } for _, row in df.iterrows()
    ]
    return player_data


def _postprocess_benchmarks(
        hero_id: int, heroes_benchmarks: HeroBenchmarks, match_length: int,
        benchmark_percentile: int
    ) -> dict:
    """
    Adds average stats for this hero.
    :param hero_id:
    :param heroes_benchmarks:
    :return:
    """
    player_data = dict()
    benchmark = heroes_benchmarks.get_benchmark(hero_id, benchmark_percentile)
    match_length = match_length / 60
    player_data['benchmarks'] = {
        'Total gold': int(benchmark.gold_per_min * match_length),
        'Total xp': int(benchmark.xp_per_min * match_length),
        'Total kills': round(benchmark.kills_per_min * match_length),
        'Total last hits': round(benchmark.last_hits_per_min * match_length),
        'Total damage': round(benchmark.hero_damage_per_min * match_length)
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
        match_length: int, minute_interval: int, benchmark_percentile: int
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
    player_data.update(_postprocess_common_data(df, winning_team, minute_interval))
    # add the stats
    player_data.update(_postprocess_player_data(df))
    player_data["final stats"] = _postprocess_final_stats(final_stats[slot])
    # add the benchmarks
    player_data.update(_postprocess_benchmarks(hero_id, heroes_benchmarks, match_length, benchmark_percentile))

    return player_data