import pandas as pd
from gameplay_summary.entities import Team, Benchmark, HeroBenchmarks
from gameplay_summary.settings import Settings

class PostProcessor:
    def __init__(self, heroes_benchmarks: HeroBenchmarks, settings: Settings):
        self.heroes_benchmarks = heroes_benchmarks
        self.settings = settings

    def _postprocess_benchmarks(
            self,
            hero_id: int, match_length: int,
    ) -> dict:
        """
        Adds average stats for this hero.
        """
        benchmark = self.heroes_benchmarks.get_benchmark(
            hero_id, self.settings.BENCHMARK_PERCENTILE
        )
        match_length = match_length / 60
        return {
            'Total gold': int(benchmark.gold_per_min * match_length),
            'Total xp': int(benchmark.xp_per_min * match_length),
            'Total kills': round(benchmark.kills_per_min * match_length),
            'Total last hits': round(benchmark.last_hits_per_min * match_length),
            'Total damage': round(benchmark.hero_damage_per_min * match_length)
        }

    def _postprocess_common_data(self, df: pd.DataFrame, winning_team: Team, slot: int) -> dict:
        """
        Adds data that doesn't change during the game.
        :param df:
        :param winning_team:
        :return:
        """
        hero_name = df['localized_hero_name'].unique()[0]
        player_data = {}
        player_data['team'] = get_player_team(slot).value
        player_data['win'] = player_data['team'] == winning_team.value
        player_data['hero'] = hero_name
        player_data["interval"] = self.settings.MINUTE_INTERVAL
        return player_data

    def postprocess_data(
            self,
            df: pd.DataFrame, winning_team: Team, final_stats: dict,
            match_length: int, slot: int
    ) -> dict:
        """
        Postprocess the data per player.
        """

        # extract the hero name from the first row
        hero_id = int(df['hero_id'].unique()[0])

        # add common data
        player_data = self._postprocess_common_data(df, winning_team, slot)
        # add the stats
        player_data["stats"] = _postprocess_player_data(df)
        player_data["final stats"] = _postprocess_final_stats(final_stats[slot])
        # add the benchmarks
        player_data["benchmarks"] = self._postprocess_benchmarks(hero_id, match_length)

        return player_data

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

def _postprocess_player_data(df: pd.DataFrame) -> list:
    """
    Adds the stats for 5 minute intervals.
    :param df:
    :return:
    """
    df = df.sort_values(by='minute')

    columns_to_clean = [
        "gold", "xp", "lh", "denies", "kills", "deaths", "assists", "kda", "dpm", "teamfight_participation"
    ]
    int_columns = [
        "gold", "xp", "lh", "denies", "kills", "deaths", "assists", "dpm", "teamfight_participation"
    ]
    for columns in columns_to_clean:
        df[columns] = df[columns].apply(lambda x: 0 if pd.isna(x) else x)
    for columns in int_columns:
        df[columns] = df[columns].astype(int)

    return [
        {
            "minute": row['minute'],
            f"gold per minute": row['gold'],
            f"last hits": row['lh'],
            f"denies": row['denies'],
            f"xp per minute": row['xp'],
            f"kills": row['kills'],
            f"deaths": row['deaths'],
            f"assists": row['assists'],
            f"KDA": round(row["kda"], 1),
            f"damage per minute":  row['dpm'],
            "teamfight seconds": row['teamfight_participation']
        } for _, row in df.iterrows()
    ]

def _postprocess_final_stats(final_stats: dict[str, float]) -> dict:
    output = {
        key: int(value)
        for key, value in final_stats.items()
    }
    return output
