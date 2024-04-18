from pathlib import Path
import json
from dataclasses import dataclass
from pydantic import BaseModel
from enum import Enum


@dataclass
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
                percentile_benchmarks[int(percentile)] = Benchmark(
                    **{
                        stat_name: benchmark_values[stat_name][percentile]
                        for stat_name in stats_names
                    }
                )
            self.benchmarks[int(hero_id)] = percentile_benchmarks

    def get_benchmark(self, hero_id: int, percentile: int) -> Benchmark:
        return self.benchmarks[hero_id][percentile]


class Team(Enum):
    radiant = "radiant"
    dire = "dire"


class DownloadableMatch(BaseModel):
    match_id: int
    replay_salt: int
    cluster: int

