import requests
import json
import time
from pathlib import Path


# heroes ids are 1, ..., 138
HEROES_IDS = list(range(1, 139))
URL = "https://api.opendota.com/api/benchmarks?hero_id="


def fetch_benchmarks_from_opendota() -> list[dict]:
    """
    Fetch the benchmarks from opendota
    :param hero_ids: list of hero ids
    :return: list of dictionaries
    """
    stats = []
    for hero_id in HEROES_IDS:
        response = requests.get(URL + str(hero_id))
        print(f"Status: {response.status_code}, Hero: {hero_id}/{len(HEROES_IDS)}")
        data = response.json()
        stats.append(data)
        # opendota has a rate limit of 60 requests per minute
        time.sleep(2)
    return stats


def process_stats(stats: list[dict]) -> dict:
    heroes_benchmarks = {
        bm['hero_id']: {
            stat_name: {
                int(val['percentile'] * 100): val['value']
                for val in arr_stats
            }
            for stat_name, arr_stats in bm['result'].items()
        }
        for bm in stats
    }
    return heroes_benchmarks


def fetch_benchmarks(path: Path) -> None:
    """
    Fetch the benchmarks from opendota and save them to a file
    :param path: path to the file
    :return: None
    """
    stats = fetch_benchmarks_from_opendota()
    heroes_benchmarks = process_stats(stats)
    with open(path, "w") as file:
        json.dump(heroes_benchmarks, file, indent=4)


if __name__ == "__main__":
    fetch_benchmarks(Path("data/benchmarks.json"))
