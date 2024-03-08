import jsonlines
import pandas as pd
from pathlib import Path
import json

class Config:
    MINUTE_INTERVAL = 5
    MAX_PLAYERS = 10
    COLUMNS_TO_KEEP = [
        "time", "gold", "lh", "xp", "kills", "deaths", "assists",
        "denies", "level", "obs_placed", "sen_placed",
        "towers_killed", "stuns",

    ]



def read_raw_data(path: Path) -> list[dict]:
    events = []
    with jsonlines.open(path) as reader:
        for obj in reader:
            if "type" not in obj:
                continue
            if obj["type"] == "interval":
                events.append(obj)
    return events

def split_data_by_minute(df: pd.DataFrame) -> pd.DataFrame | None:
    df["minute"] = df["time"] / 60
    max_minute = int(df["minute"].max())
    all_slices = []
    for minute in range( 0, max_minute, Config.MINUTE_INTERVAL):
        all_slices.append(df.loc[df["minute"] == minute])
    if len(all_slices) == 0:
        return None
    full_df = pd.concat(all_slices)
    return full_df

def split_data_by_player(df: pd.DataFrame) -> list[pd.DataFrame]:
    slots_list = []
    for slot in range(0, Config.MAX_PLAYERS):
        slots_list.append(df.loc[df["slot"] == slot])
    return slots_list


def convert_to_dict(frames: list[pd.DataFrame], columns_to_keep: list[str] = None) -> dict[int, dict]:
    output_data = dict()
    for i, df in enumerate(frames):
        hero_id = df["hero_id"].iloc[0]
        unit = df["unit"].iloc[0]
        team = "radiant" if i < 5 else "dire"
        df = df if columns_to_keep is None else df[columns_to_keep]
        output_data[i] = {
            "hero_id": hero_id,
            "unit": unit,
            "team": team,
            "stats": df.to_dict(orient="records")
        }

    return output_data



def prepare_data(path: Path) -> dict[int, dict]:
    events = read_raw_data(path)
    df = pd.DataFrame(events)
    slices_df = split_data_by_minute(df)
    slices_df = split_data_by_player(slices_df)
    return convert_to_dict(slices_df, Config.COLUMNS_TO_KEEP)

def parse_replays(folder: Path, output_path: Path):
    replays = dict()
    for file in folder.iterdir():
        if file.suffix == ".jsonlines":
            data = prepare_data(file)
            replays[file.stem] = data
    output_path.write_text(json.dumps(replays, indent=4))


if __name__ == "__main__":
    parse_replays(Path("raw_replays"), Path("output.json"))


