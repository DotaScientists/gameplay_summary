import re
from pathlib import Path
import json
import copy

INTERVAL_TEMPLATE = """
{
    Minute: <MINUTE>,
    Gold per minute: <GPM>,
    Last hits: <LAST_HITS>,
    Denies: <DENIES>,
    Xp per minute: <XPM>,
    Kills: <KILLS>,
    Deaths: <DEATHS>,
    Assists: <ASSISTS>,
    KDA: <KDA>,
    Damage per minute: <DPM>,
    Seconds in teamfight: <TEAMFIGHT_SECONDS>
},
"""

PROMPT_TEMPLATE = """
Dota 2 is a popular multiplayer online battle arena (MOBA) game.
As a professional Dota 2 analyst, your task is to analyze the provided data for a specific player in a Dota 2 match and evaluate their performance.
Your analysis should provide recommendations for improvement.
Your analysis should not exceed 10 sentences.
Think step by step.
Don't include any information that you are not confident in.
Include only valuable information that will help understand the performance of this player.
The data includes various statistics recorded at different time intervals during the match,
such as gold per minute, last hits per minute, kills, deaths, assists, and damage per minute.
In the last parts of the game the XP earned can drop to 0, because the hero achieved the max level and can't get any more xp.

General information:
The team <MATCH_OUTCOME>
The Hero played by the player: <HERO>

Per <MINUTE_INTERVAL> minute interval statistics:
<INTERVALS>

Total values achieved by <HERO> by the end of the match:
Total kills: <TOTAL_KILLS>
Total deaths: <TOTAL_DEATHS>
Total assists: <TOTAL_ASSISTS>
Total KDA: <TOTAL_KDA>


Here is the data comparing total values achieved by this player vs other players playing <HERO>:
Total gold: <TOTAL_GOLD> vs average <BENCHMARK_GOLD>
Total XP <TOTAL_XP> vs <BENCHMARK_XP>
Total last hist: <TOTAL_LH> vs average <BENCHMARK_LH>
Total Kills <TOTAL_KILLS> vs average <BENCHMARK_KILLS>
Total damage <TOTAL_DAMAGE> vs average <BENCHMARK_DAMAGE>
"""
class IntervalRegex:
    def __init__(self):
        self.minute_regex = re.compile("<MINUTE>")
        self.gpm_regex = re.compile("<GPM>")
        self.last_hits_regex = re.compile("<LAST_HITS>")
        self.denies_regex = re.compile("<DENIES>")
        self.xpm_regex = re.compile("<XPM>")
        self.kills_regex = re.compile("<KILLS>")
        self.deaths_regex = re.compile("<DEATHS>")
        self.assists_regex = re.compile("<ASSISTS>")
        self.kda_regex = re.compile("<KDA>")
        self.dpm_regex = re.compile("<DPM>")
        self.teamfight_seconds_regex = re.compile("<TEAMFIGHT_SECONDS>")

class PromptRegex:
    def __init__(self):
        self.friendly_team_regex = re.compile("<FRIENDLY_TEAM>")
        self.enemy_team_regex = re.compile("<ENEMY_TEAM>")
        self.match_outcome_regex = re.compile("<MATCH_OUTCOME>")
        self.hero_regex = re.compile("<HERO>")
        self.minute_interval_regex = re.compile("<MINUTE_INTERVAL>")
        self.intervals_regex = re.compile("<INTERVALS>")
        self.total_kills_regex = re.compile("<TOTAL_KILLS>")
        self.total_deaths_regex = re.compile("<TOTAL_DEATHS>")
        self.total_assists_regex = re.compile("<TOTAL_ASSISTS>")
        self.total_kda_regex = re.compile("<TOTAL_KDA>")
        self.total_gold_regex = re.compile("<TOTAL_GOLD>")
        self.benchmark_gold_regex = re.compile("<BENCHMARK_GOLD>")
        self.total_xp_regex = re.compile("<TOTAL_XP>")
        self.benchmark_xp_regex = re.compile("<BENCHMARK_XP>")
        self.total_lh_regex = re.compile("<TOTAL_LH>")
        self.benchmark_lh_regex = re.compile("<BENCHMARK_LH>")
        self.total_kills_regex = re.compile("<TOTAL_KILLS>")
        self.benchmark_kills_regex = re.compile("<BENCHMARK_KILLS>")
        self.total_damage_regex = re.compile("<TOTAL_DAMAGE>")
        self.benchmark_damage_regex = re.compile("<BENCHMARK_DAMAGE>")

class PromptGenerator:

    def __init__(self):
        self.interval_regex = IntervalRegex()
        self.prompt_regex = PromptRegex()


    def _process_common_data(self, prompt: str, hero_data: dict ) -> str:
        match_outcome = "won" if hero_data["win"] == 1 else "lost"
        prompt = self.prompt_regex.match_outcome_regex.sub(match_outcome, prompt)
        prompt = self.prompt_regex.hero_regex.sub(hero_data["hero"], prompt)
        prompt = self.prompt_regex.minute_interval_regex.sub(str(hero_data["interval"]), prompt)
        return prompt

    def _process_interval_data(self, prompt: str, hero_data: dict) -> str:
        intervals = []
        for interval_data in hero_data["stats"]:
            interval_text = copy.deepcopy(INTERVAL_TEMPLATE)
            interval_text = self.interval_regex.minute_regex.sub(str(interval_data["minute"]), interval_text)
            interval_text = self.interval_regex.gpm_regex.sub(str(interval_data["gold per minute"]), interval_text)
            interval_text = self.interval_regex.last_hits_regex.sub(str(interval_data["last hits"]), interval_text)
            interval_text = self.interval_regex.denies_regex.sub(str(interval_data["denies"]), interval_text)
            interval_text = self.interval_regex.xpm_regex.sub(str(interval_data["xp per minute"]), interval_text)
            interval_text = self.interval_regex.kills_regex.sub(str(interval_data["kills"]), interval_text)
            interval_text = self.interval_regex.deaths_regex.sub(str(interval_data["deaths"]), interval_text)
            interval_text = self.interval_regex.assists_regex.sub(str(interval_data["assists"]), interval_text)
            interval_text = self.interval_regex.kda_regex.sub(str(interval_data["KDA"]), interval_text)
            interval_text = self.interval_regex.dpm_regex.sub(str(interval_data["damage per minute"]), interval_text)
            interval_text = self.interval_regex.teamfight_seconds_regex.sub(str(interval_data["teamfight seconds"]), interval_text)
            intervals.append(interval_text)
        prompt = self.prompt_regex.intervals_regex.sub("".join(intervals), prompt)
        return prompt

    def _process_total_data(self, prompt: str, hero_data: dict) -> str:
        total_data = hero_data["final stats"]
        prompt = self.prompt_regex.total_kills_regex.sub(str(total_data["Total kills"]), prompt)
        prompt = self.prompt_regex.total_deaths_regex.sub(str(total_data["Total deaths"]), prompt)
        prompt = self.prompt_regex.total_assists_regex.sub(str(total_data["Total assists"]), prompt)
        prompt = self.prompt_regex.total_kda_regex.sub(str(total_data["Total KDA"]), prompt)
        return prompt

    def _process_benchmark_data(self, prompt: str, hero_data: dict) -> str:
        total_data = hero_data["final stats"]
        benchmark_data = hero_data["benchmarks"]
        prompt = self.prompt_regex.total_gold_regex.sub(str(total_data["Total gold"]), prompt)
        prompt = self.prompt_regex.benchmark_gold_regex.sub(str(benchmark_data["Total gold"]), prompt)
        prompt = self.prompt_regex.total_xp_regex.sub(str(total_data["Total xp"]), prompt)
        prompt = self.prompt_regex.benchmark_xp_regex.sub(str(benchmark_data["Total xp"]), prompt)
        prompt = self.prompt_regex.total_lh_regex.sub(str(total_data["Total last hits"]), prompt)
        prompt = self.prompt_regex.benchmark_lh_regex.sub(str(benchmark_data["Total last hits"]), prompt)
        prompt = self.prompt_regex.total_kills_regex.sub(str(total_data["Total kills"]), prompt)
        prompt = self.prompt_regex.benchmark_kills_regex.sub(str(benchmark_data["Total kills"]), prompt)
        prompt = self.prompt_regex.total_damage_regex.sub(str(total_data["Total damage"]), prompt)
        prompt = self.prompt_regex.benchmark_damage_regex.sub(str(benchmark_data["Total damage"]), prompt)
        return prompt

    def generate_prompt(self, parsed_replay_path: Path):
        with open(parsed_replay_path, "r") as file:
            data: dict = json.load(file)
        for replay_id, replay_data in data.items():
            for slot, hero_data in replay_data.items():
                prompt = copy.deepcopy(PROMPT_TEMPLATE)
                prompt = self._process_common_data(prompt, hero_data)
                prompt = self._process_interval_data(prompt, hero_data)
                prompt = self._process_total_data(prompt, hero_data)
                prompt = self._process_benchmark_data(prompt, hero_data)
                yield prompt


if __name__ == "__main__":
    prompt_generator = PromptGenerator()
    for prompt in prompt_generator.generate_prompt(Path('7623910241.json'),):
        Path("output_prompt.txt").write_text(prompt)
        break



