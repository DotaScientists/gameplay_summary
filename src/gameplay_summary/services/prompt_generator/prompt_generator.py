import copy

from gameplay_summary.services.prompt_generator.templates import IntervalRegex, PromptRegex, PROMPT_TEMPLATE, \
    INTERVAL_TEMPLATE


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

    def _compare(self, hero_value: float, benchmark_value: float) -> str:
        if hero_value > benchmark_value:
            return "higher than"
        elif hero_value < benchmark_value:
            return "lower than"
        else:
            return "equal to"

    def _process_comparison_data(self, prompt: str, hero_data: dict) -> str:
        total_data = hero_data["final stats"]
        benchmark_data = hero_data["benchmarks"]
        prompt = self.prompt_regex.gold_comparison_regex.sub(self._compare(total_data["Total gold"], benchmark_data["Total gold"]), prompt)
        prompt = self.prompt_regex.xp_comparison_regex.sub(self._compare(total_data["Total xp"], benchmark_data["Total xp"]), prompt)
        prompt = self.prompt_regex.lh_comparison_regex.sub(self._compare(total_data["Total last hits"], benchmark_data["Total last hits"]), prompt)
        prompt = self.prompt_regex.kills_comparison_regex.sub(self._compare(total_data["Total kills"], benchmark_data["Total kills"]), prompt)
        prompt = self.prompt_regex.damage_comparison_regex.sub(self._compare(total_data["Total damage"], benchmark_data["Total damage"]), prompt)
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

    def generate_prompt(self, replay_data: dict):
        for slot, hero_data in replay_data.items():
            prompt = copy.deepcopy(PROMPT_TEMPLATE)
            prompt = self._process_common_data(prompt, hero_data)
            prompt = self._process_interval_data(prompt, hero_data)
            prompt = self._process_total_data(prompt, hero_data)
            prompt = self._process_benchmark_data(prompt, hero_data)
            prompt = self._process_comparison_data(prompt, hero_data)
            output = prompt.split("<DATA_START>")
            yield slot, output[0], output[1]
