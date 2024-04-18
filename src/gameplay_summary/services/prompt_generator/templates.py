import re


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
<DATA_START>
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

