from groq import Groq
from groq import Stream
from groq.lib.chat_completion_chunk import ChatCompletionChunk
from dataclasses import dataclass
from gameplay_summary.settings import Settings
import time
from retry import retry

@dataclass
class PromptOutput:
    output: str
    input_tokens: int
    output_tokens: int
    model: str
    timestamp: int

    match_id: int
    slot: int
    instruction_prompt: str
    data_prompt: str


class GroqConnector:
    def __init__(
        self,
        settings: Settings
    ):
        self.client = Groq(api_key=settings.GROQ_API_KEY)
        self.settings = settings
        self.last_request_time = 0

    @retry(tries=5, delay=10)
    def _get_single_completion(self, instruction_prompt: str, data_prompt: str) -> Stream[ChatCompletionChunk]:
        delay_time = self.settings.GROQ_DELAY - (time.time() - self.last_request_time)
        delay_time = max(0.0, delay_time)
        if delay_time:
            time.sleep(delay_time)
        completion = self.client.chat.completions.create(
            model=self.settings.GROQ_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": instruction_prompt
                },
                {
                    "role": "user",
                    "content": data_prompt
                }
            ],
            temperature=self.settings.GROQ_TEMPERATURE,
            max_tokens=self.settings.GROQ_MAX_TOKENS,
            top_p=1,
            stream=False,
        )
        self.last_request_time = time.time()
        return completion

    def _parse_response(self, completion: Stream[ChatCompletionChunk]) -> PromptOutput:
        output = None
        input_tokens = None
        output_tokens = None
        model = None
        for chunk in completion:
            if chunk[0] == "choices":
                output = chunk[1][0].message.content
            elif chunk[0] == "model":
                model = chunk[1]
            elif chunk[0] == "usage":
                input_tokens = chunk[1].prompt_tokens
                output_tokens = chunk[1].completion_tokens
        output = PromptOutput(
            output=output,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            model=model,
            timestamp=int(time.time()),
            match_id=0,
            slot=0,
            instruction_prompt="",
            data_prompt=""
        )
        return output

    def get_response(self, instruction_prompt: str, data_prompt: str) -> PromptOutput:
        completion = self._get_single_completion(instruction_prompt, data_prompt)
        output = self._parse_response(completion)
        output.instruction_prompt = instruction_prompt
        output.data_prompt = data_prompt
        return output
