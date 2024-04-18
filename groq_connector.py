from groq import Groq
import os
from groq import Stream
from groq.lib.chat_completion_chunk import ChatCompletionChunk
from dataclasses import dataclass

@dataclass
class PromptOutput:
    output: str
    input_tokens: int
    output_tokens: int
    model: str


class GroqConnector:
    def __init__(
        self,
        model: str,
        temperature: float = 0.5,
        max_tokens: int = 1000,
    ):
        self.client = Groq(api_key=os.environ["GROQ_API_KEY"])
        self.model = model
        self.temperature = temperature
        self.max_tokens = max_tokens

    def _get_single_completion(self, instruction_prompt: str, data_prompt: str) -> Stream[ChatCompletionChunk]:
        completion = self.client.chat.completions.create(
            model=self.model,
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
            temperature=self.temperature,
            max_tokens=self.max_tokens,
            top_p=1,
            stream=False,
        )
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
        output = PromptOutput(output, input_tokens, output_tokens, model)
        return output

    def get_response(self, instruction_prompt: str, data_prompt: str) -> PromptOutput:
        completion = self._get_single_completion(instruction_prompt, data_prompt)
        output = self._parse_response(completion)
        return output
