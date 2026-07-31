"""Five-way mock LLM fanout demo, unchanged across executor backends."""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from pydantic import BaseModel

from .agent import Agent, AgentContext, EmptyState, RetryPolicy
from .events import JsonlEventSink
from .executors import DistributedExecutor, LocalExecutor
from .workflow import Workflow


class Prompt(BaseModel):
    text: str


class Completion(BaseModel):
    text: str


class CompletionList(BaseModel):
    items: list[Completion]


class Combined(BaseModel):
    text: str


class MockLLMAgent(Agent[Prompt, Completion, EmptyState]):
    input_type = Prompt
    output_type = Completion
    retry_policy = RetryPolicy(max_attempts=4, initial_backoff_s=0.001)
    timeout_s = 1.0

    def __init__(self, label: str) -> None:
        self.label = label

    async def run(
        self, input: Prompt, state: EmptyState, context: AgentContext
    ) -> Completion:
        # One attempt-local RNG, derived by the runtime from seed/run-id/attempt,
        # makes both injected failures and sleep durations backend-independent.
        should_fail = context.rng.random() < 0.30
        await asyncio.sleep(context.rng.uniform(0.005, 0.025))
        if should_fail:
            raise ConnectionError("injected transient LLM failure")
        return Completion(text=f"{self.label}: {input.text}")


class ConcatenateAgent(Agent[CompletionList, Combined, EmptyState]):
    input_type = CompletionList
    output_type = Combined

    async def run(
        self, input: CompletionList, state: EmptyState, context: AgentContext
    ) -> Combined:
        return Combined(text=" | ".join(item.text for item in input.items))


def build_workflow() -> Workflow:
    return Workflow(Prompt).fanout_reduce(
        "llms",
        [MockLLMAgent(f"model-{index}") for index in range(5)],
        ConcatenateAgent(),
    )


async def run_demo(executor_name: str, event_path: Path, seed: int) -> Combined:
    event_path.unlink(missing_ok=True)
    sink = JsonlEventSink(event_path)
    executor = (
        LocalExecutor(sink)
        if executor_name == "local"
        else DistributedExecutor(max_workers=2, sink=sink)
    )
    result = await executor.run(build_workflow(), Prompt(text="explain PSI"), seed=seed)
    return Combined.model_validate(result)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--executor", choices=("local", "distributed"), default="local")
    parser.add_argument("--seed", type=int, default=16)
    parser.add_argument("--events", type=Path, default=Path("events.jsonl"))
    args = parser.parse_args()
    result = asyncio.run(run_demo(args.executor, args.events, args.seed))
    print(result.text)
    print(f"events: {args.events}")


if __name__ == "__main__":
    main()
