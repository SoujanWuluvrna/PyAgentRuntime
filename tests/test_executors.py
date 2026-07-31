import asyncio

from pydantic import BaseModel

from pyagents import Agent, AgentContext, DistributedExecutor, LocalExecutor
from pyagents.agent import EmptyState, RetryPolicy
from pyagents.demo import Combined, Prompt, build_workflow
from pyagents.events import MemoryEventSink
from pyagents.workflow import Workflow


async def _run(executor_type):
    sink = MemoryEventSink()
    executor = (
        executor_type(sink=sink)
        if executor_type is LocalExecutor
        else executor_type(max_workers=2, sink=sink)
    )
    result = await executor.run(build_workflow(), Prompt(text="hello"), seed=0)
    events = [event.model_dump(mode="json") for event in sink.events]
    return result, events


async def test_local_and_distributed_are_identical():
    local_result, local_events = await _run(LocalExecutor)
    distributed_result, distributed_events = await _run(DistributedExecutor)
    assert local_result == distributed_result
    assert local_events == distributed_events
    assert any(event["type"] == "run_retried" for event in local_events)
    assert [event["sequence"] for event in local_events] == list(range(len(local_events)))


class SlowInput(BaseModel):
    value: str


class SlowOutput(BaseModel):
    value: str


class SlowAgent(Agent[SlowInput, SlowOutput, EmptyState]):
    input_type = SlowInput
    output_type = SlowOutput
    retry_policy = RetryPolicy(max_attempts=1)
    timeout_s = 0.02

    async def run(self, input, state, context):
        await asyncio.sleep(10)
        return SlowOutput(value=input.value)


async def test_distributed_timeout_terminates_worker():
    sink = MemoryEventSink()
    executor = DistributedExecutor(max_workers=1, sink=sink)
    try:
        await executor.run(Workflow(SlowInput).then("slow", SlowAgent()), {"value": "x"})
    except RuntimeError:
        pass
    # The scheduler should report terminal timeouts as well as successful traces.
    assert any(event.type == "run_timed_out" for event in sink.events)


async def test_local_timeout_is_classified_as_timeout():
    sink = MemoryEventSink()
    executor = LocalExecutor(sink=sink)
    try:
        await executor.run(Workflow(SlowInput).then("slow", SlowAgent()), {"value": "x"})
    except RuntimeError:
        pass
    assert [event.type for event in sink.events] == ["run_started", "run_timed_out"]


class RetryState(BaseModel):
    attempts_seen: int = 0


class StatefulAgent(Agent[SlowInput, SlowOutput, RetryState]):
    input_type = SlowInput
    output_type = SlowOutput
    state_type = RetryState
    retry_policy = RetryPolicy(max_attempts=2, initial_backoff_s=0)

    async def run(self, input, state, context):
        state.attempts_seen += 1
        if state.attempts_seen == 1:
            raise ConnectionError("fail once")
        return SlowOutput(value=f"{input.value}:{state.attempts_seen}")


async def test_state_survives_distributed_retry_boundary():
    sink = MemoryEventSink()
    result = await DistributedExecutor(max_workers=1, sink=sink).run(
        Workflow(SlowInput).then("stateful", StatefulAgent()), {"value": "x"}
    )
    assert result == SlowOutput(value="x:2")
    run_ids = {event.run_id for event in sink.events}
    assert len(run_ids) == 1
    assert [event.type for event in sink.events] == [
        "run_started",
        "run_failed",
        "run_retried",
        "run_started",
        "run_finished",
    ]
