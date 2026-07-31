import asyncio
import json
import multiprocessing as mp

import pytest
from pydantic import BaseModel

from pyagents import Agent, AgentExecutionError, DistributedExecutor, LocalExecutor
from pyagents.agent import EmptyState, RetryPolicy
from pyagents.demo import Prompt, build_workflow
from pyagents.events import EVENT_ADAPTER, JsonlEventSink, MemoryEventSink
from pyagents.workflow import Workflow


async def _run(executor_type):
    sink = MemoryEventSink()
    executor = (
        executor_type(sink=sink)
        if executor_type is LocalExecutor
        else executor_type(max_workers=2, sink=sink)
    )
    result = await executor.run(build_workflow(), Prompt(text="explain PSI"), seed=16)
    events = [event.model_dump(mode="json") for event in sink.events]
    return result, events


async def test_local_and_distributed_are_identical():
    local_result, local_events = await _run(LocalExecutor)
    distributed_result, distributed_events = await _run(DistributedExecutor)
    assert local_result == distributed_result
    assert local_events == distributed_events
    assert any(event["type"] == "run_retried" for event in local_events)
    llm_attempts = sum(
        event["type"] == "run_started" and event["agent"].startswith("llms[")
        for event in local_events
    )
    injected_failures = sum(event["type"] == "run_failed" for event in local_events)
    assert injected_failures == 2
    assert injected_failures / llm_attempts == pytest.approx(0.30, abs=0.02)
    assert [event["sequence"] for event in local_events] == list(
        range(len(local_events))
    )


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
    before = {process.pid for process in mp.active_children()}
    sink = MemoryEventSink()
    executor = DistributedExecutor(max_workers=1, sink=sink)
    with pytest.raises(AgentExecutionError):
        await executor.run(
            Workflow(SlowInput).then("slow", SlowAgent()), {"value": "x"}
        )
    after = {process.pid for process in mp.active_children()}
    assert after == before
    assert [event.type for event in sink.events] == ["run_started", "run_timed_out"]


async def test_local_timeout_is_classified_as_timeout():
    sink = MemoryEventSink()
    executor = LocalExecutor(sink=sink)
    with pytest.raises(AgentExecutionError):
        await executor.run(
            Workflow(SlowInput).then("slow", SlowAgent()), {"value": "x"}
        )
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


class MappingInput(BaseModel):
    values: dict[str, int]


class MappingAgent(Agent[MappingInput, SlowOutput, EmptyState]):
    input_type = MappingInput
    output_type = SlowOutput

    async def run(self, input, state, context):
        return SlowOutput(value=str(sum(input.values.values())))


async def test_run_id_uses_canonical_mapping_order():
    workflow = Workflow(MappingInput).then("mapping", MappingAgent())
    first_sink = MemoryEventSink()
    second_sink = MemoryEventSink()
    await LocalExecutor(first_sink).run(workflow, {"values": {"a": 1, "b": 2}})
    await LocalExecutor(second_sink).run(workflow, {"values": {"b": 2, "a": 1}})
    assert first_sink.events[0].run_id == second_sink.events[0].run_id


class InvalidOutputAgent(Agent[SlowInput, SlowOutput, EmptyState]):
    input_type = SlowInput
    output_type = SlowOutput
    retry_policy = RetryPolicy(max_attempts=2, initial_backoff_s=0)

    async def run(self, input, state, context):
        return {"wrong": input.value}  # type: ignore[return-value]


async def test_invalid_agent_output_fails_at_boundary_and_emits_terminal_trace():
    sink = MemoryEventSink()
    with pytest.raises(AgentExecutionError, match="ValidationError"):
        await LocalExecutor(sink).run(
            Workflow(SlowInput).then("invalid", InvalidOutputAgent()), {"value": "x"}
        )
    assert [event.type for event in sink.events] == [
        "run_started",
        "run_failed",
        "run_retried",
        "run_started",
        "run_failed",
    ]


async def test_jsonl_sink_round_trips_as_typed_events(tmp_path):
    path = tmp_path / "nested" / "events.jsonl"
    path.parent.mkdir()
    await LocalExecutor(JsonlEventSink(path)).run(
        Workflow(SlowInput).then("mapping", StatefulAgent()), {"value": "x"}
    )
    lines = path.read_text(encoding="utf-8").splitlines()
    events = [EVENT_ADAPTER.validate_python(json.loads(line)) for line in lines]
    assert [event.sequence for event in events] == list(range(len(events)))
