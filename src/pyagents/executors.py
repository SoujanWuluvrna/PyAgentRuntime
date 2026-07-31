"""Shared scheduler with interchangeable local and process invocation backends."""

from __future__ import annotations

import asyncio
import hashlib
import json
import multiprocessing as mp
import random
from abc import ABC, abstractmethod
from dataclasses import dataclass
from multiprocessing.process import BaseProcess
from typing import Any

from pydantic import BaseModel

from .agent import Agent, AgentContext
from .events import (
    EventSink,
    JsonlEventSink,
    RunFailed,
    RunFinished,
    RunRetried,
    RunStarted,
    RunTimedOut,
)
from .workflow import AgentNode, Workflow


class AgentExecutionError(RuntimeError):
    def __init__(self, message: str, trace: tuple[_TraceItem, ...]) -> None:
        super().__init__(message)
        self.trace = trace


@dataclass(frozen=True)
class _AttemptResult:
    ok: bool
    output: dict[str, Any] | None
    state: dict[str, Any]
    error_type: str | None = None
    message: str | None = None
    timed_out: bool = False


@dataclass(frozen=True)
class _TraceItem:
    kind: str
    attempt: int
    details: dict[str, Any]


@dataclass(frozen=True)
class _InvocationResult:
    output: BaseModel
    trace: tuple[_TraceItem, ...]


def _stable_seed(seed: int, run_id: str, attempt: int) -> int:
    digest = hashlib.sha256(f"{seed}:{run_id}:{attempt}".encode()).digest()
    return int.from_bytes(digest[:8], "big")


def _run_id(node: AgentNode, input_value: BaseModel) -> str:
    identity = f"{node.agent.__class__.__module__}.{node.agent.__class__.__qualname__}"
    canonical = json.dumps(
        input_value.model_dump(mode="json", exclude_none=False),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    payload = f"{node.name}\0{identity}\0{canonical}".encode()
    return hashlib.sha256(payload).hexdigest()


async def _execute_attempt(
    agent: Agent[Any, Any, Any],
    input_data: dict[str, Any],
    state_data: dict[str, Any],
    run_id: str,
    seed: int,
    attempt: int,
) -> _AttemptResult:
    state = agent.state_type.model_validate(state_data)
    try:
        typed_input = agent.validate_input(input_data)
        context = AgentContext(
            run_id=run_id,
            workflow_seed=seed,
            attempt=attempt,
            rng=random.Random(_stable_seed(seed, run_id, attempt)),
        )
        raw_output = await agent.run(typed_input, state, context)
        output = agent.validate_output(raw_output)
        return _AttemptResult(
            True, output.model_dump(mode="json"), state.model_dump(mode="json")
        )
    # The retry policy deliberately applies to all ordinary agent exceptions.
    except Exception as exc:  # noqa: BLE001
        return _AttemptResult(
            False,
            None,
            state.model_dump(mode="json"),
            type(exc).__name__,
            str(exc),
        )


def _process_entry(connection: Any, args: tuple[Any, ...]) -> None:
    """One-attempt worker entry point; must stay at module scope for spawn."""
    try:
        result = asyncio.run(_execute_attempt(*args))
        connection.send(result)
    # A child must report even SystemExit/KeyboardInterrupt as a failed attempt.
    except BaseException as exc:  # noqa: BLE001
        connection.send(
            _AttemptResult(False, None, args[2], type(exc).__name__, str(exc))
        )
    finally:
        connection.close()


class _Executor(ABC):
    def __init__(self, sink: EventSink | None = None) -> None:
        self.sink = sink or JsonlEventSink()
        self._sequence = 0

    @abstractmethod
    async def _attempt(
        self,
        agent: Agent[Any, Any, Any],
        input_data: dict[str, Any],
        state_data: dict[str, Any],
        run_id: str,
        seed: int,
        attempt: int,
    ) -> _AttemptResult: ...

    async def _invoke(
        self, node: AgentNode, value: BaseModel, seed: int
    ) -> _InvocationResult:
        typed_input = node.agent.validate_input(value.model_dump(mode="json"))
        run_id = _run_id(node, typed_input)
        state = node.agent.initial_state().model_dump(mode="json")
        trace: list[_TraceItem] = []
        policy = node.agent.retry_policy

        for attempt in range(1, policy.max_attempts + 1):
            trace.append(_TraceItem("started", attempt, {}))
            result = await self._attempt(
                node.agent,
                typed_input.model_dump(mode="json"),
                state,
                run_id,
                seed,
                attempt,
            )
            state = result.state
            if result.ok:
                output = node.agent.validate_output(result.output)
                trace.append(_TraceItem("finished", attempt, {}))
                return _InvocationResult(output, tuple(trace))

            if result.timed_out:
                trace.append(
                    _TraceItem(
                        "timed_out", attempt, {"timeout_s": node.agent.timeout_s}
                    )
                )
            else:
                trace.append(
                    _TraceItem(
                        "failed",
                        attempt,
                        {
                            "error_type": result.error_type or "Exception",
                            "message": result.message or "",
                        },
                    )
                )

            if attempt < policy.max_attempts:
                backoff = policy.backoff_for(attempt)
                trace.append(_TraceItem("retried", attempt, {"backoff_s": backoff}))
                await asyncio.sleep(backoff)
            else:
                raise AgentExecutionError(
                    f"{node.name} failed after {attempt} attempts: "
                    f"{result.error_type}: {result.message}",
                    tuple(trace),
                )
        raise AssertionError("unreachable")

    def _emit_trace(
        self, node: AgentNode, value: BaseModel, trace: tuple[_TraceItem, ...]
    ) -> None:
        run_id = _run_id(node, node.agent.validate_input(value.model_dump(mode="json")))
        event_types = {
            "started": RunStarted,
            "finished": RunFinished,
            "failed": RunFailed,
            "retried": RunRetried,
            "timed_out": RunTimedOut,
        }
        for item in trace:
            event = event_types[item.kind](
                sequence=self._sequence,
                run_id=run_id,
                agent=node.name,
                attempt=item.attempt,
                **item.details,
            )
            self.sink.emit(event)
            self._sequence += 1

    async def run(
        self, workflow: Workflow, input: object, *, seed: int = 0
    ) -> BaseModel:
        self._sequence = 0
        value = workflow.validate_input(input)
        for node in workflow.nodes:
            if isinstance(node, AgentNode):
                try:
                    result = await self._invoke(node, value, seed)
                except AgentExecutionError as exc:
                    self._emit_trace(node, value, exc.trace)
                    raise
                self._emit_trace(node, value, result.trace)
                value = result.output
                continue

            fanout_input = value
            gathered = await asyncio.gather(
                *(self._invoke(child, fanout_input, seed) for child in node.agents),
                return_exceptions=True,
            )
            for child, item in zip(node.agents, gathered, strict=True):
                if isinstance(item, AgentExecutionError):
                    self._emit_trace(child, fanout_input, item.trace)
                elif isinstance(item, BaseException):
                    raise item
                else:
                    self._emit_trace(child, fanout_input, item.trace)
            failures = [item for item in gathered if isinstance(item, BaseException)]
            if failures:
                raise failures[0]
            results = [item for item in gathered if isinstance(item, _InvocationResult)]
            reducer_input = node.reducer.agent.input_type.model_validate(
                {"items": [result.output.model_dump(mode="json") for result in results]}
            )
            try:
                reduced = await self._invoke(node.reducer, reducer_input, seed)
            except AgentExecutionError as exc:
                self._emit_trace(node.reducer, reducer_input, exc.trace)
                raise
            self._emit_trace(node.reducer, reducer_input, reduced.trace)
            value = reduced.output
        return workflow.output_type.model_validate(value)


class LocalExecutor(_Executor):
    """Runs attempts as asyncio tasks in this process."""

    async def _attempt(
        self,
        agent: Agent[Any, Any, Any],
        input_data: dict[str, Any],
        state_data: dict[str, Any],
        run_id: str,
        seed: int,
        attempt: int,
    ) -> _AttemptResult:
        coroutine = _execute_attempt(
            agent, input_data, state_data, run_id, seed, attempt
        )
        try:
            if agent.timeout_s is None:
                return await coroutine
            return await asyncio.wait_for(coroutine, timeout=agent.timeout_s)
        except TimeoutError:
            return _AttemptResult(
                False, None, state_data, "TimeoutError", "attempt timed out", True
            )


class DistributedExecutor(_Executor):
    """Runs each attempt in a fresh spawned process, bounded by worker count.

    A fresh process makes timeout cancellation hard: terminate, then join. The
    tradeoff is process-start overhead, acceptable for this deliberately small
    substrate and explicit in the README.
    """

    def __init__(
        self,
        max_workers: int = 2,
        sink: EventSink | None = None,
        *,
        terminate_grace_s: float = 0.1,
    ) -> None:
        super().__init__(sink)
        if max_workers < 1:
            raise ValueError("max_workers must be positive")
        if terminate_grace_s < 0:
            raise ValueError("terminate_grace_s must be non-negative")
        self._slots = asyncio.Semaphore(max_workers)
        self._context = mp.get_context("spawn")
        self._terminate_grace_s = terminate_grace_s

    async def _attempt(
        self,
        agent: Agent[Any, Any, Any],
        input_data: dict[str, Any],
        state_data: dict[str, Any],
        run_id: str,
        seed: int,
        attempt: int,
    ) -> _AttemptResult:
        args = (agent, input_data, state_data, run_id, seed, attempt)
        async with self._slots:
            return await asyncio.to_thread(
                self._blocking_attempt, args, agent.timeout_s
            )

    def _stop_process(self, process: BaseProcess) -> None:
        """Stop a worker within a bounded interval, escalating to SIGKILL."""
        if not process.is_alive():
            process.join()
            return
        process.terminate()
        process.join(timeout=self._terminate_grace_s)
        if process.is_alive():
            process.kill()
            process.join()

    def _blocking_attempt(
        self, args: tuple[Any, ...], timeout_s: float | None
    ) -> _AttemptResult:
        parent, child = self._context.Pipe(duplex=False)
        process = self._context.Process(target=_process_entry, args=(child, args))
        try:
            try:
                process.start()
            # multiprocessing may raise several platform/pickling exception types.
            except Exception as exc:  # noqa: BLE001
                return _AttemptResult(
                    False,
                    None,
                    args[2],
                    type(exc).__name__,
                    f"worker could not start: {exc}",
                )
            finally:
                child.close()

            if not parent.poll(timeout_s):
                self._stop_process(process)
                return _AttemptResult(
                    False, None, args[2], "TimeoutError", "attempt timed out", True
                )
            try:
                result = parent.recv()
            except EOFError:
                process.join()
                return _AttemptResult(
                    False,
                    None,
                    args[2],
                    "WorkerLostError",
                    f"worker exited with code {process.exitcode} without a result",
                )
            process.join()
            if not isinstance(result, _AttemptResult):
                return _AttemptResult(
                    False,
                    None,
                    args[2],
                    "WorkerProtocolError",
                    f"worker returned unexpected {type(result).__name__}",
                )
            return result
        finally:
            parent.close()
            if process.is_alive():
                self._stop_process(process)
