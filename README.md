# PyAgentRuntime

> **One Workflow. Two Execution Backends. Reliable Agent Orchestration.**

[![Python 3.11+](https://img.shields.io/badge/Python-3.11%2B-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![Pydantic v2](https://img.shields.io/badge/Pydantic-v2-E92063?logo=pydantic&logoColor=white)](https://docs.pydantic.dev/)
[![Tests](https://img.shields.io/badge/tests-10%20passing-brightgreen)](#verification)

`pyagents` is a compact Python runtime for composing reliable, typed asynchronous
agent workflows. The same declarative `Workflow` can run locally with `asyncio`
or across isolated worker processes without changing the workflow definition.

## Why I built it

Agent demos are easy to create, but reliable execution becomes harder once they
need parallel work, runtime type safety, retries, timeouts, deterministic logs,
and process isolation. This project explores those systems concerns in a small,
readable implementation rather than hiding them behind a large framework.

## What it demonstrates

- Typed agent inputs, outputs, and retry-persistent state with Pydantic
- Sequential and parallel fanout/reduce workflow composition
- One workflow API for local and multi-process execution
- Bounded worker concurrency and hard cancellation of timed-out processes
- Deterministic retries, invocation IDs, and JSONL event logs
- Matching observable behavior across both execution backends

> **Project status:** Educational prototype. It demonstrates runtime and
> distributed-systems design decisions, but it is not presented as a
> production-ready remote orchestration platform.

## Quick start

Requires Python 3.11+.

```bash
python -m venv .venv
. .venv/bin/activate
pip install -e '.[dev]'

pyagents-demo --executor local --seed 16 --events events-local.jsonl
pyagents-demo --executor distributed --seed 16 --events events-distributed.jsonl
diff events-local.jsonl events-distributed.jsonl
pytest -q
ruff check src tests
mypy src/pyagents
```

The executor is the only changed line:

```python
executor = LocalExecutor(sink)
executor = DistributedExecutor(max_workers=2, sink=sink)

result = await executor.run(build_workflow(), Prompt(text="explain PSI"), seed=16)
```

The demo fans one `Prompt` into five `MockLLMAgent` instances and reduces their
five `Completion` models into one `Combined` model. Every attempt has a
deterministic 30% transient-failure probability and a deterministic short sleep.

```text
Prompt
  ├── model-0 ─┐
  ├── model-1  │
  ├── model-2  ├── ConcatenateAgent ──> Combined result
  ├── model-3  │
  └── model-4 ─┘
```

The agents intentionally simulate model calls; no API key or paid model service
is required to run the demo.

## Project structure

```text
src/pyagents/
├── agent.py       # Typed agent contract, context, state, and retry policy
├── workflow.py    # Sequence and fanout/reduce workflow construction
├── executors.py   # Local and multi-process execution backends
├── events.py      # Typed events and pluggable event sinks
└── demo.py        # Deterministic five-agent example
tests/             # Runtime, failure, timeout, and backend-parity tests
```

## One-page architecture

```text
Pydantic input
      |
      v
Workflow (immutable node view; sequence + fanout/reduce; build-time checks)
      |
      v
Shared parent scheduler (DAG order, retries, backoff, run IDs, event ordering)
      |                                   |
      | one-attempt interface             +--> EventSink --> JSONL
      v
 Local backend                  Distributed backend
 asyncio task                   bounded slots + fresh spawned process/attempt
                                          |
                                     Pipe result/error/state
```

**Contract boundary.** An `Agent` declares explicit Pydantic `input_type`,
`output_type`, and optional `state_type`, plus async `run`. Inputs and outputs
are validated at workflow entry, before every invocation, after every return,
and across the process pipe. Explicit model attributes are intentional: unlike
generic introspection, they remain obvious and robust under inheritance and
process pickling.

**Composition.** `Workflow.then` expresses sequence; `fanout_reduce` expresses
parallel branches and their join. The builder rejects incompatible adjacent
models, heterogeneous fanout output types, duplicate node names, and reducers
without the exact `list[BranchOutput]` contract. The graph representation is
kept minimal because the exercise workload needs only these DAG shapes.

**Scheduler/executor boundary.** The shared scheduler owns graph traversal,
retry policy, deterministic IDs/RNG, and event publication. A backend executes
exactly one attempt. The local backend uses an asyncio task. The distributed
backend bounds concurrency with a semaphore and uses a fresh spawned process
per attempt. If five branches meet two worker slots, three wait; there is no
unbounded process creation.

**Timeout cancellation.** Local timeout uses `asyncio.wait_for`, so cancellation
is cooperative (an agent can suppress `CancelledError`). Distributed timeout
terminates and joins the attempt's process, then escalates to `kill()` after a
bounded grace interval if it ignores termination. This provides hard
cancellation and leaves no contaminated worker to reuse. Fresh processes cost
startup time but make the failure boundary unusually clear for a small exercise.

**Deterministic observability.** Workers return attempt traces; they never write
the sink. After concurrent branches settle, the parent publishes traces in
declaration order and assigns monotonic `sequence` values. Thus wall-clock race
order cannot alter the log. Events intentionally omit timestamps and process
IDs. With canonical model JSON and a fixed seed, local and distributed logs are
byte-identical.

## Serialization and identity

Pydantic's JSON-mode dictionaries cross the process boundary, carried by the
stdlib `multiprocessing.Pipe` (which uses pickle for transport). Pydantic data
serialization makes contract validation explicit and JSON-compatible; pickle
is used only as trusted local transport for agent objects and envelopes. This
is **not safe for untrusted workers or messages**. A production transport would
use versioned JSON/MessagePack envelopes plus an agent registry instead of
shipping Python objects.

An invocation ID is the SHA-256 hex digest of the node name, fully qualified
agent class, and canonical validated input JSON (including recursively sorted
mapping keys). Retries keep
the same ID. Node name distinguishes five instances of the same agent class.
The ID is an idempotency key, not a result cache.

## Delivery guarantee

The execution guarantee is **at-least-once attempts, at-most-one successful
result observed by this scheduler invocation**. A timeout races with completion:
the worker may have performed an external side effect before being killed, then
the scheduler retries. Exactly-once effects are impossible here without a
transactional external system. Agents performing effects must pass `run_id` as
an idempotency key to that system. The runtime does not claim exactly-once.

State is reconstructed once per invocation and serialized back after every
attempt, including ordinary failures, so mutations persist across retries.
State from a hard-killed/timed-out process cannot be recovered; it remains at
the last acknowledged attempt boundary.

## Events

The discriminated Pydantic union contains `RunStarted`, `RunFinished`,
`RunFailed`, `RunRetried`, and `RunTimedOut`. `JsonlEventSink` is the default;
`MemoryEventSink` demonstrates pluggability. Error type/message are observable,
while tracebacks are deliberately excluded from the stable public event schema.

## Verification

The repository is checked with:

```bash
pytest -q
ruff check src tests
mypy src/pyagents
```

## Honest scope and TODOs

- Process-per-attempt is simple and gives hard cancellation, but is too costly
  for short production tasks. A supervised warm pool with disposable children
  would be the next step.
- This is one scheduler process, not a durable distributed control plane. There
  is no recovery after scheduler death, durable queue, heartbeat, or remote host.
- DAG support is intentionally limited to sequential stages and fanout/reduce;
  there is no arbitrary edge builder, conditional branch, streaming, or cycle
  detection beyond what the API makes impossible.
- Structural sequence compatibility is conservative. It compares field names
  and annotations, not every Pydantic validator or JSON Schema constraint.
- JSONL append and sequence assignment are safe inside one scheduler, not across
  multiple schedulers writing the same file.
- Retry classification is currently “all exceptions.” Production code needs an
  explicit retryable-error policy and jitter (deterministically sourced in tests).
- No secrets isolation or sandboxing exists; worker agent code is trusted.
