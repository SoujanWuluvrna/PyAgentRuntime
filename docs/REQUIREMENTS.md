# Assignment traceability

This matrix maps each requested capability to its implementation and executable
evidence so a reviewer can audit the submission quickly.

| Requirement | Implementation | Evidence |
|---|---|---|
| Pydantic input/output | `Agent.input_type`, `output_type`; validation before and after every attempt | Invalid-input and invalid-output tests |
| Optional retry-persistent state | `state_type`, `initial_state`; state returned in acknowledged attempt envelopes | Distributed state retry test |
| Async agent method | Abstract `Agent.run` | Demo and test agents |
| Runtime types at boundaries | Workflow entry, invocation input, worker state, output, reducer input, workflow output | Contract and parity tests |
| Sequence | `Workflow.then` | Workflow compatibility test |
| Fanout and reduce | `Workflow.fanout_reduce`, `FanoutNode` | Five-agent demo and parity test |
| Build-time compatibility | Model compatibility, homogeneous branches, exact reducer item type, agent configuration | Workflow tests |
| Local executor | Asyncio `LocalExecutor` | Parity and local-timeout tests |
| Distributed executor | Spawn-based `DistributedExecutor` | Parity, state, and hard-timeout tests |
| One-line switch | Same `Workflow`; only executor constructor differs | Demo and README |
| Exponential retries | Scheduler-owned `RetryPolicy.backoff_for` | Seeded trace and retry test |
| Per-agent timeout | `asyncio.wait_for` locally, process deadline remotely | Timeout tests |
| Hard distributed cancellation | Terminate, bounded join, kill fallback, final join | No-child-left timeout assertion |
| Idempotent run ID | SHA-256 of node, class, canonical validated input | Retry and mapping-order tests |
| Typed events | Discriminated Pydantic `RunEvent` union | JSONL typed round-trip test |
| JSONL default/pluggable sink | `JsonlEventSink`, `MemoryEventSink`, `EventSink` | Parity and JSONL tests |
| Deterministic event log | Attempt RNG and parent-owned logical ordering | Byte comparison and parity test |
| Seeded ~30% failures | Bernoulli failure in `MockLLMAgent` | Default demo produces visible retries |
| Fanout exceeds workers | Five branches with two distributed slots | Demo configuration |
| Serialization rationale | Pydantic JSON-mode payloads over trusted pickle transport | README |
| Delivery guarantee | At-least-once attempts; external idempotency required | README |
| Honest limitations | Explicit scope/TODO section | README |
| AI use | Decisions, corrections, and export caveat | `docs/AI_TRANSCRIPT.md` |

## Verification commands

```bash
python -m pytest -q
ruff check src tests
mypy src/pyagents
pyagents-demo --executor local --seed 16 --events events-local.jsonl
pyagents-demo --executor distributed --seed 16 --events events-distributed.jsonl
cmp events-local.jsonl events-distributed.jsonl
```
