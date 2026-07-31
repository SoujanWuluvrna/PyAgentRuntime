from pydantic import BaseModel
import pytest

from pyagents import Agent, AgentContext, Workflow
from pyagents.agent import EmptyState


class A(BaseModel):
    value: str


class B(BaseModel):
    count: int


class BadAgent(Agent[B, A, EmptyState]):
    input_type = B
    output_type = A

    async def run(self, input, state, context):
        return A(value=str(input.count))


def test_rejects_incompatible_sequence_at_build_time():
    with pytest.raises(TypeError, match="expects B"):
        Workflow(A).then("bad", BadAgent())


def test_rejects_invalid_workflow_input_at_runtime():
    with pytest.raises(Exception):
        Workflow(A).validate_input({"value": 123})
