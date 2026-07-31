import pytest
from pydantic import BaseModel, ValidationError

from pyagents import Agent, Workflow
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
    with pytest.raises(ValidationError):
        Workflow(A).validate_input({"value": 123})


class InvalidTimeoutAgent(Agent[A, A, EmptyState]):
    input_type = A
    output_type = A
    timeout_s = 0

    async def run(self, input, state, context):
        return input


def test_rejects_invalid_agent_configuration_at_build_time():
    with pytest.raises(ValueError, match="timeout_s must be positive"):
        Workflow(A).then("invalid", InvalidTimeoutAgent())
