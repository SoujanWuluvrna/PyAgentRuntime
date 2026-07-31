"""Typed agent contract and execution context."""

from __future__ import annotations

import random
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Generic, TypeVar

from pydantic import BaseModel, ConfigDict, Field

InputT = TypeVar("InputT", bound=BaseModel)
OutputT = TypeVar("OutputT", bound=BaseModel)
StateT = TypeVar("StateT", bound=BaseModel)


class EmptyState(BaseModel):
    """Default state for stateless agents."""


class RetryPolicy(BaseModel):
    model_config = ConfigDict(frozen=True)

    max_attempts: int = Field(default=3, ge=1)
    initial_backoff_s: float = Field(default=0.01, ge=0)
    multiplier: float = Field(default=2.0, ge=1)

    def backoff_for(self, failed_attempt: int) -> float:
        return self.initial_backoff_s * self.multiplier ** (failed_attempt - 1)


@dataclass(frozen=True)
class AgentContext:
    run_id: str
    workflow_seed: int
    attempt: int
    rng: random.Random


class Agent(ABC, Generic[InputT, OutputT, StateT]):
    """An async unit of work with Pydantic contracts at every boundary.

    Concrete agents explicitly declare their model classes. This is slightly
    verbose, but remains reliable under decorators, inheritance and pickling.
    """

    input_type: type[InputT]
    output_type: type[OutputT]
    state_type: type[StateT] = EmptyState  # type: ignore[assignment]
    retry_policy: RetryPolicy = RetryPolicy()
    timeout_s: float | None = None

    @classmethod
    def validate_definition(cls) -> None:
        """Fail early when a concrete agent declares an invalid contract."""
        for attribute in ("input_type", "output_type", "state_type"):
            model_type = getattr(cls, attribute, None)
            if not isinstance(model_type, type) or not issubclass(
                model_type, BaseModel
            ):
                raise TypeError(
                    f"{cls.__name__}.{attribute} must be a Pydantic model class"
                )
        if cls.timeout_s is not None and cls.timeout_s <= 0:
            raise ValueError(f"{cls.__name__}.timeout_s must be positive")
        if not isinstance(cls.retry_policy, RetryPolicy):
            raise TypeError(f"{cls.__name__}.retry_policy must be a RetryPolicy")

    def initial_state(self) -> StateT:
        return self.state_type.model_validate({})

    @abstractmethod
    async def run(self, input: InputT, state: StateT, context: AgentContext) -> OutputT:
        """Execute one attempt. State mutations survive later retries locally."""

    def validate_input(self, value: object) -> InputT:
        return self.input_type.model_validate(value)

    def validate_output(self, value: object) -> OutputT:
        return self.output_type.model_validate(value)
