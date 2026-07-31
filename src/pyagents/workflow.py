"""Declarative DAG builder with conservative model compatibility checks."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, get_args, get_origin

from pydantic import BaseModel

from .agent import Agent


@dataclass(frozen=True)
class AgentNode:
    name: str
    agent: Agent[Any, Any, Any]


@dataclass(frozen=True)
class FanoutNode:
    name: str
    agents: tuple[AgentNode, ...]
    reducer: AgentNode


Node = AgentNode | FanoutNode


def _compatible(source: type[BaseModel], target: type[BaseModel]) -> bool:
    """Accept subclasses or structurally identical Pydantic field annotations."""
    if issubclass(source, target):
        return True
    source_fields = {k: v.annotation for k, v in source.model_fields.items()}
    target_fields = {k: v.annotation for k, v in target.model_fields.items()}
    return source_fields == target_fields


class Workflow:
    def __init__(self, input_type: type[BaseModel]) -> None:
        self.input_type = input_type
        self._nodes: list[Node] = []
        self._output_type: type[BaseModel] = input_type
        self._names: set[str] = set()

    @property
    def nodes(self) -> tuple[Node, ...]:
        return tuple(self._nodes)

    @property
    def output_type(self) -> type[BaseModel]:
        return self._output_type

    def _claim(self, name: str) -> None:
        if name in self._names:
            raise ValueError(f"duplicate node name: {name}")
        self._names.add(name)

    def then(self, name: str, agent: Agent[Any, Any, Any]) -> Workflow:
        self._claim(name)
        if not _compatible(self._output_type, agent.input_type):
            raise TypeError(
                f"{name} expects {agent.input_type.__name__}, "
                f"previous node emits {self._output_type.__name__}"
            )
        self._nodes.append(AgentNode(name, agent))
        self._output_type = agent.output_type
        return self

    def fanout_reduce(
        self,
        name: str,
        agents: list[Agent[Any, Any, Any]],
        reducer: Agent[Any, Any, Any],
    ) -> Workflow:
        if not agents:
            raise ValueError("fanout requires at least one agent")
        self._claim(name)
        children: list[AgentNode] = []
        for index, agent in enumerate(agents):
            child_name = f"{name}[{index}]"
            self._claim(child_name)
            if not _compatible(self._output_type, agent.input_type):
                raise TypeError(f"{child_name} input is incompatible with fanout input")
            children.append(AgentNode(child_name, agent))

        emitted = agents[0].output_type
        if any(agent.output_type is not emitted for agent in agents[1:]):
            raise TypeError("all fanout agents must emit the same model type")
        reducer_field = reducer.input_type.model_fields.get("items")
        if reducer_field is None:
            raise TypeError("reducer input model must have an 'items' field")
        annotation = reducer_field.annotation
        if get_origin(annotation) is not list or get_args(annotation) != (emitted,):
            raise TypeError(
                f"reducer items must be list[{emitted.__name__}], got {annotation}"
            )

        reducer_name = f"{name}.reduce"
        self._claim(reducer_name)
        self._nodes.append(FanoutNode(name, tuple(children), AgentNode(reducer_name, reducer)))
        self._output_type = reducer.output_type
        return self

    def validate_input(self, value: object) -> BaseModel:
        return self.input_type.model_validate(value)
