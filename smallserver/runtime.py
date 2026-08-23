"""Configuration for SmallOS runtimes created by SmallServer."""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Mapping


@dataclass(frozen=True)
class ManagedRuntimeConfig:
    """SmallOS settings used only when :meth:`SmallServer.listen` owns runtime creation.

    Caller-supplied runtimes keep their existing SmallOS configuration and
    reject this setting rather than being mutated behind the caller's back.
    """

    task_capacity: int = 2**10
    priority_levels: int = 10
    io_buffer_length: int = 1024
    eternal_watchers: bool = False
    client_defaults: Mapping[str, Mapping[str, int]] | None = None

    def __post_init__(self) -> None:
        self._positive_int("task_capacity", self.task_capacity)
        self._positive_int("priority_levels", self.priority_levels)
        if self.priority_levels < 2:
            raise ValueError("priority_levels must be at least 2")
        self._non_negative_int("io_buffer_length", self.io_buffer_length)
        if type(self.eternal_watchers) is not bool:
            raise TypeError("eternal_watchers must be a boolean")
        if self.client_defaults is None:
            return
        if not isinstance(self.client_defaults, Mapping):
            raise TypeError("client_defaults must be a mapping or None")
        normalized: dict[str, Mapping[str, int]] = {}
        for section, values in self.client_defaults.items():
            if not isinstance(section, str) or not section:
                raise TypeError("client_defaults section names must be non-empty strings")
            if not isinstance(values, Mapping):
                raise TypeError("client_defaults sections must be mappings")
            section_values: dict[str, int] = {}
            for name, value in values.items():
                if not isinstance(name, str) or not name:
                    raise TypeError("client_defaults setting names must be non-empty strings")
                self._non_negative_int(
                    "client_defaults.{}.{}".format(section, name), value
                )
                section_values[name] = value
            normalized[section] = MappingProxyType(section_values)
        object.__setattr__(self, "client_defaults", MappingProxyType(normalized))

    @staticmethod
    def _positive_int(name: str, value: int) -> None:
        if type(value) is not int:
            raise TypeError("{} must be an integer".format(name))
        if value <= 0:
            raise ValueError("{} must be greater than 0".format(name))

    @staticmethod
    def _non_negative_int(name: str, value: int) -> None:
        if type(value) is not int:
            raise TypeError("{} must be an integer".format(name))
        if value < 0:
            raise ValueError("{} must be 0 or greater".format(name))

    def to_smallos_config(self) -> dict[str, object]:
        """Return fresh plain data accepted by ``SmallOS(config=...)``."""
        client_defaults = None
        if self.client_defaults is not None:
            client_defaults = {
                section: dict(values)
                for section, values in self.client_defaults.items()
            }
        return {
            "task_capacity": self.task_capacity,
            "priority_levels": self.priority_levels,
            "io_buffer_length": self.io_buffer_length,
            "eternal_watchers": self.eternal_watchers,
            "client_defaults": client_defaults,
        }
