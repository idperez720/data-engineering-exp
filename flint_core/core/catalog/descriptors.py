"""This module implements structural descriptors proxies to enhance IDE autocomplete."""

from typing import Any


class DatasetDescriptor:
    """Metaprogramming proxy engine routing dynamic dot-notation object paths access."""

    __slots__ = ("_dataset_name",)

    def __init__(self, dataset_name: str) -> None:
        """Injects identity strings bindings directly into descriptors frameworks instance.

        Args:
            dataset_name: Target alphanumeric identification string context key.
        """
        self._dataset_name: str = dataset_name

    def __get__(self, instance: Any, owner: Any) -> Any:
        """Intercepts system property lookups requests mapping entities objects out.

        Args:
            instance: Parent class instance execution anchor container frame.
            owner: Technical ownership class metadata types structures mapping.
        """
        if instance is None:
            return self
        return instance.get_dataset(self._dataset_name)
