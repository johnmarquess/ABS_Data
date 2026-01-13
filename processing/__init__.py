"""Processing utilities for transforming ABS Census datasets."""

from importlib import import_module
from typing import TYPE_CHECKING

__all__ = [
    "process_c21_g01_sa2",
    "transform_c21_g01_sa2",
    "split_code_and_label",
    "process_c21_g19_sa2",
    "transform_c21_g19_sa2",
]

if TYPE_CHECKING:  # for static type checkers only
    from processing.c21_g01_sa2 import (
        process_c21_g01_sa2,
        transform_c21_g01_sa2,
        split_code_and_label,
    )
    from processing.c21_g19_sa2 import (
        process_c21_g19_sa2,
        transform_c21_g19_sa2,
    )


def __getattr__(name):
    """Lazily import processing modules to avoid side effects on package import."""

    if name in __all__:
        if name.startswith("process_c21_g19") or name.startswith("transform_c21_g19"):
            module = import_module("processing.c21_g19_sa2")
            return getattr(module, name)
        module = import_module("processing.c21_g01_sa2")
        return getattr(module, name)
    raise AttributeError(f"module 'processing' has no attribute {name}")
