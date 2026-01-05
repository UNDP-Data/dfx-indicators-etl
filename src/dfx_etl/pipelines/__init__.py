"""
Pipelines and pipeline components for data sources. Source-specific processing logic is defined in
respective submodules.
"""

import importlib
import pkgutil

from ._pipeline import Pipeline

__all__ = ["Pipeline", "list_pipelines", "get_pipeline"]


def list_pipelines() -> list[str]:
    """
    List pipelines available in the packages.

    Returns
    -------
    list[str]
        List of pipelines names that can be used to get a specific pipeline.
    """
    return [
        name
        for module_info in pkgutil.iter_modules(__path__)
        if not (name := module_info.name).startswith("_")
    ]


def get_pipeline(name: str) -> Pipeline:
    """
    Get a runable pipeline.

    Parameters
    ----------
    name : str
        Name of the pipeline as returned in `list_pipelines`.

    Returns
    -------
    Pipeline
        Runable pipeline instance with retriever and transformer.

    Raises
    ------
    ValueError
        If `name` does not match any existing pipeline.
    """
    pipelines = set(list_pipelines())
    if not name in pipelines:
        raise ValueError(
            f"Pipeline '{name}' does not exist. Available pipelines: {sorted(pipelines)}"
        )
    module = importlib.import_module(f".{name}", package=__package__)
    return Pipeline(
        retriever=module.Retriever(),
        transformer=module.Transformer(),
    )
