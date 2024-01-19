"Modules for individual datasets"

import pkgutil
from pathlib import Path
import importlib

from .base import DugPipeline

def get_pipeline_classes():
    """Return a list of all defined pipeline classes
    """

    base_path = Path(__file__).resolve().parent

    for (_, mod_name, _) in pkgutil.iter_modules([base_path]):
        if mod_name == 'base':
            continue

        # No need to actuall get the module symbol, once it's imported, it will
        # show up below in __subclasses__.
        importlib.import_module(f"{__name__}.{mod_name}")
    pipeline_list = []

    for subclass in DugPipeline.__subclasses__():
        pipeline_name = getattr(subclass, 'pipeline_name', None)
        if pipeline_name:
            pipeline_list.append(subclass)
    return pipeline_list
