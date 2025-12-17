from typing import Dict, List, Union

import pluggy
import sys
from . import hookspecs

pm: pluggy.PluginManager = pluggy.PluginManager("sqlite_utils")
pm.add_hookspecs(hookspecs)

if not getattr(sys, "_called_from_test", False):
    # Only load plugins if not running tests
    pm.load_setuptools_entrypoints("sqlite_utils")


def get_plugins() -> List[Dict[str, Union[str, List[str]]]]:
    plugins: List[Dict[str, Union[str, List[str]]]] = []
    plugin_to_distinfo = dict(pm.list_plugin_distinfo())
    for plugin in pm.get_plugins():
        hookcallers = pm.get_hookcallers(plugin) or []
        plugin_info: Dict[str, Union[str, List[str]]] = {
            "name": plugin.__name__,
            "hooks": [h.name for h in hookcallers],
        }
        distinfo = plugin_to_distinfo.get(plugin)
        if distinfo:
            plugin_info["version"] = distinfo.version
            plugin_info["name"] = distinfo.project_name
        plugins.append(plugin_info)
    return plugins
