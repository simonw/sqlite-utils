from typing import Dict, List, Union

import pluggy
import sys
from . import hookspecs

pm: pluggy.PluginManager = pluggy.PluginManager("sqlite_utils")
pm.add_hookspecs(hookspecs)
_plugins_loaded = False


def ensure_plugins_loaded() -> None:
    global _plugins_loaded
    if _plugins_loaded or getattr(sys, "_called_from_test", False):
        return
    pm.load_setuptools_entrypoints("sqlite_utils")
    _plugins_loaded = True


def get_plugins() -> List[Dict[str, Union[str, List[str]]]]:
    ensure_plugins_loaded()
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
