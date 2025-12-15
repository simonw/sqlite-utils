from typing import Any, Dict, List

import pluggy
import sys
from . import hookspecs

pm: pluggy.PluginManager = pluggy.PluginManager("sqlite_utils")
pm.add_hookspecs(hookspecs)

if not getattr(sys, "_called_from_test", False):
    # Only load plugins if not running tests
    pm.load_setuptools_entrypoints("sqlite_utils")


def get_plugins() -> List[Dict[str, Any]]:
    plugins: List[Dict[str, Any]] = []
    plugin_to_distinfo = dict(pm.list_plugin_distinfo())
    for plugin in pm.get_plugins():
        hookcallers = pm.get_hookcallers(plugin)
        plugin_info: Dict[str, Any] = {
            "name": plugin.__name__,
            "hooks": [h.name for h in hookcallers] if hookcallers else [],
        }
        distinfo = plugin_to_distinfo.get(plugin)
        if distinfo:
            plugin_info["version"] = distinfo.version
            plugin_info["name"] = distinfo.project_name
        plugins.append(plugin_info)
    return plugins
