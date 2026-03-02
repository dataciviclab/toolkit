"""Built-in plugin modules.

Plugin registration is explicit via `toolkit.core.registry.register_builtin_plugins()`.

Not all plugins have the same product weight:

- `local_file` and `http_file` are builtin stable sources used by the canonical
  workflow.
- other modules in this package may be peripheral or experimental and should not
  be treated as part of the default dataset-repo contract.
"""
