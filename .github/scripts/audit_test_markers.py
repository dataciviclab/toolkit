#!/usr/bin/env python3
"""Backward-compatibility shim — delegates to ``lab_connectors.testing.audit_markers``.

If ``lab-connectors`` is not installed, prints a clear error and exits.

New code should use ``audit-test-markers`` CLI command directly
(installed via ``pip install lab-connectors``).
"""

import sys

try:
    from lab_connectors.testing.audit_markers import main
except ImportError:
    print(
        "ERROR: lab-connectors not installed.\n"
        "\n"
        "This script requires lab-connectors to be installed:\n"
        "  pip install lab-connectors\n"
        "\n"
        "Or use the audit-test-markers CLI directly after installation.",
        file=sys.stderr,
    )
    sys.exit(2)

if __name__ == "__main__":
    raise SystemExit(main())
