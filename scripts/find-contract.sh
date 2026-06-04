#!/usr/bin/env bash
# find-contract.sh — cerca una funzione/contratto nei moduli centrali del toolkit.
#
# Uso:
#   ./scripts/find-contract.sh sql_literal     # cerca "sql_literal" per nome
#   ./scripts/find-contract.sh truncate        # cerca "truncate" nel nome o docstring
#   ./scripts/find-contract.sh -d "quote"      # cerca solo nelle definizioni (def/class)
#
# I moduli scanditi sono quelli di toolkit/core/, toolkit/plugins/_http_utils,
# toolkit/profile/. Se non trovi lì, probabilmente non esiste come contratto centrale.
#
# Per una mappa completa: ./scripts/generate-contracts-map.py

set -euo pipefail

TOOLKIT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
SEARCH_DIRS=(
    "$TOOLKIT_DIR/toolkit/core"
    "$TOOLKIT_DIR/toolkit/plugins/_http_utils.py"
    "$TOOLKIT_DIR/toolkit/profile/raw.py"
)

pattern="${1:-}"
if [ -z "$pattern" ]; then
    echo "Uso: $0 [-d] <pattern>"
    echo "  -d    cerca solo nelle definizioni (def/class)"
    exit 1
fi

only_defs=false
if [ "$1" = "-d" ]; then
    only_defs=true
    pattern="${2:-}"
    if [ -z "$pattern" ]; then
        echo "Uso: $0 -d <pattern>"
        exit 1
    fi
fi

found=0
for dir in "${SEARCH_DIRS[@]}"; do
    if [ -f "$dir" ]; then
        targets=("$dir")
    else
        targets=("$dir"/*.py)
    fi

    for file in "${targets[@]}"; do
        [ -f "$file" ] || continue
        basename="${file#$TOOLKIT_DIR/}"

        if [ "$only_defs" = true ]; then
            matches=$(grep -n "^\(async \)\?\(def \|class \)" "$file" \
                | grep -i "$pattern" || true)
        else
            matches=$(grep -i "$pattern" "$file" || true)
        fi

        if [ -n "$matches" ]; then
            echo "━━━ $basename"
            echo "$matches"
            echo ""
            found=$((found + 1))
        fi
    done
done

if [ "$found" -eq 0 ]; then
    echo "Nessun risultato per '$pattern' nei contratti centrali."
    echo "Suggerimento: se stai cercando una funzione che non esiste,"
    echo "valuta se aggiungerla in toolkit/core/ invece che localmente."
    exit 1
fi
