#!/usr/bin/env bash
# Rebuild the bundled Tailwind CSS after changing classes in web/templates or web/static JS.
set -euo pipefail
cd "$(dirname "$0")/.."
npx --yes tailwindcss@3.4.19 \
  --config web/tailwind/tailwind.config.cjs \
  --input web/tailwind/input.css \
  --output web/static/vendor/tailwind.css \
  --minify
