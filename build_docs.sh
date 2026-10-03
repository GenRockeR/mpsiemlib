#!/usr/bin/env bash
# Сборка документации mpsiemlib (MkDocs + mkdocstrings из docstring'ов).
# Установка зависимостей: poetry install --with docs
set -euo pipefail

cd "$(dirname "$0")"

if [[ -x ".venv/bin/mkdocs" ]]; then
  MKDOCS=".venv/bin/mkdocs"
elif command -v poetry >/dev/null 2>&1; then
  MKDOCS="poetry run mkdocs"
else
  MKDOCS="mkdocs"
fi

# --strict: предупреждения docstring'ов считаются ошибкой сборки
$MKDOCS build --strict "$@"

echo "Готово: site/"
