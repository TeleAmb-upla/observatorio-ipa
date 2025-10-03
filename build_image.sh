#! /usr/bin/env bash

# export VERSION from uv (do not change)
export VERSION=$(uv version --short)
# export WEB_VERSION from [tool.webapp] version in pyproject.toml
export WEB_VERSION=$(grep -A 1 '\[tool.webapp\]' pyproject.toml | grep 'version' | sed 's/.*= *\"\(.*\)\".*/\1/')

# Usage: ./build_image.sh <group>
GROUP="${1:-default}"
docker buildx bake "$GROUP"