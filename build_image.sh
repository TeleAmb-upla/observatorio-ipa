#! /usr/bin/env bash

# export VERSION=$(sed -n "s/version = \"\(.*\)\"/\1/p" pyproject.toml)
export VERSION=$(uv version --short)
docker buildx bake 