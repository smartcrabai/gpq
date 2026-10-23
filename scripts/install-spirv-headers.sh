#!/usr/bin/env bash
# Installs SPIRV-Headers at the upstream commit pinned below, so that
# `find_package(SPIRV-Headers CONFIG)` resolves under PREFIX. The upstream
# CMakeLists.txt installs the package config to share/cmake/SPIRV-Headers
# (SPIRV-HeadersConfig.cmake) under the install prefix.
#
# Usage: install-spirv-headers.sh PREFIX
# When run on a GitHub Actions runner, also exports CMAKE_PREFIX_PATH=PREFIX
# to later steps.
set -euo pipefail

# Tag vulkan-sdk-1.4.363.0 of KhronosGroup/SPIRV-Headers.
readonly SPIRV_HEADERS_COMMIT=496543121ce6419f23d6fa5d7194ba66c36212d2

prefix="${1:?usage: install-spirv-headers.sh PREFIX}"
mkdir -p "$prefix"

# Windows runners execute this under Git Bash, where CMake needs mixed-style
# Windows paths.
if command -v cygpath >/dev/null 2>&1; then
  prefix="$(cygpath -m "$prefix")"
fi

src="$(mktemp -d)/SPIRV-Headers"
git init -q "$src"
git -C "$src" remote add origin https://github.com/KhronosGroup/SPIRV-Headers.git
git -C "$src" fetch -q --depth 1 origin "$SPIRV_HEADERS_COMMIT"
git -C "$src" checkout -q FETCH_HEAD

cmake -S "$src" -B "$src/build" \
  -DCMAKE_INSTALL_PREFIX="$prefix" \
  -DSPIRV_HEADERS_ENABLE_INSTALL=ON
cmake --build "$src/build" --target install

if [ -n "${GITHUB_ENV:-}" ]; then
  echo "CMAKE_PREFIX_PATH=$prefix" >> "$GITHUB_ENV"
fi
