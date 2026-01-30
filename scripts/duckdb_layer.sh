#!/usr/bin/env bash
set -euo pipefail

# ===  CONFIGURATION  ===================================================

# DuckDB version to download extensions for (must match your binary)
DUCKDB_VERSION="${1:-1.4.3}"

# Platform name: linux_arm64 or linux_amd64, etc.
PLATFORM="${2:-linux_arm64}"

# Extensions list (comma-separated), e.g.: httpfs,aws,parquet
EXTENSIONS="${3:-httpfs,aws,postgres}"


# ===  OUTPUT LOCATIONS  =================================================

WORKDIR=$(pwd)/duckdb_layer_build
EXT_DIR="${WORKDIR}/extensions"
LAYER_ROOT="${WORKDIR}/layer"
ZIP_OUT="duckdb-extensions-${PLATFORM}-v${DUCKDB_VERSION}.zip"

# ===  PREPARE DIRECTORIES  =============================================

echo "Cleaning up old build at ${WORKDIR}..."
rm -rf "${WORKDIR}"
mkdir -p "${EXT_DIR}"
mkdir -p "${LAYER_ROOT}/duckdb/extensions"

# ===  DOWNLOAD & DECOMPRESS EXTENSIONS  ================================

IFS=',' read -r -a EXT_ARR <<< "${EXTENSIONS}"

for ext in "${EXT_ARR[@]}"; do
    ext_trimmed=$(echo "${ext}" | xargs)
    echo "Downloading ${ext_trimmed} extension for platform ${PLATFORM}..."

    URL="http://extensions.duckdb.org/v${DUCKDB_VERSION}/${PLATFORM}/${ext_trimmed}.duckdb_extension.gz"
    OUT_GZ="${EXT_DIR}/${ext_trimmed}.duckdb_extension.gz"
    OUT_BIN="${EXT_DIR}/${ext_trimmed}.duckdb_extension"

    # Fetch extension gz
    curl -fSL "${URL}" -o "${OUT_GZ}"

    # Decompress
    echo "Decompressing ${OUT_GZ} → ${OUT_BIN}..."
    gzip -dk "${OUT_GZ}"
done

# ===  ORGANIZE LAYER STRUCTURE  ========================================

echo "Organizing Lambda layer directory structure..."

# extensions go under duckdb/extensions in layer
cp "${EXT_DIR}"/*.duckdb_extension "${LAYER_ROOT}/duckdb/extensions/"

# (Optional) Place README
cat > "${LAYER_ROOT}/README.txt" <<EOF
DuckDB extensions layer
Version: ${DUCKDB_VERSION}
Platform: ${PLATFORM}
Extensions: ${EXTENSIONS}
EOF

# ===  ZIP LAMBDA LAYER  =================================================

echo "Packaging into ${ZIP_OUT}..."
cd "${LAYER_ROOT}"
zip -r "../${ZIP_OUT}" .
cd -

echo "Done! Generated Lambda layer zip: ${ZIP_OUT}"