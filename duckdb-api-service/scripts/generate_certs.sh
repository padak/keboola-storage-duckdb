#!/bin/bash
# Generate self-signed SSL certificates for PG Wire server
# Usage: ./scripts/generate_certs.sh [output_dir]

set -e

OUTPUT_DIR="${1:-./certs}"
DAYS_VALID=365
KEY_SIZE=2048
COUNTRY="CZ"
STATE="Prague"
LOCALITY="Prague"
ORGANIZATION="Keboola"
COMMON_NAME="localhost"

echo "Generating SSL certificates in ${OUTPUT_DIR}..."

# Create output directory
mkdir -p "${OUTPUT_DIR}"

# Generate private key
openssl genrsa -out "${OUTPUT_DIR}/server.key" ${KEY_SIZE}

# Generate certificate signing request (CSR)
openssl req -new \
    -key "${OUTPUT_DIR}/server.key" \
    -out "${OUTPUT_DIR}/server.csr" \
    -subj "/C=${COUNTRY}/ST=${STATE}/L=${LOCALITY}/O=${ORGANIZATION}/CN=${COMMON_NAME}"

# Generate self-signed certificate
openssl x509 -req \
    -days ${DAYS_VALID} \
    -in "${OUTPUT_DIR}/server.csr" \
    -signkey "${OUTPUT_DIR}/server.key" \
    -out "${OUTPUT_DIR}/server.crt"

# Set permissions
chmod 600 "${OUTPUT_DIR}/server.key"
chmod 644 "${OUTPUT_DIR}/server.crt"

# Clean up CSR
rm -f "${OUTPUT_DIR}/server.csr"

echo "SSL certificates generated:"
echo "  Certificate: ${OUTPUT_DIR}/server.crt"
echo "  Private key: ${OUTPUT_DIR}/server.key"
echo ""
echo "To use with PG Wire server:"
echo "  python -m src.pgwire_server --ssl-cert ${OUTPUT_DIR}/server.crt --ssl-key ${OUTPUT_DIR}/server.key"
echo ""
echo "To connect with psql:"
echo "  psql 'host=localhost port=5432 sslmode=require user=<workspace_username>'"
