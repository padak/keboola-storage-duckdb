#!/bin/bash
#
# Manual testing script for DuckDB API Service
#
# Usage:
#   ./scripts/manual_test.sh           # Run all tests
#   ./scripts/manual_test.sh health    # Run specific test
#
# Prerequisites:
#   - Server running on localhost:8000
#   - jq installed (for pretty JSON output)
#

BASE_URL="${BASE_URL:-http://localhost:8000}"
PROJECT_ID="manual_test_$(date +%s)"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}DuckDB API Manual Tests${NC}"
echo -e "${BLUE}========================================${NC}"
echo "Base URL: $BASE_URL"
echo "Project ID: $PROJECT_ID"
echo ""

# Helper function
run_test() {
    local name="$1"
    local method="$2"
    local endpoint="$3"
    local data="$4"

    echo -e "\n${YELLOW}>>> $name${NC}"
    echo -e "${BLUE}$method $endpoint${NC}"

    if [ -n "$data" ]; then
        echo "Data: $data"
        curl -s -X "$method" "$BASE_URL$endpoint" \
            -H "Content-Type: application/json" \
            -d "$data" | jq .
    else
        curl -s -X "$method" "$BASE_URL$endpoint" | jq .
    fi
}

# Health check
health() {
    run_test "Health Check" "GET" "/health"
}

# Backend init
backend_init() {
    run_test "Backend Init" "POST" "/backend/init"
}

# Project operations
create_project() {
    run_test "Create Project" "POST" "/projects" \
        "{\"id\": \"$PROJECT_ID\", \"name\": \"Manual Test Project\"}"
}

get_project() {
    run_test "Get Project" "GET" "/projects/$PROJECT_ID"
}

list_projects() {
    run_test "List Projects" "GET" "/projects"
}

project_stats() {
    run_test "Project Stats" "GET" "/projects/$PROJECT_ID/stats"
}

# Bucket operations
create_bucket() {
    run_test "Create Bucket" "POST" "/projects/$PROJECT_ID/buckets" \
        '{"name": "in_c_sales", "description": "Sales data"}'
}

list_buckets() {
    run_test "List Buckets" "GET" "/projects/$PROJECT_ID/buckets"
}

get_bucket() {
    run_test "Get Bucket" "GET" "/projects/$PROJECT_ID/buckets/in_c_sales"
}

# Table operations
create_table() {
    run_test "Create Table (orders)" "POST" "/projects/$PROJECT_ID/buckets/in_c_sales/tables" \
        '{
            "name": "orders",
            "columns": [
                {"name": "id", "type": "INTEGER", "nullable": false},
                {"name": "customer", "type": "VARCHAR"},
                {"name": "amount", "type": "DOUBLE"},
                {"name": "created_at", "type": "TIMESTAMP"}
            ],
            "primary_key": ["id"]
        }'
}

create_table_simple() {
    run_test "Create Table (customers)" "POST" "/projects/$PROJECT_ID/buckets/in_c_sales/tables" \
        '{
            "name": "customers",
            "columns": [
                {"name": "id", "type": "INTEGER"},
                {"name": "name", "type": "VARCHAR"},
                {"name": "email", "type": "VARCHAR"}
            ]
        }'
}

list_tables() {
    run_test "List Tables" "GET" "/projects/$PROJECT_ID/buckets/in_c_sales/tables"
}

get_table() {
    run_test "Get Table (ObjectInfo)" "GET" "/projects/$PROJECT_ID/buckets/in_c_sales/tables/orders"
}

preview_table() {
    run_test "Preview Table" "GET" "/projects/$PROJECT_ID/buckets/in_c_sales/tables/orders/preview?limit=10"
}

delete_table() {
    echo -e "\n${YELLOW}>>> Delete Table${NC}"
    echo -e "${BLUE}DELETE /projects/$PROJECT_ID/buckets/in_c_sales/tables/orders${NC}"
    curl -s -X DELETE "$BASE_URL/projects/$PROJECT_ID/buckets/in_c_sales/tables/orders" -w "\nHTTP Status: %{http_code}\n"
}

# Cleanup
delete_bucket() {
    echo -e "\n${YELLOW}>>> Delete Bucket${NC}"
    echo -e "${BLUE}DELETE /projects/$PROJECT_ID/buckets/in_c_sales${NC}"
    curl -s -X DELETE "$BASE_URL/projects/$PROJECT_ID/buckets/in_c_sales" -w "\nHTTP Status: %{http_code}\n"
}

delete_project() {
    echo -e "\n${YELLOW}>>> Delete Project${NC}"
    echo -e "${BLUE}DELETE /projects/$PROJECT_ID${NC}"
    curl -s -X DELETE "$BASE_URL/projects/$PROJECT_ID" -w "\nHTTP Status: %{http_code}\n"
}

# Run all tests
run_all() {
    health
    backend_init
    create_project
    get_project
    list_projects
    project_stats
    create_bucket
    list_buckets
    get_bucket
    create_table
    create_table_simple
    list_tables
    get_table
    preview_table
    project_stats

    echo -e "\n${GREEN}========================================${NC}"
    echo -e "${GREEN}All basic tests completed!${NC}"
    echo -e "${GREEN}========================================${NC}"
    echo ""
    echo "To cleanup, run:"
    echo "  $0 cleanup"
    echo ""
    echo "Project ID for manual testing: $PROJECT_ID"
}

cleanup() {
    echo -e "${YELLOW}Cleaning up...${NC}"
    delete_table
    delete_bucket
    delete_project
    echo -e "\n${GREEN}Cleanup done!${NC}"
}

# Main
case "${1:-all}" in
    health) health ;;
    init) backend_init ;;
    project) create_project && get_project ;;
    bucket) create_bucket && get_bucket ;;
    table) create_table && get_table ;;
    preview) preview_table ;;
    stats) project_stats ;;
    cleanup) cleanup ;;
    all) run_all ;;
    *)
        echo "Usage: $0 [command]"
        echo ""
        echo "Commands:"
        echo "  all      - Run all tests (default)"
        echo "  health   - Health check only"
        echo "  init     - Backend init"
        echo "  project  - Create and get project"
        echo "  bucket   - Create and get bucket"
        echo "  table    - Create and get table"
        echo "  preview  - Preview table"
        echo "  stats    - Project statistics"
        echo "  cleanup  - Delete test resources"
        ;;
esac
