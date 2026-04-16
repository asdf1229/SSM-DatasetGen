#!/usr/bin/env bash
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${PROJECT_ROOT}"

PYTHON_BIN="${PYTHON:-python3}"
TASK_DIR="configs/ofat_tasks"
DATA_TASKS="${TASK_DIR}/data_graph_tasks.csv"
QUERY_TASKS="${TASK_DIR}/query_graph_tasks.csv"
RAW_REAL_GRAPHS="${RAW_REAL_GRAPHS:-datasets/raw/real_graphs}"
VALIDATION_REPORT="datasets/manifests/graph_validation_report.csv"
DATA_MANIFEST="datasets/manifests/data_graph_manifest.csv"
QUERY_MANIFEST="datasets/manifests/query_graph_manifest.csv"
RUN_REAL_CONVERSION="${RUN_REAL_CONVERSION:-auto}"

GENERATED_DATA_PATHS=(
    "${TASK_DIR}"
    "datasets/real"
    "datasets/synthetic"
    "datasets/queries"
    "${VALIDATION_REPORT}"
    "${DATA_MANIFEST}"
    "${QUERY_MANIFEST}"
)

TOTAL_STEPS=7
CURRENT_STEP=0
STEP_NAMES=()
STEP_STATUSES=()
STEP_DONE_COUNTS=()
STEP_FAILURE_COUNTS=()
STEP_NOTES=()
STEP_LOG_DIR="$(mktemp -d "${TMPDIR:-/tmp}/ssm-ofat-pipeline.XXXXXX")"
CURRENT_STEP_LOG=""

cleanup() {
    rm -rf "${STEP_LOG_DIR}"
}
trap cleanup EXIT

log() {
    printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

clean_generated_data() {
    local path
    local removed_count=0

    log "Cleaning generated data from previous runs."
    for path in "${GENERATED_DATA_PATHS[@]}"; do
        if [[ -z "${path}" || "${path}" == "/" || "${path}" == "." ]]; then
            log "Refusing to remove unsafe generated data path: ${path}"
            return 1
        fi
        if [[ -e "${path}" ]]; then
            if ! rm -rf "${path}"; then
                log "Failed to remove ${path}"
                return 1
            fi
            removed_count=$((removed_count + 1))
            log "Removed ${path}"
        fi
    done

    if [[ "${removed_count}" -eq 0 ]]; then
        log "No generated data from previous runs found."
    fi
}

count_visible_files() {
    local path="$1"
    if [[ ! -e "${path}" ]]; then
        printf '0'
        return
    fi

    if [[ -f "${path}" ]]; then
        case "$(basename "${path}")" in
            .*) printf '0' ;;
            *) printf '1' ;;
        esac
        return
    fi

    find "${path}" -type f ! -name '.*' | wc -l | tr -d '[:space:]'
}

count_graph_files() {
    local path="$1"
    if [[ ! -e "${path}" ]]; then
        printf '0'
        return
    fi

    if [[ -f "${path}" ]]; then
        case "$(basename "${path}")" in
            .*|*.csv|*.json|*.yaml|*.yml|*.md) printf '0' ;;
            *) case "${path}" in */query_graph/*) printf '0' ;; *) printf '1' ;; esac ;;
        esac
        return
    fi

    find "${path}" \
        -type f \
        ! -path '*/query_graph/*' \
        ! -name '.*' \
        ! -name '*.csv' \
        ! -name '*.json' \
        ! -name '*.yaml' \
        ! -name '*.yml' \
        ! -name '*.md' \
        | wc -l | tr -d '[:space:]'
}

count_synthetic_data_graph_files() {
    local path="$1"
    local packaged_count
    if [[ ! -e "${path}" ]]; then
        printf '0'
        return
    fi

    packaged_count="$(
        find "${path}" \
            -type f \
            -name 'graph_g.txt' \
            ! -path '*/query_graph/*' \
            | wc -l | tr -d '[:space:]'
    )"
    if [[ "${packaged_count}" != "0" ]]; then
        printf '%s' "${packaged_count}"
        return
    fi

    count_graph_files "${path}"
}

count_packaged_query_graph_files() {
    local path="$1"
    if [[ ! -e "${path}" ]]; then
        printf '0'
        return
    fi

    find "${path}" \
        -type f \
        -path '*/query_graph/*' \
        ! -name '.*' \
        ! -name '*.csv' \
        ! -name '*.json' \
        ! -name '*.yaml' \
        ! -name '*.yml' \
        ! -name '*.md' \
        | wc -l | tr -d '[:space:]'
}

csv_count_rows() {
    local path="$1"
    if [[ ! -f "${path}" ]]; then
        printf '0'
        return
    fi

    "${PYTHON_BIN}" -c 'import csv, sys
with open(sys.argv[1], "r", encoding="utf-8", newline="") as handle:
    print(sum(1 for _ in csv.DictReader(handle)))
' "${path}" 2>/dev/null || printf '0'
}

csv_count_column_true() {
    local path="$1"
    local column="$2"
    if [[ ! -f "${path}" ]]; then
        printf '0'
        return
    fi

    "${PYTHON_BIN}" -c 'import csv, sys
path, column = sys.argv[1], sys.argv[2]
count = 0
with open(path, "r", encoding="utf-8", newline="") as handle:
    for row in csv.DictReader(handle):
        if row.get(column, "").strip().lower() in {"1", "true", "yes", "valid"}:
            count += 1
print(count)
' "${path}" "${column}" 2>/dev/null || printf '0'
}

csv_count_column_not_true() {
    local path="$1"
    local column="$2"
    if [[ ! -f "${path}" ]]; then
        printf '0'
        return
    fi

    "${PYTHON_BIN}" -c 'import csv, sys
path, column = sys.argv[1], sys.argv[2]
count = 0
with open(path, "r", encoding="utf-8", newline="") as handle:
    for row in csv.DictReader(handle):
        if row.get(column, "").strip().lower() not in {"1", "true", "yes", "valid"}:
            count += 1
print(count)
' "${path}" "${column}" 2>/dev/null || printf '0'
}

log_count_or_default() {
    local pattern="$1"
    local fallback="$2"
    local count=""

    if [[ -n "${CURRENT_STEP_LOG}" && -f "${CURRENT_STEP_LOG}" ]]; then
        count="$(sed -nE "s/.*${pattern}.*/\\1/p" "${CURRENT_STEP_LOG}" | tail -n 1)"
    fi

    if [[ -n "${count}" ]]; then
        printf '%s' "${count}"
    else
        printf '%s' "${fallback}"
    fi
}

reset_summary() {
    SUMMARY_DONE_COUNT=0
    SUMMARY_FAILURE_COUNT=0
    SUMMARY_NOTE=""
}

summarize_make_ofat_configs() {
    local data_count
    local query_count
    data_count="$(csv_count_rows "${DATA_TASKS}")"
    query_count="$(csv_count_rows "${QUERY_TASKS}")"

    SUMMARY_DONE_COUNT=$((data_count + query_count))
    SUMMARY_FAILURE_COUNT=0
    SUMMARY_NOTE="data_tasks=${data_count}, query_tasks=${query_count}"
}

summarize_convert_real_graphs() {
    local raw_count
    local converted_count
    local real_count
    raw_count="$(count_visible_files "${RAW_REAL_GRAPHS}")"
    converted_count="$(log_count_or_default 'converted ([0-9]+) real graph file' "0")"
    real_count="$(count_graph_files "datasets/real")"

    SUMMARY_DONE_COUNT="${converted_count}"
    SUMMARY_FAILURE_COUNT=0
    SUMMARY_NOTE="raw_inputs=${raw_count}, real_graphs=${real_count}"
}

summarize_generate_synthetic_graphs() {
    local task_count
    local generated_count
    local graph_count
    task_count="$(csv_count_rows "${DATA_TASKS}")"
    generated_count="$(log_count_or_default 'generated ([0-9]+) synthetic graph file' "0")"
    graph_count="$(count_synthetic_data_graph_files "datasets/synthetic")"

    SUMMARY_DONE_COUNT="${generated_count}"
    SUMMARY_FAILURE_COUNT=0
    SUMMARY_NOTE="data_tasks=${task_count}, synthetic_graphs=${graph_count}"
}

summarize_validate_graphs() {
    local row_count
    local invalid_count
    row_count="$(log_count_or_default 'wrote validation report with ([0-9]+) row' "$(csv_count_rows "${VALIDATION_REPORT}")")"
    invalid_count="$(csv_count_column_not_true "${VALIDATION_REPORT}" "is_valid")"

    SUMMARY_DONE_COUNT="${row_count}"
    SUMMARY_FAILURE_COUNT="${invalid_count}"
    SUMMARY_NOTE="report=${VALIDATION_REPORT}"
}

summarize_build_data_manifest() {
    local row_count
    local invalid_count
    row_count="$(log_count_or_default 'wrote data graph manifest with ([0-9]+) row' "$(csv_count_rows "${DATA_MANIFEST}")")"
    invalid_count="$(csv_count_column_not_true "${DATA_MANIFEST}" "is_valid")"

    SUMMARY_DONE_COUNT="${row_count}"
    SUMMARY_FAILURE_COUNT="${invalid_count}"
    SUMMARY_NOTE="manifest=${DATA_MANIFEST}"
}

summarize_generate_query_graphs() {
    local generated_count
    local graph_count
    local valid_data_count
    local query_task_count
    generated_count="$(log_count_or_default 'generated ([0-9]+) query graph file' "0")"
    graph_count="$(count_packaged_query_graph_files "datasets/synthetic")"
    if [[ "${graph_count}" == "0" ]]; then
        graph_count="$(count_graph_files "datasets/queries")"
    fi
    valid_data_count="$(csv_count_column_true "${DATA_MANIFEST}" "is_valid")"
    query_task_count="$(csv_count_rows "${QUERY_TASKS}")"

    SUMMARY_DONE_COUNT="${generated_count}"
    SUMMARY_FAILURE_COUNT=0
    SUMMARY_NOTE="valid_data_graphs=${valid_data_count}, query_tasks=${query_task_count}, query_graphs=${graph_count}"
}

summarize_build_query_manifest() {
    local row_count
    row_count="$(log_count_or_default 'wrote query graph manifest with ([0-9]+) row' "$(csv_count_rows "${QUERY_MANIFEST}")")"

    SUMMARY_DONE_COUNT="${row_count}"
    SUMMARY_FAILURE_COUNT=0
    SUMMARY_NOTE="manifest=${QUERY_MANIFEST}"
}

record_step_result() {
    STEP_NAMES+=("$1")
    STEP_STATUSES+=("$2")
    STEP_DONE_COUNTS+=("$3")
    STEP_FAILURE_COUNTS+=("$4")
    STEP_NOTES+=("$5")
}

print_summary() {
    local index

    log ""
    log "OFAT pipeline summary:"
    printf '%-4s %-28s %-10s %-10s %-10s %s\n' "#" "step" "status" "done" "failed" "note"
    printf '%-4s %-28s %-10s %-10s %-10s %s\n' "--" "----" "------" "----" "------" "----"

    for ((index = 0; index < ${#STEP_NAMES[@]}; index++)); do
        printf '%-4s %-28s %-10s %-10s %-10s %s\n' \
            "$((index + 1))" \
            "${STEP_NAMES[${index}]}" \
            "${STEP_STATUSES[${index}]}" \
            "${STEP_DONE_COUNTS[${index}]}" \
            "${STEP_FAILURE_COUNTS[${index}]}" \
            "${STEP_NOTES[${index}]}"
    done
}

finish() {
    local exit_code="$1"
    print_summary
    exit "${exit_code}"
}

start_step() {
    local name="$1"
    local description="$2"
    CURRENT_STEP=$((CURRENT_STEP + 1))

    log ""
    log "Step ${CURRENT_STEP}/${TOTAL_STEPS}: ${name}"
    log "Description: ${description}"
}

skip_step() {
    local name="$1"
    local description="$2"
    local note="$3"

    start_step "${name}" "${description}"
    log "Skipped: ${note}"
    record_step_result "${name}" "skipped" "0" "0" "${note}"
}

run_step() {
    local name="$1"
    local description="$2"
    local required="$3"
    local summarize_func="$4"
    local exit_code

    shift 4
    start_step "${name}" "${description}"
    CURRENT_STEP_LOG="${STEP_LOG_DIR}/step_${CURRENT_STEP}.log"

    "$@" 2>&1 | tee "${CURRENT_STEP_LOG}"
    exit_code="${PIPESTATUS[0]}"

    reset_summary
    "${summarize_func}"

    if [[ "${exit_code}" -ne 0 ]]; then
        SUMMARY_FAILURE_COUNT=$((SUMMARY_FAILURE_COUNT + 1))
        record_step_result "${name}" "failed" "${SUMMARY_DONE_COUNT}" "${SUMMARY_FAILURE_COUNT}" "${SUMMARY_NOTE}; exit_code=${exit_code}"
        if [[ "${required}" == "required" ]]; then
            log "Required step failed; stopping the pipeline."
            finish "${exit_code}"
        fi
        log "Optional step failed; recorded the failure and continuing."
        return 0
    fi

    record_step_result "${name}" "ok" "${SUMMARY_DONE_COUNT}" "${SUMMARY_FAILURE_COUNT}" "${SUMMARY_NOTE}"
}

clean_generated_data || finish 1

run_step \
    "Expand OFAT configs" \
    "Expand data/query OFAT configs into reusable task parameter files for later steps." \
    "required" \
    "summarize_make_ofat_configs" \
    "${PYTHON_BIN}" SSM-Pipeline/make_ofat_configs.py --output-dir "${TASK_DIR}"

case "${RUN_REAL_CONVERSION}" in
    auto)
        if [[ "$(count_visible_files "${RAW_REAL_GRAPHS}")" -eq 0 ]]; then
            skip_step \
                "Convert real graphs" \
                "Convert raw real graphs into standard .txt files; safe to skip when no raw inputs exist." \
                "no raw real graph files under ${RAW_REAL_GRAPHS}"
        else
            run_step \
                "Convert real graphs" \
                "Convert raw real graphs into standard .txt files; this optional step will not block synthetic graph generation if it fails." \
                "optional" \
                "summarize_convert_real_graphs" \
                "${PYTHON_BIN}" SSM-GraphGen/convert_real_graphs.py --input "${RAW_REAL_GRAPHS}"
        fi
        ;;
    1|true|yes|always)
        run_step \
            "Convert real graphs" \
            "Run real graph conversion as requested; this optional step will not block synthetic graph generation if it fails." \
            "optional" \
            "summarize_convert_real_graphs" \
            "${PYTHON_BIN}" SSM-GraphGen/convert_real_graphs.py --input "${RAW_REAL_GRAPHS}"
        ;;
    0|false|no|skip)
        skip_step \
            "Convert real graphs" \
            "Convert raw real graphs into standard .txt files; currently skipped by RUN_REAL_CONVERSION." \
            "RUN_REAL_CONVERSION=${RUN_REAL_CONVERSION}"
        ;;
    *)
        log "RUN_REAL_CONVERSION=${RUN_REAL_CONVERSION} is not recognized; use auto/1/0."
        finish 2
        ;;
esac

run_step \
    "Generate synthetic graphs" \
    "Generate synthetic data graphs from data_graph_tasks.csv; existing outputs are handled by the child script's normal skip logic." \
    "required" \
    "summarize_generate_synthetic_graphs" \
    "${PYTHON_BIN}" SSM-GraphGen/generate_synthetic_graphs.py --tasks "${DATA_TASKS}"

run_step \
    "Validate data graphs" \
    "Validate standard-format real and synthetic graphs, then write the graph validation report." \
    "required" \
    "summarize_validate_graphs" \
    "${PYTHON_BIN}" SSM-GraphGen/validate_graph_format.py

run_step \
    "Build data manifest" \
    "Summarize real and synthetic graphs into the data graph manifest used by query generation." \
    "required" \
    "summarize_build_data_manifest" \
    "${PYTHON_BIN}" SSM-Pipeline/build_data_graph_manifest.py

run_step \
    "Generate query graphs" \
    "Generate query graphs for each valid data graph using query_graph_tasks.csv; the child script skips this step if the generator is missing." \
    "required" \
    "summarize_generate_query_graphs" \
    "${PYTHON_BIN}" SSM-QueryGen/generate_query_graphs.py --tasks "${QUERY_TASKS}"

run_step \
    "Build query manifest" \
    "Scan the query graph output directory and build the final query graph manifest." \
    "required" \
    "summarize_build_query_manifest" \
    "${PYTHON_BIN}" SSM-Pipeline/build_query_graph_manifest.py

finish 0
