#!/usr/bin/env bash
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${PROJECT_ROOT}"

PYTHON_BIN="${PYTHON:-python3}"
DATASET_SCOPE="${DATASET_SCOPE:-all}"
RUN_ID="${RUN_ID:-}"
OUTPUT_BASE="${OUTPUT_BASE:-datasets/runs}"
OUTPUT_ROOT="${OUTPUT_ROOT:-}"
PIPELINE_OVERWRITE="${PIPELINE_OVERWRITE:-0}"
RUN_REAL_CONVERSION="${RUN_REAL_CONVERSION:-auto}"
RAW_REAL_GRAPHS="${RAW_REAL_GRAPHS:-datasets/raw/real_graphs}"

usage() {
    cat <<'EOF'
Usage: bash SSM-Pipeline/run_ofat_pipeline.sh [options]

Options:
  --scope all|real|synthetic     Select which data graph source(s) to process.
                                 Defaults to all. Can also use DATASET_SCOPE.
  --run-id ID                    Output folder name. Defaults to YYYYMMDD_HHMMSS.
                                 Can also use RUN_ID.
  --output-base DIR              Parent directory for timestamped runs.
                                 Defaults to datasets/runs. Can also use OUTPUT_BASE.
  --output-root DIR              Full output directory. Overrides --output-base/--run-id.
                                 Can also use OUTPUT_ROOT.
  --raw-real-graphs PATH         Raw real graph file or directory.
                                 Defaults to datasets/raw/real_graphs.
  --run-real-conversion VALUE    auto, 1, or 0. Defaults to auto.
  --overwrite                    Overwrite files when child generators support it.
  -h, --help                     Show this help message.
EOF
}

require_value() {
    local option="$1"
    local value="${2-}"
    if [[ -z "${value}" ]]; then
        printf 'Missing value for %s\n' "${option}" >&2
        usage >&2
        exit 2
    fi
}

while [[ "$#" -gt 0 ]]; do
    case "$1" in
        --scope)
            require_value "$1" "${2-}"
            DATASET_SCOPE="$2"
            shift 2
            ;;
        --scope=*)
            DATASET_SCOPE="${1#*=}"
            shift
            ;;
        --run-id)
            require_value "$1" "${2-}"
            RUN_ID="$2"
            shift 2
            ;;
        --run-id=*)
            RUN_ID="${1#*=}"
            shift
            ;;
        --output-base)
            require_value "$1" "${2-}"
            OUTPUT_BASE="$2"
            shift 2
            ;;
        --output-base=*)
            OUTPUT_BASE="${1#*=}"
            shift
            ;;
        --output-root)
            require_value "$1" "${2-}"
            OUTPUT_ROOT="$2"
            shift 2
            ;;
        --output-root=*)
            OUTPUT_ROOT="${1#*=}"
            shift
            ;;
        --raw-real-graphs)
            require_value "$1" "${2-}"
            RAW_REAL_GRAPHS="$2"
            shift 2
            ;;
        --raw-real-graphs=*)
            RAW_REAL_GRAPHS="${1#*=}"
            shift
            ;;
        --run-real-conversion)
            require_value "$1" "${2-}"
            RUN_REAL_CONVERSION="$2"
            shift 2
            ;;
        --run-real-conversion=*)
            RUN_REAL_CONVERSION="${1#*=}"
            shift
            ;;
        --overwrite)
            PIPELINE_OVERWRITE=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            printf 'Unknown option: %s\n' "$1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

case "${DATASET_SCOPE}" in
    all|real|synthetic) ;;
    *)
        printf 'DATASET_SCOPE=%s is not recognized; use all, real, or synthetic.\n' "${DATASET_SCOPE}" >&2
        exit 2
        ;;
esac

if [[ -z "${RUN_ID}" ]]; then
    RUN_ID="$(date '+%Y%m%d_%H%M%S')"
fi
if [[ -z "${OUTPUT_ROOT}" ]]; then
    OUTPUT_ROOT="${OUTPUT_BASE}/${RUN_ID}"
fi

TASK_DIR="${OUTPUT_ROOT}/configs/ofat_tasks"
DATA_TASKS="${TASK_DIR}/data_graph_tasks.csv"
QUERY_TASKS="${TASK_DIR}/query_graph_tasks.csv"
REAL_DIR="${OUTPUT_ROOT}/real"
SYNTHETIC_DIR="${OUTPUT_ROOT}/synthetic"
QUERIES_DIR="${OUTPUT_ROOT}/queries"
MANIFEST_DIR="${OUTPUT_ROOT}/manifests"
VALIDATION_REPORT="${MANIFEST_DIR}/graph_validation_report.csv"
DATA_MANIFEST="${MANIFEST_DIR}/data_graph_manifest.csv"
QUERY_MANIFEST="${MANIFEST_DIR}/query_graph_manifest.csv"
EMPTY_REAL_DIR="${OUTPUT_ROOT}/_empty_real"
EMPTY_SYNTHETIC_DIR="${OUTPUT_ROOT}/_empty_synthetic"
OVERWRITE_ARGS=()
if [[ "${PIPELINE_OVERWRITE}" == "1" || "${PIPELINE_OVERWRITE}" == "true" || "${PIPELINE_OVERWRITE}" == "yes" ]]; then
    OVERWRITE_ARGS=(--overwrite)
fi

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

scope_includes_real() {
    case "${DATASET_SCOPE}" in
        all|real) return 0 ;;
        *) return 1 ;;
    esac
}

scope_includes_synthetic() {
    case "${DATASET_SCOPE}" in
        all|synthetic) return 0 ;;
        *) return 1 ;;
    esac
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
    real_count="$(count_graph_files "${REAL_DIR}")"

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
    graph_count="$(count_synthetic_data_graph_files "${SYNTHETIC_DIR}")"

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
    graph_count="$(count_graph_files "${QUERIES_DIR}")"
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

mkdir -p "${OUTPUT_ROOT}" || finish 1
log "Dataset scope: ${DATASET_SCOPE}"
log "Run output root: ${OUTPUT_ROOT}"

run_step \
    "Expand OFAT configs" \
    "Expand data/query OFAT configs into reusable task parameter files for later steps." \
    "required" \
    "summarize_make_ofat_configs" \
    "${PYTHON_BIN}" SSM-Pipeline/make_ofat_configs.py --output-dir "${TASK_DIR}" --scope "${DATASET_SCOPE}"

if scope_includes_real; then
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
                    "Convert raw real graphs into standard .txt files for this run." \
                    "optional" \
                    "summarize_convert_real_graphs" \
                    "${PYTHON_BIN}" SSM-GraphGen/convert_real_graphs.py --input "${RAW_REAL_GRAPHS}" --output "${REAL_DIR}" "${OVERWRITE_ARGS[@]}"
            fi
            ;;
        1|true|yes|always)
            run_step \
                "Convert real graphs" \
                "Run real graph conversion as requested for this run." \
                "optional" \
                "summarize_convert_real_graphs" \
                "${PYTHON_BIN}" SSM-GraphGen/convert_real_graphs.py --input "${RAW_REAL_GRAPHS}" --output "${REAL_DIR}" "${OVERWRITE_ARGS[@]}"
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
else
    skip_step \
        "Convert real graphs" \
        "Convert raw real graphs into standard .txt files." \
        "scope=${DATASET_SCOPE}"
fi

if scope_includes_synthetic; then
    run_step \
        "Generate synthetic graphs" \
        "Generate synthetic data graphs from data_graph_tasks.csv for this run." \
        "required" \
        "summarize_generate_synthetic_graphs" \
        "${PYTHON_BIN}" SSM-GraphGen/generate_synthetic_graphs.py --tasks "${DATA_TASKS}" --output-dir "${SYNTHETIC_DIR}" "${OVERWRITE_ARGS[@]}"
else
    skip_step \
        "Generate synthetic graphs" \
        "Generate synthetic data graphs from data_graph_tasks.csv." \
        "scope=${DATASET_SCOPE}"
fi

VALIDATION_INPUTS=()
if scope_includes_real; then
    VALIDATION_INPUTS+=("${REAL_DIR}")
fi
if scope_includes_synthetic; then
    VALIDATION_INPUTS+=("${SYNTHETIC_DIR}")
fi

run_step \
    "Validate data graphs" \
    "Validate standard-format graphs selected by scope, then write the graph validation report." \
    "required" \
    "summarize_validate_graphs" \
    "${PYTHON_BIN}" SSM-GraphGen/validate_graph_format.py --input "${VALIDATION_INPUTS[@]}" --report "${VALIDATION_REPORT}"

REAL_MANIFEST_DIR="${EMPTY_REAL_DIR}"
SYNTHETIC_MANIFEST_DIR="${EMPTY_SYNTHETIC_DIR}"
if scope_includes_real; then
    REAL_MANIFEST_DIR="${REAL_DIR}"
fi
if scope_includes_synthetic; then
    SYNTHETIC_MANIFEST_DIR="${SYNTHETIC_DIR}"
fi

run_step \
    "Build data manifest" \
    "Summarize selected data graphs into the manifest used by query generation." \
    "required" \
    "summarize_build_data_manifest" \
    "${PYTHON_BIN}" SSM-Pipeline/build_data_graph_manifest.py --real-dir "${REAL_MANIFEST_DIR}" --synthetic-dir "${SYNTHETIC_MANIFEST_DIR}" --output "${DATA_MANIFEST}"

run_step \
    "Generate query graphs" \
    "Generate query graphs for each valid selected data graph using query_graph_tasks.csv." \
    "required" \
    "summarize_generate_query_graphs" \
    "${PYTHON_BIN}" SSM-QueryGen/generate_query_graphs.py --manifest "${DATA_MANIFEST}" --tasks "${QUERY_TASKS}" --output-dir "${QUERIES_DIR}" "${OVERWRITE_ARGS[@]}"

run_step \
    "Build query manifest" \
    "Scan the query graph output directory and build the final query graph manifest." \
    "required" \
    "summarize_build_query_manifest" \
    "${PYTHON_BIN}" SSM-Pipeline/build_query_graph_manifest.py --queries-dir "${QUERIES_DIR}" --tasks "${QUERY_TASKS}" --output "${QUERY_MANIFEST}"

finish 0
