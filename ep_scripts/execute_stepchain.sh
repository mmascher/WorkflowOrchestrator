#!/bin/bash
# Execute a stepchain job: extract request+PSets tarball, set up CMSSW per step,
# apply precomputed PSet tweaks (from job file), run cmsRun.
#
# Usage: execute_stepchain.sh <request_psets.tar.gz> <job_N.json>
#
# Tarball contains request.json and PSets/ (from event_splitter). job_N.json is per job.
#
# Intended to be executed on Grid worker nodes where the CMS environment is available (see setup_cmsset call).

set -e
set -o pipefail
set -x # MYTEST

# Resolve to absolute paths so they work after we cd to TMP_DIR (e.g. inside Singularity)
resolve_abs() {
    case "$1" in
        /*) echo "$1" ;;
        *)  echo "$(pwd)/$1" ;;
    esac
}

# Exit codes per https://twiki.cern.ch/twiki/bin/view/CMSPublic/JobExitCodes and WMCore WMExceptions
EXIT_INVALID_ARGS=50113      # Executable did not get enough arguments
EXIT_MISSING_INPUT=80000     # Internal error in job wrapper (missing input files)
EXIT_CFG_GEN=10040           # failed to generate cmsRun cfg file at runtime
EXIT_SCRAM=71                # Failed to initiate Scram project (already in WMExceptions)
EXIT_CMSRUN_UNKNOWN=50116    # Could not determine exit code of cmsRun executable at runtime
EXIT_STAGEOUT=60324          # Other stageout exception

# Parse argv and validate; sets TARBALL_PATH and JOB_FILE (absolute), exits on error.
parse_and_validate_args() {
    if [ "$#" -ne 2 ]; then
        echo "Usage: $(basename "$0") <request_psets.tar.gz> <job_N.json>"
        exit $EXIT_INVALID_ARGS
    fi
    TARBALL_PATH=$(resolve_abs "$1")
    JOB_FILE=$(resolve_abs "$2")
    if [ ! -f "$TARBALL_PATH" ]; then
        echo "Error: tarball not found: $TARBALL_PATH"
        exit $EXIT_MISSING_INPUT
    fi
    if [ ! -f "$JOB_FILE" ]; then
        echo "Error: job file not found: $JOB_FILE"
        exit $EXIT_MISSING_INPUT
    fi

    # This is where stage_out, submit_env and other scripts live
    SCRIPT_DIR="$(cd "$(dirname "$TARBALL_PATH")" && pwd)"
}

# Print current environment, one variable per line, each prefixed with ---ENV--- 
print_env() {
    env | sort | while IFS= read -r line; do
        echo "---ENV--- $line"
    done
}

# Print HTCondor job ad file, each line prefixed with ---JOB_AD--- (no-op if not set or missing)
print_condor_job_ad() {
    if [ -n "${_CONDOR_JOB_AD:-}" ] && [ -f "$_CONDOR_JOB_AD" ]; then
        while IFS= read -r line; do
            echo "---JOB_AD--- $line"
        done < "$_CONDOR_JOB_AD"
    fi
}

# Print HTCondor machine ad file, each line prefixed with ---MACHINE_AD--- (no-op if not set or missing)
print_condor_machine_ad() {
    if [ -n "${_CONDOR_MACHINE_AD:-}" ] && [ -f "$_CONDOR_MACHINE_AD" ]; then
        while IFS= read -r line; do
            echo "---MACHINE_AD--- $line"
        done < "$_CONDOR_MACHINE_AD"
    fi
}

# Run SCRAM setup through cmsRun in a subshell so LD_LIBRARY_PATH etc. do not leak.
# Call with current directory = step directory (STEP_DIR). Uses SCRAM_ARCH, CMSSW_VERSION, STEP_NUM.
run_step_in_cms_env() {
    (
    source "$SCRIPT_DIR/submit_env.sh"
    setup_cmsset
    export SCRAM_ARCH
    scram project "$CMSSW_VERSION" || { echo "scram project failed"; exit $EXIT_SCRAM; }
    cd "$CMSSW_VERSION"
    set +x # MYTEST
    eval $(scram runtime -sh)
    set -x # MYTEST
    cd ..

    edm_pset_pickler.py --input "PSet_base.py" --output_pkl "Pset.pkl" || {
        echo "edm_pset_pickler failed for step $STEP_NUM"
        exit $EXIT_CFG_GEN
    }

    edm_pset_tweak.py \
        --input_pkl "Pset.pkl" \
        --output_pkl "Pset.pkl" \
        --json "tweak.json" \
        --create_untracked_psets || { echo "edm_pset_tweak failed"; exit $EXIT_CFG_GEN; }

    cmssw_handle_nEvents.py --input_pkl "Pset.pkl" --output_pkl "Pset.pkl" || {
        echo "cmssw_handle_nEvents failed for step $STEP_NUM"
        exit $EXIT_CFG_GEN
    }

    cat > Pset_cmsRun.py << 'WRAPPER'
import pickle
with open('Pset.pkl', 'rb') as f:
    process = pickle.load(f)
WRAPPER

    export FRONTIER_LOG_LEVEL=warning
    echo "Executing cmsRun -j job_report.xml Pset_cmsRun.py"
#    mkdir -p "$CMSSW_BASE/SITECONF/local/JobConfig" # MYTEST
#    cp /home/marco/development/results/WMCore/manual_test/site-local-config.xml "$CMSSW_BASE/SITECONF/local/JobConfig/site-local-config.xml" # MYTEST
#    unset FRONTIER_PROXY # MYTEST
#    unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY # MYTEST
#    unset all_proxy ALL_PROXY no_proxy NO_PROXY # MYTEST
#    CMS_PATH="$CMSSW_BASE" cmsRun -j job_report.xml Pset_cmsRun.py # MYTEST
    cmsRun -j job_report.xml Pset_cmsRun.py
    CMSRUN_EXIT=$?
    if [ "$CMSRUN_EXIT" -ne 0 ]; then
        echo "cmsRun failed for step $STEP_NUM (exit code $CMSRUN_EXIT)"
        exit $CMSRUN_EXIT
    fi
    # cmsRun can exit 0 despite failures - check job report (FrameworkJobReport)
    if [ -f "job_report.xml" ] && grep -qE 'FrameworkError|Status="Failed"' job_report.xml 2>/dev/null; then
        # Try to extract ExitStatus from FrameworkError to use as exit code
        REPORT_EXIT=$(grep -oE 'ExitStatus="[0-9]+"' job_report.xml 2>/dev/null | head -1 | grep -oE '[0-9]+')
        if [ -n "$REPORT_EXIT" ]; then
            echo "Job report indicates cmsRun failure for step $STEP_NUM (ExitStatus=$REPORT_EXIT)"
            exit $REPORT_EXIT
        fi
        echo "Job report indicates cmsRun failure for step $STEP_NUM (FrameworkError or Status=Failed)"
        exit $EXIT_CMSRUN_UNKNOWN
    fi
    )
}

# Setup step dir, write tweak.json, run cmsRun. Requires JOB_FILE, STEP_NUM in env; COPY_IDX if step1 with num_copies>1.
run_step_in_dir() {
    local dir=$1
    cd "$dir" || return 1
    cp "$BASE_PSET" PSet_base.py
    write_tweak_json || return 1
    run_step_in_cms_env || return 1
}

# Write tweak.json from job file. Requires JOB_FILE, STEP_NUM in env; COPY_IDX if step1 with num_copies>1.
write_tweak_json() {
    python3 -S -c "
import json, os, sys
job = json.load(open(os.environ['JOB_FILE']))
step_num = int(os.environ['STEP_NUM'])
copy_idx = os.environ.get('COPY_IDX', '')
if copy_idx != '':
    tweak = job['tweaks']['1'][int(copy_idx)]
else:
    sk = str(step_num)
    if 'tweaks' not in job or sk not in job['tweaks']:
        print('Precomputed tweak for step %s not found' % step_num, file=sys.stderr)
        sys.exit(1)
    tweak = job['tweaks'][sk]
with open('tweak.json', 'w') as f:
    json.dump(tweak, f, indent=2)
"
}

# Stage-out: transfer output files for steps with KeepOutput=true via stage_out.py
# Uses REQUEST_JSON and TMP_DIR from the calling script.
run_stageout() {
    echo "========== Stage-out (steps with KeepOutput=true) =========="
    STAGEOUT_SCRIPT="$SCRIPT_DIR/stage_out.py"
    if [ ! -f "$STAGEOUT_SCRIPT" ]; then
        echo "Stage-out skipped: stage_out.py not found at $STAGEOUT_SCRIPT"
        return 0
    fi
    (
    source "$SCRIPT_DIR/submit_env.sh"
    #setup_local_env # MYTEST
    #export CVMFS="/cvmfs/cms.cern.ch" # MYTEST
    export PYTHONPATH="$PYTHONPATH:$SCRIPT_DIR/WMCore.zip"
    set +x # MYTEST
    setup_cmsset
    setup_python_comp
    set -x # MYTEST
    if [ -z "${SITECONFIG_PATH:-}" ] && [ -z "${WMAGENT_SITE_CONFIG_OVERRIDE:-}" ]; then
        echo "Stage-out skipped (SITECONFIG_PATH not set)."
        return 0
    fi
    "$STAGEOUT_SCRIPT" --request "$REQUEST_JSON" --work-dir "$TMP_DIR" || { echo "Stage-out failed"; exit $EXIT_STAGEOUT; }
    )
    STAGEOUT_EXIT=$?
    if [ "$STAGEOUT_EXIT" -ne 0 ]; then
        echo "Stage-out failed (exit code $STAGEOUT_EXIT)"
        exit $STAGEOUT_EXIT
    fi
    echo "Stage-out completed."
}

parse_and_validate_args "$@"

echo "Starting execute_stepchain.sh: tarball=$TARBALL_PATH job=$JOB_FILE"
print_env
print_condor_job_ad
print_condor_machine_ad

INITIAL_DIR=$(pwd)
TMP_DIR=$(mktemp -d -t stepchain-XXXXXXXXXX)
echo "Created temporary directory: $TMP_DIR"

# Cleanup: create output tarball (excluding *.root) and remove TMP_DIR.
# Runs on normal exit and on SIGTERM/SIGINT so tar is created even if the process is killed.
cleanup() {
    if [ "${CLEANUP_DONE:-0}" -eq 1 ]; then return; fi
    CLEANUP_DONE=1
    echo "======== Exiting at $(TZ=GMT date) ========"
    if [ -n "${TMP_DIR:-}" ] && [ -d "$TMP_DIR" ]; then
        echo "Creating output.tgz from $TMP_DIR (excluding *.root)"
        tar czf "$INITIAL_DIR/output.tgz" --exclude='*.root' -C "$(dirname "$TMP_DIR")" "$(basename "$TMP_DIR")" 2>/dev/null || true
        rm -rf "$TMP_DIR"
    fi
}
trap 'cleanup; exit 143' SIGTERM
trap 'cleanup; exit 130' SIGINT
trap cleanup EXIT

cd "$TMP_DIR"

echo "Extracting $TARBALL_PATH"
tar -xzf "$TARBALL_PATH"
if [ ! -f "request.json" ] || [ ! -d "PSets" ]; then
    echo "Error: tarball must contain request.json and PSets/"
    exit $EXIT_MISSING_INPUT
fi

REQUEST_JSON="$TMP_DIR/request.json"
PSETS_DIR="$TMP_DIR/PSets"

# Number of steps from request (runs in original env; CMS setup is in subshell below)
NUM_STEPS=$(python3 -S -c "import json; r=json.load(open('$REQUEST_JSON')); print(r.get('StepChain', 1))")
echo "StepChain has $NUM_STEPS steps"

# Run each step
for STEP_NUM in $(seq 1 "$NUM_STEPS"); do
    echo "========== Step $STEP_NUM =========="
    STEP_KEY="Step${STEP_NUM}"
    STEP_DIR="$TMP_DIR/step${STEP_NUM}"
    mkdir -p "$STEP_DIR"

    # Step config from request (runs in original env; CMS setup is in subshell below)
    CMSSW_VERSION=$(python3 -S -c "import json; r=json.load(open('$REQUEST_JSON')); print(r['$STEP_KEY']['CMSSWVersion'])")
    SCRAM_ARCH=$(python3 -S -c "import json; r=json.load(open('$REQUEST_JSON')); a=r['$STEP_KEY'].get('ScramArch',['slc7_amd64_gcc700']); print(a[0] if isinstance(a,list) else a)")
    STEP_NAME=$(python3 -S -c "import json; r=json.load(open('$REQUEST_JSON')); print(r['$STEP_KEY']['StepName'])")

    BASE_PSET="$PSETS_DIR/PSet_cmsRun${STEP_NUM}_${STEP_NAME}.py"
    if [ ! -f "$BASE_PSET" ]; then
        echo "Error: Base PSet not found: $BASE_PSET"
        exit $EXIT_CFG_GEN
    fi

    NUM_COPIES=$(python3 -S -c "import json; r=json.load(open('$REQUEST_JSON')); print(r.get('Step1',{}).get('NumCopies',1))")

    if [ "$STEP_NUM" -eq 1 ] && [ "$NUM_COPIES" -gt 1 ]; then
        # Step 1 with num_copies > 1: use precomputed tweaks from event_splitter, run cmsRun copies in parallel
        PIDS=()
        for COPY_IDX in $(seq 0 $((NUM_COPIES - 1))); do
            (
            COPY_DIR="$TMP_DIR/step1/copy${COPY_IDX}"
            mkdir -p "$COPY_DIR"
            export JOB_FILE SCRIPT_DIR STEP_NUM CMSSW_VERSION SCRAM_ARCH COPY_IDX
            run_step_in_dir "$COPY_DIR" || exit $EXIT_CFG_GEN
            echo "======== copy $COPY_IDX completed at $(TZ=GMT date) ========"
            ) &
            PIDS+=($!)
        done
        FAILED=0
        for p in "${PIDS[@]}"; do
            if ! wait "$p"; then FAILED=1; fi
        done
        if [ "$FAILED" -ne 0 ]; then
            echo "Step 1 failed (one or more copies failed)"
            exit $EXIT_CFG_GEN
        fi
    else
        # Normal flow: single tweak, single cmsRun
        unset COPY_IDX
        export JOB_FILE STEP_NUM
        run_step_in_dir "$STEP_DIR" || { echo "Step $STEP_NUM failed"; exit $EXIT_CFG_GEN; }
    fi

    cd "$TMP_DIR"
    echo "======== Step $STEP_NUM completed at $(TZ=GMT date) ========"
done

echo "All steps completed successfully."

run_stageout

echo "execute_stepchain.sh completed successfully."
exit 0
