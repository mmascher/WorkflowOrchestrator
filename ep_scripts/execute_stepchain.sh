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

# Stage-out: transfer output files for steps with KeepOutput=true via stage_out.py (requires SITECONFIG_PATH and WMCore on PYTHONPATH).
# Uses REQUEST_JSON and TMP_DIR from the calling script.
run_stageout() {
    if [ -z "${SITECONFIG_PATH:-}" ] && [ -z "${WMAGENT_SITE_CONFIG_OVERRIDE:-}" ]; then
        echo "Stage-out skipped (SITECONFIG_PATH not set)."
        return 0
    fi
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

TMP_DIR=$(mktemp -d -t stepchain-XXXXXXXXXX)
echo "Created temporary directory: $TMP_DIR"
#trap "rm -rf '$TMP_DIR'" EXIT # MYTEST
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
        # Step 1 with num_copies > 1: split event interval, run cmsRun copies in parallel
        OUTPUT_MODULE=$(python3 -S -c "import json; r=json.load(open('$REQUEST_JSON')); print(r.get('Step2',{}).get('InputFromOutputModule','RAWSIMoutput'))")
        PIDS=()
        for COPY_IDX in $(seq 0 $((NUM_COPIES - 1))); do
            (
            COPY_DIR="$TMP_DIR/step1/copy${COPY_IDX}"
            mkdir -p "$COPY_DIR"
            cd "$COPY_DIR"
            cp "$BASE_PSET" PSet_base.py
            export JOB_FILE SCRIPT_DIR STEP_NUM CMSSW_VERSION SCRAM_ARCH
            export NUM_COPIES OUTPUT_MODULE COPY_IDX
            python3 -S -c "
import json, os, re, sys
job = json.load(open(os.environ['JOB_FILE']))
tweak = job['tweaks']['1'].copy()
fe_str = tweak.get('process.source.firstEvent', '')
m = re.search(r'uint32\s*\(\s*(\d+)\s*\)', fe_str)
first_event = int(m.group(1)) if m else 0
me_str = tweak.get('process.maxEvents', '')
m = re.search(r'int32\s*\(\s*(\d+)\s*\)', me_str)
total = int(m.group(1)) if m else 0
num_copies = int(os.environ['NUM_COPIES'])
copy_idx = int(os.environ['COPY_IDX'])
per_copy = total // num_copies
remainder = total % num_copies
count = per_copy + (1 if copy_idx < remainder else 0)
base = first_event
for i in range(copy_idx):
    base += per_copy + (1 if i < remainder else 0)
first_event_i = base
tweak['process.source.firstEvent'] = 'customTypeCms.untracked.uint32(%d)' % first_event_i
tweak['process.maxEvents'] = 'customTypeCms.untracked.PSet(input=cms.untracked.int32(%d))' % count
out_mod = os.environ['OUTPUT_MODULE']
tweak['process.%s.fileName' % out_mod] = \"customTypeCms.untracked.string('file:%s.root')\" % out_mod
with open('tweak.json', 'w') as f:
    json.dump(tweak, f, indent=2)
" || { echo "Failed to write tweak.json for copy $COPY_IDX"; exit $EXIT_CFG_GEN; }
            run_step_in_cms_env
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
        cd "$STEP_DIR"
        cp "$BASE_PSET" "$STEP_DIR/PSet_base.py"

        export JOB_FILE STEP_NUM
        python3 -S -c "
import json, os, sys
job_file = os.environ['JOB_FILE']
step_num = int(os.environ['STEP_NUM'])
job = json.load(open(job_file))
step_key = str(step_num)
if 'tweaks' not in job or step_key not in job['tweaks']:
    print('Precomputed tweak for step %s not found in %s' % (step_num, job_file), file=sys.stderr)
    sys.exit(1)
with open('tweak.json', 'w') as f:
    json.dump(job['tweaks'][step_key], f, indent=2)
" || { echo "Failed to write tweak.json from job file"; exit $EXIT_CFG_GEN; }

        run_step_in_cms_env || { STEP_EXIT=$?; echo "Step $STEP_NUM failed (exit code $STEP_EXIT)"; exit $STEP_EXIT; }
    fi

    cd "$TMP_DIR"
done

echo "All steps completed successfully."

run_stageout

echo "execute_stepchain.sh completed successfully."
exit 0
