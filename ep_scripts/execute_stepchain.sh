#!/bin/bash
# Execute a stepchain job: extract request+PSets tarball, set up CMSSW per step,
# apply precomputed PSet tweaks (from job file), run cmsRun.
#
# Usage: execute_stepchain.sh <request_psets.tar.gz> <job_N.json>
#
# Tarball contains request.json and PSets/ (from EventSplitter). job_N.json is per job.
#
# Intended to be executed on Grid worker nodes where the CMS environment is available (see setup_cmsset call)).

set -e

# Resolve to absolute paths so they work after we cd to TMP_DIR (e.g. inside Singularity)
resolve_abs() {
    case "$1" in
        /*) echo "$1" ;;
        *)  echo "$(pwd)/$1" ;;
    esac
}

# Parse argv and validate; sets TARBALL_PATH and JOB_FILE (absolute), exits on error.
parse_and_validate_args() {
    if [ "$#" -ne 2 ]; then
        echo "Usage: $(basename "$0") <request_psets.tar.gz> <job_N.json>"
        exit 1
    fi
    TARBALL_PATH=$(resolve_abs "$1")
    JOB_FILE=$(resolve_abs "$2")
    if [ ! -f "$TARBALL_PATH" ]; then
        echo "Error: tarball not found: $TARBALL_PATH"
        exit 1
    fi
    if [ ! -f "$JOB_FILE" ]; then
        echo "Error: job file not found: $JOB_FILE"
        exit 1
    fi
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
    source /srv/submit_env.sh
    setup_cmsset
    export SCRAM_ARCH
    scram project "$CMSSW_VERSION" || { echo "scram project failed"; exit 71; }
    cd "$CMSSW_VERSION"
    eval $(scram runtime -sh)
    cd ..

    edm_pset_pickler.py --input "PSet_base.py" --output_pkl "Pset.pkl" || {
        echo "edm_pset_pickler failed for step $STEP_NUM"
        exit 1
    }

    edm_pset_tweak.py \
        --input_pkl "Pset.pkl" \
        --output_pkl "Pset.pkl" \
        --json "tweak.json" \
        --create_untracked_psets || { echo "edm_pset_tweak failed"; exit 1; }

    cmssw_handle_nEvents.py --input_pkl "Pset.pkl" --output_pkl "Pset.pkl" || {
        echo "cmssw_handle_nEvents failed for step $STEP_NUM"
        exit 1
    }

    cat > Pset_cmsRun.py << 'WRAPPER'
import pickle
with open('Pset.pkl', 'rb') as f:
    process = pickle.load(f)
WRAPPER

    export FRONTIER_LOG_LEVEL=warning
    echo "Executing cmsRun -j job_report.xml Pset_cmsRun.py"
#    mkdir -p "$CMSSW_BASE/SITECONF/local/JobConfig" #MYTEST
#    cp /home/marco/development/results/WMCore/manual_test/site-local-config.xml "$CMSSW_BASE/SITECONF/local/JobConfig/site-local-config.xml" #MYTEST#
#    unset FRONTIER_PROXY #MYTEST
#    unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY #MYTEST
#    unset all_proxy ALL_PROXY no_proxy NO_PROXY #MYTEST
#   CMS_PATH="$CMSSW_BASE" cmsRun -j job_report.xml Pset_cmsRun.py || { echo "cmsRun failed for step $STEP_NUM"; exit 1; } #MYTEST
    cmsRun -j job_report.xml Pset_cmsRun.py || { echo "cmsRun failed for step $STEP_NUM"; exit 1; }
    )
}

parse_and_validate_args "$@"

echo "Starting execute_stepchain.sh: tarball=$TARBALL_PATH job=$JOB_FILE"
print_env
print_condor_job_ad
print_condor_machine_ad

TMP_DIR=$(mktemp -d -t stepchain-XXXXXXXXXX)
echo "Created temporary directory: $TMP_DIR"
#trap "rm -rf '$TMP_DIR'" EXIT #MYTEST
cd "$TMP_DIR"

echo "Extracting $TARBALL_PATH"
tar -xzf "$TARBALL_PATH"
if [ ! -f "request.json" ] || [ ! -d "PSets" ]; then
    echo "Error: tarball must contain request.json and PSets/"
    exit 1
fi

REQUEST_JSON="$TMP_DIR/request.json"
PSETS_DIR="$TMP_DIR/PSets"

# Number of steps from request (runs in original env; CMS setup is in subshell below)
NUM_STEPS=$(python3 -S -c "import json; r=json.load(open('$REQUEST_JSON')); print(r.get('StepChain', 1))")
echo "StepChain has $NUM_STEPS steps"


set -x ##MYTEST
# Run each step
for STEP_NUM in $(seq 1 "$NUM_STEPS"); do
    echo "========== Step $STEP_NUM =========="
    STEP_KEY="Step${STEP_NUM}"
    STEP_DIR="$TMP_DIR/step${STEP_NUM}"
    mkdir -p "$STEP_DIR"
    cd "$STEP_DIR"

    # Step config from request (runs in original env; CMS setup is in subshell below)
    CMSSW_VERSION=$(python3 -S -c "import json; r=json.load(open('$REQUEST_JSON')); print(r['$STEP_KEY']['CMSSWVersion'])")
    SCRAM_ARCH=$(python3 -S -c "import json; r=json.load(open('$REQUEST_JSON')); a=r['$STEP_KEY'].get('ScramArch',['slc7_amd64_gcc700']); print(a[0] if isinstance(a,list) else a)")
    STEP_NAME=$(python3 -S -c "import json; r=json.load(open('$REQUEST_JSON')); print(r['$STEP_KEY']['StepName'])")

    BASE_PSET="$PSETS_DIR/PSet_cmsRun${STEP_NUM}_${STEP_NAME}.py"
    if [ ! -f "$BASE_PSET" ]; then
        echo "Error: Base PSet not found: $BASE_PSET"
        exit 1
    fi
    cp "$BASE_PSET" "$STEP_DIR/PSet_base.py"

    # Write precomputed tweak from job file (runs in original env; only needs job file, not PSet.pkl)
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
" || { echo "Failed to write tweak.json from job file"; exit 1; }

    run_step_in_cms_env || { echo "Step $STEP_NUM failed (scram/pickler/tweak/cmsRun)"; exit 1; }

    cd "$TMP_DIR"
done

echo "All steps completed successfully."
# Cleanup via trap
