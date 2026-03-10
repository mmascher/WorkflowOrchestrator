set -x
condor_rm -all
rm -rf test_jdl*
WO_DIR=~/repos/WorkflowOrchestrator
export PYTHONPATH="$WO_DIR/samples/htcondor/WMCore.zip"

mkdir test_jdl
cd test_jdl
mkdir -p log out err results

cp "$WO_DIR/ep_scripts/execute_stepchain.sh" .
cp "$WO_DIR/ep_scripts/submit_env.sh" .
cp "$WO_DIR/ep_scripts/stage_out.py" .
cp "$WO_DIR/ep_scripts/create_report.py" .
cp "$WO_DIR/samples/htcondor/WMCore.zip" .

cp "$WO_DIR/samples/htcondor/job.jdl" .
cp "$WO_DIR/ep_scripts/run.sh" .
cp "$WO_DIR/samples/htcondor/sitelist.txt" .


$WO_DIR/src/python/job_splitters/event_splitter.py   --request $WO_DIR/samples/cmsunified_task_SMP-RunIISummer20UL17pp5TeVwmLHEGS-00007__v1_T_251014_173511_792/request.json   --splitting $WO_DIR/samples/cmsunified_task_SMP-RunIISummer20UL17pp5TeVwmLHEGS-00007__v1_T_251014_173511_792/splitting.json   --psets $WO_DIR/samples/cmsunified_task_SMP-RunIISummer20UL17pp5TeVwmLHEGS-00007__v1_T_251014_173511_792/PSets/   --output-dir event_splitter_out

"$WO_DIR/src/python/micro_agent/create_stepchain_jdl.py" \
  --event-splitter-dir event_splitter_out/ \
  --request "$WO_DIR/samples/cmsunified_task_SMP-RunIISummer20UL17pp5TeVwmLHEGS-00007__v1_T_251014_173511_792/request.json" \
  --proxy /tmp/x509up_u$(id -u) \
  --sitelist sitelist.txt

cd event_splitter_out
# Shortening job 0 so we get some results quickly
sed -i.bak 's/cms.untracked.int32(830)/cms.untracked.int32(10)/g' job1.json
cd ..

# Reducing the number of total jobs for my personal test
sed -i.bak 's/18073/100/g' stepchain.jdl
condor_submit stepchain.jdl

# Giving job 0 a higher priority to get some results quicly for my tests
condor_qedit -const 'Owner == "mmascher" && ProcID==0' JobPrio 1
