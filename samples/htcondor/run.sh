set -x

chmod +x execute_stepchain.sh
./execute_stepchain.sh request_psets.tar.gz job*.json

tar cvzf output.tgz /tmp/stepchain-*
