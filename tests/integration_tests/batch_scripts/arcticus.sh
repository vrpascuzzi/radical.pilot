#!/bin/bash
#COBALT --jobname=rp_integration_test
#COBALT -o rp_integration_test.$jobid.out
#COBALT -e rp_integration_test.$jobid.err
#COBALT -n 1
#COBALT --proccount=96
#COBALT -t 30
#COBALT -q arcticus

# ------------------------------------------------------------------------------
# Test files
TEST="radical.pilot/tests/integration_tests/test_lm/test_ssh.py
      radical.pilot/tests/integration_tests/test_lm/test_mpirun.py"

# ------------------------------------------------------------------------------
# Test folder, the same as the PBS job's submission folder
# cd $PBS_O_WORKDIR
rm -rf radical.pilot testing *.log
git clone --branch feature/arcticus_support https://github.com/vrpascuzzi/radical.pilot.git

# ------------------------------------------------------------------------------
# Load required modules and setup virtualenv
module purge
module use /soft/modulefiles
module load spack/linux-rhel7-x86_64
module load openmpi/4.1.1-gcc
module load py-virtualenv
virtualenv --system-site-packages $PWD/ve
source $PWD/ve/bin/activate
pip install pytest

# ------------------------------------------------------------------------------
# Test execution
# pip install ./radical.pilot --upgrade
pip install ./radical.pilot
pytest -vvv $TEST > output.log 2>&1

# ------------------------------------------------------------------------------
# Test Reporting
if test "$?" = 1 ;
then
    echo 'Test failed'
    tests/bin/radical-pilot-test-issue -r 'ANL Arcticus' -l output.log
    curl -H "Accept: application/vnd.github.everest-preview+json" \
    -H "Authorization: token $GIT_TOKEN" \
    --request POST \
    --data '{"event_type": "test_bridges2", "client_payload": { "text": "failure"}}' \
    https://api.github.com/repos/radical-cybertools/radical.pilot/dispatches
else
    echo 'Everything went well'
    curl -H "Accept: application/vnd.github.everest-preview+json" \
    -H "Authorization: token $GIT_TOKEN" \
    --request POST \
    --data '{"event_type": "test_arcticus", "client_payload": { "text": "success"}}' \
    https://api.github.com/repos/radical-cybertools/radical.pilot/dispatches
fi
