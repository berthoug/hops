#!/bin/bash
## confirm the rmiregistry killing process, every time we should restart the rmiregistry
ps ax | grep "rmiregistry" | awk {'print $1'} | head -n 1 | xargs kill -9
ps ax | grep "rmiregistry" | awk {'print $1'} | head -n 1 | xargs kill -9

## clear the class path and export again 
export CLASSPATH=" "; export CLASSPATH="sls-backup/DistributedLoadSimulator-2.4.0.jar:$CLASSPATH"

### not start the rmiregisttry in background

rmiregistry &

cp sls-backup/yarn-site.xml /home/sri/hop_distro/hadoop-2.4.0/etc/hadoop/yarn-site.xml
cp sls-backup/sls-runner.xml /home/sri/hop_distro/hadoop-2.4.0/etc/hadoop/sls-runner.xml

cp -rf sls-backup/output  /home/sri/hop_distro/hadoop-2.4.0/share/hadoop/tools/sls/
cp sls-backup/slsrun.sh /home/sri/hop_distro/hadoop-2.4.0/share/hadoop/tools/sls/bin
cp sls-backup/DistributedLoadSimulator-2.4.0.jar  /home/sri/hop_distro/hadoop-2.4.0/share/hadoop/tools/lib

cd /home/sri/hop_distro/hadoop-2.4.0/share/hadoop/tools/sls;

./bin/slsrun.sh --input-sls=output/sls-jobs.json --output-dir=output --nodes=output/sls-nodes.json --print-simulation --distributed-mode --rt-address=193.10.64.85 --rm-address=193.10.64.85

