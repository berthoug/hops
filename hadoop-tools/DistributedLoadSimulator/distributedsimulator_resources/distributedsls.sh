#!/bin/bash
## get the hadoop home path from user
basedir=$1
## in the distributed mode, scheduler should clean the database and initialize with good values
distributedcomponentmode=$2

## number of nodes variable, so we can copy the corresponding jobs and nodes files to output folder
numberofnodes=$3

if [ -z "${basedir}" ]; then
    echo "Hadoop HOP distro dirctory can not be empty!!"
    echo "usage : ./distributedsls.sh /home/sri/batchmode/hop_distro scheduler 1000"
    exit
fi

if [ -z "${numberofnodes}" ]; then
    echo "Should provide the number of nodes"
    echo "usage : ./distributedsls.sh /home/sri/batchmode/hop_distro scheduler 1000"
    exit
fi



## confirm the rmiregistry killing process, every time we should restart the rmiregistry
ps axf | grep "rmiregistry" | grep -v grep | awk '{print "kill -9 " $1}' | sh
ps axf | grep "SLSRunner" | grep -v grep | awk '{print "kill -9 " $1}' | sh
## clear the class path and export again 
export CLASSPATH=" "; export CLASSPATH="DistributedLoadSimulator-2.4.0.jar:$CLASSPATH"

### now start the rmiregisttry in background
rmiregistry &

rm  $basedir/hadoop-2.4.0/logs/*.log

cp yarn-site.xml                        $basedir/hadoop-2.4.0/etc/hadoop/yarn-site.xml
cp sls-runner.xml                       $basedir/hadoop-2.4.0/etc/hadoop/sls-runner.xml
cp tracefiles/$numberofnodes/sls*       output/
cp -rf output                           $basedir/hadoop-2.4.0/share/hadoop/tools/sls/
cp slsrun.sh                            $basedir/hadoop-2.4.0/share/hadoop/tools/sls/bin
cp DistributedLoadSimulator-2.4.0.jar   $basedir/hadoop-2.4.0/share/hadoop/tools/lib

## if it is scheduler, format the database
if [ "$distributedcomponentmode" = "scheduler" ];
 then
$basedir/hadoop-2.4.0/bin/yarn io.hops.metadata.util.DistributedRTRMEvaluation format
 fi

cd $basedir/hadoop-2.4.0/share/hadoop/tools/sls;
##start the component
./bin/slsrun.sh --input-sls=output/sls-jobs.json --output-dir=output --nodes=output/sls-nodes.json --print-simulation --distributed-mode --yarnnode --rt-address=193.10.64.20 --rm-address=193.10.64.86 --yarn-directory=$basedir