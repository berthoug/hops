#!/bin/bash
## confirm the rmiregistry killing process, every time we should restart the rmiregistry
command_1='simulator'
command_2='standalone'
hadoopbasedir="/home/sri/realsimulator/hop_distro"

ps axf | grep "rmiregistry" | grep -v grep | awk '{print "kill -9 " $1}' | sh
ps axf | grep "AppMasterProcess" | grep -v grep | awk '{print "kill -9 " $1}' | sh
ps axf | grep "SLSRunner" | grep -v grep | awk '{print "kill -9 " $1}' | sh


## clear the class path and export again 
export CLASSPATH=" "; export CLASSPATH="DistributedLoadSimulator-2.4.0.jar:$CLASSPATH"

### now start the rmiregisttry in background
rmiregistry &

###cleaning the output directory 
rm -rf $hadoopbasedir/hadoop-2.4.0/share/hadoop/tools/sls/output/
#### cleaning the existing yarn-site.xml to replace exisinting one
rm $hadoopbasedir/hadoop-2.4.0/etc/hadoop/yarn-site.xml

### lets delete all the previous yarn.log
rm $hadoopbasedir/hadoop-2.4.0/logs/*.log


cp       sls-runner.xml                      $hadoopbasedir/hadoop-2.4.0/etc/hadoop/sls-runner.xml
cp  -rf  output                              $hadoopbasedir/hadoop-2.4.0/share/hadoop/tools/sls/
cp       slsrun.sh                           $hadoopbasedir/hadoop-2.4.0/share/hadoop/tools/sls/bin
cp       DistributedLoadSimulator-2.4.0.jar  $hadoopbasedir/hadoop-2.4.0/share/hadoop/common/lib


if [ "$1" = "$command_1" ];
 then
 echo "Simulator mode is strating ...."
  cp yarn-site.xml $hadoopbasedir/hadoop-2.4.0/etc/hadoop/yarn-site.xml
  cd $hadoopbasedir/hadoop-2.4.0/share/hadoop/tools/sls;
##cloud 3 scheduler 193.10.64.86
 ./bin/slsrun.sh --input-sls=output/sls-jobs.json --output-dir=output --nodes=output/sls-nodes.json --print-simulation --loadsimulator-mode --rt-address=193.10.64.20 --rm-address=193.10.64.20 --rmi-address=192.168.0.109 --parallelsimulator
else
  echo "=================  Formating the database ===================================="
  $hadoopbasedir/hadoop-2.4.0/bin/yarn rmRTEval format
  cp distributedsimulator_resources/standalone-yarn-site.xml $hadoopbasedir/hadoop-2.4.0/etc/hadoop/yarn-site.xml
  cd $hadoopbasedir/hadoop-2.4.0/share/hadoop/tools/sls;
 ./bin/slsrun.sh --input-sls=output/sls-jobs.json --output-dir=output --nodes=output/sls-nodes.json --print-simulation --standalone-mode  --rt-address=193.10.64.109 --rm-address=193.10.64.109
fi
