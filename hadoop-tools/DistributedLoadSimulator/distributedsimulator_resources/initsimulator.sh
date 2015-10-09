#!/bin/bash


basedir=$1
if [ -z "${basedir}" ]; then
    echo "Hadoop HOP distro dirctory can not be empty. <Ex : ./distributedsls.sh /home/sri/batchmode/hop_distro>"
    exit
fi

## confirm the rmiregistry killing process, every time we should restart the rmiregistry
ps axf | grep "rmiregistry" | grep -v grep | awk '{print "kill -9 " $1}' | sh
ps axf | grep "AppMasterProcess" | grep -v grep | awk '{print "kill -9 " $1}' | sh
ps axf | grep "SLSRunner" | grep -v grep | awk '{print "kill -9 " $1}' | sh

## clear the class path and export again 
export CLASSPATH=" "; export CLASSPATH="DistributedLoadSimulator-2.4.0.jar:$CLASSPATH"

### now start the rmiregisttry in background
rmiregistry &

###cleaning the output directory 
rm -rf $basedir/hadoop-2.4.0/share/hadoop/tools/sls/output/
#### cleaning the existing yarn-site.xml to replace exisinting one
#rm $basedir/hadoop-2.4.0/etc/hadoop/yarn-site.xml

### lets delete all the previous yarn.log
rm $basedir/hadoop-2.4.0/logs/*.log


cp       sls-runner.xml                      $basedir/hadoop-2.4.0/etc/hadoop/sls-runner.xml
cp  -rf  output                              $basedir/hadoop-2.4.0/share/hadoop/tools/sls/
cp       slsrun.sh                           $basedir/hadoop-2.4.0/share/hadoop/tools/sls/bin
cp       DistributedLoadSimulator-2.4.0.jar  $basedir/hadoop-2.4.0/share/hadoop/common/lib


echo "Simulator mode is strating ...."
sed -i 's|true|false|g' $basedir/hadoop-2.4.0/etc/hadoop/yarn-site.xml
sed -i 's|-Xmx20000m|-Xmx1000m|g' $basedir/hadoop-2.4.0/bin/yarn
cp $basedir/hadoop-2.4.0/etc/hadoop/yarn-site.xml ./
cd $basedir/hadoop-2.4.0/share/hadoop/tools/sls;
cp $basedir/hadoop-2.4.0/etc/hadoop/yarn-site.xml ./
##cloud 3 scheduler 193.10.64.86
echo "./bin/slsrun.sh --input-sls=$2 --output-dir=output --nodes=$3 --print-simulation --loadsimulator-mode --rt-address=$4 --rm-address=$5 --rmi-address=$6 --parallelsimulator --yarn-directory=$basedir $7 $8"
./bin/slsrun.sh --input-sls=$2 --output-dir=output --nodes=$3 --print-simulation --loadsimulator-mode --rt-address=$4 --rm-address=$5 --rmi-address=$6 --parallelsimulator --yarn-directory=$basedir $7 $8
