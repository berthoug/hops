#!/bin/bash                                                                                                                                                                                 
## confirm the rmiregistry killing process, every time we should restart the rmiregistry                                                                                                    
command_1='simulator'
command_2='standalone'
hadoopbasedir="../../../.."

ps axf | grep "rmiregistry" | grep -v grep | awk '{print "kill -9 " $1}' | sh
ps axf | grep "AppMasterProcess" | grep -v grep | awk '{print "kill -9 " $1}' | sh



## clear the class path and export again                                                                                                                                                    
export CLASSPATH=" "; export CLASSPATH="DistributedLoadSimulator-2.4.0.jar:$CLASSPATH"

### now start the rmiregisttry in background                                                                                                                                                
rmiregistry &

###cleaning the output directory                                                                                                                                                            
rm -rf output/
#### cleaning the existing yarn-site.xml to replace exisinting one                                                                                                                          
#rm $hadoopbasedir/etc/hadoop/yarn-site.xml                                                                                                                                                 

### lets delete all the previous yarn.log                                                                                                                                                   
rm $hadoopbasedir/logs/yarn.*


cp       sls-backup/sls-runner.xml                      $hadoopbasedir/etc/hadoop/sls-runner.xml
#cp  -rf  sls-backup/output                              $hadoopbasedir/share/hadoop/tools/sls/                                                                                             
#cp       sls-backup/hop-leader-election-2.4.0.jar       $hadoopbasedir/share/hadoop/yarn/lib                                                                                               
cp       sls-backup/slsrun.sh                           $hadoopbasedir/share/hadoop/tools/sls/bin
cp       DistributedLoadSimulator-2.4.0.jar  $hadoopbasedir/share/hadoop/common/lib


if [ "$1" = "$command_1" ];
 then
 echo "Simulator mode is strating ...."
#  cp sls-backup/yarn-site.xml $hadoopbasedir/etc/hadoop/yarn-site.xml                                                                                                                      
cp $hadoopbasedir/etc/hadoop/yarn-site.xml ./
 cd $hadoopbasedir/share/hadoop/tools/sls;
 ./bin/slsrun.sh --input-sls=sls-jobs.json --output-dir=output --nodes=sls-nodes.json --print-simulation --loadsimulator-mode --rt-address=$2 --rm-address=$2:25001
else
  echo "=================  Formating the database ===================================="
  $hadoopbasedir/bin/yarn rmRTEval format
  cp sls-backup/standalone-yarn-site.xml $hadoopbasedir/etc/hadoop/yarn-site.xml
  cd $hadoopbasedir/share/hadoop/tools/sls;
 ./bin/slsrun.sh --input-sls=sls-jobs.json --output-dir=output --nodes=sls-nodes.json --print-simulation --standalone-mode  --rt-address=193.10.64.109 --rm-address=193.10.64.109
fi


