#!/bin/bash

user="gautier"

basedir=$1
if [ -z "${basedir}" ]; then
    echo "Hadoop HOP distro dirctory can not be empty. <Ex : ./distributedsls.sh /home/sri/batchmode/hop_distro>"
    exit
fi
thisSimIp=$2
remoteSimIp=$3
if [ -z "${remoteSimIp}" ]; then
    echo "Remote simulator ip is not set or empty ."
    exit
fi

for nm in {1000..1000..1000}
   do
                echo "[Simulation] going to simulate $nm number of node managers"

                echo "================= Preparing the trace for node mangers - $nm ======================"


                ## copy new sls-jobs and sls-node files in to both simulators
                ssh $user@$remoteSimIp "cp $basedir/distributedsimulator_resources/tracefiles/$nm/sls-*.json $basedir/distributedsimulator_resources/output"
                cp tracefiles/$nm/sls-*.json  output

                ## start the remote resource manager        
                 ssh $user@$remoteSimIp "cd $basedir/distributedsimulator_resources; ./initsimulator.sh $basedir output/sls-jobs_1.json output/sls-nodes_1.json $4 $thisSimIp" &

                 ### start the simulator on this host 
                ./initsimulator.sh $basedir output/sls-jobs_0.json output/sls-nodes_0.json $4 $remoteSimIp

                 ### once this host experiments is done , kill the remote one too
                 ssh $user@$remoteSimIp "cd $basedir/distributedsimulator_resources; ./killsimulator.sh"
                 ## kill host simulator process
                 ./killsimulator.sh

                 hoststarttime=$(grep "Application_initial_registeration_time" $basedir/hadoop-2.4.0/logs/yarn.log | awk '{print $11}')
                 remotestarttimetmp=$(ssh $user@$remoteSimIp "grep Application_initial_registeration_time $basedir/hadoop-2.4.0/logs/yarn.log")
                 remotestartime=$(echo $remotestarttimetmp | awk '{print $11}')

                 hostendtime=$(grep "Distributed_Simulator_shutting_down" $basedir/hadoop-2.4.0/logs/yarn.log | awk '{print $11}')


                 remoteendtimetmp=$(ssh $user@$remoteSimIp "grep Distributed_Simulator_shutting_down $basedir/hadoop-2.4.0/logs/yarn.log")
                 remoteendtime=$(echo $remoteendtimetmp | awk '{print $11}')

                 finalstarttime=$(( hoststarttime < remotestartime ? hoststarttime : remotestartime ))
                 finalendtime=$(( hostendtime > remotendtime ? hostendtime : remoteendtime ))

                 totalexetime=$((finalendtime-finalstarttime))
                 echo "################  Total execution time #######################"
                 echo " simulation time : $totalexetime ms"
                 echo "################  Total execution time #######################"
     done
