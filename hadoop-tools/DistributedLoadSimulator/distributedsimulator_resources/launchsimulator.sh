#!/bin/bash
#./launchsimulator.sh /home/gautier/hop_distro cloud5 cloud6 cloud11 bbc1 bbc3:25001 bbc3 bbc2 5000
user="gautier"

basedir=$1
if [ -z "${basedir}" ]; then
    echo "Hadoop HOP distro dirctory can not be empty. <Ex : ./distributedsls.sh /home/sri/batchmode/hop_distro>"
    exit
fi
thisSimIp=$2
remoteSim1=$3
remoteSim2=$4
remoteSim3=$5
scp=$6
sc=$7
rt=$8
nm=$9
if [ -z "${remoteSim2}" ]; then
    echo "Remote simulator ip is not set or empty ."
    exit
fi

#for nm in {1000..4000..1000}
#do
   
echo "hop $nm" >> $basedir/hadoop-2.4.0/share/hadoop/tools/sls/simulationsDuration

                echo "================= Preparing the trace for node mangers - $nm ======================"

                    ## copy new sls-jobs and sls-node files in to both simulators
                ssh $user@$remoteSim1 "cp $basedir/distributedsimulator_resources/tracefiles/$nm/sls-*.json $basedir/distributedsimulator_resources/output"
		ssh $user@$remoteSim2 "cp $basedir/distributedsimulator_resources/tracefiles/$nm/sls-*.json $basedir/distributedsimulator_resources/output"
		ssh $user@$remoteSim3 "cp $basedir/distributedsimulator_resources/tracefiles/$nm/sls-*.json $basedir/distributedsimulator_resources/output"
                cp tracefiles/$nm/sls-*.json  output

for i in {1..5}
   do
                echo "[Simulation] going to simulate $nm number of node managers"


		echo "start scheduler"
#		ssh $user@$sc "rm $basedir/hadoop-2.4.0/logs/*"
		ssh $user@$sc "$basedir/hadoop-2.4.0/sbin/yarn-daemon.sh start resourcemanager"

#		echo "start rm"
#		ssh $user@$sc "rm $basedir/hadoop-2.4.0/logs/*"
#		ssh $user@$sc "/home/gautier/hadoop/hadoop-2.4.0/sbin/yarn-daemon.sh start resourcemanager"

		
		sleep 5s
		echo "start rt"
#		ssh $user@$rt "rm $basedir/hadoop-2.4.0/logs/*"
		ssh $user@$rt "$basedir/hadoop-2.4.0/sbin/yarn-daemon.sh start resourcemanager"

		
		sleep 5s
		

		echo "start simulators"
                ## start the remote resource manager
		echo "1"
                ssh $user@$remoteSim1 "cd $basedir/distributedsimulator_resources; ./initsimulator.sh $basedir output/sls-jobs_1_$i.json output/sls-nodes_1_$i.json $rt $scp $thisSimIp,$remoteSim2,$remoteSim3" &
		echo "2"
                 ssh $user@$remoteSim2 "cd $basedir/distributedsimulator_resources; ./initsimulator.sh $basedir output/sls-jobs_2_$i.json output/sls-nodes_2_$i.json $rt $scp $thisSimIp,$remoteSim1,$remoteSim3" &
		echo "2"
                 ssh $user@$remoteSim3 "cd $basedir/distributedsimulator_resources; ./initsimulator.sh $basedir output/sls-jobs_2_$i.json output/sls-nodes_3_$i.json $rt $scp $thisSimIp,$remoteSim1,$remoteSim2" &
                 ### start the simulator on this host
		 echo "3"
                ./initsimulator.sh $basedir output/sls-jobs_0_$i.json output/sls-nodes_0_$i.json $rt $scp $remoteSim1,$remoteSim2,$remoteSim3 --isLeader --simulation-duration=600

                 ### once this host experiments is done , kill the remote one too
                ssh $user@$remoteSim1 "cd $basedir/distributedsimulator_resources; ./killsimulator.sh"
                ssh $user@$remoteSim2 "cd $basedir/distributedsimulator_resources; ./killsimulator.sh"
                 ## kill host simulator process
                 ./killsimulator.sh

                 hoststarttime=$(grep "Application_initial_registeration_time" $basedir/hadoop-2.4.0/logs/yarn.log | awk '{print $11}')
                 remotestarttimetmp=$(ssh $user@$remoteSimIp "grep Application_initial_registeration_time $basedir/hadoop-2.4.0/logs/yarn.log")
                 remotestartime=$(echo $remotestarttimetmp | awk '{print $11}')

                 hostendtime=$(grep "Distributed_Simulator_shutting_down" $basedir/hadoop-2.4.0/logs/yarn.log | awk '{print $11}')


                 remoteendtimetmp=$(ssh $user@$remoteSimIp "grep Distributed_Simulator_shutting_down $basedir/hadoop-2.4.0/logs/yarn.log")
                 remoteendtime=$(echo $remoteendtimetmp | awk '{print $11}')

                 finalstarttime=$(( hoststarttime < remotestartime ? hoststarttime : remotestartime ))
                 finalendtime=$(( hostendtime > remoteendtime ? hostendtime : remoteendtime ))

                 totalexetime=$((finalendtime-finalstarttime))
                 echo "################  Total execution time #######################"
                 echo " simulation time : $totalexetime ms"
                 echo "################  Total execution time #######################"
                 ssh $user@$remoteSim1 "cp $basedir/hadoop-2.4.0/logs/yarn.log $basedir/distributedsimulator_resources/log_$i"
                 ssh $user@$remoteSim2 "cp $basedir/hadoop-2.4.0/logs/yarn.log $basedir/distributedsimulator_resources/log_$i"
		 ssh $user@$remoteSim3 "cp $basedir/hadoop-2.4.0/logs/yarn.log $basedir/distributedsimulator_resources/log_$i"  
                 cp $basedir/hadoop-2.4.0/logs/yarn.log $basedir/distributedsimulator_resources/log_$i

		 echo "stop scheduler"
		ssh $user@$sc "$basedir/hadoop-2.4.0/sbin/yarn-daemon.sh stop resourcemanager && $basedir/hadoop-2.4.0/bin/hadoop namenode -format" 
		ssh $user@$sc "mv $basedir/hadoop-2.4.0/logs/*.log $basedir/hadoop-2.4.0/logs/sv/yarn_$i.log "



#		echo "stop rm"
#		ssh $user@$rt "/home/gautier/hadoop/hadoop-2.4.0/sbin/yarn-daemon.sh stop resourcemanager"
#		ssh $user@$rt "mv /home/gautier/hadoop/hadoop-2.4.0/logs/*.log $basedir/hadoop-2.4.0/logs/sv/yarn_$i.log "

		sleep 10s
		echo "stop rt"
		ssh $user@$rt "$basedir/hadoop-2.4.0/sbin/yarn-daemon.sh stop resourcemanager"
		ssh $user@$rt "mv $basedir/hadoop-2.4.0/logs/*.log $basedir/hadoop-2.4.0/logs/sv/yarn_$i.log "
     done
done
