#!/bin/bash
#./launchsimulator.sh /home/gautier/hop_distro bbc5 bbc1 bbc3:25001 bbc3 bbc2 5000
user="gautier"

basedir=$1
if [ -z "${basedir}" ]; then
    echo "Hadoop HOP distro dirctory can not be empty. <Ex : ./distributedsls.sh /home/sri/batchmode/hop_distro>"
    exit
fi
thisSimIp=$2
remoteSim1=$3
remoteSim2=$4
scp=$5
sc=$6
rt1=$7
rt2=$8
nm=${9}


## copy new sls-jobs and sls-node files in to both simulators
#echo "cp 1"
#ssh $user@$remoteSim1 "cp $basedir/distributedsimulator_resources/tracefiles/sls-*.json $basedir/distributedsimulator_resources/output"
#echo "cp 2"
#ssh $user@$remoteSim2 "cp $basedir/distributedsimulator_resources/tracefiles/sls-*.json $basedir/distributedsimulator_resources/output"
#echo "cp 5"
#cp tracefiles/sls-*.json  output


for nm in {9000..9000..1000}
do


    echo "hop $nm" >> $basedir/hadoop-2.4.0/share/hadoop/tools/sls/simulationsDuration
    
    echo "================= Preparing the trace for node mangers - $nm ======================"


    for i in {1..1}
    do
        echo "[Simulation] going to simulate $nm number of node managers"


	echo "start scheduler"
	ssh $user@$sc "rm $basedir/hadoop-2.4.0/logs/*"
	ssh $user@$sc "$basedir/hadoop-2.4.0/sbin/yarn-daemon.sh start resourcemanager"

	#echo "start rm"
	#		ssh $user@$sc "rm $basedir/hadoop-2.4.0/logs/*"
	#ssh $user@$sc "/home/gautier/hadoop/hadoop-2.4.0/sbin/yarn-daemon.sh start resourcemanager"

		
	sleep 5s
	echo "start rt1"
	ssh $user@$rt1 "rm $basedir/hadoop-2.4.0/logs/*"
	ssh $user@$rt1 "$basedir/hadoop-2.4.0/sbin/yarn-daemon.sh start resourcemanager"

	echo "start rt2"
	ssh $user@$rt2 "rm $basedir/hadoop-2.4.0/logs/*"
	ssh $user@$rt2 "$basedir/hadoop-2.4.0/sbin/yarn-daemon.sh start resourcemanager"

		
	sleep 5s
		

	echo "start simulators"
        ## start the remote resource manager
	echo "1"
        ssh $user@$remoteSim1 "cd $basedir/distributedsimulator_resources; ./initsimulator.sh $basedir output/sls-jobs_1_$i\_$nm.json output/sls-nodes_1_$i\_$nm.json $rt1\,$rt2 $scp $thisSimIp,$remoteSim2" &
	ssh $user@$remoteSim2 "cd $basedir/distributedsimulator_resources; ./initsimulator.sh $basedir output/sls-jobs_2_$i\_$nm.json output/sls-nodes_2_$i\_$nm.json $rt1\,$rt2 $scp $thisSimIp,$remoteSim1" &
	### start the simulator on this host
	echo "5"
        ./initsimulator.sh $basedir output/sls-jobs_0_$i\_$nm.json output/sls-nodes_0_$i\_$nm.json $rt1\,$rt2 $scp $remoteSim1,$remoteSim2 --isLeader --simulation-duration=600

        ### once this host experiments is done , kill the remote one too
        ssh $user@$remoteSim1 "cd $basedir/distributedsimulator_resources; ./killsimulator.sh"
	ssh $user@$remoteSim2 "cd $basedir/distributedsimulator_resources; ./killsimulator.sh"
        ## kill host simulator process
        ./killsimulator.sh

        ssh $user@$remoteSim1 "cp $basedir/hadoop-2.4.0/logs/yarn.log $basedir/distributedsimulator_resources/log_$i"
	ssh $user@$remoteSim2 "cp $basedir/hadoop-2.4.0/logs/yarn.log $basedir/distributedsimulator_resources/log_$i"
        cp $basedir/hadoop-2.4.0/logs/yarn.log $basedir/distributedsimulator_resources/log_$i

	echo "stop scheduler"
	ssh $user@$sc "$basedir/hadoop-2.4.0/sbin/yarn-daemon.sh stop resourcemanager && $basedir/hadoop-2.4.0/bin/hadoop namenode -format" 
	#ssh $user@$sc "mv $basedir/hadoop-2.4.0/logs/*.log $basedir/hadoop-2.4.0/logs/sv/yarn_$i.log "
	


	#echo "stop rm"
	#ssh $user@$rt "/home/gautier/hadoop/hadoop-2.4.0/sbin/yarn-daemon.sh stop resourcemanager"
	#ssh $user@$rt "mv /home/gautier/hadoop/hadoop-2.4.0/logs/*.log $basedir/hadoop-2.4.0/logs/sv/yarn_$i.log "
	
	sleep 10s
	echo "stop rt1"
	ssh $user@$rt1 "$basedir/hadoop-2.4.0/sbin/yarn-daemon.sh stop resourcemanager"
	#ssh $user@$rt1 "mv $basedir/hadoop-2.4.0/logs/*.log $basedir/hadoop-2.4.0/logs/sv/yarn_$i.log "

	echo "stop rt2"
	ssh $user@$rt2 "$basedir/hadoop-2.4.0/sbin/yarn-daemon.sh stop resourcemanager"
	#ssh $user@$rt2 "mv $basedir/hadoop-2.4.0/logs/*.log $basedir/hadoop-2.4.0/logs/sv/yarn_$i.log "

    done
done
