#!/bin/bash

user="root"
simmachines=(cloud1.sics.se cloud2.sics.se cloud3.sics.se cloud4.sics.se cloud6.sics.se)
basedir=$1
if [ -z "${basedir}" ]; then
    echo "Hadoop HOP distro dirctory can not be empty. <Ex : ./distributedsls.sh /home/sri/batchmode/hop_distro>"
    exit
fi
remoteSimIp=$2
if [ -z "${remoteSimIp}" ]; then
    echo "Remote simulator ip is not set or empty ."
    exit
fi

## this function need which host actual simulator script is starting
## all other machines will be connected by ssh
calculateSimulationTime(){
          starttime=0
          minstarttime=0
          endtime=0
          maxendtime=0
  for machine in "${simmachines[@]}"
	do
        if test "$machine" = "$1"  ## argument 1 is host machine name
           then
             starttime=$(grep "Application_initial_registeration_time" $basedir/hadoop-2.4.0/logs/yarn.log | awk '{print $11}')
             endtime=$(grep "Distributed_Simulator_shutting_down" $basedir/hadoop-2.4.0/logs/yarn.log | awk '{print $11}')
           else
             tmpstarttime=$(ssh $user@$machine "grep Application_initial_registeration_time $basedir/hadoop-2.4.0/logs/yarn.log")
             starttime=$(echo $tmpstarttime | awk '{print $11}')
             tmpendtime=$(ssh $user@$machine "grep Distributed_Simulator_shutting_down $basedir/hadoop-2.4.0/logs/yarn.log")
             endtime=$(echo $tmpendtime | awk '{print $11}')
        fi

        if ((minstarttime == 0))
            then
               minstarttime=$starttime
            else
            minstarttime=$(( minstarttime < starttime ? minstarttime : starttime ))          
        fi
        if ((maxendtime == 0))
            then
               maxendtime=$endtime
            else
            maxendtime=$(( maxendtime > endtime ? maxendtime : endtime ))          
        fi
         echo "are we ok : $1 ----- $starttime  -- min : $minstarttime" 
         echo "$endtime   --- max : $maxendtime" 
	done

        totalexetime=$((maxendtime-minstarttime))
        echo "################  Total execution time #######################"
        echo " simulation time : $totalexetime ms"
        echo "################  Total execution time #######################"

}
for nm in {2000..2000..1000}
   do
                echo "[Simulation] going to simulate $nm number of node managers"

                echo "================= Preparing the trace for node mangers - $nm ======================"


                ## copy new sls-jobs and sls-node files in to both simulators
                ssh $user@$2 "cp $basedir/distributedsimulator_resources/tracefiles/$nm/sls-*.json $basedir/distributedsimulator_resources/output"
                ssh $user@$3 "cp $basedir/distributedsimulator_resources/tracefiles/$nm/sls-*.json $basedir/distributedsimulator_resources/output"
                ssh $user@$4 "cp $basedir/distributedsimulator_resources/tracefiles/$nm/sls-*.json $basedir/distributedsimulator_resources/output"
                cp tracefiles/$nm/sls-jobs.json  output
                cp tracefiles/$nm/sls-nodes.json output

                ## before start the simulators , check machines time, this will affect total simulation time
                ## remote machine time
                remotetime=$(ssh $user@$remoteSimIp "date")
                hosttime=$(date)
                echo "Remote machine time : $remotetime  - host machine time : $hosttime"
                ## start the remote resource manager        
                ssh $user@$2 "cd $basedir/distributedsimulator_resources; ./initsimulator.sh $basedir" &

                ssh $user@$3 "cd $basedir/distributedsimulator_resources; ./initsimulator.sh $basedir" &

                ssh $user@$4 "cd $basedir/distributedsimulator_resources; ./initsimulator.sh $basedir" &
                 ### start the simulator on this host 
                ./initsimulator.sh $basedir

                 ### once this host experiments is done , kill the remote one too
                ssh $user@$remoteSimIp "cd $basedir/distributedsimulator_resources; ./killsimulator.sh"
                 ## kill host simulator process
                ./killsimulator.sh
                calculateSimulationTime cloud2.sics.se

     done