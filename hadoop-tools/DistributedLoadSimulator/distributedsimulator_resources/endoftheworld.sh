#!/bin/bash
basedir="/home/sri/batchmode/hop_distro/distributedsimulator_resources"
instancemachines=( bbc2.sics.se cloud3.sics.se cloud4.sics.se )
user="root"

for machine in "${instancemachines[@]}"
 do
    ssh $user@$machine "$basedir/killsimulator.sh" \> /dev/null 2\>\&1
 done
## finally kill the own machine
./killsimulator.sh.sh \> /dev/null 2\>\&1

clear
