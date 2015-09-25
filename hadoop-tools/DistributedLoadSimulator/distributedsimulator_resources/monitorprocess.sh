#!/bin/bash

## This script is used to check the number of running AppMaster process in simulator instances.
## we have to execute this script on single once simulator instance where we run other instances

simmachines=(cloud1.sics.se cloud3.sics.se cloud4.sics.se cloud6.sics.se)

while true
 do
    local=$(ps axf | grep AppMasterProcess | grep -v grep | wc -l)
    echo "machine - cloud2.sics.se =  $local"
for machine in "${simmachines[@]}"
        do
          process=$(ssh root@$machine "ps axf | grep AppMasterProcess | grep -v grep | wc -l")
          echo "machine - $machine =  $process"
        done
   echo "====================================="
   sleep 2
done
