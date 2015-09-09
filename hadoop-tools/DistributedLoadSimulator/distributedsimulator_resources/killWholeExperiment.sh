#!/bin/bash
basedir="/home/sri/batchmode"
ssh root@bbc2.sics.se "$basedir/killSLSRunner.sh" \> /dev/null 2\>\&1

ssh root@cloud4.sics.se "/home/sri/realsimulator/distributedsimulator_resources/killsimulator.sh" \> /dev/null 2\>\&1
./killsimulator.sh.sh \> /dev/null 2\>\&1

clear
