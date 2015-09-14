#!/bin/bash

## this script will kill all the simulator related processes

ps axf | grep "rmiregistry" | grep -v grep | awk '{print "kill -9 " $1}' | sh
ps axf | grep "AppMasterProcess" | grep -v grep | awk '{print "kill -9 " $1}' | sh
ps axf | grep "SLSRunner" | grep -v grep | awk '{print "kill -9 " $1}' | sh
