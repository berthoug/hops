#!/bin/bash
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License. See accompanying LICENSE file.
#

###############################################################################
YARN_DIRECTORY=""

printUsage() {
  echo "Usage: slsrun.sh <OPTIONS>"
  echo "                 --input-rumen|--input-sls=<FILE1,FILE2,...>"
  echo "                 --output-dir=<SLS_SIMULATION_OUTPUT_DIRECTORY>"
  echo "                 [--nodes=<SLS_NODES_FILE>]"
  echo "                 [--track-jobs=<JOBID1,JOBID2,...>]"
  echo "                 [--print-simulation]"
  echo "                 [--yarnnode]"
  echo "                 [--distriubted-mode]"
  echo "		 [--loadsimulator-mode]"
  echo "                 [--rt-address]"
  echo "		 [--rm-address]"
  echo                  
}
###############################################################################
parseArgs() {
  for i in $*
  do
    case $i in
    --input-rumen=*)
      inputrumen=${i#*=}
      ;;
    --input-sls=*)
      inputsls=${i#*=}
      ;;
    --output-dir=*)
      outputdir=${i#*=}
      ;;
    --nodes=*)
      nodes=${i#*=}
      ;;
    --track-jobs=*)
      trackjobs=${i#*=}
      ;;
    --print-simulation)
      printsimulation="true"
      ;;
    --yarnnode)
      yarnnode="true"
      ;;
    --distributed-mode)
      distributedmode="true"
      ;;
    --loadsimulator-mode)
      loadsimulatormode="true"
      ;;
    --rt-address=*)
      rtaddress=${i#*=}
      ;;
    --rm-address=*)
      rmaddress=${i#*=}
      ;;
    --parallelsimulator)
      parallelsimulator="true"
      ;;
    --stopappsimulation)
      stopappsimulation="true"
      ;;
      --rmi-address=*)
      rmiaddress=${i#*=}
      ;;
    --yarn-directory=*)
      YARN_DIRECTORY=${i#*=}
      ;;
    --isLeader)
      ISLEADER="true"
      ;;
    --simulation-duration=*)
      SIMULATIONDURATION=${i#*=}
      ;;
    *)
      echo "Invalid option"
      echo
      printUsage
      exit 1
      ;;
    esac
  done

  if [[ "${inputrumen}" == "" && "${inputsls}" == "" ]] ; then
    echo "Either --input-rumen or --input-sls must be specified"
    echo
    printUsage
    exit 1
  fi

  if [[ "${outputdir}" == "" ]] ; then
    echo "The output directory --output-dir must be specified"
    echo
    printUsage
    exit 1
  fi
}

runSimulation() {

  if [[ "${inputsls}" == "" ]] ; then
    args="-inputrumen ${inputrumen}"
  else
    args="-inputsls ${inputsls}"
  fi

  args="${args} -output ${outputdir}"

  if [[ "${nodes}" != "" ]] ; then
    args="${args} -nodes ${nodes}"
  fi
  
  if [[ "${trackjobs}" != "" ]] ; then
    args="${args} -trackjobs ${trackjobs}"
  fi
  
  if [[ "${printsimulation}" == "true" ]] ; then
    args="${args} -printsimulation"
  fi
 
  if [[ "${yarnnode}" == "true" ]] ; then
    args="${args} -yarnnode"
  fi

  if [[ "${distributedmode}" == "true" ]] ; then
    args="${args} -distributedmode"
  fi

  if [[ "${loadsimulatormode}" == "true" ]] ; then
    args="${args} -loadsimulatormode"
  fi

  if [[ "${rtaddress}" != "" ]] ; then
    args="${args} -rtaddress ${rtaddress}"
  fi
 
  if [[ "${rmaddress}" != "" ]] ; then
    args="${args} -rmaddress ${rmaddress}"
  fi
 
  if [[ "${parallelsimulator}" == "true" ]] ; then
    args="${args} -parallelsimulator ${parallelsimulator}"
  fi

  if [[ "${stopappsimulation}" == "true" ]] ; then
    args="${args} -stopappsimulation ${stopappsimulation}"
  fi
 
  if [[ "${rmiaddress}" != "" ]] ; then
    args="${args} -rmiaddress ${rmiaddress}"
  fi

  if [[ "$ISLEADER" != "" ]] ; then
      args="${args} -isLeader true"
  fi

  if [[ "$SIMULATIONDURATION" != "" ]] ; then
      args="${args} -simulationDuration ${SIMULATIONDURATION}"
  fi
 $YARN_DIRECTORY/hadoop-2.4.0/bin/yarn org.apache.hadoop.distributedloadsimulator.sls.SLSRunner ${args}
}
###############################################################################

parseArgs "$@"
runSimulation

exit 0

