#!/bin/bash

function usage
{
cat << EOF
usage: $0 options

This script triggers matching workflow when conditions are riped.

OPTIONS:
   -h      show usage
   -t      test type, can be 'unit' or 'integrate'
   -v      verbose
   -r      run script
EOF
}

##get script options
while getopts "hrvt:" opt
do
	case $opt in
		h)
			usage
			exit 1
			;;
		t)
			TEST_OPTION=$OPTARG
			;;
		v)
			VERBOSE=1
			;;
		r)
			RUN=1
			;;
		?)
			RUN=1
			exit
			;;
	esac
done

#decide what to run depending on parameters
if [[ ${VERBOSE} -eq 1 ]];then
	echo "set -x"
	set -x	
	debug="-x"
fi

if [[ ${TEST_OPTION} != "" && ${TEST_OPTION} != "unit" && ${TEST_OPTION} != "functional" ]];then
	usage
	exit 1
elif [[ ${TEST_OPTION} == "unit" ]];then
	echo "running test cases"
	bash ${debug} unittest-matching-trigger.sh
	count=$(cat unit_test_result.txt | grep -c "PASSED")
	if [[ ${count} -eq 14 ]];then
		echo "Congrats! Unit test successful"
	else
		echo "WARNING! Unit test failed"	
	fi
elif [[ ${TEST_OPTION} == "functional" ]]; then
	echo "running functional test cases"
	bash ${debug} functionaltest-matching-trigger.sh
	bash ${debug} functionaltest-matching-trigger-trap.sh
	count=$(cat test_case_result.txt | grep -c "PASSED")
	if [[ ${count} -eq 9 ]];then
		echo "Congrats! Functional test successful"
	else
		echo "WARNING! Functional test failed"	
	fi
fi

#if run option is set, then begin the matching
if [[ ${RUN} -eq 1 ]];then
	echo "begin triggering matching"
	source ./matching-trigger.cfg
	source ./util_func.sh
	source ./matching-trigger.sh
	begin_matching_trigger
	echo "end triggering matching"
fi


