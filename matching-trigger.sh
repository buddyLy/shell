#!/bin/bash
########################################################
#Program: matching-automation
#Desc: This script will control when matching kicks off by checking touchfile from address std,
#and when validation kicks off by checking touchfile from decode step.
########################################################

function initialize_matching_automation
{
	log_this "initializing matching automation"
	source ~/.profile
	source /u/applic/ckp/automation/matching-automation/matching-trigger.cfg
	source /u/applic/ckp/automation/matching-automation/util_func.sh

	isDev
	if [[ $? -eq ${TRUE} ]];then
		OOZIE_SERVER=${OOZIE_SERVER_DEV}
		TOUCHFILE_LOC=${TOUCHFILE_LOC_DEV}
		MATCHING_VAL_LOC=${MATCHING_VAL_LOC_DEV}
		TRACKER_DB=${TRACKER_DB_DEV}
		SUPPORT_EMAIL=${SUPPORT_EMAIL_DEV}
		MATCHING_WF_CFG=${MATCHING_WF_CFG_DEV}
		HDFS_TOUCHFILE_LOC=${HDFS_TOUCHFILE_LOC_DEV}
		PAGER_FILE=${PAGER_FILE_DEV}
		LOG_DIR=${LOG_DIR_DEV}
	fi

	isCert
	if [[ $? -eq ${TRUE} ]];then
		OOZIE_SERVER=${OOZIE_SERVER_CERT}
		TOUCHFILE_LOC=${TOUCHFILE_LOC_CERT}
		MATCHING_VAL_LOC=${MATCHING_VAL_LOC_CERT}
		TRACKER_DB=${TRACKER_DB_CERT}
		SUPPORT_EMAIL=${SUPPORT_EMAIL_CERT}
		MATCHING_WF_CFG=${MATCHING_WF_CFG_CERT}
		HDFS_TOUCHFILE_LOC=${HDFS_TOUCHFILE_LOC_CERT}
		PAGER_FILE=${PAGER_FILE_CERT}
		LOG_DIR=${LOG_DIR_CERT}
	fi

	isProd
	if [[ $? -eq ${TRUE} ]];then
		OOZIE_SERVER=${OOZIE_SERVER_PROD}
		TOUCHFILE_LOC=${TOUCHFILE_LOC_PROD}
		MATCHING_VAL_LOC=${MATCHING_VAL_LOC_PROD}
		TRACKER_DB=${TRACKER_DB_PROD}
		SUPPORT_EMAIL=${SUPPORT_EMAIL_PROD}
		MATCHING_WF_CFG=${MATCHING_WF_CFG_PROD}
		HDFS_TOUCHFILE_LOC=${HDFS_TOUCHFILE_LOC_PROD}
		PAGER_FILE=${PAGER_FILE_PROD}
		LOG_DIR=${LOG_DIR_PROD}
	fi
	
	#captures errors on piped commands
	set -o pipefail
}

function is_matching_done
{
	local matching_id="$1"
	
	DATE=$(date +%Y-%m-%d)
	CHECKJOB=$(oozie jobs -oozie "${OOZIE_SERVER}" -localtime -len 1  -filter name="${matching_id}"\;status=SUCCEEDED | grep -c "${DATE}")
	if [ "$CHECKJOB" = "1" ]
	then
	    echo "Matching is Complete"
	    return "${TRUE}"
	else
		echo "Matching is not complete...Retrying after some time"
	    return "${FALSE}"
	fi
}

function does_touchfile_exist
{
	#returns true if found, false otherwise
	[[ -f "${TOUCHFILE_LOC}/${START_TOUCHFILE}" ]] && return "${TRUE}" || return "${FALSE}"
}

function is_readyto_validate
{
	#test for existence of the done file, returns true if found, false otherwise
	hadoop fs -test -f "${HDFS_TOUCHFILE_LOC}/${HDFS_TOUCHFILE}" && return "${TRUE}" || return "${FALSE}"
}

function log_this
{
	local msg=$1
	echo "$msg"
	current_time=$(date +"%Y-%m-%d %H:%M:%S")
	echo "${current_time} ${msg}" >> "${LOG_DIR}/${LOGFILE}"
}

function trigger_matching_oozie
{
	log_this "Triggering matching workflow"
	local job_oozie_id
	job_oozie_id=$(oozie job -oozie "${OOZIE_SERVER}" -config "${MATCHING_WF_CFG}" -run)
	#CHECKJOB=$(oozie job -oozie "${OOZIE_SERVER}" -config "${MATCHING_WF_CFG}" -run | wc -l)
	#if nothing returns, then job didn't get kicked off 
	if [[ ${job_oozie_id} = "" ]];then
		return "${ERROR}"
	fi
	log_this "Kicked off $job_oozie_id"
	return "${SUCCESS}"
}

function remove_hdfs_touchfile
{
	hadoop fs -rm "${HDFS_TOUCHFILE_LOC}/${HDFS_TOUCHFILE}" && return "${TRUE}" || return "${FALSE}"
}


function remove_touch_file
{
	local touchfile="$1"
	rm "${touchfile}" || return "${ERROR}"
	return "${SUCCESS}"
}

function create_touch_file
{
	local touchfile="$1"
	touch "${touchfile}" || return "${ERROR}"
	return "${SUCCESS}"
}

function call_validate_script
{
	local scriptname=$1; shift
	local cfg=$1; shift
	#local tables=$1; shift
	
	cd ${MATCHING_VAL_LOC} || return "${ERROR}"
	bash "${scriptname}" "${cfg}" || return "${ERROR}" && return "${SUCCESS}"
}

function is_validate_successful
{
	#date should be in this format mm/dd/yyyy, ie 1/11/2016
	#get run date of validation
	local today
	local yesterday
	local failure_count

	today=$(date +"%Y-%m-%d" || exit 1) || error_page_exit "${FUNCNAME[0]}" "1" "get current date" "${LINENO} ERROR get date"
	yesterday=$(date --date="1 days ago" +"%Y-%m-%d" || exit 1) || error_page_exit "${FUNCNAME[0]}" "1" "get yesterdays date" "${LINENO} ERROR get date"

	#if there are any failure, then validation is not successful
	failure_count_query="select source_count, hive_count, valid_result from ${TRACKER_DB}.${TRACKER_TABLE} where source_database='common_decode' and run_date='${today}' and valid_result='Fail'"
	failure_count=$(hive -S -e "${failure_count_query}" | wc -l || exit 1) || error_page_exit "${FUNCNAME[0]}" "1" "failure count query" "${LINENO} ERROR get failure from hive"


	#check for yesterday as run date could have been yesterday
	if [[ ${failure_count} -eq 0 ]];then
		failure_count_query="select source_count, hive_count, valid_result from ${TRACKER_DB}.${TRACKER_TABLE} where source_database='common_decode' and run_date='${yesterday}' and valid_result='Fail'"
		failure_count=$(hive -S -e "${failure_count_query}" | wc -l || exit 1) || error_page_exit "${FUNCNAME[0]}" "1" "failure count query for yesterdays date" "${LINENO} ERROR get failure from hive"
	fi
	
	#if failure count is not zero and the table is for rc_cust_id_decode, then source_count shoudl be bigger than hive count by 2
	if [[ ${failure_count} -eq 0 ]];then
		return "${TRUE}"
	elif [[ ${failure_count} -eq 1 ]];then
		failure_result=$(hive -S -e "${failure_count_query}" || exit 1) || error_page_exit "${FUNCNAME[0]}" "1" "failure result" "${LINENO} ERROR get failure from hive"
		#get the source_count and hive count for comparision
		local source_count
		source_count=$(echo "${failure_result}" | awk '{print $1}')
		local hive_count
		hive_count=$(echo "${failure_result}" | awk '{print $2}')
		local count_diff=0
		count_diff=$(echo "${source_count} - ${hive_count}" | bc -l)
		if [[ ${count_diff} -eq 2 ]];then
			return "${TRUE}"
		fi
	fi

	return "${FALSE}"
}

function validate_matching
{
	call_validate_script "${MATCHING_VAL_SCRIPT}" "${MATCHING_VAL_CFG}" || return "${ERROR}"
	is_validate_successful && return "${SUCCESS}"
	#call_validate_script "${MATCHING_VAL_SCRIPT}" "${MATCHING_VAL_CFG}" "${MATCHING_VAL_TABLES}" || return "${ERROR}"
	return "${ERROR}"
}

function change_group
{
	local group=$1; shift
	local folder=$1

	hadoop fs -chgrp -R "${group} ${folder}" || return "${ERROR}"
	return "${SUCCESS}"
}

function change_permission
{
	local permission=$1; shift
	local folder=$1
	hadoop fs -chmod -R "${permission}" "${folder}" || return "${ERROR}"
	return "${SUCCESS}"
}

function sanity_check
{
	log_this "Sanity check"
	#if both touch files exist, then something is wrong	
	is_readyto_validate && does_touchfile_exist && return "${ERROR}"
	return "${SUCCESS}"
}

#main starts here
function begin_matching_trigger
{
	#prepare for matching
	initialize_matching_automation

	sanity_check || error_page_exit "${FUNCNAME[0]}" "1" "sanity_check" "${LINENO} ERROR Matching triggering sanity check failed"

	#if touchfile exists after matching finishes to begin validation, then start validation, else exit
	#if it doesn't exist, it either means it's still running, or validation is already done.
	is_readyto_validate
	if [[ $? -eq ${TRUE} ]];then
		#remove touch file right away
		remove_hdfs_touchfile || error_page_exit "${FUNCNAME[0]}" "1" "is_readyto_validate" "${LINENO} ERROR can't remove hdfs touch file"

		validate_matching	
		if [[ $? -eq "${SUCCESS}" ]];then
			#create touch file for location
			log_this "Validation successful, creating touch file: ${TOUCHFILE_LOC}/${FINISH_TOUCHFILE}"
			create_touch_file "${TOUCHFILE_LOC}/${FINISH_TOUCHFILE}"
		else
			error_page_exit "${FUNCNAME[0]}" "1" "validate_matching" "${LINENO} ERROR Matching validation failed"
			exit 1
		fi
	fi
	
	#if still running, then no touch file, process will exit
	#if validation is already done and matching isn't running, touchfile will not exist, exit
	#if touchfile exist for matching, matching will start, else exit
	does_touchfile_exist
	if [[ $? -eq ${TRUE} ]];then
		#removes matching trigger touch file so matching would not start again
		log_this "Remove touch file ${TOUCHFILE_LOC}/${START_TOUCHFILE}"
		remove_touch_file "${TOUCHFILE_LOC}/${START_TOUCHFILE}" || error_page_exit "${FUNCNAME[0]}" "1" "does_touchfile_exist" "${LINENO} ERROR can't remove matching triggering touch file"
		
		trigger_matching_oozie
		if [[ $? -eq ${ERROR} ]];then
			error_page_exit "${FUNCNAME[0]}" "1" "trigger_matching_oozie" "${LINENO} ERROR unable to trigger matching oozie workflow"
		fi
	fi
}

