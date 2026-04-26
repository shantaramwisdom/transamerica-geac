#!/bin/bash
########################################################################################################
# DESCRIPTION - Script to run Finance Curated Pyspark Jobs from Sources like IDL (Bestow/ltcg) / GDQ (VantageP/ALM)/ Datastage (BaNCS)
# USAGE 1  - sh /appLication/financedw/curated/scripts/load_curated.sh -s Bestow -d currentbatch_party -f DAILY -j XXXXXX -g I
# USAGE 2  - sh /appLication/financedw/curated/scripts/load_curated.sh -s Bestow -d party -f DAILY -j XXXXXX -g I
# USAGE 3  - sh /appLication/financedw/curated/scripts/load_curated.sh -s Bestow -d completedbatch_party -f DAILY -j XXXXXX -g I
# USAGE 4  - sh /appLication/financedw/curated/scripts/load_curated.sh -s VantageP65 -d currentbatch_party -f DAILY -j XXXXXX -g G
# USAGE 5  - sh /appLication/financedw/curated/scripts/load_curated.sh -s VantageP65 -d party -f DAILY -j XXXXXX -g G
# USAGE 6  - sh /appLication/financedw/curated/scripts/load_curated.sh -s VantageP65 -d completedbatch_party -f DAILY -j XXXXXX -g G
# USAGE 7  - sh /appLication/financedw/curated/scripts/load_curated.sh -s BaNCS -d currentbatch_party -f DAILY -j XXXXXX -g D
# USAGE 8  - sh /appLication/financedw/curated/scripts/load_curated.sh -s BaNCS -d party -f DAILY -j XXXXXX -g D
# USAGE 9  - sh /appLication/financedw/curated/scripts/load_curated.sh -s BaNCS -d completedbatch_party -f DAILY -j XXXXXX -g D
# USAGE 10 - sh /appLication/financedw/curated/scripts/load_curated.sh -s ALM -d currentbatch_fund -f MONTHLY -j XXXXXX -g G
# USAGE 11 - sh /appLication/financedw/curated/scripts/load_curated.sh -s ERPDW -d currentbatch_accounts_payable_vendor_master -f DAILY -j XXXXXX -g C
# USAGE 12 - sh /appLication/financedw/curated/scripts/load_curated.sh -s arr -d currentbatch_general_ledger_header -f DAILY -j XXXXXX -g A
# changes: 09/07/2023 - Anvitha Chandra - Initial Development
########################################################################################################

DIR="${BASH_SOURCE%/*}"
if [[ ! -d "$DIR" ]]; then DIR="$PWD"; fi

job_name=''

while getopts s:d:j:f:g:i:r:q: option
do
   case "$option" in
      s)
         source_system_name="$OPTARG"
        ;;
      d)
         domain_name="$OPTARG"
        ;;
      j)
         job_name="$OPTARG"
        ;;
      f)
         frequency="$OPTARG"
        ;;
      g)
         src_flag="$OPTARG"
        ;;
      i)
         initial_load="$OPTARG"
        ;;
      r)
         rerun_flag="$OPTARG"
        ;;
      q)
         read_s3="$OPTARG"
        ;;
      \?) echo "Invalid option"
          exit 99
        ;;
   esac
done

if [[ -z $source_system_name ]] || [[ -z $domain_name ]] || [[ -z $frequency ]] || [[ -z $src_flag ]]; then
   echo "ERROR 10: Mandatory argument/s (SOURCE SYSTEM (s) or DOMAIN NAME (d) or FREQUENCY (f) or SOURCE (g) is/are not provided, please check"
   exit 10
fi

if [[ $src_flag != 'G' ]] && [[ $src_flag != 'D' ]] && [[ $src_flag != 'I' ]] && [[ $src_flag != 'C' ]] && [[ $src_flag != 'A' ]]; then
   echo "ERROR 20: SOURCE Flag (g) Allows only Values I/D/G/C/A for IDL/Datastage/GDQ/Curated/ARR. But Received $src_flag"
   exit 20
fi

if [[ -z $job_name ]]; then
   job_name=NOT_DEFINED
fi

if [[ -z $read_as ]] || [[ $read_as != 'Y' ]] && [[ $read_as != 'N' ]]; then
   read_s3='N'
fi

if [[ -z $initial_load ]] || [[ $initial_load != 'Y' ]] && [[ $initial_load != 'N' ]]; then
   initial_load='N'
fi

if [[ $initial_load == 'Y' ]] && [[ $source_system_name != Vantagep ]]; then
   initial_load='N'
fi

if [[ -z $rerun_flag ]] || [[ $rerun_flag != 'Y' ]] && [[ $rerun_flag != 'N' ]]; then
   rerun_flag='N'
fi

source "$DIR/script.properties"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)

dmn=$(echo ${domain_name} | cut -d'_' -f1)
dmn1=$(echo ${domain_name} | cut -d'_' -f2)
dmn2=$(echo ${domain_name} | cut -d'_' -f3)

if [[ ${dmn1} == 'completedbatch' ]] || [[ ${dmn1} == 'currentbatch' ]] && [[ ! -z ${dmn2} ]]; then
   dmn1=${domain_name%/*}
elif [[ ${dmn} == 'completedbatch' ]] || [[ ${dmn} == 'currentbatch' ]] && [[ ! -z ${dmn1} ]] && [[ -z ${dmn2} ]]; then
   dmn=${domain_name}
elif [[ ${dmn} != 'completedbatch' ]] && [[ ${dmn} == 'currentbatch' ]]; then
   dms=${domain_name}
fi

if [[ ${dmn} == 'completedbatch' ]] || [[ ${dmn} == 'currentbatch' ]]; then
   mkdir -p $logFilePath/${source_system_name}/${dmn}/${dmn1}
   LOGFILE="$logFilePath/${source_system_name}/${dmn}/${dmn1}/curated_load_${TIMESTAMP}.log"
   ERRORFILE="$logFilePath/${source_system_name}/${dmn}/${dmn1}/curated_load_${TIMESTAMP}.err"
else
   mkdir -p $logFilePath/${source_system_name}/${dmn}
   LOGFILE="logFilePath/${source_system_name}/curated_load_${TIMESTAMP}.log"
   ERRORFILE="logFilePath/${source_system_name}/curated_load_${TIMESTAMP}.err"
fi

if [[ $env == 'prd' ]] || [[ $env == 'mdl' ]]; then
   initial_load='N'
fi

echo Log File is $LOGFILE | tee -a $LOGFILE
echo Error File is $ERRORFILE | tee -a $LOGFILE
echo "Data Insert Script for source_system_name ${source_system_name} and domain name ${domain_name} started at: $(date +%Y-%m-%d' '%H:%M:%S)" | tee -a $LOGFILE

print_details()
{
   echo "Environment: $env" | tee -a $LOGFILE
   echo "Project: $project" | tee -a $LOGFILE
   echo "Domain/Segment Name: $domain_name" | tee -a $LOGFILE
   echo "Source System: $source_system_name" | tee -a $LOGFILE
   echo "Control M Job Name: $job_name" | tee -a $LOGFILE
   echo "Frequency: $frequency" | tee -a $LOGFILE
   echo "Vantage Systems Initial Load: $initial_load" | tee -a $LOGFILE
   echo "Rerun Flags: $rerun_flag" | tee -a $LOGFILE
   echo "Read SQL Files from S3: $read_s3" | tee -a $LOGFILE
   echo "Source Flag (I=IDL/D=Datastage/G=GDQ/C=Curated/A=ARR): $src_flag" | tee -a $LOGFILE
}

print_details

spark-submit --master yarn $spark_properties \
$DIR/load_curated.py -s $source_system_name -d $domain_name -j $job_name -f $frequency -sf $src_flag -i $initial_load -r $rerun_flag -rs $read_s3 2>> $ERRORFILE | tee -a $LOGFILE

RC=${PIPESTATUS[0]}

if [[ $RC -ne 0 ]]; then
   echo "ERROR 20: Curated Load Script for Source System: ${source_system_name} and Domain: ${domain_name} failed with RC at: $(date +%Y-%m-%d' '%H:%M:%S)" | tee -a $LOGFILE
   exit $RC
fi
echo "Curated Load Script for Source System: ${source_system_name} and Domain: ${domain_name} completed at: $(date +%Y-%m-%d' '%H:%M:%S)" | tee -a $LOGFILE

