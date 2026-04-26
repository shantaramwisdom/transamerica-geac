#######################################   DEVELOPMENT LOG   #########################################
# DESCRIPTION - Finance Curated Pyspark jobs
# USAGE 1: spark-submit /application/financedw/curated/scripts/load_curated.py -s Bestow -d currentbatch_party
#          $ daily -sf J -D XXXXXX
# USAGE 2: spark-submit /application/financedw/curated/scripts/load_curated.py -s Bestow -d party
#          $ daily -sf J -D XXXXXX
# USAGE 3: spark-submit /application/financedw/curated/scripts/load_curated.py -s Bestow -d completedbatch_party
#          $ daily -sf J -D XXXXXX
# USAGE 4: spark-submit /application/financedw/curated/scripts/load_curated.py -s Vantage -d party
#          $ daily -sf J -D XXXXXX
# USAGE 5: spark-submit /application/financedw/curated/scripts/load_curated.py -s BaNCS -d party
#          $ daily -sf J -D XXXXXX
#
# 09/07/2023 - Anvitha Chandra/Sangram Patil - Initial Development
####################################################################################################
import argparse
import traceback
import os
import sys
import uuid
import pyaes
import binascii
# import apscheduler.schedulers.background
# import farmhash
# import ctypes
from datetime import datetime
from functools import reduce
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
path = os.path.dirname(os.path.realpath(__file__))

print("Current Directory Path -", path)
parent = os.path.abspath(os.path.join(path, os.pardir))
print("Parent Directory Path -", parent)
sys.path.append(parent)
from common_utils import *

#Main Module to Do Various Controls
def do_all_controls(spark, source_system_name, batch_frequency, domain, domain_name, cycle_date, batchid, gl_header, gl_line_item, curated_database, 
                    source_flag, curated_table_name, controlm_job,   ----config, admin_system_short, read_from_s3, oc_controls_querypath, s3_querypath_oc, 
                    kc_controls_querypath, s3_querypath_kc2, do_oc_controls_flag, do_kc2_controls_flag, start_time_overall):
    global phase_name
    phase_name = 'curated controls'
    print("--------------------------------------------------------")
    if (gl_line_item or gl_header) and cycle_date and batchid:
        check_reinstate_processed_errors(spark, cycle_date, domain, cycle_date, batchid, gl_header, gl_line_item, curated_database, curated_table_name, config)
        build_prerequisites_gl(spark, source_system_name, domain, cycle_date, batchid, gl_header, gl_line_item, curated_database, sum_oc_df = True)
        build_prerequisites_gl(spark, source_system_name, domain, cycle_date, batchid, gl_header, gl_line_item, curated_database, error_cleared = True)
    if do_oc_controls_flag == 'Y' and cycle_date and batchid:
        do_operational_controls(spark, cycle_date, batchid, source_system_name, domain_name,   -phase_name, curated_database, curated_table_name, 
                                batch_frequency, controlm_job,     -source_flag,  oc_controls_querypath, config, admin_system_short, read_from_s3, s3_querypath_oc)
        if config.get('derived_domain') and domain_name in do_additional_controls_derived_domain_list:
            phase_name = 'curated additional controls'
            do_additional_controls(spark, cycle_date, --batchid, --source_system_name,  ----domain, domain_name, phase_name, batch_frequency, 
                                controlm_job, oc_controls_querypath, s3_querypath_oc, config, read_from_s3)
    else:
        log.info("Skipping Operational Controls, as already processed")
        print("--------------------------------------------------------")
    if do_kc2_controls_flag == 'Y' and cycle_date and batchid:
        phase_name = 'curated kc2 controls'
        do_kc2(spark, cycle_date, batchid, source_system_name, domain_name, phase_name, curated_database, curated_table_name, 
               batch_frequency, source_flag, controlm_job, kc_controls_querypath, gl_line_item, gl_header, config, read_from_s3, s3_querypath_kc2)
    elif do_kc2_controls_flag == 'N':
        log.info("Skipped KC2 Controls Logic as the Source System/Domain Combination don’t have KC2 Controls Enabled")
    else:
        log.info("Skipping KC2 Controls, as already processed")
        print("--------------------------------------------------------")
    end_time_overall = datetime.now(est_tz)
    time_taken = calc_time_taken(start_time_overall, end_time_overall)
    log.info(f"Time Taken for Loading the Entire Batch: {time_taken}(h:m:s)")


#Main Module to Load Curated Tables
def load_curated(spark, source_system_name, domain, domain_name, batch_frequency, cycle_date, batchid, source_flag, filenames, read_from_s3, df_list, segmentname, 
                 curated_database,gl_header, gl_line_items, do_intraday_multibatch_run_flag, do_delete_logic_flag, rerun_flag,
                 do_cdc_flag, apply_ifrs17_logic, apply_ifrs17_logic_one_time, querypath, s3_querypath_common, common_querypath, redshift_query, config):
    source_df_cnt, valid_records_cnt, soft_error_cnt, hard_error_cnt, final_df_cnt = 0, 0, 0, 0, 0
    if segmentname not in ['currentbatch', 'completedbatch']:
        if config.get('derived_domain') and config.get('load_pattern') == 'type2':
            query = f"""select count(*) from (select 1 from {config['redshift_target_schema']}.{domain_name} 
                         where source_system_name = '{source_system_name}' and (cycledate > '{cycle_date}' or batchid > {batchid}) limit 1)"""
            result_ = get_jdbc_df(spark, source='findw', query=query)
            if result_.collect()[0][0]:
                raise Exception(f"Type2 Domain {domain_name} is not empty for {source_system_name} for cycle_date/batchid greater than {cycle_date}/{batchid}")
    setup_prerequisites(spark, cycle_date, batchid, segmentname, domain, batch_frequency, source_system_name, config, common_querypath, gl_header, gl_line_item, 
                        curated_database, querypath, read_from_s3, s3_querypath_common, source_flag, apply_ifrs17_logic_one_time, rerun_flag)
    for filename in filenames:
        df_cnt = None
        start_time = datetime.now(est_tz)
        df_name, writemode, filename, prepared_df, df_cnt = read_and_prepare_df(spark, querypath, filename, read_from_s3, config)
        globals()[df_name] = prepared_df
        df_list.append(df_name)
        if writemode:
            log.info(f"Spark writemode for {df_name}: {writemode}")
        if writemode and df_name != 'finaldf':
            # records insert/update to other curated tables and Other Databases. e.g. oracle/RDM
            do_intermediate_table_loads(spark, cycle_date, batchid, source_system_name, domain, writemode, df_name, filename, curated_database, gl_header, gl_line_item)
            print("--------------------------------------------------------")
        if df_name == 'finaldf':
            finaldf = prepared_df
        if df_name == 'finaldf' and segmentname not in ['currentbatch', 'completedbatch'] and not gl_header and not gl_line_item:
            if config.get('load_pattern') == 'type2':
                df_cnt, finaldf = finaldf_remove_dups_and_expired(spark, source_system_name, finaldf, df_name)
        end_time = datetime.now(est_tz)
        time_taken = calc_time_taken(start_time, end_time)
        if df_name == 'source_df' and gl_header:
            gl_validate_header_source_df(spark, domain_name, source_system_name)
        if df_name == 'valid_records' and gl_line_item:
            gl_validate_line_valid_records_df(spark, cycle_date, batchid, domain_name, source_system_name, curated_database)
        elif apply_ifrs17_logic and df_name == 'gl_source_data':
            ifrs17_prepare_common_dataframes(spark, common_querypath, s3_querypath_common, read_from_s3, config, config['ifrs17_pattern_path'])
            print("--------------------------------------------------------")
        if gl_header or gl_line_item:
            if df_name == 'source_df':
                source_df_cnt = df_cnt
            elif df_name == 'valid_records':
                valid_records_cnt = df_cnt
            elif writemode == 'append' and df_name == 'curated_error':
                hard_error_cnt = df_cnt
            elif writemode == 'overwrite' and df_name == 'curated_error':
                soft_error_cnt = df_cnt
            elif df_name == 'finaldf':
                final_df_cnt = df_cnt
                if final_df_cnt != source_df_cnt - (soft_error_cnt + hard_error_cnt):
                    log.warning(f"Final DF Count {final_df_cnt} not equal to Source DF Count {source_df_cnt} - (Soft Error DF Count {soft_error_cnt} + Hard/Reprocessed Error DF Count {hard_error_cnt})")
                else:
                    log.info(f"Final DF Count {final_df_cnt} matches to Source DF Count {source_df_cnt} - (Soft Error DF Count {soft_error_cnt} + Hard/Reprocessed Error DF Count {hard_error_cnt})")
                if gl_line_item:
                    if final_df_cnt != valid_records_cnt:
                        log.warning(f"Final DF Count {final_df_cnt} not equal to Valid Records DF Count {valid_records_cnt}")
                    else:
                        log.info(f"Final DF Count {final_df_cnt} matched to Valid Records DF Count {valid_records_cnt}")
    if 'completedbatch' in domain:
        tablename = 'completedbatch'
    elif 'currentbatch' in domain:
        tablename = 'currentbatch'
        if not df_cnt:
            raise Exception("No Batches Left to be processed!!!!")
    else:
        tablename = '{0}_{1}'.format(source_system_name, domain)
    if 'completedbatch' in segmentname:
        result_ = get_jdbc_df(spark, source='findw', redshift_query).cache()
        cb_cnt = result_.collect()[0][0]
        bt_cnt = result_.collect()[0][1]
        result.unpersist(blocking=True)
        if bt_cnt != cb_cnt:
            lft_cnt = cb_cnt - bt_cnt
            cb_cnt = cb_cnt
            raise Exception(f"No Data Loaded!!! Still {lft_cnt} of {cb_cnt} Batches Left to Process in Curated Layer. Please validate ETL Loads and Redshift Batch Tracking.")
    log.info("Curated Table Name : " + tablename)
    start_time = datetime.now(est_tz)
    if do_cdc_flag == 'Y':
        finaldf, cdc_delta_cnt = do_cdc(spark, source_system_name, domain_name, batch_frequency, cycle_date, batchid, curated_database, tablename, finaldf, do_delete_logic_flag)
        config['cdc_delta_cnt'] = cdc_delta_cnt
    elif segmentname not in ['currentbatch', 'completedbatch']:
        log.info("Skipping CDC Process as its not enabled")
    if source_flag == 'G' and segmentname not in ['currentbatch', 'completedbatch']:
        df_cnt = finaldf.filter("coalesce(operationcode,'NULL') != 'DELT'").count()
        finaldf.filter("coalesce(operationcode,'NULL') != 'DELT'").drop("operationcode","fltr").repartition(2).write.mode('{0}'.format(writemode)).insertInto('{0}.{1}'.format(curated_database, tablename))
    elif config.get('redshift_target_schema') == 'erpdw' and config.get('load_pattern') == 'append':
        if df_cnt:
            finaldf.repartition(2).write.mode('{0}'.format(writemode)).insertInto('{0}.{1}'.format(curated_database, tablename))
        if gl_line_item or gl_header:
            gl_header_line_validate_finaldf(spark, source_system_name, domain_name, cycle_date, batchid, finaldf, df_cnt, gl_line_item, gl_header, curated_database, curated_table_name )
    else:
        if source_system_name.lower() in ("ltcg","ltcghybrid") and domain == "party":
            finaldf.drop("fltr","contractnumber").repartition(2).write.mode('{0}'.format(writemode)).insertInto('{0}.{1}'.format(curated_database, tablename))
        else:
            finaldf.drop("fltr").repartition(2).write.mode('{0}'.format(writemode)).insertInto('{0}.{1}'.format(curated_database, tablename))
    end_time = datetime.now(est_tz)
    time_taken = calc_time_taken(start_time, end_time)
    log.info(f"Count of Inserted Records for Curated Table {tablename} is: {df_cnt} at {end_time.strftime('%Y-%m-%d %H:%M:%S')}. Time Taken: {time_taken(h:m:s)}")
    if segmentname in ['currentbatch'] and do_intraday_multibatch_run_flag == 'N':
        validate_currentbatch(spark, curated_database, source_system_name, domain_name, batch_frequency, segmentname)
    print("--------------------------------------------------------")
    if do_delete_logic_flag == 'Y' and segmentname not in ['currentbatch','completedbatch']:
        do_delete_logic(spark, cycle_date, batchid, source_system_name, domain_name, batch_frequency, finaldf, curated_database, tablename, source_flag)
    else:
        log.info("Skipped Identifying Deletes Logic as the Source System/Domain Combination don't have Delete Logic Enabled")
        print("--------------------------------------------------------")
    if source_flag == 'C' and segmentname in ['currentbatch','completedbatch']:
        derived_domain_tracker(spark, source_system_name, domain_name, batch_frequency, segmentname)
    for df in df_list:
        exec(f"{df}.unpersist(blocking=True)")
    return tablename


#Main Module for the Program
def main_curated(spark, source_system_name, domain, domain_name, batch_frequency, loop_counter, segmentname, source_database, curated_database, 
                 curated_table_name, admin_system_short, source_flag, ovrrd_phase_name, vantage_initial_load, controlm_job, read_from_s3, filenames, 
                 s3_querypath_common, common_querypath, querypath, oc_controls_querypath, s3_querypath_oc, kc_controls_querypath, s3_querypath_kc2, 
                 gl_header, gl_line_item, gl_combiner, glerp_header_only_domain, glerp_soft_error_max_age_days, refresh_mview,
                 do_delete_logic_flag, do_intraday_multibatch_run_flag, rerun_flag, do_oc_controls_flag, do_kc2_controls_flag, do_cdc_flag, redshift_query, config):
    loop = 0
    apply_ifrs17_logic = False
    apply_ifrs17_logic_one_time = False
    global cycle_date, batchid, phase_name, drvd_domain
    while loop < loop_counter:
        start_time_overall = datetime.now(est_tz)
        loop += 1
        phase_name = 'curated'
        df_list = []
        print("--------------------------------------------------------")
        print("--------------------------------------------------------")
        log.info(f"Processing Batch {loop} of total {loop_counter} batches")
        if loop > 1:
            spark.catalog.clearCache()
        if 'currentbatch' not in segmentname:
            cycle_date, batchid = getBatchid(spark, source_database, curated_database, source_system_name, segmentname, domain_name, batch_frequency, \
                                             config, admin_system_short, source_flag, phase_name, ovrrd_phase_name, vantage_initial_load)
        if cycle_date and batchid and 'currentbatch' not in segmentname:
            monthend_flag, weekend_flag = setup_dates_and_batchtracking(spark, source_system_name, batchid, cycle_date, drvd_domain, 
                                                                        segmentname, phase_name, batch_frequency, config, controlm_job)
        if (cycle_date and batchid) or 'currentbatch' in segmentname:
            if loop == 1 and segmentname not in ['currentbatch', 'completedbatch'] and domain_name in ['general_ledger_header','general_ledger_line_item'] \
                and source_system_name in ifrs17_config_dict['source_system_names']:
                apply_ifrs17_logic = True
                apply_ifrs17_logic_one_time = True
                #Remove Below Line to Enable IFRS17 LOGIC
                apply_ifrs17_logic = apply_ifrs17_logic_one_time = False
            else:
                apply_ifrs17_logic_one_time = False
            curated_table_name = load_curated(spark, source_system_name, domain, domain_name, batch_frequency, cycle_date, batchid, source_flag, filenames, read_from_s3, df_list, 
                                              segmentname, curated_database, gl_header, gl_line_item, do_intraday_multibatch_run_flag,
                                                do_delete_logic_flag, rerun_flag, do_cdc_flag, apply_ifrs17_logic, apply_ifrs17_logic_one_time, querypath, s3_querypath_common, 
                                            common_querypath, redshift_query, config)
        else:
            log.info(f"Skipping Curated Load for {segmentname}, as already processed")
        if segmentname not in ['currentbatch']:
            if cycle_date and batchid:
                batchtracking_logging(source_system_name, drvd_domain, batchid, status='complete', cycle_date, insert_update_flg='update', phase_name, batch_frequency)
            if refresh_mview:
                if segmentname == 'completedbatch':
                    refresh_materialized_view(domain_name)
                else:
                    if drvd_domain == 'generalledgertrialbalance':
                        phase_name = 'datawarehouse'
                        drvd_domain = domain_name
                        batchtracking_logging(source_system_name, drvd_domain, batchid, status='in-progress', cycle_date, insert_update_flg='insert', phase_name, batch_frequency, controlm_job)
                        batchtracking_logging(source_system_name, drvd_domain, batchid, status='complete', cycle_date, insert_update_flg='update', phase_name, batch_frequency)
        end_time_data_load = datetime.now(est_tz)
        time_taken = calc_time_taken(start_time_overall, end_time_data_load)
        log.info(f"Time Taken for Curated Data Load for the Batch: {time_taken}:(h:m:s)")
        if do_oc_controls_flag == 'Y' or do_kc2_controls_flag == 'Y':
            do_all_controls(spark, source_system_name, batch_frequency, domain, domain_name, cycle_date, batchid, gl_header, gl_line_item, curated_database, 
                            source_flag, curated_table_name, controlm_job, config, admin_system_short, read_from_s3, oc_controls_querypath, 
                            s3_querypath_oc, kc_controls_querypath, s3_querypath_kc2, do_oc_controls_flag, do_kc2_controls_flag, start_time_overall)
        elif segmentname not in ['completedbatch']:
            log.info("Skipped OC/KC2 Controls Logic as the Source System/Domain Combination don't have OC/KC2 Controls Enabled")
            phase_name = 'curated controls'
            batchtracking_logging(source_system_name, drvd_domain, batchid, status='in-progress', cycle_date, insert_update_flg='insert', phase_name, batch_frequency, controlm_job)
            batchtracking_logging(source_system_name, drvd_domain, batchid, status='complete', cycle_date, insert_update_flg='update', phase_name, batch_frequency)
        else:
            getBatchid(spark, source_database, curated_database, source_system_name, segmentname, domain_name, batch_frequency, config, 
                       admin_system_short, source_flag, phase_name, ovrrd_phase_name, vantage_initial_load)
            end_time_overall = datetime.now(est_tz)
            time_taken = calc_time_taken(start_time_overall, end_time_overall)
            log.info(f"Time Taken for Curated Data Load for the Batch: {time_taken}(h:m:s)")
    else:
        if gl_line_item or gl_header:
            gl_final_actions(spark, source_system_name, domain_name, batch_frequency, curated_database, curated_table_name, 
                             gl_combiner, gl_header, gl_line_item, glerp_header_only_domain, glerp_soft_error_max_age_days)
        if domain not in ['currentbatch','completedbatch']:
            log.info("No more Batch Left to be Loaded")


# Used by Vantage Sources and ALI to generate DocumentID
def generateSomeUUID(x):
    class NULL_NAMESPACE:
        bytes = b''
    if x is not None and len(x):
        return str(uuid.uuid3(NULL_NAMESPACE, x))
    else:
        return x


# Used by BaNCS to generate DocumentID/CDE hashvalue
def generateSomeUUID_uuid5(x):
    if x is not None and len(x):
        return str(uuid.uuid5(uuid.NAMESPACE_X500, x))
    else:
        return x


# Used by GL for Accounting ID
'''
def farm_hash(x):
    #if x:
    #    return int(str(ctypes.c_long(farmhash.fingerprint64(str(x)))).strip('c_long()'))
    #else:
    #    return x
    try:
        if x:
            return int(str(ctypes.c_long(farmhash.fingerprint64(str(x)))).strip('c_long()'))
        else: 
            return x
    except Exception as e:
        return int(str(ctypes.c_long(farmhash.fingerprint64(str(x.encode())))).strip('c_long()'))
'''


# Used to Check Date Format
def python_date_format_checker(string_dt, dt_format, ignore_zeroes_null = False):
    if ignore_zeroes_null:
        int_dt = None
        try:
            int_dt = int(string_dt)
        except Exception as ex:
            pass
        if int_dt is not None:
            string_dt = str(int_dt)
            if string_dt == '0':
                string_dt = None
        if not string_dt:
            return True
    elif not string_dt:
        return False
    if string_dt and dt_format:
        try:
            date_val = datetime.strptime(string_dt, dt_format)
            return True
        except Exception as e:
            return False
    else:
        raise Exception ("python_date_format_checker Requires both String Date and Date Format Parameters")


# Used to Remove Junk Characters in Field
def ascii_ignore(x):
    if x is not None and len(x):
        return x.encode('ascii','ignore').decode('ascii')
    else:
        return x


# Define Custom AES encryption function
def aes_encrypt(decrypted_msg):
    if not decrypted_msg:
        return None
    key = '9yqJqVGhsCjZTS38'.encode('utf-8')
    aes = pyaes.AESModeOfOperationCTR(key)
    cipher_txt = aes.encrypt(decrypted_msg)
    cipher_txt2 = binascii.hexlify(cipher_txt)
    return str(cipher_txt2.decode('utf-8'))


# Define Custom AES decryption function
def aes_decrypt(encrypted_msg):
    if not encrypted_msg:
        return None
    key = '9YqJqV6hSCjZTS38'.encode('utf-8')
    aes = pyaes.AESModeOfOperationCTR(key)
    encrypted_msg = binascii.unhexlify(encrypted_msg)
    encrypted_msg = aes.decrypt(encrypted_msg)
    return str(encrypted_msg.decode('utf-8'))


if __name__ == "__main__":
    try:
        log.info("Start: " + datetime.now(est_tz).strftime('%Y-%m-%d %H:%M:%S'))

        # Parse CommandLine Arguments
        parser = argparse.ArgumentParser()
        parser.add_argument('--source_system_name', '-s', required = True, type=str, dest = 'source_system_name')
        parser.add_argument('--domain', '-d', required = True, type=str, dest = 'domain')
        parser.add_argument('--batch_frequency', '-f', required = True, type=str, dest = 'batch_frequency', choices = ['DAILY','MONTHLY','WEEKLY','QUARTERLY'])
        parser.add_argument('--source_flag', '-sf', required = True, type=str, dest = 'source_flag', default = 'G', choices = ['I','G','D','C','A'])
        parser.add_argument('--controlm_job', '-j', required = False, type=str, dest = 'controlm_job', default = 'NOT_DEFINED')
        parser.add_argument('--vantage_initial_load', '-i', required = False, type=str, dest = 'vantage_initial_load', default = 'N')
        parser.add_argument('--read_from_s3', '-rs', required = False, type=str, dest = 'read_from_s3', default = 'N')
        parser.add_argument('--rerun_flag', '-r', required = False, type=str, dest = 'rerun_flag', default = 'N', choices = ['Y','N'])

        variableList = dir()
        home = path
        args = parser.parse_args()
        source_system_name = args.source_system_name
        domain = args.domain
        controlm_job = args.controlm_job
        batch_frequency = args.batch_frequency
        source_flag = args.source_flag
        vantage_initial_load = args.vantage_initial_load
        read_from_s3 = args.read_from_s3
        rerun_flag = args.rerun_flag

        variables = common_vars(source_system_name, domain, batch_frequency)
        curated_database = variables['curated']
        datalake_ref_db = variables['datalake_ref_db']
        datalake_ref_edmcs_db = variables['datalake_ref_edmcs_db']
        glerp_outbound_sources = variables['glerp_outbound_sources']
        glerp_soft_error_max_age_days = variables['glerp_soft_error_max_age_days']
        glerp_header_only_domain = variables['glerp_header_only_domain']
        idl_source_system_name = variables['idl_source_system_name']
        source_initial_cut_off = variables['source_initial_cut_off']
        curated_initial_cut_off = variables['curated_initial_cut_off']
        refresh_mview = variables['refresh_mview']
        file_path_filler = source_system_name
        domain_name = domain
        segmentname = domain

        if read_from_s3 == 'Y':
            read_from_s3 = True
        else:
            read_from_s3 = False

        if 'currentbatch' in domain or 'completedbatch_' in domain:
            segmentname = domain.split('_')[0]
            domain_name = '_'.join(domain.split('_')[1:])
            appname = f'{source_system_name} {domain_name} {segmentname}'
            if 'currentbatch' in domain:
                sourcename = {'I':'idl','G':'gdq','D':'datastage','C':'curated','A':'arr'}.get(source_flag)
                querypath = f"{home}/Query/{segmentname}/{sourcename}/"
                s3_querypath = f"{project}/scripts/curated/Query/{segmentname}/{sourcename}/"
            else:
                querypath = f"{home}/Query/{segmentname}/"
                s3_querypath = f"{project}/scripts/curated/Query/{segmentname}/"

        query = f"""select load_pattern, target_schema from financedsworchestration.hash_definition 
                    where domain_name = '{get_domain_overrrd(domain_name, source_system_name)}'"""
        result = load_table(source='findw', query, disable_retry = True)
        if len(result) == 0:
            raise Exception(f"Hash Definition has no entry for '{domain_name}'")
        elif len(result) > 1:
            raise Exception(f"Hash Definition has multiple entries for '{domain_name}'")
        load_pattern = result[0][0]
        redshift_target_schema = result[0][1]

        if redshift_target_schema == 'erpdw':
            file_path_filler = f"erpdw/{source_system_name}"
            if source_system_name in ['ltcg','ltcghybrid','Bestow'] or 'Vantage' in source_system_name:
                file_path_filler = 'erpdw/findw'

        adminsystem_mapper = {'VantageP65SPL':'spl','VantageP5':'p5','VantageP6':'p6','VantageP65':'p65','VantageP75':'p75'}
        admin_system_short = adminsystem_mapper.get(source_system_name, source_system_name)

        common_querypath = f"{home}/Query/common/"
        oc_controls_querypath = f"{home}/Query/{file_path_filler}/{domain}/Controls/"
        kc_controls_querypath = f"{oc_controls_querypath}/KC2/"
        s3_querypath_common = f"{project}/scripts/curated/Query/common/"
        s3_querypath_oc = f"{project}/scripts/curated/Query/{file_path_filler}/{domain}/Controls/"
        s3_querypath_kc2 = f"{project}/scripts/curated/Query/{file_path_filler}/{domain}/Controls/KC2/"

        if segmentname not in ['currentbatch','completedbatch']:
            domain_name = domain
            segmentname = domain
            appname = f'{source_system_name} {domain_name} {segmentname}'
            querypath = f"{home}/Query/{file_path_filler}/{domain}/"
            s3_querypath = f"{project}/scripts/curated/Query/{file_path_filler}/{domain}/"

        if source_flag == 'I':
            source_database = variables['datalake_curated']
        elif source_flag == 'G':
            source_database = variables['gdq']
        elif source_flag == 'D':
            source_database = variables['datastage']
        elif source_flag == 'C' and domain_name in use_curated_source_for_derived_domain_list:
            source_database = curated_database
        elif source_flag == 'A':
            source_database = variables['arr_curated']
        else:
            source_database = None

        log.info(f"Project is: {project}")
        if source_flag != 'C' or (source_flag == 'C' and domain_name in use_curated_source_for_derived_domain_list):
            log.info(f"Source Database (Glue) is: {source_database}")
        else:
            log.info(f"Source Database (Redshift) is: {findw_db}")
        log.info(f"Target Database is: {curated_database}")
        log.info(f"IDL Reference Databases if applicable for the Source System: {datalake_ref_db}, {datalake_ref_edmcs_db}")
        if 'currentbatch' in domain:
            if idl_source_system_name != source_system_name:
                log.info(f"IDL Source System Name (For RDS BatchTracking) is: {idl_source_system_name}")
            if source_initial_cut_off:
                if source_flag == 'I':
                    log.info(f"IDL Initial Cut Off Date (For RDS BatchTracking) is: {source_initial_cut_off}")
                    source_initial_cut_off = f"AND cycle_date >= '{source_initial_cut_off}'"
                elif source_flag == 'G':
                    log.info(f"GDQ Initial Cut Off Date (For GDQ CompletedBatch) is: {source_initial_cut_off}")
                    source_initial_cut_off = f"AND cast(a.rtaa_effective_date AS date) >= '{source_initial_cut_off}'"
            current_batch_source_ovrd = current_batch_source_ovrd_dict.get(source_system_name, {}).get(domain_name, [])
            if current_batch_source_ovrd:
                log.info(f"Current Batch Source Override is: {current_batch_source_ovrd}")
                current_batch_source_ovrd = "'" + "','".join(current_batch_source_ovrd) + "'"
        if segmentname not in ['completedbatch'] and curated_initial_cut_off:
            log.info(f"Curated Initial Cut Off Date (For Derived Domain BatchTracking) is: {curated_initial_cut_off}")
            curated_initial_cut_off = f"AND a.cycle_date >= '{curated_initial_cut_off}'"
        log.info(f"S3 SQL Code Location is: s3://{s3_code_bucket}/{s3_querypath}")

        if (source_flag == "G" and source_system_name not in ['VantageP5','VantageP6','VantageP65','VantageP75','VantageP65SPL','ALM']) \
            or (source_flag == "D" and source_system_name not in ['VantageP5','VantageP6','VantageP65','VantageP75','VantageP65SPL','ALM']):
            raise Exception(f"Invalid Source Flag - {source_flag} and Source System Name - {source_system_name} Combination")

        variableList = list(set(dir()) - set(variableList) - set(['variableList']))
        config = dict((k, eval(k)) for k in variableList)
        config['environment'] = environment

        # Initialize Spark
        appname = f"Curated Load {appname}"
        spark_builder = SparkSession.builder.appName(appname)
        if source_flag == 'A':
            spark_builder = spark_builder \
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
                .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
                .config("spark.sql.catalog.iceberg.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
                .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        spark = spark_builder \
            .config("hive.exec.dynamic.partition", "true") \
            .config("hive.exec.dynamic.partition.mode", "nonstrict") \
            .enableHiveSupport() \
            .getOrCreate()

        checkpoint_dir = f"s3://{s3_extract_bucket}/miscellaneous/{project}/curated/checkpoint/"
        spark.sparkContext.setCheckpointDir(checkpoint_dir)
        checkpoint_dir = spark.sparkContext.getCheckpointDir()
        log.info(f"Spark Checkpoint directory: {checkpoint_dir}")

        sc = spark.sparkContext
        sc.setLogLevel("ERROR")
        appid = sc.applicationId

        spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        spark.conf.set("spark.sql.tungsten.enabled", "true")
        spark.conf.set("spark.sql.broadcastTimeout", "3600")
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "104857600")
        spark.udf.register("generateSameUUID", generateSameUUID, StringType())
        spark.udf.register("generateSameUUID_uuid5", generateSameUUID_uuid5, StringType())
        spark.udf.register("ascii_ignore", ascii_ignore, StringType())
        # spark.udf.register("farm_hash", farm_hash, StringType())
        spark.udf.register("python_date_format_checker", python_date_format_checker, StringType())
        spark.udf.register("aes_encrypt_udf", aes_encrypt, StringType())
        spark.udf.register("aes_decrypt_udf", aes_decrypt, StringType())
        aes_encrypt_udf = F.udf(aes_encrypt, StringType())
        aes_decrypt_udf = F.udf(aes_decrypt, StringType())

        if segmentname not in ['currentbatch', 'completedbatch']:
            if domain == 'claimbenefit' or (gl_get_source_system_ovrd(source_system_name) == 'findw' and domain in gl_error_handling_domains):
                spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

        batchid = None
        cycle_date = None
        phase_name = 'curated'
        loop_counter = 1
        drvd_domain = domain_name
        ovrrd_phase_name = 'curated'
        do_oc_controls_flag = 'Y'
        do_delete_logic_flag = 'Y'
        do_kc2_controls_flag = 'N'
        do_monthend_logic_flag = 'N'
        do_weekend_logic_flag = 'N'
        do_cdc_flag = 'N'
        do_intraday_multibatch_run_flag = 'N'
        gl_header = {}
        gl_line_item = {}
        gl_combiner = {}
        curated_table_name = None
        count_errors = 0
        cdc_delta_cnt = 0

        log.info(f"Spark Application Name: {appname}")
        log.info(f"Spark Application Id: {appid}")
        #sched = apscheduler.schedulers.background.BackgroundScheduler({'apscheduler.job_defaults.max_instances': 3, 'apscheduler.timezone': 'America/New_york'})
        #kwargs_ = {'appname': appname, 'appid': appid, 'sched': sched, 'log': log}
        #sched.add_job(kill_duplicate_apps,'interval', seconds=30, kwargs=kwargs_)
        #sched.start()
        kwargs_ = {'appname': appname, 'appid': appid, 'sched': sched, 'log': log}
        kill_duplicate_apps(**kwargs_)

        if segmentname not in ['completedbatch']:
            if segmentname not in ['currentbatch']:
                ovrrd_phase_name = 'curated controls'
                qry =f"""select do_kc2_controls_flag, do_operational_controls_flag, do_delete_logic_flag, do_monthend_logic_flag,
                    do_weekend_logic_flag, do_intraday_multibatch_run_flag, do_cdc_flag
                    from financedworchestration.domain_tracker
                    where source_system_name = '{source_system_name}' and domain_name = '{domain_name}'
                    and batch_frequency = '{batch_frequency}'"""
                result = get_jdbc_df(spark, source= 'findw', qry).cache()
                if not result.count():
                    raise Exception(f"Domain {domain_name}, Source System {source_system_name} and Batch Frequency {batch_frequency} is not defined in financedworchestration.domain_tracker table.")
                elif result.count() == 1:
                    if segmentname not in ['currentbatch']:
                        do_kc2_controls_flag = result.collect()[0][0]
                        do_oc_controls_flag = result.collect()[0][1]
                        do_delete_logic_flag = result.collect()[0][2]
                        do_monthend_logic_flag = result.collect()[0][3]
                        do_weekend_logic_flag = result.collect()[0][4]
                        do_cdc_flag = result.collect()[0][6]
                    else:
                        do_delete_logic_flag = 'N'
                        do_oc_controls_flag = 'N'
                    do_intraday_multibatch_run_flag = result.collect()[0][5]
                    if do_oc_controls_flag == 'N':
                        ovrrd_phase_name = 'curated'
                else:
                    raise Exception(f"Domain {domain_name}, Source System {source_system_name} and Batch Frequency {batch_frequency} have duplicates in financedworchestration.domain_tracker")
                result.unpersist(blocking=True)
                if segmentname not in ['currentbatch']:
                    if do_oc_controls_flag == 'Y':
                        log.info(f"S3 SQL Code Location For OC is: s3://{s3_code_bucket}/{s3_querypath_oc}")
                    if do_kc2_controls_flag == 'Y':
                        log.info(f"S3 SQL Code Location For KC2 is: s3://{s3_code_bucket}/{s3_querypath_kc2}")
            else:
            do_delete_logic_flag = 'N'
            do_oc_controls_flag = 'N'

        print ("------------------------------------------------------------")
        log.info(f"""This Domain is part of {redshift_target_schema} and its Load Pattern is {load_pattern}""")
        log.info(f"""Do Delete Logic Flag: {do_delete_logic_flag}""")
        log.info(f"""Do OC Controls Flag: {do_oc_controls_flag}""")
        log.info(f"""Do KC2 Controls Flag: {do_kc2_controls_flag}""")
        log.info(f"""Do Monthend Special Handling for {source_system_name}: {do_monthend_logic_flag}""")
        log.info(f"""Do Weekend Special Handling for {source_system_name}: {do_weekend_logic_flag}""")
        log.info(f"""Do Intraday Multibatch Run Flag: {do_intraday_multibatch_run_flag}""")
        log.info(f"""Do CDC Flag: {do_cdc_flag}""")

        if source_flag == 'G' and segmentname not in ['currentbatch', 'completedbatch']:
            if source_system_name == 'VantageP65SPL':
                system_name = "P65S VAR"
                log.info(f"GDQ Mainfile Key system_name: {system_name}")
                config['system_name'] = system_name
            elif source_system_name == 'VantageP65':
                system_name = "P65 VAR"
                log.info(f"GDQ Mainfile Key system_name: {system_name}")
                config['system_name'] = system_name
            else:
                log.info(f"GDQ Mainfile Key system_name: {admin_system_short}")
                config['system_name'] = admin_system_short.upper()

        redshift_query = f"""select count(distinct cb.batch_id) cb_cnt, count(distinct bt.batch_id) bt_cnt
            from
                financedwcurated.currentbatch cb
                left join (SELECT * FROM
                    (SELECT source_system_name, domain_name, batch_id, cycle_date, load_status
                        , row_number() over (partition by source_system_name, batch_id order by insert_timestamp desc) as fltr
                    FROM
                        financedworchestration.batch_tracking bt
                    WHERE
                        domain_name = '{domain_name}'
                        AND phase_name = '{ovrrd_phase_name}'
                        AND source_system_name = '{source_system_name}')
                WHERE fltr = 1 AND load_status = 'complete') bt on cb.batch_id = bt.batch_id and cb.cycle_date = bt.cycle_date
            and cb.source_system = bt.source_system_name and cb.domain_name = bt.domain_name
            where cb.source_system = '{source_system_name}' and cb.batch_frequency = '{batch_frequency}' and cb.domain_name = '{domain_name}'"""

        if 'completedbatch' in segmentname:
            drvd_domain = segmentname + '_' + domain_name
        elif 'currentbatch' not in segmentname:
            if do_intraday_multibatch_run_flag == 'N':
                validate_currentbatch(spark, curated_database, source_system_name, domain_name, batch_frequency, segmentname)
        qry=f"""select count(distinct batch_id) batch_id from {curated_database}.currentbatch
                where source_system = '{source_system_name}' and domain_name = '{domain_name}' and batch_frequency = '{batch_frequency}'"""
        result = spark.sql(qry)
        loop_counter = result.collect()[0][0]
        if not loop_counter:
            loop_counter = 0
        else:
            result = get_jdbc_df(spark, source='findw', redshift_query).cache()
            cb_cnt = result.collect()[0][0]
            bt_cnt = result.collect()[0][1]
            result.unpersist(blocking=True)
            if cb_cnt > bt_cnt and bt_cnt != 0:
                log.info(f"Number of Batches Already Processed In MultiBatch Load - {bt_cnt}")
                loop_counter = loop_counter - bt_cnt

        log.info(f"Number of Batches to be Processed: {loop_counter}")

        filenames = []
        curated_table_name = f"{source_system_name}_{domain}"
        config['curated_table_name'] = curated_table_name
        filenames = validate_code_files(querypath, s3_querypath, read_from_s3)

        if source_flag == 'C' and segmentname == 'currentbatch':
            derived_domain_prerequisites(spark, source_system_name, segmentname, domain_name, batch_frequency, curated_database, config, rerun_flag, read_from_s3, querypath, s3_querypath)

        if refresh_mview and segmentname not in ['completedbatch']:
            refresh_mview = False

        if segmentname not in ['currentbatch', 'completedbatch'] and config.get('redshift_target_schema') == 'erpdw':
            if domain in gl_error_handling_domains:
                log.info(f"Maximum Days After which Soft Errors will be Moved as Hard Errors: {glerp_soft_error_max_age_days}")
                if source_system_name in glerp_outbound_sources:
                    log.info(f"{source_system_name} is an Outbound Source for GL/ERP")
                else:
                    log.info(f"{source_system_name} is an Inbound Source for GL/ERP")
            if domain in gl_activityaccounting_needed_domains:
                #Will Need a Change for this Line
                skip_List = ['recorded_timestamp', 'cycle_date', 'batch_id', 'original_batch_id', 'original_cycle_date', 'activity_accounting_id']
                gl_line_item = {'domain' : domain}
                df_ = spark.sql(f"select * from {curated_database}.{curated_table_name} limit 0")
                cols_list = [ 'nvl(df.' + i + ', '')' for i in df_.columns if i not in skip_List ]
                cols_list = ','.join(cols_list)
                natural_key_query = f"select natural_key_list from financedworchestration.hash_definition hd where domain_name = '{domain}'"
                naturalKey = get_jdbc_df(spark, source='findw', natural_key_query).collect()[0][0]
                naturalKey_List = naturalKey.replace(' ', '').split(',')
                cols_list_order = [ 'df.' + i for i in df_.columns if i not in skip_List + naturalKey_List ]
                cols_list_order = ','.join(cols_list_order)
                gl_activityaccountingId = f"""
                    row_number() over (partition by {naturalKey} order by {cols_list_order}) part_fltr,
                    date_format(from_utc_timestamp(CURRENT_TIMESTAMP, 'US/Central'), 'yyyyMMdd') ||
                    case when '{source_system_name}' in ({glerp_outbound_sources}) then '1' else '0' end ||
                    case when xxhash64(concat_ws('|', part_fltr, {cols_list})) < 0
                        then '0' || lpad(cast(abs(xxhash64(concat_ws('|', part_filtr, {cols_list}))) as decimal(20)), 20, '0')
                        else '1' || lpad(cast(abs(xxhash64(concat_ws('|', part_filtr, {cols_list}))) as decimal(20)), 20, '0')
                    end as activity_accounting_id"""
                gl_activityaccountingId = gl_activityaccountingId.replace('df.', '')
                config['gl_activityaccountingId'] = gl_activityaccountingId
            else:
                gl_header = {'domain' : domain}
            gl_combiner = gl_header.copy()
            gl_combiner.update(gl_line_item)
            refresh_mview = True
        else:
            glerp_soft_error_max_age_days = None

        main_curated(spark, source_system_name, domain, domain_name, batch_frequency, loop_counter, segmentname, source_database, curated_database,
                    curated_table_name, admin_system_short, source_flag, ovrrd_phase_name, vantage_initial_load, controlm_job, read_from_s3, filenames,
                    s3_querypath_common, common_querypath, querypath, oc_controls_querypath, kc_controls_querypath, s3_querypath_oc, s3_querypath_kc2,
                    gl_header, gl_line_item, gl_combiner, glerp_header_only_domain, glerp_soft_error_max_age_days, refresh_mview,
                    do_delete_logic_flag, do_intraday_multibatch_run_flag, rerun_flag, do_oc_controls_flag, do_kc2_controls_flag, do_cdc_flag, redshift_query, config)

    except Exception as ex:
        traceback.print_exc()
        if 'currentbatch' not in domain:
            if cycle_date and batchId:
                batchtracking_loggging(source_system_name, drvd_domain, batchId, source_flag="failed", cycle_date, insert_update_flg="update", phase_name, batch_frequency, controlm_job, skip_batch_tracker="None", ex)
            if refresh_mview:
                view_name = None
                if gl_combiner:
                    view_name = domain + "_error"
                elif segmentname in ['completedbatch']:
                    view_name = domain
                if view_name:
                    refresh_materialized_view(view_name)
                else:
                    log.error("No Materialized VIEW ???")
        log.exception("Failing the script due to following Exception ", ex)
        log.info("End: " + datetime.now(est_tz).strftime("%Y-%m-%d %H:%M:%S"))
        sys.exit(255)

    finally:
        #sched.remove_all_jobs()
        #sched.shutdown(wait=False)
        try:
            delete_s3_objects(checkpoint_dir)
            pass
        except Exception as ex:
            pass
        log.info("Script completed")
        log.info("End: " + datetime.now(est_tz).strftime("%Y-%m-%d %H:%M:%S"))