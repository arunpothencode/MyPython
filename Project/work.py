from pyspark.sql.functions import collect_set,explode,lit,split
from pyspark.sql.types import *
import itertools
import sys
from datetime import datetime
import pandas as pd

def get_jdbc_detail(sqllinkedservice,dbname):
    jdbc_str = mssparkutils.credentials.getFullConnectionString(sqllinkedservice) #'AzureSqlConnection'
    jdbc_dict = {i.split('=',1)[0]:i.split('=',1)[1] for i in jdbc_str.split(';')}
    jdbc_dict['url'] = jdbc_dict['properties'].replace('tcp:','jdbc:sqlserver://').replace(',',':')+f";database={dbname}" #opleusdevsqldwh01
    return jdbc_dict

def get_storage_detail(linkedservice):
    sas_str = mssparkutils.credentials.getFullConnectionString(linkedservice) 
    sas_dict = {i.split('=',1)[0]:i.split('=',1)[1] for i in sas_str.split(';')}
    if 'adls' in sas_dict['url']:
        sas_dict['adls_url'] = sas_dict['url'].replace('https://','abfss://opdwworkspace-synapse@')
        spark.conf.set(sas_dict['adls_url'],sas_dict['token'])
    else:
        sas_dict['blob_url'] = sas_dict['url'].replace('https://', 'fs.azure.sas.landingzone.').replace('/','')
        spark.conf.set(sas_dict['blob_url'],sas_dict['token'])
    return sas_dict

def set_adls_token(linkedservice_adls=linkedservice_adls):
    adls_sas_token = get_storage_detail(linkedservice_adls)
    if adls_sas_token['token'] is not None:
        return 'SUCCESS'
    else:
        return 'FAIL'


def set_blob_token(linkedservice_blob=linkedservice_blob):
    blob_sas_token = get_storage_detail(linkedservice_blob)
    if blob_sas_token['token'] is not None:
        return 'SUCCESS'
    else:
        return 'FAIL'

def get_source_file(fl_path,fl_pattern,memname,FileName=FileName):
    """Function get_source_file(fl_path,fl_pattern) takes the file path and return a tuple of (path,name,stream_name)"""
    set_adls_token()
    set_blob_token()
    src_files = []
    for i in mssparkutils.fs.ls(f"{fl_path}"):
        if i.size > 1 and fl_pattern in i.name:
            if memname != 'NA' and (memname in i.name) and i.name == FileName:
                src_files.append((i.path,i.name,fl_pattern))
    return src_files if src_files else ValueError

# def get_source_file_details_gbo(fl_path):
#     """Function get_source_file_details_gbo(fl_path) takes the file path and return a tuple of (path,name,stream_name)"""
#     gbo_files = []
#     for i in mssparkutils.fs.ls(f"{fl_path}"):
#         if i.size > 1 and i.name.startswith('GSM_SAN'):
#             if i.name.startswith('GSM_SAN_Detail'):
#                 gbo_files.append((i.path,i.name,'Detail'))
#             else:
#                 gbo_files.append((i.path,i.name,'ISR'))
#     return gbo_files

def read_any_csv(src_dir,fil_name,fil_delim,spark_ses):
    """Function read_any_csv(src_dir,fil_name,fil_delim,spark_ses) returns a dataframe"""
    if 'landingzone' in src_dir:
        dftest = spark_ses.read.format('csv').option('sep',f'{fil_delim}').option("quote",'"').option("escape",'"').option('header','true').load(f"{src_dir}/{fil_name}")
        if 'GSM_SAN_Detail' in fil_name:
            col_list = [i for i in dftest.columns if i.startswith('_c')]
            dftest = dftest.drop(*col_list) 
            col_list = dftest.columns
            select_query = ' ,'.join([f'split(san,"{fil_delim}{fil_delim}")'+'['+str(i[0])+']'+" "+ i[1] for i in enumerate(list(col_list))])
            dftest.registerTempTable('dftest')
            dftest = spark.sql(f'select {select_query}  from dftest')
            return dftest
        elif 'GSM_SAN_ISR' in fil_name:
            col_list = [i for i in dftest.columns if i.startswith('_c')]
            dftest = dftest.drop(*col_list) 
            return dftest
        else:
            return dftest

    else:
        return spark_ses.read.format('csv').option('sep',f'{fil_delim}').option("quote",'"').option("escape",'"').option('header','true').load(f"{src_dir}/{fil_name}")

def read_db(sq,spark_sess,linkedservice_sql=linkedservice_sql,dbname=dbname):
    """Function read_db(sq,spark_sess) take sql query and returns a dataframe"""
    
    jdbc_dict = get_jdbc_detail(linkedservice_sql,dbname)
    jdbc_read_df = spark_sess.read.format("jdbc").option("dbtable",f"({sq}) as ta").option("url",jdbc_dict['url']).option("user",'BIUSER').option("password",jdbc_dict['token']).load()
    return jdbc_read_df

def write_db(table_name,write_df,spark_sess,write_mode='append',linkedservice_sql=linkedservice_sql,dbname=dbname):
    """Function write_db(table_name,write_df,spark_sess) takes table name and data frame write to table"""
    # jdbcserver ='opleusdevsps01.database.windows.net'
    # jdbcport = '1433'
    # jbbcdb =  'opleusdevsqldwh01'
    # jdbcurl = 'jdbc:sqlserver://{}:{};database={}'.format(jdbcserver,jdbcport,jbbcdb)
    jdbc_dict = get_jdbc_detail(linkedservice_sql,dbname)
    write_df.write.format("com.microsoft.sqlserver.jdbc.spark").mode(f"{write_mode}").option("url",jdbc_dict['url']).option("dbtable",f"{table_name}").option("user",'BIUSER').option("password",jdbc_dict['token']).option("mssqlIsolationLevel","READ_UNCOMMITTED").save()
    return "SUCCESS"

def read_rule_table(sq,sess):
    df = read_db(f'{sq}',spark)
    df_col = df.columns
    df_data = df.collect()
    return [i.ruletype for i in df_data]

def list_metadata(sq,sess):
    """Function read_metadata(crm_name,memberfirmname) reads from the table and returns metadata dict"""
    
    #sq= f"select MemberFirmName,LandingFolder,	LandingArchiveFolder,	RawZoneValid,	RawZoneArchiveValid,	RawZoneInvalid,	RawZoneArchiveInvalid,	StagingZoneValid,	StagingZoneArchiveValid,	StagingZoneInvalid,	StagingZoneArchiveInvalid,	StagingZoneTransformation,	StagingZoneArchiveTransformation,	StagingZoneProcessed,	FileNamePattern,	DelimiterName,MappingField,FileColumn FileColumn,b.CRMSourceName CRMSourceName, d.ValidationType fValidationType,d.executionlevel fexecutionlevel,c.TransformationType,c.executionlevel texecutionlevel,e.ExecutionLevel dexecutionlevel,e.ValidationType dValidationType from Preprocess.tblFileMetaData a inner join Preprocess.tblMasterSourceData b on a.DataSourceId=b.DataSourceId inner join [Preprocess].[tblTransformationTypeFields] c on b.DataSourceId  = c.DataSourceId inner join [Preprocess].[tblFileValidationType] d on c.DataSourceId  = d.DataSourceId inner join [Preprocess].[tblDataValidationType] e on d.DataSourceId  = e.DataSourceId where b.CRMSourceName='{crm_name}' and memberfirmname = '{memberfirmname}' and mappingfield is not null"
    my_df = read_db(sq,spark)
    my_col = my_df.columns
    li_sq = 'select collect_set(' + ',collect_set('.join([i+') '+i for i in my_col]) + ' from reg_my_df'
    reg_my_df = my_df.registerTempTable("reg_my_df")
    #reg_my_df_set = spark.sql("select collect_set(MemberFirmName) MemberFirmName,collect_set(LandingFolder) LandingFolder,collect_set(LandingArchiveFolder) LandingArchiveFolder,collect_set(RawZoneValid) RawZoneValid,collect_set(RawZoneArchiveValid) RawZoneArchiveValid,collect_set(RawZoneInvalid) RawZoneInvalid,collect_set(RawZoneArchiveInvalid) RawZoneArchiveInvalid,collect_set(StagingZoneValid) StagingZoneValid,collect_set(StagingZoneArchiveValid) StagingZoneArchiveValid,collect_set(StagingZoneInvalid) StagingZoneInvalid,collect_set(StagingZoneArchiveInvalid) StagingZoneArchiveInvalid,collect_set(StagingZoneTransformation) StagingZoneTransformation,collect_set(StagingZoneArchiveTransformation) StagingZoneArchiveTransformation,collect_set(StagingZoneProcessed) StagingZoneProcessed,collect_set(FileNamePattern) FileNamePattern,collect_set(DelimiterName) DelimiterName,collect_set(MappingField) MappingField,collect_set(FileColumn) FileColumn,collect_set(CRMSourceName) CRMSourceName,collect_set(fValidationType) fValidationType,collect_set(fexecutionlevel) fexecutionlevel,collect_set(TransformationType) TransformationType,collect_set(texecutionlevel) texecutionlevel,collect_set(dexecutionlevel) dexecutionlevel,collect_set(dValidationType) dValidationType from reg_my_df" )
    reg_my_df_set = spark.sql(f'{li_sq}')                                                                        
    my_col_list = reg_my_df_set.columns
    my_col_data = reg_my_df_set.collect()

    return {i:j for i,j in zip(my_col_list,*my_col_data)}

# def country_check(crm_src_name,mem_firm_name,fl_name):
#     """Function country_check(crm_src_name,mem_firm_name,fl_name) is used to check memberfirmname present in file"""
#     meta_dict = read_metadata(crm_src_name,mem_firm_name)
#     if f"_{meta_dict['MemberFirmName']}_" in fl_name:
#         return 'PASS'
#     else:
#         return 'FAIL'

def check_size(test_df,spark_sess):
    """Function check_size(test_df,spark_sess) is used to check the size of the file"""
    return 'PASS' if test_df.count() > 1 else 'FAIL'

def filecol_match_mappingcol(flist,mlist):
    """Function filecol_match_mappingcol(flist,mlist) creates a select column list based on the mapping available"""
    mlist_inter_flist = set(mlist).intersection(set(flist))
    [flist.remove(i) for i in mlist_inter_flist]
    [mlist.remove(i) for i in mlist_inter_flist]
    my_diff = list(itertools.zip_longest(flist,mlist,fillvalue='ZZ'))
    my_select_data = ','.join((*['`'+i+'`' for i in mlist_inter_flist], *['`'+i[0]+'`'+' '+'`'+i[1]+'`' for i in my_diff]))
    return my_select_data.replace('`ZZ`',"' '")

def add_audit_col(dfs,stream_code,spark_ses,bid,eid,cname=cname):
    """Function add_audit_col(dfs,stream_code,spark_ses,bid,eid) adds audit columns to dataframe"""
    if stream_code == 'GSM_SAN_Detail':
        cntry_code = 'Primary_Country_of_Work_ISO_Country_Code'
    elif stream_code == 'GSM_SAN_ISR':
        cntry_code = 'Reporting_Country_ISO_Country_Code'
    else:
        cntry_code = f'"{stream_code}"'
        
    cur_date = f"'{datetime.utcnow()}'"
    df = dfs.registerTempTable('df')
    BatchID,ExecutionID = bid,eid
    #'BatchID','ExecutionID','MemberFirmCRM','MemberFirmName','TimeStamp','RowNumber'
    if stream_code in ['GSM_SAN_Detail','GSM_SAN_ISR']:
        df1 = spark_ses.sql(f"select *, {str(BatchID)} BatchID,{str(ExecutionID)} ExecutionID,'{stream_code.replace('GSM','GBO')}' CRMSourceName,{cntry_code} MemberFirmName,{cur_date} TimeStamp, ROW_NUMBER() over (order by san) RowNumber from df")
    elif cname == 'SAP_IBS':
        df1 = spark_ses.sql(f"select *, {str(BatchID)} BatchID,{str(ExecutionID)} ExecutionID,'{cname}' CRMSourceName,{cntry_code} MemberFirmName,{cur_date} TimeStamp, ROW_NUMBER() over (order by `Opportunity ID`) RowNumber from df")
    elif cname == 'NON_IBS':
        df1 = spark_ses.sql(f"select *, {str(BatchID)} BatchID,{str(ExecutionID)} ExecutionID,'{cname}' CRMSourceName,{cntry_code} MemberFirmName,{cur_date} TimeStamp, ROW_NUMBER() over (order by `Opportunity ID (OpportunityLocalID)`) RowNumber from df")
    return df1

def write_to_target(wdf,tgt_fold,sub_fold,tgt_fl_name,fl_delim,fl_pattern,spark_sess):
    """Function write_to_target(wdf,tgt_fold,sub_fold,tgt_fl_name,fl_delim,fl_pattern,spark_sess) will write to target path"""
    #print('{}/{}/{}'.format(tgt_fold,sub_fold,fl_pattern))
    #print('{}/{}/{}'.format(tgt_fold,sub_fold,tgt_fl_name))
    wdf.coalesce(1).write.format('csv').option('overwrite','true').option('header','true').save('{}/{}/{}'.format(tgt_fold,sub_fold,fl_pattern),delimiter = fl_delim)
    
    for fname in mssparkutils.fs.ls('{}/{}/{}'.format(tgt_fold,sub_fold,fl_pattern)):
        if fname.name.startswith('part-'):
            mssparkutils.fs.mv('{}/{}/{}/{}'.format(tgt_fold,sub_fold,fl_pattern,fname.name),'{}/{}/{}'.format(tgt_fold,sub_fold,str(tgt_fl_name)),create_path=True,overwrite=True)
    mssparkutils.fs.rm('{}/{}/{}'.format(tgt_fold,sub_fold,fl_pattern),recurse=True)
    return "SUCCESS"

def write_log_file(folder_name,file_name,bid,linkedservice_adls = linkedservice_adls):
    #starttm = timeit.default_timer()
    adls_dict = get_storage_detail(linkedservice_adls)
    tgt_path = f"{adls_dict['adls_url']}/{folder_name}/temp"
    mydf = read_db(f'select * from Log.tblDataValidationInterimDetails where batchid={bid}',spark)
    if set_adls_token() == 'SUCCESS':
        mydf.coalesce(1).write.format('csv').option('overwrite','true').option('header','true').save(tgt_path)

        for fname in mssparkutils.fs.ls(tgt_path):
            if fname.name.startswith('part-'):
                mssparkutils.fs.mv(str(fname.path),str(fname.path).replace(f'temp/{str(fname.name)}',file_name),overwrite=True)
            
            
        mssparkutils.fs.rm(tgt_path,recurse=True)
        #endtm = timeit.default_timer()
        #print(f'write_log_file took {endtm - starttm} secs')
        return 'SUCCESS'

def create_log_df(log_dict):
    """Function create_log_df(log_dict) will take input a dict and return a dataframe"""
    dfl_schema = StructType([StructField('BatchID',IntegerType(),True),StructField('ExecutionID',IntegerType(),True),StructField('CRMSourceName',StringType(), True),StructField('ExecutionLevel',StringType(),True),StructField('ValidationType',StringType(),True),StructField('ValidationStatus',StringType(),True),StructField('FieldDescription',StringType(),True),StructField('Count',LongType(),True)])
    pdf = pd.DataFrame(log_dict,index=[0])
    return spark.createDataFrame(pdf,schema=dfl_schema)

def get_metadata(sq,sess):
    """Function get_metadata(sq,sess) returns dict based on the query"""
    my_df = read_db(sq,spark)
    my_col = my_df.columns
    my_val = my_df.collect()
    return {i:my_val[0][i] for i in my_col}

def data_validation(crm_name,col_name):
    if crm_name == 'GBO_SAN_ISR':
        isr_dv = {'Null Check':"select *,'NullCheck' ErrorType,case when ((SAN is not null) and (Process_Date is not null) and (Period is not null) and ((Hours is not null) or (Reporting_Country_ISO_Country_Code is not null) or (Reporting_Country is not null))) then 'PASS' else 'FAIL' end ErrorStatus from df",
        'DataType Check': "select SAN, Process_Date, TM1_Reference, Period, Hours, Fee_Billed_USD, Internal_Fee_Billed_USD, Recoverable_Expenses_USD, Revenues_USD, Interfirm_Billings_Rcvd_USD, Unbilled_WIP_USD, Reporting_Country_ISO_Country_Code, Reporting_Country, BatchID, ExecutionID, CRMSourceName, MemberFirmName, TimeStamp, RowNumber,concat(ErrorType, ',' ,'DataTypeCheck') ErrorType, concat(ErrorStatus, (case when (cast (san as int) like SAN) and (to_timestamp(Process_Date) is not null) and (cast (period as int) like period)  then ',PASS' else ',FAIL' end)) ErrorStatus from df",
        'NumVal Check':"select SAN, Process_Date, TM1_Reference, Period, Hours, Fee_Billed_USD, Internal_Fee_Billed_USD, Recoverable_Expenses_USD, Revenues_USD, Interfirm_Billings_Rcvd_USD, Unbilled_WIP_USD, Reporting_Country_ISO_Country_Code, Reporting_Country, BatchID, ExecutionID, CRMSourceName, MemberFirmName, TimeStamp, RowNumber,concat(ErrorType, ',' ,'NumSumCheck') ErrorType,concat(ErrorStatus, (case when ((cast (Hours as int) + cast (Fee_Billed_USD as int) + cast (Internal_Fee_Billed_USD as int) + cast (Recoverable_Expenses_USD as int) + cast (Revenues_USD as int) + cast (Interfirm_Billings_Rcvd_USD as int) + cast (Unbilled_WIP_USD as int)) > 0) then  ',PASS' else ',FAIL' end)) ErrorStatus from df",
        'Orphan Check':"select SAN, Process_Date, TM1_Reference, Period, Hours, Fee_Billed_USD, Internal_Fee_Billed_USD, Recoverable_Expenses_USD, Revenues_USD, Interfirm_Billings_Rcvd_USD, Unbilled_WIP_USD, Reporting_Country_ISO_Country_Code, Reporting_Country, BatchID, ExecutionID, CRMSourceName, MemberFirmName, TimeStamp, RowNumber,concat(ErrorType, ',' ,'OrphanSANCheck') ErrorType,concat(ErrorStatus, ',PASS') ErrorStatus from df where san in (select distinct san from df_detail) union select SAN, Process_Date, TM1_Reference, Period, Hours, Fee_Billed_USD, Internal_Fee_Billed_USD, Recoverable_Expenses_USD, Revenues_USD, Interfirm_Billings_Rcvd_USD, Unbilled_WIP_USD, Reporting_Country_ISO_Country_Code, Reporting_Country, BatchID, ExecutionID, CRMSourceName, MemberFirmName, TimeStamp, RowNumber,concat(ErrorType, ',' ,'OrphanSANCheck') ErrorType,concat(ErrorStatus, ',FAIL') ErrorStatus from df where san not in (select distinct san from df_detail)",
        'invalid':"select * from df where instr(ErrorStatus,'FAIL') > 0",
        'valid': "select * from df where not (instr(ErrorStatus,'FAIL') > 0)"}
        return isr_dv

    if crm_name == 'GBO_SAN_Detail':
        detail_dv = {'Null Check':"select *,'NullCheck' ErrorType,case when ((SAN is not null) and (Global_Account_Name is not null) and (Global_Account_DUNS is not null) and (Global_Account_GIS_ID is not null) and (Global_Pipeline_Account_Name is not null) and (Global_Pipeline_Account_DUNS is not null) and (Global_Pipeline_Account_GIS_ID is not null) and (Global_Pipeline_Account_GUP_Name is not null) and (Global_Pipeline_Account_GUP_DUNS is not null) and (Global_Pipeline_Account_GUP_GIS_ID is not null) and (SAN_Issue_Date is not null) and (Confidential is not null) and (Request_ID is not null) and (Primary_Country_of_Work_Region is not null) and (Primary_Country_of_Work is not null) and (Primary_Country_of_Work_ISO_Country_Code is not null) and (Engagement_Partner is not null) and (Engagement_Partner_Email is not null) and (Engagement_Partner_Phone is not null) and (Primary_Service_Function is not null) and (Process_Date is not null)) then 'PASS' else 'FAIL' end ErrorStatus from df",
        'DataType Check': "select SAN,Currency,Global_Account_Name,Global_Account_DUNS,Global_Account_GIS_ID,Global_Pipeline_Account_Name,Global_Pipeline_Account_DUNS,Global_Pipeline_Account_GIS_ID,Global_Pipeline_Account_GUP_Name,Global_Pipeline_Account_GUP_DUNS,Global_Pipeline_Account_GUP_GIS_ID,SAN_Issue_Date,SAN_Status,SAN_Status_Date,Request_ID,First_Submit_Date,Overall_Request_Status,Confidential,First_Engaging_Party,First_Engaging_Party_In_Tree,First_Engaging_Party_DUNS,First_Engaging_Party_GIS_ID,Billing_Party,Billing_Party_In_Tree,Billing_Party_DUNS,Billing_Party_GIS_ID,Additional_Engaging_Parties,Additional_Engaging_Parties_DUNS,Additional_Engaging_Parties_GIS_ID,Proposed_Total_Fee_Amount_From_USD,Proposed_Total_Fee_Amount_To_USD,Proposed_Total_Fee_Amount_From,Proposed_Total_Fee_Amount_To,Proposed_Start_Date,Proposed_End_Date,Primary_Country_of_Work_Region,Primary_Country_of_Work,Primary_Country_of_Work_ISO_Country_Code,Engagement_Partner,Engagement_Partner_Email,Engagement_Partner_Phone,Engagement_Partner_Country,Requestor,Requestor_Email,Primary_Service_Function,Primary_Service,Primary_Service_Desc,Type_of_Work,Primary_Service_Potentially_Prohibited_For_Audit_Clients,Additional_Services_Potentially_Prohibited,Other_Potentially_Prohibited_Services,Description_of_Service,Permissible_Service_Reason,Won_Lost_Status,Won_Lost_Status_Date,Won_Lost_Status_Update_Method,Country_Engagement_Status,Client_Type_of_Service,Client_Approval_Request_Reference,Client_Contact,Client_Contact_Email,Client_Contact_Phone,Break_Out_Fees_By_Service,Proposed_Fee_Arrangements,Proposed_Fee_Arrangement_Other,Process_Date,TM1_Reference,BatchID,ExecutionID,CRMSourceName,MemberFirmName,TimeStamp,RowNumber,concat(ErrorType, ',' ,'DataTypeCheck') ErrorType, concat(ErrorStatus, (case when (cast (san as int) like SAN) and (to_timestamp(Process_Date) is not null)  then ',PASS' else ',FAIL' end)) ErrorStatus from df",
        'Period Check':"select SAN,Currency,Global_Account_Name,Global_Account_DUNS,Global_Account_GIS_ID,Global_Pipeline_Account_Name,Global_Pipeline_Account_DUNS,Global_Pipeline_Account_GIS_ID,Global_Pipeline_Account_GUP_Name,Global_Pipeline_Account_GUP_DUNS,Global_Pipeline_Account_GUP_GIS_ID,SAN_Issue_Date,SAN_Status,SAN_Status_Date,Request_ID,First_Submit_Date,Overall_Request_Status,Confidential,First_Engaging_Party,First_Engaging_Party_In_Tree,First_Engaging_Party_DUNS,First_Engaging_Party_GIS_ID,Billing_Party,Billing_Party_In_Tree,Billing_Party_DUNS,Billing_Party_GIS_ID,Additional_Engaging_Parties,Additional_Engaging_Parties_DUNS,Additional_Engaging_Parties_GIS_ID,Proposed_Total_Fee_Amount_From_USD,Proposed_Total_Fee_Amount_To_USD,Proposed_Total_Fee_Amount_From,Proposed_Total_Fee_Amount_To,Proposed_Start_Date,Proposed_End_Date,Primary_Country_of_Work_Region,Primary_Country_of_Work,Primary_Country_of_Work_ISO_Country_Code,Engagement_Partner,Engagement_Partner_Email,Engagement_Partner_Phone,Engagement_Partner_Country,Requestor,Requestor_Email,Primary_Service_Function,Primary_Service,Primary_Service_Desc,Type_of_Work,Primary_Service_Potentially_Prohibited_For_Audit_Clients,Additional_Services_Potentially_Prohibited,Other_Potentially_Prohibited_Services,Description_of_Service,Permissible_Service_Reason,Won_Lost_Status,Won_Lost_Status_Date,Won_Lost_Status_Update_Method,Country_Engagement_Status,Client_Type_of_Service,Client_Approval_Request_Reference,Client_Contact,Client_Contact_Email,Client_Contact_Phone,Break_Out_Fees_By_Service,Proposed_Fee_Arrangements,Proposed_Fee_Arrangement_Other,Process_Date,TM1_Reference,BatchID,ExecutionID,CRMSourceName,MemberFirmName,TimeStamp,RowNumber,concat(ErrorType, ',' ,'PeriodCheck') ErrorType,concat(ErrorStatus, (case when (cast(months_between(current_date(),to_timestamp(SAN_Issue_Date)) as int)  between 0 and 24) then  ',PASS' else ',FAIL' end)) ErrorStatus, (select case when cast(months_between(current_date() , min_sid) as int) > 24 then 'Excess data more than 24 months' when cast(months_between(current_date() , min_sid) as int) < 12 then 'Deficient data less than 12 months' end mdat from (select min(SAN_Issue_Date) min_sid from df)) GBO_reporting  from df",
        'invalid':"select * from df where instr(ErrorStatus,'FAIL') > 0",
        'valid': "select * from df where not (instr(ErrorStatus,'FAIL') > 0)"}
        return detail_dv
    
    if crm_name == 'SAP_IBS':
        ibs_dv = {'Null Check':"select *,'NullCheck' as ErrorType,case when  (`Lead Country / Country of Origin` is null) or (`Opportunity ID` is null)  then 'FAIL' else 'PASS' end ErrorStatus from df",
        'EmptyLine Check':f"select {col_name}, CountryCode, `BatchID`, `ExecutionID`, `CRMSourceName`, `MemberFirmName`, `TimeStamp`, `RowNumber`,concat(ErrorType, ',' ,'EmptyLineCheck') ErrorType,concat(ErrorStatus,(case when  (`Primary Entity Name` is null) and (`Primary Entity ID` is null) and (`Opportunity ID` is null) and (`Primary Entity DUNS` is null) and (`Sentinel Approval No` is null)  then ',FAIL' else ',PASS' end)) as ErrorStatus  from df",
        'DataType Check': f"select {col_name}, CountryCode, `BatchID`, `ExecutionID`, `CRMSourceName`, `MemberFirmName`, `TimeStamp`, `RowNumber`,concat(ErrorType, ',' ,'DataTypeCheck') ErrorType,concat(ErrorStatus,(case when (cast(`Sentinel Approval No` as int) like `Sentinel Approval No` and length(`Sentinel Approval No`) >= 10) and (to_date(Coalesce(`Last Changed Date`,'01-01-1900',`Last Changed Date`),'dd-MM-yyyy') is not null) and (to_date(Coalesce(`Close Date`,'01-01-1900',`Close Date`),'dd-MM-yyyy') is not  null) and (to_date(Coalesce(`Created On`,'01-01-1900',`Created On`),'dd-MM-yyyy') is not null) and (to_date(Coalesce(`Expected Decision date`,'01-01-1900',`Expected Decision date`),'dd-MM-yyyy') is not null) and (to_date(Coalesce(`Engagement Start Date`,'01-01-1900',`Engagement Start Date`),'dd-MM-yyyy') is not null) and (to_date(Coalesce(`Engagement End Date`,'01-01-1900',`Engagement End Date`),'dd-MM-yyyy') is not null) and (to_date(Coalesce(`ME OPP Creation Date`,'01-01-1900',`ME OPP Creation Date`),'dd-MM-yyyy') is not null) and (to_date(Coalesce(`ME CIP Creation Date`,'01-01-1900',`ME CIP Creation Date`),'dd-MM-yyyy') is not null) and (to_date(Coalesce(`ME Collaborative CIP Completion Date`,'01-01-1900',`ME Collaborative CIP Completion Date`),'dd-MM-yyyy') is  not null) then ',PASS' else ',Warning' end)) ErrorStatus from df",
        'Warning': "select *,(case when ((`Primary Entity Name` is null) or (`Primary Entity ID` is null) or length(nvl(`Primary Entity Name`,'0')) < 2) or (nvl(`Other member firm Fees`,'0') < 2) or (`Last Changed Date` like '01-01-1900') or (`ME OPP Creation Date` like '01-01-1900') or (`Last Changed Date` like '01-01-1900') or (`MarketEDGE Score` is null) or (cast(`MarketEDGE Score` as int) like `MarketEDGE Score`)  or (`Probability of Success` is null) or (cast(`Probability of Success` as int)  like `Probability of Success`) or (`Engagement Start Date` like '01-01-1900') or (cast(nvl(`Overall Opportunity value`,'0') as Int) > 1) then 'Y' else 'N' end) warning from df",
        'invalid':"select * from df where instr(ErrorStatus,'FAIL') > 0",
        'valid': "select a.* from df a where not (instr(a.ErrorStatus,'FAIL') > 0) and nvl(a.`Last Changed Date`,0) = (select max(nvl(b.`Last Changed Date`,0)) from df b where a.`Opportunity ID` = b.`Opportunity ID`)"}
        return ibs_dv

    if crm_name == 'NON_IBS':
        non_ibs_dv = {'Null Check':"select *,'NullCheck' as ErrorType,case when  (`Lead Country` is null) or (`Opportunity ID (OpportunityLocalID)` is null)  then 'FAIL' else 'PASS' end ErrorStatus from df",
        'Warning': "select *,'NA' warning from df",
        'invalid':"select * from df where instr(ErrorStatus,'FAIL') > 1",
        'valid': "select a.* from df a where not (instr(a.ErrorStatus,'FAIL') > 1) and nvl(a.`Date (Stage Changed)`,0) = (select max(nvl(b.`Date (Stage Changed)`,0)) from df b where b.`Opportunity ID (OpportunityLocalID)` = a.`Opportunity ID (OpportunityLocalID)`)"}
        return non_ibs_dv
    #'EmptyLine Check':"select `Opportunity ID`, `Primary Entity ID`, `Primary Entity Name`, `GIS ID`, `Primary Entity - GUP Entity ID`, `Primary Entity - GUP Name`, `Primary Entity - GUP DUNS`, `Primary Entity DUNS`,`Global Opportunity Sector description`, `Global Opportunity SIC`, `Opportunity Description`, `Expected Decision date`, `Engagement Start Date`, `Engagement End Date`, `Opportunity Manager Name`, `Opportunity Manager Email`, `Opportunity Partner Name`, `Opportunity Partner Email`, `Probability of Success`, `Accessibility description`, `Contract Type description`, `Competitive Bid`, `Sentinel Approval No`, `Competitior Lost To`, `Close Date`, `Created on`, `Last Changed Date`, `Primary Sentinel Service Type Code & Description`, `Primary Type of work description`, `Other service types`, `Other member firm Fees`, `Overall Opportunity value`, `Reference Currency/Fees Currency`, `Outcome description`, `Reason description`, `Secondary Client Issue description`, `IBS Contact Name`, `Competitor Name`, `Alliance Partner`, `Pursue Stage description`, `RFP Received`, `Proposal Due Date`, `Client Presentation Date`, `Other KPMG Member Firm Name`, `Lead Country / Country of Origin`, `Strategic Growth Initiative - Global Value`, `ME OPP Complete`, `ME CIP Complete`, `MarketEDGE Score`, `ME Collaborative CIP Completed`, `ME Collaborative OPP Completed`, `ME OPP Creation Date`, `ME CIP Creation Date`, `Date of ME OSC Score`, `ME Collaborative CIP Completion Date`, `ME Collaboration OPP Completion Date`, CountryCode, `BatchID`, `ExecutionID`, `CRMSourceName`, `MemberFirmName`, `TimeStamp`, `RowNumber`,concat(ErrorType, ',' ,'EmptyLineCheck') ErrorType,concat(ErrorStatus,(case when  (`Primary Entity Name` is null) and (`Primary Entity ID` is null) and (`Opportunity ID` is null) and (`Primary Entity DUNS` is null) and (`Sentinel Approval No` is null)  then ',FAIL' else ',PASS' end)) as ErrorStatus  from df",
    #'DataType Check': "select `Opportunity ID`, `Primary Entity ID`, `Primary Entity Name`, `GIS ID`, `Primary Entity - GUP Entity ID`, `Primary Entity - GUP Name`, `Primary Entity - GUP DUNS`, `Primary Entity DUNS`, 'Sector3' as `Global Opportunity Sector description`, 'Service3' as `Global Opportunity SIC`, `Opportunity Description`, `Expected Decision date`, `Engagement Start Date`, `Engagement End Date`, `Opportunity Manager Name`, `Opportunity Manager Email`, `Opportunity Partner Name`, `Opportunity Partner Email`, `Probability of Success`, `Accessibility description`, `Contract Type description`, `Competitive Bid`, `Sentinel Approval No`, `Competitior Lost To`, `Close Date`, `Created on`, `Last Changed Date`, `Primary Sentinel Service Type Code & Description`, `Primary Type of work description`, `Other service types`, `Other member firm Fees`, `Overall Opportunity value`, `Reference Currency/Fees Currency`, `Outcome description`, `Reason description`, `Secondary Client Issue description`, `IBS Contact Name`, `Competitor Name`, `Alliance Partner`, `Pursue Stage description`, `RFP Received`, `Proposal Due Date`, `Client Presentation Date`, `Other KPMG Member Firm Name`, 'Country4' as `Lead Country / Country of Origin`, `Strategic Growth Initiative - Global Value`, `ME OPP Complete`, `ME CIP Complete`, `MarketEDGE Score`, `ME Collaborative CIP Completed`, `ME Collaborative OPP Completed`, `ME OPP Creation Date`, `ME CIP Creation Date`, `Date of ME OSC Score`, `ME Collaborative CIP Completion Date`, `ME Collaboration OPP Completion Date`, CountryCode, `BatchID`, `ExecutionID`, `CRMSourceName`, `MemberFirmName`, `TimeStamp`, `RowNumber`,concat(ErrorType, ',' ,'DataTypeCheck') ErrorType,concat(ErrorStatus,(case when (cast(`Sentinel Approval No` as int) like `Sentinel Approval No` and length(`Sentinel Approval No`) >= 10) and (to_date(Coalesce(`Last Changed Date`,'01-01-1900',`Last Changed Date`),'dd-MM-yyyy') is not null) and (to_date(Coalesce(`Close Date`,'01-01-1900',`Close Date`),'dd-MM-yyyy') is not  null) and (to_date(Coalesce(`Created On`,'01-01-1900',`Created On`),'dd-MM-yyyy') is not null) and (to_date(Coalesce(`Expected Decision date`,'01-01-1900',`Expected Decision date`),'dd-MM-yyyy') is not null) and (to_date(Coalesce(`Engagement Start Date`,'01-01-1900',`Engagement Start Date`),'dd-MM-yyyy') is not null) and (to_date(Coalesce(`Engagement End Date`,'01-01-1900',`Engagement End Date`),'dd-MM-yyyy') is not null) and (to_date(Coalesce(`ME OPP Creation Date`,'01-01-1900',`ME OPP Creation Date`),'dd-MM-yyyy') is not null) and (to_date(Coalesce(`ME CIP Creation Date`,'01-01-1900',`ME CIP Creation Date`),'dd-MM-yyyy') is not null) and (to_date(Coalesce(`ME Collaborative CIP Completion Date`,'01-01-1900',`ME Collaborative CIP Completion Date`),'dd-MM-yyyy') is  not null) then ',PASS' else ',Warning' end)) ErrorStatus from df",



def data_trans(crm_name):
    if crm_name == 'SAP_IBS':
        ibs_trans = {'dfExchangRate_temp':"spark.read.format('com.databricks.spark.csv').option('sep',',').option('header','true').option('inferSchema','true').load(Master_name_ExchangeRate)",
        'df_AddProjectDuration':"select *,case when ceil(datediff(to_date(A.`Engagement End Date`,'dd-MM-yyyy'),to_date(A.`Engagement Start Date`,'dd-MM-yyyy'))/365.25)=0 then 1 when ceil(datediff(to_date(A.`Engagement End Date`,'dd-MM-yyyy'),to_date(A.`Engagement Start Date`,'dd-MM-yyyy'))/365.25) is null then 1 Else ceil(datediff(to_date(A.`Engagement End Date`,'dd-MM-yyyy'),to_date(A.`Engagement Start Date`,'dd-MM-yyyy'))/365.25) End as ProjectDuration from df_temp A",
        'df_AddExchangeRate':"select *,  Case When B.ExchangeRate is not null then (A.`Overall Opportunity value`*Coalesce(B.ExchangeRate,1))/(A.ProjectDuration) Else (1*A.`Overall Opportunity value`)/(A.ProjectDuration) End `Annual fee in USD`  from dfAddProjectDuration_temp A Left join (Select  Country, Year, ExchangeRate from dfExchangRate_temp) B on A.CountryCode=B.Country and Coalesce(Year(to_date(A.`Created on`,'dd-MM-yyyy')),year(current_date()))=B.Year",
        'df_Transformed':"select `Opportunity ID`, `Primary Entity ID`, `Primary Entity Name`, `GIS ID`, `Primary Entity - GUP Entity ID`, `Primary Entity - GUP Name`, `Primary Entity - GUP DUNS`, `Primary Entity DUNS`, `Global Opportunity Sector description` ,`Global Opportunity SIC`, `Opportunity Description`, `Expected Decision date`, `Engagement Start Date`, `Engagement End Date`, `Opportunity Manager Name`, `Opportunity Manager Email`, `Opportunity Partner Name`, `Opportunity Partner Email`, `Probability of Success`, `Accessibility description`, `Contract Type description`, `Competitive Bid`, `Sentinel Approval No`, `Competitior Lost To`, `Close Date`, `Created on`, `Last Changed Date`, `Primary Sentinel Service Type Code & Description`, `Primary Type of work description`, `Other service types`, `Other member firm Fees`, `Overall Opportunity value`, `Reference Currency/Fees Currency`, `Outcome description`, `Reason description`, `Secondary Client Issue description`, `IBS Contact Name`, `Competitor Name`, `Alliance Partner`, `Pursue Stage description`, `RFP Received`, `Proposal Due Date`, `Client Presentation Date`, `Other KPMG Member Firm Name`, `Lead Country / Country of Origin`  , `Strategic Growth Initiative - Global Value`, `ME OPP Complete`, `ME CIP Complete`, `MarketEDGE Score`, `ME Collaborative CIP Completed`, `ME Collaborative OPP Completed`, `ME OPP Creation Date`, `ME CIP Creation Date`, `Date of ME OSC Score`, `ME Collaborative CIP Completion Date`, `ME Collaboration OPP Completion Date`, `CountryCode`, `BatchID`, `ExecutionID`, `CRMSourceName`, `MemberFirmName`, `TimeStamp`,`Annual fee in USD`,Cast(RowNumber as int) as RowNumber from dfTransformed_temp"}
        return ibs_trans

    if crm_name == 'GBO_SAN_ISR':
        isr_trans = {'df':"select * from df"}
        return isr_trans

    if crm_name == 'GBO_SAN_Detail':
        detail_trans = {'df_detail_conf':"select SAN,Currency,Global_Account_Name,Global_Account_DUNS,Global_Account_GIS_ID,Global_Pipeline_Account_Name,Global_Pipeline_Account_DUNS,Global_Pipeline_Account_GIS_ID,Global_Pipeline_Account_GUP_Name,Global_Pipeline_Account_GUP_DUNS,Global_Pipeline_Account_GUP_GIS_ID,SAN_Issue_Date,null SAN_Status,null SAN_Status_Date,Request_ID,null First_Submit_Date,null Overall_Request_Status,Confidential,null First_Engaging_Party,null First_Engaging_Party_In_Tree,null First_Engaging_Party_DUNS,null First_Engaging_Party_GIS_ID,null Billing_Party,null Billing_Party_In_Tree,null Billing_Party_DUNS,null Billing_Party_GIS_ID,null Additional_Engaging_Parties,null Additional_Engaging_Parties_DUNS,null Additional_Engaging_Parties_GIS_ID,null Proposed_Total_Fee_Amount_From_USD,null Proposed_Total_Fee_Amount_To_USD,null Proposed_Total_Fee_Amount_From,null Proposed_Total_Fee_Amount_To,null Proposed_Start_Date,null Proposed_End_Date,Primary_Country_of_Work_Region,Primary_Country_of_Work,Primary_Country_of_Work_ISO_Country_Code,Engagement_Partner,Engagement_Partner_Email,Engagement_Partner_Phone,null Engagement_Partner_Country,null Requestor,null Requestor_Email,Primary_Service_Function,null Primary_Service,null Primary_Service_Desc,null Type_of_Work,null Primary_Service_Potentially_Prohibited_For_Audit_Clients,null Additional_Services_Potentially_Prohibited,null Other_Potentially_Prohibited_Services,null Description_of_Service,Permissible_Service_Reason,null Won_Lost_Status,null Won_Lost_Status_Date,null Won_Lost_Status_Update_Method,null Country_Engagement_Status,null Client_Type_of_Service,null Client_Approval_Request_Reference,null Client_Contact,null Client_Contact_Email,null Client_Contact_Phone,null Break_Out_Fees_By_Service,null Proposed_Fee_Arrangements,null Proposed_Fee_Arrangement_Other,Process_Date,null TM1_Reference,BatchID,ExecutionID,CRMSourceName,MemberFirmName,TimeStamp,RowNumber,ErrorType,ErrorStatus,GBO_reporting from df_temp where confidential = 'Y' union select * from df_temp where not (confidential = 'Y')",
        'df_detail_non_conf':"select * from df_temp where confidential = 'Y' and (nvl(length(SAN_Status),0) + nvl(length( SAN_Status_Date),0) + nvl(length( Request_ID),0) + nvl(length( First_Submit_Date),0) + nvl(length( Overall_Request_Status),0) + nvl(length( Confidential),0) + nvl(length( First_Engaging_Party),0) + nvl(length( First_Engaging_Party_In_Tree),0) + nvl(length( First_Engaging_Party_DUNS),0) + nvl(length( First_Engaging_Party_GIS_ID),0) + nvl(length( Billing_Party),0) + nvl(length( Billing_Party_In_Tree),0) + nvl(length( Billing_Party_DUNS),0) + nvl(length( Billing_Party_GIS_ID),0) + nvl(length( Additional_Engaging_Parties),0) + nvl(length( Additional_Engaging_Parties_DUNS),0) + nvl(length( Additional_Engaging_Parties_GIS_ID),0) + nvl(length( Proposed_Total_Fee_Amount_From_USD),0) + nvl(length( Proposed_Total_Fee_Amount_To_USD),0) + nvl(length( Proposed_Total_Fee_Amount_From),0) + nvl(length( Proposed_Total_Fee_Amount_To),0) + nvl(length( Proposed_Start_Date),0) + nvl(length( Proposed_End_Date),0) + nvl(length( Primary_Country_of_Work_Region),0) + nvl(length( Engagement_Partner_Country),0) + nvl(length( Requestor),0) + nvl(length( Requestor_Email),0) + nvl(length( Primary_Service_Function),0) + nvl(length( Primary_Service),0) + nvl(length( Primary_Service_Desc),0) + nvl(length( Type_of_Work),0) + nvl(length( Primary_Service_Potentially_Prohibited_For_Audit_Clients),0) + nvl(length( Additional_Services_Potentially_Prohibited),0) + nvl(length( Other_Potentially_Prohibited_Services),0) + nvl(length( Description_of_Service),0) + nvl(length( Permissible_Service_Reason),0) + nvl(length( Won_Lost_Status),0) + nvl(length( Won_Lost_Status_Date),0) + nvl(length( Won_Lost_Status_Update_Method),0) + nvl(length( Country_Engagement_Status),0) + nvl(length( Client_Type_of_Service),0) + nvl(length( Client_Approval_Request_Reference),0) + nvl(length( Client_Contact),0) + nvl(length( Client_Contact_Email),0) + nvl(length( Client_Contact_Phone),0) + nvl(length( Break_Out_Fees_By_Service),0) + nvl(length( Proposed_Fee_Arrangements),0) + nvl(length( Proposed_Fee_Arrangement_Other),0) + nvl(length( TM1_Reference),0) > 0)"}
        return detail_trans


