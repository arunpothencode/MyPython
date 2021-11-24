# orchestration of the above mentioned function
from pyspark.sql.functions import collect_set,explode,lit,split
def framework_action(cname,memname,bid,eid):
    """Function framework_action(cname,memname,bid,eid) takes these parameter transfer files to adls and generate mapping query"""
    #downloading metadata cname = SAP_IBS and memname = US
    sq = f"select * from master.vwMasterSourceData where crmsourcename ='{cname}' and memberfirmname = '{memname}' and isactive=1 and isonboarded=1"
    fwrk_metadata = get_metadata(sq,spark)
    did,fl_path,fil_delim,fl_pattern,sub_fold=fwrk_metadata['DataSourceID'],f"wasbs://{fwrk_metadata['BlobContainer']}@{fwrk_metadata['BlobAccount']}/{fwrk_metadata['SubFolder']}",fwrk_metadata['DelimiterName'][0],fwrk_metadata['FileNamePattern'],fwrk_metadata['SubFolder']
    tgt_fold = f"abfss://{fwrk_metadata['Container']}@{fwrk_metadata['AccountName']}.dfs.core.windows.net/LandingZone"
    sq_col_mapping = f"select * from Preprocess.tblFileMetaData where datasourceid={did}"
    loc_glob_mapping = list_metadata(sq_col_mapping,spark)
    #list files from blob taking file pattern and file path
    for fl in get_source_file(fl_path,fl_pattern,memname):
        #create a data frame for all the list files
        df = read_any_csv(fl[0].replace(fl[1],''),fl[1],fil_delim[0],spark)
        #wrtie to adls gen2
        if 'SUCCESS' == write_to_target(df,tgt_fold,sub_fold,fl[1],fil_delim[0],fl_pattern,spark):
            my_dict = {'BatchID':bid,'ExecutionID':eid,'SourceType':cname,'ExecutionLevel':'Framework','ValidationType':'MappingCol','ValidationStatus':'PASS','FieldDescription':f'{fl[1]}','Count':df.count()}
            log_df = create_log_df(my_dict)
            #logging to log table
            if 'SUCCESS' == write_db('Log.tblDataValidationInterimDetails',log_df,spark):
                print(f'{fl[1]} SUCCESS')

    return filecol_match_mappingcol(loc_glob_mapping['FileColumn'],loc_glob_mapping['MappingField'])             
    

def landing_action(cname,memname,bid,eid,mqry):
    mapping_query= mqry
    #mystr_list = mapping_query.split(',')
    #mapping_query = ','.join([i for i in mystr_list if not re.match('.*date',i,re.IGNORECASE)]) +','+ ','.join(['`'+ i + '`' for i in mystr_list if re.match('.*date',i,re.IGNORECASE)])
    sq = f"select * from master.vwMasterSourceData where crmsourcename ='{cname}' and memberfirmname = '{memname}' and isactive=1 and isonboarded=1"
    fwrk_metadata = get_metadata(sq,spark)
    did,fl_path,fil_delim,fl_pattern,sub_fold=fwrk_metadata['DataSourceID'],f"abfss://{fwrk_metadata['Container']}@{fwrk_metadata['AccountName']}.dfs.core.windows.net/LandingZone/{fwrk_metadata['SubFolder']}",fwrk_metadata['DelimiterName'][0],fwrk_metadata['FileNamePattern'],fwrk_metadata['SubFolder']
    tgt_fold = f"abfss://{fwrk_metadata['Container']}@{fwrk_metadata['AccountName']}.dfs.core.windows.net/RawZone"
    #get the select list
    file_val_sq = f"select distinct ruletype from master.tblrules where validationtypeid=1"
    file_val_meta = read_rule_table(file_val_sq,spark)
    #get the list of files present in adls landing zone for given type and memberfirm and file pattern and file path
    ret_log = {}
    for fl in get_source_file(fl_path,fl_pattern,memname):
        #create a dataframe using the file path and select list 
        df = read_any_csv(fl[0].replace(fl[1],''),fl[1],fil_delim[0],spark)
        df.registerTempTable('df')
        df = spark.sql(f"select {mapping_query} from df")
        #run the file validation steps
        for valtype in file_val_meta:
            if valtype == 'FileSize':
                ret_val = check_size(df,spark)
                ret_log[valtype] = ret_val
            elif valtype == 'ColumnCount':
                ret_val = 'PASS' if len(fwrk_metadata['Col_Template'].split(',')) == len(df.columns) else 'FAIL'
                ret_log[valtype] = ret_val
            elif valtype == 'Template':
                ret_val = 'PASS' if ','.join(sorted(fwrk_metadata['Col_Template'].split(','))) == ','.join(sorted(list(df.columns))) else 'FAIL'
                ret_log[valtype] = ret_val
            elif valtype == 'CountryCode' and 'GBO_SAN' not in cname:
                ret_val = 'PASS' if f"_{fwrk_metadata['MemberFirmName']}_" in fl[1] else 'FAIL'
                ret_log[valtype] = ret_val
            my_dict = {'BatchID':bid,'ExecutionID':eid,'SourceType':cname,'ExecutionLevel':'FileValidation','ValidationType':valtype,'ValidationStatus': ret_val,'FieldDescription':'ALL','Count':df.count()}
            log_df = create_log_df(my_dict)
            #logging to log table
            #add the file validation steps log to log dataframe
            if 'SUCCESS' == write_db('Log.tblDataValidationInterimDetails',log_df,spark):
                print(f'{fl[1]} of {valtype} file validation {ret_val}')
        print(ret_log.values())
        if not 'FAIL' in list(ret_log.values()):
            if cname.__contains__('IBS'):
                #add the audit column in the file validation data frme
                df_aud = add_audit_col(df,fwrk_metadata['MemberFirmName'],spark,bid,eid)
                if 'SUCCESS' == write_to_target(df_aud,tgt_fold,f'{sub_fold}/Valid',fl[1],fil_delim[0],fl_pattern,spark):
                    print(f'{fl[1]} file move to RawZone Valid SUCCESSFUL')
            else:
                #add the audit column in the file validation data frme
                df_aud = add_audit_col(df,fwrk_metadata['FileNamePattern'],spark,bid,eid)
                if 'SUCCESS' == write_to_target(df_aud,tgt_fold,f'{sub_fold}/Valid',fl[1],fil_delim[0],fl_pattern,spark):
                    print(f'{fl[1]} file move to RawZone Valid SUCCESSFUL')
        else:
            if cname == 'SAP_IBS':
                df_aud = add_audit_col(df,fwrk_metadata['MemberFirmName'],spark,bid,eid)
                if 'SUCCESS' == write_to_target(df_aud,tgt_fold,f'{sub_fold}/Invalid',fl[1].replace('.csv','_InvalidFile.csv'),fil_delim[0],fl_pattern,spark):
                    print(f'{fl[1]} file move to RawZone Invalid SUCCESSFUL')
            else:
                df_aud = add_audit_col(df,fwrk_metadata['FileNamePattern'],spark,bid,eid)
                if 'SUCCESS' == write_to_target(df_aud,tgt_fold,f'{sub_fold}/Invalid',fl[1].replace('.csv','_InvalidFile.csv'),fil_delim[0],fl_pattern,spark):
                    print(f'{fl[1]} file move to RawZone Invalid SUCCESSFUL')

def datavalidation_action(cname,memname,bid,eid,mqry):
    crm_dv = data_validation(cname,mqry)
    #print(crm_dv)
    sq = f"select * from master.vwMasterSourceData where crmsourcename ='{cname}' and memberfirmname = '{memname}' and isactive=1 and isonboarded=1"
    fwrk_metadata = get_metadata(sq,spark)
    did,fl_path,fil_delim,fl_pattern,sub_fold=fwrk_metadata['DataSourceID'],f"abfss://{fwrk_metadata['Container']}@{fwrk_metadata['AccountName']}.dfs.core.windows.net/RawZone/{fwrk_metadata['SubFolder']}/Valid",fwrk_metadata['DelimiterName'][0],fwrk_metadata['FileNamePattern'],fwrk_metadata['SubFolder']
    tgt_fold = f"abfss://{fwrk_metadata['Container']}@{fwrk_metadata['AccountName']}.dfs.core.windows.net/StagingZone"
    #get the select list
    file_val_sq = f"select distinct ruletype from master.tblrules where validationtypeid=2"
    file_val_meta = read_rule_table(file_val_sq,spark)
    #list the file in adls rawzone
    for fl in get_source_file(fl_path,fl_pattern,memname):
        #create a dataframe using the file path and select list 
        print(fl)
        df = read_any_csv(fl[0].replace(fl[1],''),fl[1],fil_delim[0],spark)
        
        #Adding countrycode for IBS file
        if cname == 'SAP_IBS':
            df.registerTempTable('df')
            df = spark.sql("select *, MemberFirmName as CountryCode from df")
            df.registerTempTable('df')
            #display(df.limit(2))
        else:
            df.registerTempTable('df')
            #display(df.limit(2))
            #df = df_update
        
        #check the metadata for datavalidation
        counter = 0
        for dv in crm_dv:
            if dv in file_val_meta:
                df = spark.sql(f"{crm_dv[dv]}")
                df.registerTempTable('df')
                #run the validtion one by one store the log
                df_val = spark.sql(f"select * from df where split(errorstatus,',')[{counter}] = 'PASS'")
                my_dict = {'BatchID':bid,'ExecutionID':eid,'SourceType':cname,'ExecutionLevel':'DataValidation','ValidationType':dv,'ValidationStatus': 'PASS','FieldDescription':'ALL','Count':df_val.count()}
                log_df = create_log_df(my_dict)
                write_db('Log.tblDataValidationInterimDetails',log_df,spark)
                counter += 1
    
        #write to staging zone 
        df_valid = spark.sql(f"{crm_dv['valid']}")
        if df_valid.count() > 1:
            if 'SUCCESS' == write_to_target(df_valid,tgt_fold,f'{sub_fold}/Valid',fl[1],fil_delim[0],fl_pattern,spark):
                print(f"{fl[1]} Successfully written to Valid StagingZone")

        df_invalid = spark.sql(f"{crm_dv['invalid']}")
        if df_valid.count() > 1:
            if 'SUCCESS' == write_to_target(df_invalid,tgt_fold,f'{sub_fold}/Invalid',fl[1].replace('.csv','_Invalid.csv'),fil_delim[0],fl_pattern,spark):
                print(f"{fl[1].replace('.csv','_Invalid.csv')} Successfully written to Invalid StagingZone")

    #return success or failure message
    return 'SUCCESS'

def copy_to_stagingtable(cname,linkedservice_adls=linkedservice_adls):
    details_dict = get_storage_detail(linkedservice_adls)
    if cname == 'SAP_IBS':
        for element in mssparkutils.fs.ls(details_dict['adls_url']+'/StagingZone/SAP_IBS/Valid'):
            if element.name.endswith('.csv'):
                mydf = read_any_csv(details_dict['adls_url']+'/StagingZone/SAP_IBS/Valid',element.name,'|',spark)
                if mydf.count() > 1:
                    result = write_db('staging.tblOpportunitySAPIBS_temp',mydf,spark)
    elif cname == 'NON_IBS':
        for element in mssparkutils.fs.ls(details_dict['adls_url']+'/StagingZone/NON_IBS/Valid'):
            if element.name.endswith('.csv'):
                mydf = read_any_csv(details_dict['adls_url']+'/StagingZone/NON_IBS/Valid',element.name,'|',spark)
                if mydf.count() > 1:
                    result = write_db('staging.tblOpportunityNONIBS_temp',mydf,spark)

    if result == 'SUCCESS':
        print('Successfully loaded to stage table')
        return 'SUCCESS'


def file_archive(cname=cname,memname=memname):
    adls_dict = get_storage_detail(linkedservice_adls)
    blob_dict = get_storage_detail(linkedservice_blob)

    for element in mssparkutils.fs.ls(adls_dict['adls_url']+f'/LandingZone/{cname}'):
            if element.name.endswith('.csv'):
                mssparkutils.fs.cp(adls_dict['adls_url']+f'/LandingZone/{cname}/'+f'{element.name}',adls_dict['adls_url']+f'/LandingZone/{cname}/Archive/',recurse=True)
                mssparkutils.fs.rm(adls_dict['adls_url']+f'/LandingZone/{cname}/'+f'{element.name}',recurse=True)
    
    adls_val = ['RawZone','StagingZone']
    
    for val in adls_val:
        
        for element in mssparkutils.fs.ls(adls_dict['adls_url']+f'/{val}/{cname}/Valid'):
            if element.name.endswith('.csv'):
                mssparkutils.fs.cp(adls_dict['adls_url']+f'/{val}/{cname}/Valid/'+f'{element.name}',adls_dict['adls_url']+f'/{val}/{cname}/Valid/Archive/',recurse=True)
                mssparkutils.fs.rm(adls_dict['adls_url']+f'/{val}/{cname}/Valid/'+f'{element.name}',recurse=True)
        
        for element in mssparkutils.fs.ls(adls_dict['adls_url']+f'/{val}/{cname}/Invalid'):
            if element.name.endswith('.csv'):
                mssparkutils.fs.cp(adls_dict['adls_url']+f'/{val}/{cname}/Invalid/'+f'{element.name}',adls_dict['adls_url']+f'/{val}/{cname}/Invalid/Archive/',recurse=True)
                mssparkutils.fs.rm(adls_dict['adls_url']+f'/{val}/{cname}/Invalid/'+f'{element.name}',recurse=True) 

    
    # for ele in mssparkutils.fs.ls(str(blob_dict['url'].replace('https://','wasbs://landingzone@'))+f'{cname}'):
    #     if ele.name.endswith('.csv') and ele.name.__contains__(memname):
    #         mssparkutils.fs.rm(ele.path,recurse=True)
    
    return 'SUCCESS'
