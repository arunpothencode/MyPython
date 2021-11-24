if __name__ == '__main__':
    list_action = [framework_action,landing_action,datavalidation_action,copy_to_stagingtable,file_archive]
    #list_action = [framework_action,landing_action,copy_to_stagingtable,file_archive]
    #cname, memname, bid, eid = 'SAP_IBS','KE',120,121
    mqry = None
    for pipeline_stage in list_action:
        if str(pipeline_stage.__name__) == 'framework_action':
            try:
                mqry = framework_action(cname,memname,bid,eid)
                write_log_file(f'LandingZone/{cname}/Log/',f'{cname}_{memname}.log',bid)
                print('framework_action is completed successfully')
            except:
                sys.exit(f'framework_action is completed with error {sys.exc_info()}')
        elif str(pipeline_stage.__name__) == 'landing_action':
            try:
                landing_action(cname,memname,bid,eid,mqry)
                write_log_file(f'RawZone/{cname}/Log/',f'{cname}_{memname}.log',bid)
                print('landing_action is completed successfully')
            except:
                sys.exit(f'landing_action is completed with error {sys.exc_info()}')            
        elif str(pipeline_stage.__name__) == 'datavalidation_action':
            try:
                datavalidation_action(cname,memname,bid,eid,mqry)
                write_log_file(f'StagingZone/{cname}/Log/',f'{cname}_{memname}.log',bid)
                print('datavalidation_action is completed successfully')
            except:
                sys.exit(f'datavalidation_action is completed with error {sys.exc_info()}') 
        elif str(pipeline_stage.__name__) == 'copy_to_stagingtable':
            try:
                copy_to_stagingtable(cname)
                print('copy_to_stagingtable is completed successfully')
            except:
                sys.exit(f'copy_to_stagingtable is completed with error {sys.exc_info()}') 
        elif str(pipeline_stage.__name__) == 'file_archive':
            try:
                file_archive(cname)
                print('file_archive is completed successfully')
            except:
                sys.exit(f'file_archive is completed with error {sys.exc_info()}') 
                
           
        