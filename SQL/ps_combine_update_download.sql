set mapred.output.compress=true;
set hive.exec.compress.output=true;
set mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
set io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec;
set hive.hadoop.supports.splittable.combineinputformat=true;
use di_ad_hoc;

-- =============================================================================
-- TABLE
-- T1.  warehouse.ccmusg_fact_product_update
-- T2.  warehouse.ccmusg_fact_download_info
-- 
-- TASK
-- 1.  Convert 0000-00-00 to NULL value
-- 2.  Outer join update and download tables
-- 3.  Merge member_guid and version from join and update tables
-- ==============================================================================

drop table di_ad_hoc.ps_combine_update_download_xyang_tmp;

create table di_ad_hoc.ps_combine_update_download_xyang_tmp as


select
        
        (case when up.member_guid is not null then up.member_guid
              else dl.member_guid end) as member_guid,
        (case when up.to_version is not null then up.to_version
              else dl.product_version end) as version,
        
        -- update      
        update_date,
        
        -- start download
        first_dl_start_date,
        last_dl_start_date,
        num_dl_start,
        
        -- end download
        first_dl_end_date,
        last_dl_end_date,
        cancel_dl,
        
        -- extract download
        first_extract_end_date,
        last_extract_end_date,
        extract_dl,
        
        -- install download
        first_install_end_date,
        last_install_end_date,
        install_dl,
        
        -- error in download
        has_error_dl,
        num_error_dl
        
from 
        
        -- Update product version (only looking at when user updates to a certain version)
        (
        select 
                member_guid,
                to_version,
                event_date as update_date
                
        from    warehouse.ccmusg_fact_product_update
        where   product_name = 'PHOTOSHOP'
        ) as up
        
        FULL OUTER JOIN
        
        -- Download product version
        (
        select
                member_guid,
                product_version,
                
                -- start download
                min(dl_start_date) as first_dl_start_date,
                max(dl_start_date) as last_dl_start_date,
                count(dl_start_date) as num_dl_start,
                
                -- end download
                min(dl_end_date) as first_dl_end_date,
                max(dl_end_date) as last_dl_end_date,
                (case when (sum(case when dl_cancel_date is null then 0 else 1 end) >= 1) 
                      then 'Yes' else 'No' end) as cancel_dl,
                
                -- extract download     
                min(extract_end_date) as first_extract_end_date,
                max(extract_end_date) as last_extract_end_date,
                (case when (sum(case when extract_end_date is null then 0 else 1 end) >= 1) 
                      then 'Yes' else 'No' end) as extract_dl,
                
                -- install download
                min(install_end_date) as first_install_end_date,
                max(install_end_date) as last_install_end_date,
                (case when (sum(case when install_end_date is null then 0 else 1 end) >= 1) 
                      then 'Yes' else 'No' end) as install_dl,
                
                -- error in download
                (case when (sum(case when error_date is null then 0 else 1 end) >= 1) 
                      then 'Yes' else 'No' end) as has_error_dl,
                sum(case when error_date is null then 0 else 1 end) as num_error_dl
                
        from 
              (
              select 
                      member_guid,
                      product_version,
                      (case when dl_start_date = '0000-00-00' then NULL else dl_start_date end) as dl_start_date,
                      (case when dl_end_date = '0000-00-00' then NULL else dl_end_date end) as dl_end_date,
                      (case when extract_end_date = '0000-00-00' then NULL else extract_end_date end) as extract_end_date,
                      (case when install_end_date = '0000-00-00' then NULL else install_end_date end) as install_end_date,
                      (case when dl_cancel_date = '0000-00-00' then NULL else dl_cancel_date end) as dl_cancel_date,
                      (case when install_cancel_date = '0000-00-00' then NULL else install_cancel_date end) as install_cancel_date,
                      (case when error_date = '0000-00-00' then NULL else error_date end) as error_date
               
               from warehouse.ccmusg_fact_download_info
               where product_name = 'PHOTOSHOP'
                      
              ) as tmp
        
        group by member_guid, product_version
        
        ) as dl
        
        ON dl.member_guid = up.member_guid AND dl.product_version = up.to_version 

;