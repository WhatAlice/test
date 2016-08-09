set mapred.output.compress=true;
set hive.exec.compress.output=true;
set mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
set io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec;
set hive.hadoop.supports.splittable.combineinputformat=true;
use di_ad_hoc;

-- =============================================================================
-- TABLE
-- T1.  di_ad_hoc.ps_combine_update_download_xyang_tmp
-- T2.  di_ad_hoc.ps_session_restricted_xyang_tmp
-- T3.  di_analysis.di_release_dates
-- 
-- TASK
-- 1.  Merge user update/download with first use data 
-- 2.  Merge product release date with user product update/download/first use
-- 3.  Compute days from product release to user product update/download/first use
-- ==============================================================================

drop table di_ad_hoc.ps_user_product_recency_xyang_tmp;

create table di_ad_hoc.ps_user_product_recency_xyang_tmp as

select 
        
        new.member_guid,
        new.version,
        
        -- update/download
        datediff(first_dl_update, release_start) as days_release_to_dl_update,
        
        -- use
        datediff(first_session_date, release_start) as days_release_to_first_use,
        datediff(first_session_date, first_dl_update) as days_dl_update_to_first_use

from 
        -- Product update and download data combined
        (
        select 
                member_guid,
                version,
                
                (case when update_date is not null then update_date
                      when first_dl_start_date is not null then first_dl_start_date
                      else NULL end) as first_dl_update
                
        from di_ad_hoc.ps_combine_update_download_xyang_tmp
        ) as new 
        
        JOIN
        
        -- User's first use of a product version
        (
        select 
                member_guid,
                productversion,
                min(to_date(sessiontime)) as first_session_date
              
        
        from 
                di_ad_hoc.ps_session_restricted_xyang_tmp
        
        group by 
                member_guid,
                productversion
                
        ) as use
        
        ON use.productversion = new.version AND use.member_guid = new.member_guid
        
        JOIN
        
        -- Product release date
        (
        select distinct version, release_start
        from di_analysis.di_release_dates
        where product = 'PHOTOSHOP'
        ) as r
        
        ON new.version = r.version

;