set mapred.output.compress=true;
set hive.exec.compress.output=true;
set mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
set io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec;
set hive.hadoop.supports.splittable.combineinputformat=true;
use di_ad_hoc;

-- =============================================================================
-- TABLE
-- T1.  di_ad_hoc.ps_user_product_recency_xyang_tmp
-- 
-- TASK
-- 1.  Summarize for each user 
--     days from product release to user product update/download/first use
-- 2.  Exclude negative days in summary
-- ==============================================================================

drop table di_ad_hoc.ps_user_product_recency_summary_xyang_tmp;

create table di_ad_hoc.ps_user_product_recency_summary_xyang_tmp as

select 
        member_guid,
        
        min(days_release_to_dl_update) as min_days_release_to_dl_update,
        max(days_release_to_dl_update) as max_days_release_to_dl_update,
        avg(days_release_to_dl_update) as mean_days_release_to_dl_update,
        
        min(days_release_to_first_use) as min_days_release_to_first_use,
        max(days_release_to_first_use) as max_days_release_to_first_use,
        avg(days_release_to_first_use) as mean_days_release_to_first_use,
        
        min(days_dl_update_to_first_use) as min_days_dl_update_to_first_use,
        max(days_dl_update_to_first_use) as max_days_dl_update_to_first_use,
        avg(days_dl_update_to_first_use) as mean_days_dl_update_to_first_use
        
from di_ad_hoc.ps_user_product_recency_xyang_tmp
where days_release_to_dl_update >= 0 and days_release_to_first_use >= 0 and days_dl_update_to_first_use >= 0
group by member_guid

;