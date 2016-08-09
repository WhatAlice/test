set mapred.output.compress=true;
set hive.exec.compress.output=true;
set mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
set io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec;
set hive.hadoop.supports.splittable.combineinputformat=true;
use di_ad_hoc;

-- =============================================================================
-- TABLE
-- T1.  cc_photoshopstar.session_mod  --> To be subsetted
-- T2.  di_ad_hoc.CCP_member_info_clean_xyang_tmp --> Subset condition for session time
-- 
-- TASK
-- 1.  To restrict sessions under consideration to be between 
--     first_cc_subscription_date and last_cancellation_date
-- ==============================================================================

drop table di_ad_hoc.ps_session_restricted_xyang_tmp;

create table di_ad_hoc.ps_session_restricted_xyang_tmp as

select  s.*, first_cc_subscription_date, last_cancellation_date
from
        (select substr(pguid, 1, 24) as member_guid, *
        from cc_photoshopstar.session_mod) as s
        
        JOIN
        
        -- Get first and last day of subscription to limit session 
        (
        select 
                member_guid, 
                first_cc_subscription_date, 
                last_cancellation_date
                
        from 
                di_ad_hoc.CCP_member_info_clean_20160701_xyang_tmp
                
        ) as d
        
        ON s.member_guid = d.member_guid

where 
        year <= '2016' AND month <= '12'
        AND
        (
        -- To restrict session to be within first and last day of subscription
        (to_date(s.sessiontime) between d.first_cc_subscription_date and d.last_cancellation_date)
        OR (to_date(s.sessiontime) >= d.first_cc_subscription_date and d.last_cancellation_date is null)
        )

;
        