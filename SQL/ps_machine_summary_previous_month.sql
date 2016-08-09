set mapred.output.compress=true;
set hive.exec.compress.output=true;
set mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
set io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec;
set hive.hadoop.supports.splittable.combineinputformat=true;
use di_ad_hoc;

-- =============================================================================
-- TABLE
-- T1.  di_ad_hoc.ps_session_restricted_xyang_tmp
-- 
-- TASK
-- 1.  Summarize machine information for the month prior to cancellation
-- ==============================================================================

drop table di_ad_hoc.ps_machine_summary_previous_month_xyang_tmp;

create table di_ad_hoc.ps_machine_summary_previous_month_xyang_tmp as

select 
      member_guid,
      count(distinct sessiontime) as num_session,
      
      -- summarize session duration
      min(duration) as min_session_duration,
      max(duration) as max_session_duration,
      round(avg(duration),0) as mean_session_duration,
                          
      -- Summarize abnormal exit
      avg (case when exitstatus = 'abnormal' then 1 else 0 end) as proportionAbnormal
      

from 
      di_ad_hoc.ps_session_restricted_xyang_tmp 
      
where 
      -- restrict session to be one month prior to date of cancellation  
      to_date(sessiontime) between date_add(last_cancellation_date, -30) and last_cancellation_date

group by 
      member_guid

;