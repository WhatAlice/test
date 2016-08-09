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
-- 1.  Summarize machine information
-- ==============================================================================

drop table di_ad_hoc.ps_machine_summary_xyang_tmp;

create table di_ad_hoc.ps_machine_summary_xyang_tmp as

select 
      member_guid,
      count(distinct sessiontime) as num_session,
      
      -- summarize session duration
      min(duration) as min_session_duration,
      max(duration) as max_session_duration,
      round(avg(duration),0) as mean_session_duration,
      
      -- summarize session date
      min(to_date(sessiontime)) as first_session_date,
      max(to_date(sessiontime)) as last_session_date,
      datediff(max(to_date(sessiontime)), min(to_date(sessiontime))) as days_between_sessions,
      
      -- summarize product 
      count(distinct productversion) as num_productversion,
      count(distinct productbuild) as num_productbuild,
      count(distinct productlanguage) as num_productlanguage,
      (case when sum(cast(productlanguage LIKE 'en%' as INT)) >= 1 then 'Yes' else 'No' END) as use_english,
      count(distinct executableversion) as num_executableversion,
      (case when sum(cast(country LIKE 'United States' as INT)) >= 1 then 'Yes' else 'No' END) as in_US,
      
      -- summarize machine information
      count(distinct machineid) as num_machine,
      count(distinct processor) as num_processortype,     -- 586 8664 x86_64
      min(memory) as min_memory,
      max(memory) as max_memory,
      min(numprocessors) as min_numprocessor,
      max(numprocessors) as max_numprocessors,
      min(cast(substr(speed, 1, 4) as INT)) as min_processor_speed,
      max(cast(substr(speed, 1, 4) as INT)) as max_processor_speed,
      count(distinct osversion) as num_osversion,
      count(distinct ostype) as num_ostype,
      count(distinct osname) as num_osname,
      count(distinct processormodel) as num_processormodel,
      count(distinct processorname) as num_processorname,
      min(monitorcount) as min_num_monitor,
      max(monitorcount) as max_num_monitor,
      
      -- Distinguish between MAC and WIN
      (case when sum(cast((
                            (osversion LIKE 'OS%') OR
                            (osname IN ('El Capitan', 'Yosemite'))
                          ) AS INT)) >= 1 then 'Yes' else 'No' END) as has_mac,
      (case when sum(cast((
                            (osversion like '4.%') OR
                            (osversion like '5.%') OR
                            (osversion like '6.%') OR
                            (osversion like '10.%') OR
                            (osname Like 'Win%')
                          ) AS INT)) >= 1 then 'Yes' else 'No' END) as has_win,
                          
      -- Summarize abnormal exit
      avg (case when exitstatus = 'abnormal' then 1 else 0 end) as proportionAbnormal
      

from 
      di_ad_hoc.ps_session_restricted_xyang_tmp 

group by 
      member_guid

;