set mapred.output.compress=true;
set hive.exec.compress.output=true;
set mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
set io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec;
set hive.hadoop.supports.splittable.combineinputformat=true;
use di_ad_hoc;

-- =============================================================================
-- TABLE
-- T1.  di_ad_hoc.CCP_member_info_clean_20160701_xyang_tmp
-- T2.  di_ad_hoc.ps_machine_summary_xyang_tmp
-- T3.  di_ad_hoc.ps_screen_resolution_xyang_tmp
-- T4.  di_ad_hoc.ps_user_product_recency_summary_xyang_tmp
-- 
-- TASK
-- 1.  Merge data
-- ==============================================================================

drop table di_ad_hoc.ps_merged_user_machine_xyang_tmp;

create table di_ad_hoc.ps_merged_user_machine_xyang_tmp as

select 
        
        u.member_guid,
        
        -- from di_ad_hoc.CCP_member_info_clean_xyang_tmp
    		cc_joining_date,
    		first_cc_subscription_date,
    		first_cc_subscription_status,
    		days_join_to_subscribe,
    		is_free_to_paid,
    		is_joined_as_free,
    		business_group,
    		target_group,
    		target_group_recat,
    		is_adobe_employee,
    		country_code,
    		is_USA,
    		last_cancellation_date,
    		cc_cancellation_status,
    		cc_cancellation_timeTo,
    		most_launched_product,
    		most_launched_product_recat,
    		geo,
    		skill,
    		job,
    		purpose,
    		business_segment,
    		signup_source,
    		signup_source_recat,
    		signup_category,
    		entitlement_period, 
    	  market_segment,
    	  route_to_market,
        regular_or_promo,
    		
    		-- from di_ad_hoc.ps_machine_summary_xyang_tmp
    		m.num_session,
    		m.min_session_duration,
    		m.max_session_duration,
    		m.mean_session_duration,
    		first_session_date,
    		last_session_date,
    		days_between_sessions,
    		num_productversion,
    		num_productbuild,
    		num_productlanguage,
    		use_english,
    		in_US,
    		num_machine,
    		num_processortype,
    		min_memory,
    		max_memory,
    		min_numprocessor,
    		max_numprocessors,
    		min_processor_speed,
    		max_processor_speed,
    		num_osversion,
    		num_ostype,
    		num_osname,
    		num_processormodel,
    		num_processorname,
    		min_num_monitor,
    		max_num_monitor,
    		has_mac,
    		has_win,
    		(case when (has_mac = 'Yes' and has_win = 'Yes') then 'Yes'
    		      when (has_mac is null and has_win is null) then null
    		      else 'No' end) has_both,
    		m.proportionAbnormal,
    		
    		-- from di_ad_hoc.ps_screen_resolution_xyang_tmp
        maxResolution, 
        minResolution,
        hasWideScreen,
        
        -- from di_ad_hoc.ps_user_product_recency_summary_xyang_tmp
        min_days_release_to_dl_update,
        max_days_release_to_dl_update,
        mean_days_release_to_dl_update,
        (case when max_days_release_to_dl_update <= 30 then 'Always within 30 days'
              when max_days_release_to_dl_update <= 365 then 'Always within 1 year'
              when max_days_release_to_dl_update is null then null
              else 'Always greater than 1 year' end) as days_release_to_dl_update_cat,
        
        min_days_release_to_first_use,
        max_days_release_to_first_use,
        mean_days_release_to_first_use,
        (case when max_days_release_to_first_use <= 30 then 'Always within 30 days'
              when max_days_release_to_first_use <= 365 then 'Always within 1 year'
              when max_days_release_to_first_use is null then null
              else 'Always greater than 1 year' end) as days_release_to_first_use_cat,
        
        min_days_dl_update_to_first_use,
        max_days_dl_update_to_first_use,
        mean_days_dl_update_to_first_use,
        (case when max_days_dl_update_to_first_use <= 30 then 'Always within 30 days'
              when max_days_dl_update_to_first_use <= 365 then 'Always within 1 year'
              when max_days_dl_update_to_first_use is null then null
              else 'Always greater than 1 year' end) as days_dl_update_to_first_use_cat,
              
        
        -- from data_discovery.phsp_historical_clusters_names      
        c.clustername,
        c.cluster,
        
        -- from di_ad_hoc.ps_machine_summary_previous_month_xyang_tmp
        (case when r.num_session is null then 0 else r.num_session end) as num_session_previous,
        (case when r.min_session_duration is null then 0 else r.min_session_duration end) as min_session_duration_previous,
        (case when r.max_session_duration is null then 0 else r.max_session_duration end) as max_session_duration_previous,
        (case when r.mean_session_duration is null then 0 else r.mean_session_duration end) as mean_session_duration_previous,
        (case when r.proportionAbnormal is null then 0 else r.proportionAbnormal end) as proportionAbnormal_previous,
        
        -- from di_ad_hoc.ps_machine_age_xyang_tmp
        a.mean_machine_age,
        a.max_machine_age,
        a.min_machine_age
        
		
from
    
        di_ad_hoc.CCP_member_info_clean_20160701_xyang_tmp as u
        
        JOIN di_ad_hoc.ps_machine_summary_xyang_tmp as m
            ON u.member_guid = m.member_guid
        
        JOIN data_discovery.phsp_historical_clusters_names as c
            ON m.member_guid = c.member_guid
            
        LEFT OUTER JOIN di_ad_hoc.ps_screen_resolution_xyang_tmp as s
            ON s.member_guid = m.member_guid AND s.member_guid = u.member_guid
            
        LEFT OUTER JOIN di_ad_hoc.ps_user_product_recency_summary_xyang_tmp as p
            ON p.member_guid = s.member_guid AND p.member_guid = m.member_guid AND p.member_guid = u.member_guid
            
        LEFT OUTER JOIN di_ad_hoc.ps_machine_summary_previous_month_xyang_tmp as r
            ON m.member_guid = r.member_guid
            
        LEFT OUTER JOIN di_ad_hoc.ps_machine_age_xyang_tmp as a
            ON m.member_guid = a.member_guid
            
;