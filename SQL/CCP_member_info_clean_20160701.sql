set mapred.output.compress=true;
set hive.exec.compress.output=true;
set mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
set io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec;
set hive.hadoop.supports.splittable.combineinputformat=true;
use di_ad_hoc;

-- =============================================================================
-- TABLE
-- T1.  ccmusg.dim_member  --> To be subsetted
-- T2.  ccmusg.member_scd  --> Subset condition for CCP member
-- 
-- TASK
-- 1.  Restrict to CCP users
-- ==============================================================================

drop table di_ad_hoc.CCP_member_info_clean_20160701_xyang_tmp;

create table di_ad_hoc.CCP_member_info_clean_20160701_xyang_tmp as

select	
        dm.member_guid,
        
				cc_joining_date,
				first_cc_subscription_date,
				
				-- Define subscription status
				(case when isnull(first_cc_subscription_date) 
    				  then 0 else 1 
    				  end) as first_cc_subscription_status,
    				  
				(case when isnull(first_cc_subscription_date)
    					then datediff(to_date(from_unixtime(unix_timestamp())), cc_joining_date)
    					else datediff(first_cc_subscription_dts, cc_joining_date)
    					end) as days_join_to_subscribe,
				
				is_free_to_paid,
				is_joined_as_free,
				business_group,
				
				-- Re-categorize target group
				target_group,
				(case when (UPPER(target_group) LIKE '%STUDENT%') THEN 'STUDENT'
				      when (UPPER(target_group) LIKE '%FAC/STAFF%') THEN 'FAC/STAFF'
				      when (UPPER(target_group) LIKE '%ADMIN%') THEN 'ADMIN'
				      when (UPPER(target_group) LIKE '%DEVELOPER%') THEN 'DEVELOPER'
				      when (UPPER(target_group) LIKE '%DESIGNER%') THEN 'DESIGNER'
				      when (UPPER(target_group) LIKE '%MOBILE USER%') THEN 'MOBILE USER'
				      when (UPPER(target_group) LIKE '%SAAS USER%') THEN 'SAAS USER'
				      when (UPPER(target_group) LIKE '%GOVERNMENT%') THEN 'GOVERNMENT'
				      else UPPER(target_group) END) as target_group_recat,
				
				is_adobe_employee,
				
				country_code,
				(case when country_code = 'US' then 'Yes'
				      else 'No' end) as is_USA,
				
				last_cancellation_date,
				
				-- Define cancellation status
				(case when isnull(last_cancellation_date) then 0 
				      else 1 end) as cc_cancellation_status,
				(case when isnull(last_cancellation_date) 
    					then datediff(to_date(from_unixtime(unix_timestamp())), first_cc_subscription_date)
    					else datediff(last_cancellation_date, first_cc_subscription_date)
    					end) as cc_cancellation_timeTo,
    		
    		-- Re-categorize most launched product			
				most_launched_product,
				(case when (UPPER(most_launched_product) LIKE '%ACROBAT%') THEN 'ACROBAT'
				      when (UPPER(most_launched_product) LIKE '%FLASH%') THEN 'FLASH'
				      when (UPPER(most_launched_product) LIKE '%PREMIERE%') THEN 'PREMIERE'
				      when (UPPER(most_launched_product) IN 
				              ('ADOBE MEDIA ENCODER', 'CONTRIBUTE', 'CREATIVE CLOUD', 'EDGE ANIMATE',
				              'FIREWORKS', 'INCOPY', 'PRELUDE', 'PRESENTER VX', 'SPEEDGRADE')) THEN 'OTHER'
				      else UPPER(most_launched_product) END) as most_launched_product_recat,
				
				-- First product launched
				first_product_launched_as_free,
				first_product_launched_as_paid,
				(case when (UPPER(first_product_launched_as_free) LIKE '%ACROBAT%') THEN 'ACROBAT'
				      when (UPPER(first_product_launched_as_free) LIKE '%FLASH%') THEN 'FLASH'
				      when (UPPER(first_product_launched_as_free) LIKE '%PREMIERE%') THEN 'PREMIERE'
				      when (UPPER(first_product_launched_as_free) IN 
				              ('ADOBE MEDIA ENCODER', 'CONTRIBUTE', 'CREATIVE CLOUD', 'EDGE ANIMATE',
				              'FIREWORKS', 'INCOPY', 'PRELUDE', 'PRESENTER VX', 'SPEEDGRADE')) THEN 'OTHER'
				      else UPPER(first_product_launched_as_free) END) as first_product_launched_as_free_recat,
				      
				(case when (UPPER(first_product_launched_as_paid) LIKE '%ACROBAT%') THEN 'ACROBAT'
				      when (UPPER(first_product_launched_as_paid) LIKE '%FLASH%') THEN 'FLASH'
				      when (UPPER(first_product_launched_as_paid) LIKE '%PREMIERE%') THEN 'PREMIERE'
				      when (UPPER(first_product_launched_as_paid) IN 
				              ('ADOBE MEDIA ENCODER', 'CONTRIBUTE', 'CREATIVE CLOUD', 'EDGE ANIMATE',
				              'FIREWORKS', 'INCOPY', 'PRELUDE', 'PRESENTER VX', 'SPEEDGRADE')) THEN 'OTHER'
				      else UPPER(first_product_launched_as_paid) END) as first_product_launched_as_paid_recat,
				
				geo,
				skill,
				
				-- Remove unexpected value in job
				(case when (UPPER(job) IN ('', 'BYDLO', 'DDEVAUX')) THEN 'OTHER'
				      when (UPPER(job) IN ('UNEXPECTEDVALUE', 'NULL')) THEN NULL
				      else UPPER(job) END) as job,
				
				-- Remove unexpected value in purpose
				(case when (UPPER(purpose) IN ('UNEXPECTEDVALUE', 'NULL')) THEN NULL
				      when (UPPER(purpose) IN ('', 'JUST_BECAUSE')) THEN 'OTHER'
				      else UPPER(purpose) end) as purpose,
				
				
				business_segment,
				
				-- Re-categorize signup source
				signup_source,
				(case when (upper(signup_source) LIKE 'AAM') THEN 'AAM'
				      when (upper(signup_source) LIKE 'ADOBE%') THEN 'ADOBE'
				      when (upper(signup_source) LIKE 'AFTER EFFECTS%') THEN 'AFTER EFFECTS'
				      when (upper(signup_source) LIKE 'AVIARY%') THEN 'AVIARY'
				      when (upper(signup_source) LIKE 'BEHANCE%') THEN 'BEHANCE'
				      when (upper(signup_source) LIKE 'CAPTURE%') THEN 'CAPTURE'
				      when (upper(signup_source) LIKE 'COLOR') THEN 'COLOR'
				      when (upper(signup_source) LIKE 'CREATIVE CLOUD') THEN 'CREATIVE CLOUD'
				      when (upper(signup_source) LIKE 'DRAW%') THEN 'DRAW'
				      when (upper(signup_source) LIKE 'DREAMWEAVER%') THEN 'DREAMWEAVER'
				      when (upper(signup_source) LIKE 'ILLUSTRATOR') THEN 'ILLUSTRATOR'
				      when (upper(signup_source) LIKE 'INDESIGN%') THEN 'INDESIGN'
				      when (upper(signup_source) LIKE 'KULER%') THEN 'KULER'
				      when (upper(signup_source) LIKE 'LIGHTROOM%') THEN 'LIGHTROOM'
				      when (upper(signup_source) LIKE 'MARVEL%') THEN 'MARVEL'
				      when (upper(signup_source) LIKE 'MIXAMO%') THEN 'MIXAMO'
				      when (upper(signup_source) LIKE 'MUSE%') THEN 'MUSE'
				      when (upper(signup_source) LIKE 'OLD DESKTOP - NO EVENT SOURCE') THEN 'OLD DESKTOP - NO EVENT SOURCE'
				      when (upper(signup_source) LIKE 'PHONE GAP%') THEN 'PHONE GAP'
				      when (upper(signup_source) LIKE 'PHOTOSHOP%') THEN 'PHOTOSHOP'
				      when (upper(signup_source) LIKE 'PREMIERE%') THEN 'PREMIERE'
				      when (upper(signup_source) LIKE 'PREVIEW%') THEN 'PREVIEW'
				      when (upper(signup_source) LIKE 'SKETCH%') THEN 'SKETCH'
				      when (upper(signup_source) LIKE 'SLATE%') THEN 'SLATE'
				      when (upper(signup_source) LIKE 'STOCK%') THEN 'STOCK'
				      ELSE 'OTHER' END) as signup_source_recat,
				
				-- Remove MAPPING TBD
				(case when (upper(signup_category) = 'MAPPING TBD') then NULL
				      else upper(signup_category) END) as signup_category,
				      
			  -- from member_scd
			  entitlement_period, 
			  market_segment,
			  route_to_market,
        regular_or_promo
				
from 
        ccmusg.dim_member as dm
        
        INNER JOIN
        
        -- CCP restriction data
        ccmusg.member_scd as scd
        
        ON  (dm.member_guid = scd.member_guid 
            and dm.first_cc_subscription_dts = scd.effective_from_dts)
            
where 

      -- Restrict to CCP 
      (scd.entitlement_type = 'PP' and scd.phlt_flag > 0)
      
      AND
      
      -- Consider joined users
      (first_cc_subscription_date >= cc_joining_date     -- users who join and subscribe
      OR isnull(first_cc_subscription_date))             -- users who join and haven't subscribe
        
;