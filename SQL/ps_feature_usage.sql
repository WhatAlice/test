set mapred.output.compress=true;
set hive.exec.compress.output=true;
set mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
set io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec;
set hive.hadoop.supports.splittable.combineinputformat=true;
use di_ad_hoc;

-- =============================================================================
-- TABLE
-- T1.  cc_photoshopstar.hb_actions  --> To be subsetted
-- T2.  di_ad_hoc.ps_session_restricted_xyang_tmp --> Subset condition for session
-- 
-- TASK
-- 1.  Clean featurename
-- 2.  Count feature usage based on cleaned featurename
-- ==============================================================================

drop table di_ad_hoc.ps_feature_usage_xyang_tmp;

create table di_ad_hoc.ps_feature_usage_xyang_tmp as


select 
    t3.member_guid,
    t3.new_featurename,
    
    t3.sessionid,
    t3.count
    
from
    
    -- t3
    (
    select 
        t2.member_guid,
        t2.sessionid,
        t2.featurename,
        t2.featurename2,
        
        -- Clean featurename
        (regexp_replace(
    				regexp_replace(
    					regexp_replace(
    						regexp_replace(
    							regexp_replace(
    								regexp_replace(
    									regexp_replace(
    										regexp_replace(
    											regexp_replace(
    												regexp_replace(
    													regexp_replace(
    														regexp_replace(
    															regexp_replace(
    																lower(featurename2), 	    -- Make lower
    															'_{2,}', '_'), 							-- Convert consecutive '-' to single '-'
    														'_$', ''), 										-- Remove ending '_'
    													'_v[0-9]+_[0-9][a-z]$', ''), 		-- Remove ending 'v123_1a'
    												'_v([0-9]+_)+', '_'), 						-- Convert '_v12_' to '_'
    											'(_[0-9]+)+$', ''), 								-- Remove ending '_12345'
    										'_([0-9]+_)+', '_'), 									-- Convert '_1234_' to '_'
    									'_jk[m]{0,1}$', ''), 										-- Remove ending '_jk' or '_jkm'
    								'_[v|r][0-9]+$', ''), 										-- Remove ending '_v123' and '_r123'
    							'_[a-z]$', ''), 														-- Remove ending '_a' or '_b', etc ...
    						'_[0-9]+bw$', ''), 														-- Remove ending '_123bw'
    					'_x64$', ''), 																	-- Remove ending '_x64'
    				'_[0-9]+px_', '_'), 															-- Remove '_123px_'
    			'^_+', '')																					-- Remove '_' characters at the beginning of the string
    		) as new_featurename,
    		
    		t2.count
    		
    from
        
        -- t2
        (
        select
              t1.member_guid, 
              t1.sessionid,
              t1.englishname as featurename, 

              -- rough clean featurename
              (CASE	
            				WHEN (englishname LIKE 'Brightness_Contrast_%_Layer') THEN 'New_Brightness_Contrast_Layer'
            				WHEN (englishname LIKE 'Levels_%_Layer') THEN 'New_Levels_Layer'
            				WHEN (englishname LIKE 'Curves_%_Layer') THEN 'New_Curves_Layer'
            				WHEN (englishname LIKE 'Exposure_%_Layer') THEN 'New_Exposure_Layer'
            				WHEN (englishname LIKE 'Vibrance_%_Layer') THEN 'New_Vibrance_Layer'
            				WHEN (englishname LIKE 'Hue_Saturation_%_Layer') THEN 'New_Hue_Saturation_Layer'
            				WHEN (englishname LIKE 'Color_Balance_%_Layer') THEN 'New_Color_Balance_Layer'
            				WHEN (englishname LIKE 'Black___White_%_Layer') THEN 'New_Black_White_Layer'
            				WHEN (englishname LIKE 'Photo_Filter_%_Layer') THEN 'New_Photo_Filter_Layer'
            				WHEN (englishname LIKE 'Channel_Mixer_%_Layer') THEN 'New_Channel_Mixer_Layer'
            				WHEN (englishname LIKE 'Color_Lookup_%_Layer') THEN 'New_Color_Lookup_Layer'
            				WHEN (englishname LIKE 'Invert_%_Layer') THEN 'New_Invert_Layer'
            				WHEN (englishname LIKE 'Posterize_%_Layer') THEN 'New_Posterize_Layer'
            				WHEN (englishname LIKE 'Threshold_%_Layer') THEN 'New_Threshold_Layer'
            				WHEN (englishname LIKE 'Gradient_Map_%_Layer') THEN 'New_Gradient_Map_Layer'
            				WHEN (englishname LIKE 'Selective_Color_%_Layer') THEN 'New_Selective_Color_Layer'
            				WHEN (englishname LIKE 'Set_%px_space_between_%_layers') THEN 'Set_space_between_layers'
            				WHEN (englishname LIKE 'Set_auto_space_between_%_layers') THEN 'Set_auto_space_between_layers'
				            ELSE englishname
			      END) as featurename2,
			      
			      t1.count
              
        from 
                
                -- t1
                (
                select
                        f.member_guid, 
                        f.sessionid,
                        f.category,
                        f.subcategory,
                        f.englishname,
                        f.count
                
                from
                        -- Normalize English featurename
                        (
                        select 
                                substr(pguid, 1, 24) as member_guid,
                                sessionid,
                                category,
                                
                                (case when (subcategory regexp "[a-z][a-z]_[A-Z][A-Z]") then "en_US"    -- Convert lanugage keys (xx_XX) to en_US
                                      else subcategory end) as subcategory,
                                
                                regexp_replace(regexp_replace(englishname, "&(?=[a-zA-Z]+)", ""), "[^A-Za-z0-9]", "_") as englishname,
                                
                                continuouscount as count
                        
                        from cc_photoshopstar.feature
                        where (year <= "2016" AND month <= "12")
                        ) as f
                        
                ) as t1
          
          where t1.category = 'history'
                OR t1.englishname LIKE "Generator__Generated_asset_count%" 
			          OR t1.englishname = "Save_For_Web"
			          OR t1.englishname = "print_succeeded"
			          OR t1.englishname = "Save_As"
			          OR t1.englishname = "Save_Selection"
			          OR t1.englishname = "save_workspace"
        
        
        ) as t2
        
    ) as t3
    
    JOIN 
    
    -- restrict session
    di_ad_hoc.ps_session_restricted_xyang_tmp as s
    
    ON t3.member_guid = s.member_guid and t3.sessionid = s.sessionid

;