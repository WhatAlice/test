set mapred.output.compress=true;
set hive.exec.compress.output=true;
set mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
set io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec;
set hive.hadoop.supports.splittable.combineinputformat=true;
set hive.auto.convert.join=true;
use di_ad_hoc;

-- =============================================================================
-- TABLE
-- T1.  di_ad_hoc.ps_session_restricted_xyang_tmp
-- T2.  di_analysis.di_computer_dim
-- 
-- TASK
-- 1.  Compute computer age
-- ==============================================================================

drop table di_ad_hoc.ps_machine_age_xyang_tmp;

create table di_ad_hoc.ps_machine_age_xyang_tmp as

select 
        member_guid,
        avg(age) as mean_machine_age,
        max(age) as max_machine_age,
        min(age) as min_machine_age
        
from
        (
        select 
              comp.member_guid,
              cd.year,
              (2016 - cast(cd.year as int)) as age
              
        
        from 
              (
              select
                		CASE 
                			WHEN s.osversion like 'OS %' THEN regexp_replace(processormodel, ",",";")
                			WHEN s.processorname like '%i5-3317U%' and width in ('1920', '1080') and height in ('1920', '1080') 
                				THEN concat(s.processorname, '--1920X1080')
                			WHEN s.processorname like '%i5-4200U%' and width in ('1920', '1080') and height in ('1920', '1080') 
                				THEN concat(s.processorname, '--1920X1080')
                			WHEN s.processorname like '%i5-4300U%' and width in ('1920', '1080') and height in ('1920', '1080') 
                				THEN concat(s.processorname, '--1920X1080')
                			WHEN s.processorname like '%i3-4020Y%' and width in ('2160', '1440') and height in ('2160', '1440')
                				THEN concat(s.processorname, '--2160X1440')
                			WHEN s.processorname like '%i5-4300U%' and width in ('2160', '1440') and height in ('2160', '1440')
                				THEN concat(s.processorname, '--2160X1440')
                			WHEN s.processorname like '%i7-4650U%' and width in ('2160', '1440') and height in ('2160', '1440')
                				THEN concat(s.processorname, '--2160X1440')
                			WHEN s.processorname like '%m3-6Y30%' and width in ('2736', '1824') and height in ('2736', '1824')
                				THEN concat(s.processorname, '--2736X1824')
                			WHEN s.processorname like '%i5-6300U%' and width in ('2736', '1824') and height in ('2736', '1824')
                				THEN concat(s.processorname, '--2736X1824')
                			WHEN s.processorname like '%i7-6650U%' and width in ('2736', '1824') and height in ('2736', '1824')
                				THEN concat(s.processorname, '--2736X1824')
                			ELSE regexp_replace(s.processorname, ",",";") END as model_key,
                		
                		s.member_guid
        
            	from 
            	
            	      di_ad_hoc.ps_session_restricted_xyang_tmp as s
            		    left outer join 
            		    cc_photoshopstar.headlightsscreenresolution as hsr 
            		    on (s.sessionid = hsr.sessionid and (hsr.year <= '2016' and hsr.month <= '12'))                                             
        
        
        		 
            	group by
                		CASE 
                			WHEN s.osversion like 'OS %' THEN regexp_replace(processormodel, ",",";")
                			WHEN s.processorname like '%i5-3317U%' and width in ('1920', '1080') and height in ('1920', '1080') 
                				THEN concat(s.processorname, '--1920X1080')
                			WHEN s.processorname like '%i5-4200U%' and width in ('1920', '1080') and height in ('1920', '1080') 
                				THEN concat(s.processorname, '--1920X1080')
                			WHEN s.processorname like '%i5-4300U%' and width in ('1920', '1080') and height in ('1920', '1080') 
                				THEN concat(s.processorname, '--1920X1080')
                			WHEN s.processorname like '%i3-4020Y%' and width in ('2160', '1440') and height in ('2160', '1440')
                				THEN concat(s.processorname, '--2160X1440')
                			WHEN s.processorname like '%i5-4300U%' and width in ('2160', '1440') and height in ('2160', '1440')
                				THEN concat(s.processorname, '--2160X1440')
                			WHEN s.processorname like '%i7-4650U%' and width in ('2160', '1440') and height in ('2160', '1440')
                				THEN concat(s.processorname, '--2160X1440')
                			WHEN s.processorname like '%m3-6Y30%' and width in ('2736', '1824') and height in ('2736', '1824')
                				THEN concat(s.processorname, '--2736X1824')
                			WHEN s.processorname like '%i5-6300U%' and width in ('2736', '1824') and height in ('2736', '1824')
                				THEN concat(s.processorname, '--2736X1824')
                			WHEN s.processorname like '%i7-6650U%' and width in ('2736', '1824') and height in ('2736', '1824')
                				THEN concat(s.processorname, '--2736X1824')
                			ELSE regexp_replace(s.processorname, ",",";") END,
                		s.member_guid
        	    ) comp
              
              LEFT OUTER JOIN
              
              di_analysis.di_computer_dim as cd
              
              on (comp.model_key = cd.model_key)
        
        ) as tmp
        
group by 
      member_guid

;