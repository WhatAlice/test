set mapred.output.compress=true;
set hive.exec.compress.output=true;
set mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
set io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec;
set hive.hadoop.supports.splittable.combineinputformat=true;
use di_ad_hoc;

-- =============================================================================
-- TABLE
-- T1.  cc_photoshopstar.headlightsscreenresolution
-- 
-- TASK
-- 1.  Summarize screen size corresponding to min/max resolution
-- 2.  Summarize whether screen is widescreen
-- ==============================================================================

drop table di_ad_hoc.ps_screen_resolution_xyang_tmp;

create table di_ad_hoc.ps_screen_resolution_xyang_tmp as

select 
        substr(s.pguid, 1, 24) as member_guid,
        max(height*width) as maxResolution, 
        min(height*width) as minResolution,
        (case when max(width/height) >= 1.6 then 'Yes' else 'No' end) as hasWideScreen
        
from 
        cc_photoshopstar.headlightsscreenresolution as s
        
        JOIN
        
        di_ad_hoc.ps_session_restricted_xyang_tmp as m
        
        ON s.pguid = m.pguid AND s.sessionid = m.sessionid
        
where s.year <= '2016' AND s.month <= '12'
group by s.pguid
;



