set mapred.output.compress=true;
set hive.exec.compress.output=true;
set mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
set io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec;
set hive.hadoop.supports.splittable.combineinputformat=true;
use di_ad_hoc;

-- =============================================================================
-- TABLE
-- T1.  di_ad_hoc.ps_feature_usage_recount_xyang_tmp
-- T2.  di_ad_hoc.ps_cluster_6dot02_feature_mapping_exp20160311 --> to map feature
-- 
-- TASK
-- 1.  Map feature to clustergroupoing
-- ==============================================================================

drop table di_ad_hoc.ps_feature_usage_mapped_xyang_tmp;

create table di_ad_hoc.ps_feature_usage_mapped_xyang_tmp as

select 
        t.member_guid, 
        t.clustergrouping,
        
        count(distinct t.sessionid) as num_session_feature,                          -- number of sessions that use the feature
        sum(t.count)/count(distinct t.sessionid) as mean_session_count,              -- average count of feature usage over all the sessions that use the feature
        count(distinct t.sessionid)/s.total_num_session as feature_session_freq,      -- percentage of sessions that use the feature among all available sessions
        s.total_num_session


from
        
        -- feature mapping
        (
        select f.*, mp.clustergrouping
        
        from 
                di_ad_hoc.ps_feature_usage_xyang_tmp as f
                
                JOIN
                
                (select substr(new_featurename, 17) as new_featurename, clustergrouping
                from di_ad_hoc.ps_cluster_6dot02_feature_mapping_exp20160311) as mp
                
                on f.new_featurename = mp.new_featurename
        ) as t
        
        JOIN
        
        -- calculate total number of session for the defined period
        (
        select 
                member_guid,
                count(distinct sessionid) as total_num_session
                
        from  di_ad_hoc.ps_session_restricted_xyang_tmp
        group by member_guid
        ) as s

        ON t.member_guid = s.member_guid

group by t.member_guid, t.clustergrouping, s.total_num_session
;