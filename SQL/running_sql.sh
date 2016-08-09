#!/bin/bash

#==============================================================
# PHOTOSHOP 
#==============================================================
hive -f CCP_member_info_clean_20160701.sql
hive -f ps_restrict_session.sql
hive -f ps_screen_resolution.sql
hive -f ps_machine_summary.sql
# Summary user update behavior
hive -f ps_combine_update_download.sql
hive -f ps_user_product_recency.sql
hive -f ps_user_product_recency_summary.sql
# Merge data
hive -f ps_merge_user_machine.sql
# Feature usage
hive -f ps_feature_usage.sql
hive -f ps_feature_usage_mapped.sql
# Download
# hdfs dfs -cat '/user/hive/warehouse/di_ad_hoc.db/ps_merged_user_machine_xyang_tmp/*' > ./ps_merged_user_machine.gz
hdfs dfs -cat '/user/hive/warehouse/di_ad_hoc.db/ps_feature_usage_mapped_xyang_tmp/*' > ./ps_feature_usage_mapped.gz
