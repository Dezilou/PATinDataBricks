Operational JAR Logic, applied to PAT Intermediate Table - South West
NHS England South West Intelligence and Insights
The Southwest Elective Delivery Dashboard provides a summary of elective waiting lists and activity recovery. This script focuses on the elective aspect of the dashboard, tracking recovery progress against both planned targets and the 2023/24 baseline where applicable. It integrates the latest publicly available monthly data, broken down by week, and appends the most recent unpublished weekly data to ensure reporting is up to date with the latest available figures.

Scripts
Provider Focus
📝 National JAR methodology - Accident and Emergency Attendance - Provider - PAT
📝 National JAR methodology - Admitted patient care script - Provider - PAT
📝 National JAR methodology - Outpatient Attendance script - Provider - PAT

About the Scripts
The PAT and FasterSUS scripts are ran using National Planning logic, the below applies to both APC and OP scripts:

Acute Provider Only
Consultant led Specific Acute activity only
Treatment Function Code 360 and 812 (op) is excluded
Excluding Private patients
🚑 Accident and Emergency Attendance script
This script covers Emergency Care attendances, sourced from the National PAT Intermediate EC SUS table

🏥 Admitted patient care script
This script covers both elective and non-elective hospital activity, sourced from the National PAT Intermediate Admitted Patient Care SUS table

👨‍⚕️ Outpatient Attendance script
This script covers Outpatient attendances, sourced from the National PAT Intermediate OP SUS table

Built With SQL and Python in DataBricks
🛢️[DataBricks](About Databricks: The data and AI company | Databricks)
🛢️UDAL

Datasets in the lakemart on DataBricks
🛢️ Pat_intermediate_OPA 🛢️ Pat_intermediate_APC 🛢️ Pat_intermediate_AE
