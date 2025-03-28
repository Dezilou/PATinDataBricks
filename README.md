# DataBricks - Operational JAR Logic, applied to PAT Intermediate Table - South West
## Output Files by Provider and System for hosting on Futures
### NHS England South West Intelligence and Insights 

The **JAR Reconciliation Files** provides a granular breakdown of hospital activity (TFC/HRG Level), based on national planning logic. This script runs the activity views and then outputs files, complete with three tabs (APC, OP, AE) for each provider and system.
 
### Scripts
 
#### Provider Focus
📝 National JAR methodology - Accident and Emergency Attendance - Provider - PAT  
📝 National JAR methodology - Admitted patient care script - Provider - PAT  
📝 National JAR methodology - Outpatient Attendance script - Provider - PAT  
 
### About the Scripts
The PAT and FasterSUS scripts are ran using National Planning logic, the below applies to both APC and OP scripts:  
- Acute Provider Only  
- Consultant led Specific Acute activity only  
- Treatment Function Code 360 and 812 (op) is excluded  
- Excluding Private patients
 
🚑 **Accident and Emergency Attendance script**  
*This script covers Emergency Care attendances, sourced from the National PAT Intermediate EC SUS table*  
 
🏥 **Admitted patient care script**  
*This script covers both elective and non-elective hospital activity, sourced from the National PAT Intermediate Admitted Patient Care SUS table*  
 
👨‍⚕️ **Outpatient Attendance script**  
*This script covers Outpatient attendances, sourced from the National PAT Intermediate OP SUS table*  
 
### Built With SQL and Python in DataBricks
 
🛢️[DataBricks](https://www.databricks.com/company/about-us) 
🛢️[UDAL](https://rdweb.wvd.microsoft.com)
 
#### Datasets in the lakemart on DataBricks
🛢️ Pat_intermediate_OPA
🛢️ Pat_intermediate_APC
🛢️ Pat_intermediate_AE
 
### Contact
 
To find out more about the South West Intelligence and Insights Team visit our [South West Intelligence and Insights Team Futures Page](https://future.nhs.uk/SouthWestAnalytics)) or get in touch at [england.southwestanalytics@nhs.net](mailto:england.southwestanalytics@nhs.net). Alternatively, Please feel free to reach out to me directly:
 
📧 Email: [Destiny.Bradley@nhs.net](mailto:Destiny.Bradley@nhs.net)  
💬 Teams: [Join my Teams](https://teams.microsoft.com/l/chat/0/0?users=<destiny.bradley@nhs.net)
 
### Acknowledgements
Thanks to Bernardo Detanico for his ongoing support in applying National Logic and Miles Filton for his support with getting started on Databricks
 
