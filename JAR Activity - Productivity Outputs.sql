-- Databricks notebook source
-- MAGIC %md
-- MAGIC # DataBricks - Operational JAR Logic, applied to PAT Intermediate Table - South West
-- MAGIC
-- MAGIC ## Output Files by Provider and System for hosting on Futures
-- MAGIC
-- MAGIC ### NHS England South West Intelligence and Insights
-- MAGIC  
-- MAGIC The **JAR Reconciliation Files** provides a granular breakdown of hospital activity (TFC/HRG Level), based on national planning logic. This script runs the activity views and then outputs files, complete with three tabs (APC, OP, AE) for each provider and system.
-- MAGIC  
-- MAGIC
-- MAGIC ### Scripts
-- MAGIC  
-- MAGIC #### Provider Focus
-- MAGIC 📝 National JAR methodology - Accident and Emergency Attendance - Provider - PAT  
-- MAGIC 📝 National JAR methodology - Admitted patient care script - Provider - PAT   
-- MAGIC 📝 National JAR methodology - Outpatient Attendance script - Provider - PAT   
-- MAGIC
-- MAGIC ### About the Scripts
-- MAGIC The PAT and FasterSUS scripts are ran using National Planning logic, the below applies to both APC and OP scripts:  
-- MAGIC - Acute Provider Only  
-- MAGIC - Consultant led Specific Acute activity only  
-- MAGIC - Treatment Function Code 360 and 812 (op) is excluded  
-- MAGIC - Excluding Private patients
-- MAGIC  
-- MAGIC 🚑 **Accident and Emergency Attendance script**  
-- MAGIC *This script covers Emergency Care attendances, sourced from the National PAT Intermediate EC SUS table*  
-- MAGIC
-- MAGIC 🏥 **Admitted patient care script**  
-- MAGIC *This script covers both elective and non-elective hospital activity, sourced from the National PAT Intermediate Admitted Patient Care SUS table*  
-- MAGIC
-- MAGIC 👨‍⚕️ **Outpatient Attendance script**  
-- MAGIC *This script covers Outpatient attendances, sourced from the National PAT Intermediate OP SUS table*  
-- MAGIC
-- MAGIC ### Built With SQL and Python in DataBricks
-- MAGIC  
-- MAGIC 🛢️[DataBricks](About Databricks: The data and AI company | Databricks)  
-- MAGIC 🛢️[UDAL](https://rdweb.wvd.microsoft.com) 
-- MAGIC
-- MAGIC #### Datasets in the lakemart on DataBricks
-- MAGIC 🛢️ Pat_intermediate_OPA 
-- MAGIC 🛢️ Pat_intermediate_APC 
-- MAGIC 🛢️ Pat_intermediate_AE 
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 1: Load the Tables  
-- MAGIC
-- MAGIC Before we dive into the data, we need to **load the necessary tables** into the **Hive Metastore**.  
-- MAGIC
-- MAGIC #### Why is this needed?  
-- MAGIC Right now, the data does not exist in the **Lakemart**, until it's been included, we use this **temporary workaround**. Thanks to the **National Elective Team**, we have access to their **ERIC workspace** to make this possible.  
-- MAGIC
-- MAGIC #### Best Practice  
-- MAGIC Since these tables **refresh daily**, it's always best to **drop and reload** them to ensure you're working with the most up-to-date data.  
-- MAGIC

-- COMMAND ----------


--Load neccassary reference tables
drop table if exists eric.Provider_Hierarchies_DB;
create table eric.Provider_Hierarchies_DB
using parquet
location "abfss://reporting@udalstdatacuratedprod.dfs.core.windows.net/unrestricted/reference/UKHD/ODS/Provider_Hierarchies/";

drop table if exists eric.Date_Full_DB;
create table eric.Date_Full_DB
using parquet
location "abfss://unrestricted@udalstdatacuratedprod.dfs.core.windows.net/reference/Internal/Reference/Date_Full/Published/1/";

drop table if exists eric.TreatmentFunctionCodes_DB;
create table eric.TreatmentFunctionCodes_DB
using parquet
location "abfss://unrestricted@udalstdatacuratedprod.dfs.core.windows.net/aggregated/UKHF/Treatment_Function/Codes1/"
options(recursiveFileLookup=true);

--Load neccassary PAT Intermediate tables

drop table if exists eric.PAT_Intermediate_Table_APC_DB;
create table eric.PAT_Intermediate_Table_APC_DB
using parquet
location "abfss://restricted@udalstdatacuratedprod.dfs.core.windows.net/patientlevel/UDALSQLMART/PatActivity/PAT_Intermediate_Table_APC/"
options(recursiveFileLookup=true);

drop table if exists eric.PAT_Intermediate_Table_OP_DB;
create table eric.PAT_Intermediate_Table_OP_DB
using parquet
location "abfss://restricted@udalstdatacuratedprod.dfs.core.windows.net/patientlevel/UDALSQLMART/PatActivity/PAT_Intermediate_Table_OP/"
options(recursiveFileLookup=true);

drop table if exists eric.PAT_Intermediate_Table_AE_DB;
create table eric.PAT_Intermediate_Table_AE_DB
using parquet
location "abfss://restricted@udalstdatacuratedprod.dfs.core.windows.net/patientlevel/UDALSQLMART/PatActivity/PAT_Intermediate_Table_AE/"
options(recursiveFileLookup=true);
 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 2: Create the Views  
-- MAGIC
-- MAGIC There are three views using SQL Spark version of the legacy SQL view initialy created in NCDR and the replicated in UDAL, for APC, OP and AE.

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW PAT_APC AS
SELECT
    CASE
        WHEN susapc.Provider_Current IN ('RD3', 'RDZ') THEN 'R0D'
        WHEN susapc.Provider_Current = 'RBZ' THEN 'RH8'
        WHEN susapc.Provider_Current = 'RA3' THEN 'RA7'
        WHEN susapc.Provider_Current = 'RBA' THEN 'RH5'
        WHEN susapc.Provider_Current = 'R1G' THEN 'RA9'
        WHEN susapc.Provider_Current = 'RVJ13' THEN 'RVJ'
        WHEN susapc.Provider_Current = 'RA4' THEN 'RH5'
        ELSE susapc.Provider_Current
    END AS Provider_Code,
 
    CASE
        WHEN o.organisation_name = 'ROYAL DEVON AND EXETER NHS FOUNDATION TRUST' THEN 'ROYAL DEVON UNIVERSITY HEALTHCARE NHS FOUNDATION TRUST'
        WHEN o.organisation_name = 'TAUNTON AND SOMERSET NHS FOUNDATION TRUST' THEN 'SOMERSET NHS FOUNDATION TRUST'
        WHEN o.organisation_name = 'NORTHERN DEVON HEALTHCARE NHS TRUST' THEN 'ROYAL DEVON UNIVERSITY HEALTHCARE NHS FOUNDATION TRUST'
        WHEN o.organisation_name IN (
            'THE ROYAL BOURNEMOUTH AND CHRISTCHURCH HOSPITALS NHS FOUNDATION TRUST',
            'POOLE HOSPITAL NHS FOUNDATION TRUST'
        ) THEN 'UNIVERSITY HOSPITAL DORSET NHS FOUNDATION TRUST'
        WHEN o.organisation_name = 'TORBAY AND SOUTHERN DEVON HEALTH AND CARE NHS TRUST' THEN 'TORBAY AND SOUTH DEVON NHS FOUNDATION TRUST'
        WHEN o.organisation_name = 'EMERSONS GREEN NHS TREATMENT CENTRE' THEN 'NORTH BRISTOL NHS TRUST'
        WHEN o.organisation_name = 'YEOVIL DISTRICT HOSPITAL NHS FOUNDATION TRUST' THEN 'SOMERSET NHS FOUNDATION TRUST'  
        WHEN o.organisation_name = 'UNIVERSITY HOSPITALS BRISTOL NHS FOUNDATION TRUST' THEN 'UNIVERSITY HOSPITALS BRISTOL AND WESTON NHS FOUNDATION TRUST'
        ELSE o.organisation_name  
    END AS organisation_name,
 
    o.STP_Code,
    o.STP_Name,
    right(susapc.Dimention_3, 3) AS TFC,
    T.Treatment_Function_Title,
    LEFT(susapc.Dimention_7, 5) AS HRG,
    susapc.Pat_Commissioner_Type,
    CONCAT(YEAR(susapc.Discharge_Date), LPAD(MONTH(susapc.Discharge_Date),2,'0')) AS YearMonth,
 
    susapc.LOS_unadjusted,
 
    CASE 
        WHEN susapc.Der_Management_Type = 'EM' THEN 'NE' 
        ELSE susapc.Der_Management_Type 
    END AS Der_Management_Type,
 
    CASE 
        WHEN susapc.Dimention_5 = 'A: 0 day LOS' THEN '0 Day LOS' 
        ELSE '1+ Day LOS' 
    END AS LOS,
 
    susapc.Discharge_Date,
 
    COUNT(susapc.unadjusted) AS Total_Activty_Unadj,
    COUNT(susapc.adjusted) AS Total_Activty_Adj
 
FROM eric.PAT_Intermediate_Table_APC_DB AS susapc
 
LEFT JOIN eric.Provider_Hierarchies_DB o 
    ON susapc.Provider_Current = o.Organisation_Code  
 
LEFT JOIN eric.Date_Full_DB AS d 
    ON d.Full_Date = susapc.Discharge_Date
 
LEFT JOIN eric.TreatmentFunctionCodes_DB AS T 
    ON Right(susapc.Dimention_3, 3) = T.DD_Code
 
WHERE
    susapc.Discharge_Date >= '2024-04-01'
    AND susapc.Der_Management_Type IN ('EL', 'DC', 'EM', 'NE')
    AND susapc.Provider_Current IN (
        'RD1', 'RN3', 'RNZ', 'RA7', 'RVJ', 'REF', 'RA9', 'RH8',
        'RK9', 'RBD', 'R0D', 'RTE', 'RH5'
    )

and susapc.Dimention_4 = 'Specific Acute'

 --   AND right(susapc.Dimention_3, 3) NOT IN (
 --       '199', '223', '290', '291', '331', '344', '345', '346', '360',
 --       '424', '499', '501', '504', '560', '650', '651', '652', '653',
 --       '654', '655', '656', '657', '658', '659', '660', '661', '662',
 --       '700', '710', '711', '712', '713', '715', '720', '721', '722',
 --       '723', '724', '725', '726', '727', '730', '840', '920','NULL'
 --   )
 
    AND susapc.Pat_Commissioner_Type <> 'Private Patient'
 
GROUP BY
    susapc.Provider_Current,
    o.organisation_name,
    o.STP_Code,
    o.STP_Name,
    right(susapc.Dimention_3,3),
    T.Treatment_Function_Title,
    LEFT(susapc.Dimention_7,5),
    susapc.Pat_Commissioner_Type,
    susapc.Discharge_Date,
    susapc.LOS_unadjusted,
    susapc.Der_Management_Type,
    susapc.Dimention_5


-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW PAT_OP AS
SELECT
    CASE
        WHEN susop.Provider_Current IN ('RD3', 'RDZ') THEN 'R0D'
        WHEN susop.Provider_Current = 'RBZ' THEN 'RH8'
        WHEN susop.Provider_Current = 'RA3' THEN 'RA7'
        WHEN susop.Provider_Current = 'RBA' THEN 'RH5'
        WHEN susop.Provider_Current = 'R1G' THEN 'RA9'
        WHEN susop.Provider_Current = 'RVJ13' THEN 'RVJ'
        WHEN susop.Provider_Current = 'RA4' THEN 'RH5'
        ELSE susop.Provider_Current
    END AS Provider_Code,
 
    CASE
        WHEN o.organisation_name = 'ROYAL DEVON AND EXETER NHS FOUNDATION TRUST' THEN 'ROYAL DEVON UNIVERSITY HEALTHCARE NHS FOUNDATION TRUST'
        WHEN o.organisation_name = 'TAUNTON AND SOMERSET NHS FOUNDATION TRUST' THEN 'SOMERSET NHS FOUNDATION TRUST'
        WHEN o.organisation_name = 'NORTHERN DEVON HEALTHCARE NHS TRUST' THEN 'ROYAL DEVON UNIVERSITY HEALTHCARE NHS FOUNDATION TRUST'
        WHEN o.organisation_name IN (
            'THE ROYAL BOURNEMOUTH AND CHRISTCHURCH HOSPITALS NHS FOUNDATION TRUST',
            'POOLE HOSPITAL NHS FOUNDATION TRUST'
        ) THEN 'UNIVERSITY HOSPITAL DORSET NHS FOUNDATION TRUST'
        WHEN o.organisation_name = 'TORBAY AND SOUTHERN DEVON HEALTH AND CARE NHS TRUST' THEN 'TORBAY AND SOUTH DEVON NHS FOUNDATION TRUST'
        WHEN o.organisation_name = 'EMERSONS GREEN NHS TREATMENT CENTRE' THEN 'NORTH BRISTOL NHS TRUST'
        WHEN o.organisation_name = 'YEOVIL DISTRICT HOSPITAL NHS FOUNDATION TRUST' THEN 'SOMERSET NHS FOUNDATION TRUST'  
        WHEN o.organisation_name = 'UNIVERSITY HOSPITALS BRISTOL NHS FOUNDATION TRUST' THEN 'UNIVERSITY HOSPITALS BRISTOL AND WESTON NHS FOUNDATION TRUST'
        ELSE o.organisation_name  
    END AS organisation_name,
 
    o.STP_Code,
    o.STP_Name,
    right(susop.Dimention_3, 3) AS TFC,
    T.Treatment_Function_Title,
    LEFT(susop.Dimention_7, 5) AS HRG,
    susop.Pat_Commissioner_Type,
    CONCAT(YEAR(susop.Attendance_Date), LPAD(MONTH(susop.Attendance_Date),2,'0')) AS YearMonth,
    
    CASE 
        WHEN susop.Dimention_1 LIKE 'Follow%' THEN 'Outpatient Follow-Up'
        WHEN susop.Dimention_1 LIKE '1st%' THEN 'Outpatient First Appointment'
        ELSE susop.Dimention_1 
    END AS High_Level_Pod,
 
    susop.Attendance_Date,
 
    COUNT(susop.unadjusted) AS Total_Activty_Unadj,
    COUNT(susop.adjusted) AS Total_Activty_Adj
 
FROM eric.PAT_Intermediate_Table_OP_DB AS susop
 
LEFT JOIN eric.Provider_Hierarchies_DB o 
    ON susop.Provider_Current = o.Organisation_Code  
 
LEFT JOIN eric.Date_Full_DB AS d 
    ON d.Full_Date = susop.Attendance_Date
 
LEFT JOIN eric.TreatmentFunctionCodes_DB AS T 
    ON right(susop.Dimention_3, 3) = T.DD_Code
 
WHERE
    susop.Attendance_Date >= '2024-04-01'

    AND susop.Provider_Current IN (
        'RD1', 'RN3', 'RNZ', 'RA7', 'RVJ', 'REF', 'RA9', 'RH8',
        'RK9', 'RBD', 'R0D', 'RTE', 'RH5'
    )

and susop.Dimention_4 = 'Consultant led: Specific Acute'

and susop.Dimention_1 <> 'Unknown Appointment Type'

 --   AND right(susapc.Dimention_3, 3) NOT IN (
 --       '199', '223', '290', '291', '331', '344', '345', '346', '360',
 --       '424', '499', '501', '504', '560', '650', '651', '652', '653',
 --       '654', '655', '656', '657', '658', '659', '660', '661', '662',
 --       '700', '710', '711', '712', '713', '715', '720', '721', '722',
 --       '723', '724', '725', '726', '727', '730', '840', '920','NULL'
 --   )
 
    AND susop.Pat_Commissioner_Type <> 'Private Patient'
 
GROUP BY
susop.Provider_Current,
o.organisation_name,
o.STP_Code,
o.STP_Name,
right(susop.Dimention_3, 3),
T.Treatment_Function_Title,
LEFT(susop.Dimention_7, 5),
susop.Pat_Commissioner_Type,
Attendance_Date,
Dimention_1,
susop.Attendance_Date;

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW PAT_AE AS
SELECT
    CASE
        WHEN susae.Provider_Current IN ('RD3', 'RDZ') THEN 'R0D'
        WHEN susae.Provider_Current = 'RBZ' THEN 'RH8'
        WHEN susae.Provider_Current = 'RA3' THEN 'RA7'
        WHEN susae.Provider_Current = 'RBA' THEN 'RH5'
        WHEN susae.Provider_Current = 'R1G' THEN 'RA9'
        WHEN susae.Provider_Current = 'RVJ13' THEN 'RVJ'
        WHEN susae.Provider_Current = 'RA4' THEN 'RH5'
        ELSE susae.Provider_Current
    END AS Provider_Code,
 
    CASE
        WHEN o.organisation_name = 'ROYAL DEVON AND EXETER NHS FOUNDATION TRUST' THEN 'ROYAL DEVON UNIVERSITY HEALTHCARE NHS FOUNDATION TRUST'
        WHEN o.organisation_name = 'TAUNTON AND SOMERSET NHS FOUNDATION TRUST' THEN 'SOMERSET NHS FOUNDATION TRUST'
        WHEN o.organisation_name = 'NORTHERN DEVON HEALTHCARE NHS TRUST' THEN 'ROYAL DEVON UNIVERSITY HEALTHCARE NHS FOUNDATION TRUST'
        WHEN o.organisation_name IN (
            'THE ROYAL BOURNEMOUTH AND CHRISTCHURCH HOSPITALS NHS FOUNDATION TRUST',
            'POOLE HOSPITAL NHS FOUNDATION TRUST'
        ) THEN 'UNIVERSITY HOSPITAL DORSET NHS FOUNDATION TRUST'
        WHEN o.organisation_name = 'TORBAY AND SOUTHERN DEVON HEALTH AND CARE NHS TRUST' THEN 'TORBAY AND SOUTH DEVON NHS FOUNDATION TRUST'
        WHEN o.organisation_name = 'EMERSONS GREEN NHS TREATMENT CENTRE' THEN 'NORTH BRISTOL NHS TRUST'
        WHEN o.organisation_name = 'YEOVIL DISTRICT HOSPITAL NHS FOUNDATION TRUST' THEN 'SOMERSET NHS FOUNDATION TRUST'  
        WHEN o.organisation_name = 'UNIVERSITY HOSPITALS BRISTOL NHS FOUNDATION TRUST' THEN 'UNIVERSITY HOSPITALS BRISTOL AND WESTON NHS FOUNDATION TRUST'
        ELSE o.organisation_name  
    END AS organisation_name,
 
    o.STP_Code,
    o.STP_Name,
    LEFT(susae.Dimention_7, 5) AS HRG,
    susae.Pat_Commissioner_Type,
    CONCAT(YEAR(susae.Attendance_Date), LPAD(MONTH(susae.Attendance_Date),2,'0')) AS YearMonth,
    
    susae.Dimention_1 AS Metric_ID,
 
    susae.Attendance_Date,
 
    COUNT(susae.unadjusted) AS Total_Activty_Unadj,
    COUNT(susae.adjusted) AS Total_Activty_Adj
 
FROM eric.PAT_Intermediate_Table_AE_DB AS susae
 
LEFT JOIN eric.Provider_Hierarchies_DB o 
    ON susae.Provider_Current = o.Organisation_Code  
 
LEFT JOIN eric.Date_Full_DB AS d 
    ON d.Full_Date = susae.Attendance_Date

 WHERE
    susae.Attendance_Date >= '2024-04-01'

    AND susae.Provider_Current IN (
        'RD1', 'RN3', 'RNZ', 'RA7', 'RVJ', 'REF', 'RA9', 'RH8',
        'RK9', 'RBD', 'R0D', 'RTE', 'RH5'
    )


    AND susae.Pat_Commissioner_Type <> 'Private Patient'
 
GROUP BY
susae.Provider_Current,
o.organisation_name,
o.STP_Code,
o.STP_Name,
LEFT(susae.Dimention_7, 5),
susae.Pat_Commissioner_Type,
Attendance_Date,
Dimention_1,
susae.Attendance_Date;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 4: Remove old files and folders  
-- MAGIC
-- MAGIC Before running the new files, delete the old ones.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import shutil
-- MAGIC import os
-- MAGIC  
-- MAGIC local_path = r"JAR Activity - Productivity Output Files"
-- MAGIC  
-- MAGIC # Check if the directory exists
-- MAGIC if os.path.exists(local_path):
-- MAGIC     shutil.rmtree(local_path)  # Deletes the entire directory
-- MAGIC     print(f"Deleted: {local_path}")
-- MAGIC else:
-- MAGIC     print(f"Directory does not exist: {local_path}")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 4: Create the Productivity Outputs  
-- MAGIC
-- MAGIC Using the views, generate files for each provider and system.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC  
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC import pandas as pd
-- MAGIC import os
-- MAGIC from openpyxl import Workbook
-- MAGIC from openpyxl.worksheet.table import Table, TableStyleInfo
-- MAGIC from openpyxl.utils.dataframe import dataframe_to_rows
-- MAGIC  
-- MAGIC # Local OneDrive Path (Change if needed)
-- MAGIC local_path = r"JAR Activity - Productivity Output Files"
-- MAGIC  
-- MAGIC # Ensure directory exists
-- MAGIC os.makedirs(local_path, exist_ok=True)
-- MAGIC  
-- MAGIC # Read the SQL Views
-- MAGIC df_apc = spark.sql("SELECT * FROM PAT_APC")
-- MAGIC df_op = spark.sql("SELECT * FROM PAT_OP")
-- MAGIC df_ae = spark.sql("SELECT * FROM PAT_AE")
-- MAGIC  
-- MAGIC # Convert to Pandas
-- MAGIC df_apc_pandas = df_apc.toPandas()
-- MAGIC df_op_pandas = df_op.toPandas()
-- MAGIC df_ae_pandas = df_ae.toPandas()
-- MAGIC  
-- MAGIC # Get the latest available date (MMM-YYYY format)
-- MAGIC latest_date = pd.to_datetime(df_apc_pandas["Discharge_Date"]).max().strftime('%b-%Y')
-- MAGIC  
-- MAGIC # Function to save DataFrame to an Excel file with multiple sheets
-- MAGIC def save_to_excel_with_tables(apc_df, op_df, ae_df, file_name):
-- MAGIC     file_path = os.path.join(local_path, file_name)
-- MAGIC     wb = Workbook()
-- MAGIC     # Remove the default sheet created by Workbook()
-- MAGIC     default_sheet = wb.active
-- MAGIC     wb.remove(default_sheet)
-- MAGIC     # Function to add DataFrame to a sheet with a table
-- MAGIC     def add_sheet_with_table(wb, df, sheet_name):
-- MAGIC         sheet = wb.create_sheet(title=sheet_name)
-- MAGIC         for row in dataframe_to_rows(df, index=False, header=True):
-- MAGIC             sheet.append(row)
-- MAGIC         table = Table(displayName=f"{sheet_name}Table", ref=f"A1:{chr(64+df.shape[1])}{df.shape[0]+1}")
-- MAGIC         style = TableStyleInfo(name="TableStyleMedium9", showFirstColumn=False, showLastColumn=False, showRowStripes=True, showColumnStripes=True)
-- MAGIC         table.tableStyleInfo = style
-- MAGIC         sheet.add_table(table)
-- MAGIC     add_sheet_with_table(wb, apc_df, "APC")
-- MAGIC     add_sheet_with_table(wb, op_df, "OP")
-- MAGIC     add_sheet_with_table(wb, ae_df, "AE")
-- MAGIC     wb.save(file_path)
-- MAGIC     print(f"Saved: {file_path}")
-- MAGIC  
-- MAGIC # Split and Save by Organisation Name
-- MAGIC for org in df_apc_pandas["organisation_name"].unique():
-- MAGIC     org_apc_df = df_apc_pandas[df_apc_pandas["organisation_name"] == org]
-- MAGIC     org_op_df = df_op_pandas[df_op_pandas["organisation_name"] == org]
-- MAGIC     org_ae_df = df_ae_pandas[df_ae_pandas["organisation_name"] == org]
-- MAGIC     org_clean = org.replace(" ", "_").replace("/", "-")
-- MAGIC     save_to_excel_with_tables(org_apc_df, org_op_df, org_ae_df, f"{org_clean}_{latest_date}.xlsx")
-- MAGIC  
-- MAGIC # Split and Save by STP Name
-- MAGIC for stp in df_apc_pandas["STP_Name"].unique():
-- MAGIC     stp_apc_df = df_apc_pandas[df_apc_pandas["STP_Name"] == stp]
-- MAGIC     stp_op_df = df_op_pandas[df_op_pandas["STP_Name"] == stp]
-- MAGIC     stp_ae_df = df_ae_pandas[df_ae_pandas["STP_Name"] == stp]
-- MAGIC     stp_clean = stp.replace(" ", "_").replace("/", "-")
-- MAGIC     save_to_excel_with_tables(stp_apc_df, stp_op_df, stp_ae_df, f"{stp_clean}_{latest_date}.xlsx")
-- MAGIC  
-- MAGIC print("✅ Excel files successfully saved in JAR Activity - Productivity Output Files!")
-- MAGIC  
-- MAGIC
-- MAGIC  

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 5: Create Final Productivity Output File 
-- MAGIC
-- MAGIC Using the views, generate SW totals file to QA highlevel to JAR

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC import pandas as pd
-- MAGIC import os
-- MAGIC from openpyxl import Workbook
-- MAGIC from openpyxl.worksheet.table import Table, TableStyleInfo
-- MAGIC from openpyxl.utils.dataframe import dataframe_to_rows
-- MAGIC  
-- MAGIC # Local OneDrive Path (Change if needed)
-- MAGIC local_path = r"JAR Activity - Productivity Output Files"
-- MAGIC  
-- MAGIC # Ensure directory exists
-- MAGIC os.makedirs(local_path, exist_ok=True)
-- MAGIC  
-- MAGIC # Read the SQL Views
-- MAGIC df_apc = spark.sql("SELECT * FROM PAT_APC")
-- MAGIC df_op = spark.sql("SELECT * FROM PAT_OP")
-- MAGIC df_ae = spark.sql("SELECT * FROM PAT_AE")
-- MAGIC  
-- MAGIC # Convert to Pandas
-- MAGIC df_apc_pandas = df_apc.toPandas()
-- MAGIC df_op_pandas = df_op.toPandas()
-- MAGIC df_ae_pandas = df_ae.toPandas()
-- MAGIC  
-- MAGIC # Drop unnecessary fields
-- MAGIC drop_columns = ["HRG", "TFC", "organisation_name", "STP_Name", "STP_Code"]
-- MAGIC df_apc_pandas = df_apc_pandas.drop(columns=[col for col in drop_columns if col in df_apc_pandas.columns], errors='ignore')
-- MAGIC df_op_pandas = df_op_pandas.drop(columns=[col for col in drop_columns if col in df_op_pandas.columns], errors='ignore')
-- MAGIC df_ae_pandas = df_ae_pandas.drop(columns=[col for col in drop_columns if col in df_ae_pandas.columns], errors='ignore')
-- MAGIC  
-- MAGIC # Aggregate total activity columns
-- MAGIC def aggregate_activity(df, group_by_cols):
-- MAGIC     sum_cols = ["Total_Activty_Unadj", "Total_Activty_Adj"]
-- MAGIC     return df.groupby(group_by_cols, as_index=False)[sum_cols].sum()
-- MAGIC  
-- MAGIC # Define grouping columns including provider codes
-- MAGIC apc_group_by = ["Der_Management_Type", "Provider_Code"]
-- MAGIC ae_group_by = ["Metric_ID", "Provider_Code"]
-- MAGIC op_group_by = ["High_Level_Pod", "Provider_Code"]
-- MAGIC  
-- MAGIC # Aggregate data
-- MAGIC df_apc_pandas = aggregate_activity(df_apc_pandas, apc_group_by)
-- MAGIC df_op_pandas = aggregate_activity(df_op_pandas, op_group_by)
-- MAGIC df_ae_pandas = aggregate_activity(df_ae_pandas, ae_group_by)
-- MAGIC  
-- MAGIC # Get the latest available date (MMM-YYYY format)
-- MAGIC latest_date = pd.Timestamp.today().strftime('%b-%Y')
-- MAGIC  
-- MAGIC # Function to save DataFrame to an Excel file with multiple sheets
-- MAGIC def save_to_excel_with_tables(apc_df, op_df, ae_df, file_name):
-- MAGIC     file_path = os.path.join(local_path, file_name)
-- MAGIC     wb = Workbook()
-- MAGIC     # Remove the default sheet created by Workbook()
-- MAGIC     default_sheet = wb.active
-- MAGIC     wb.remove(default_sheet)
-- MAGIC     # Function to add DataFrame to a sheet with a table
-- MAGIC     def add_sheet_with_table(wb, df, sheet_name):
-- MAGIC         sheet = wb.create_sheet(title=sheet_name)
-- MAGIC         for row in dataframe_to_rows(df, index=False, header=True):
-- MAGIC             sheet.append(row)
-- MAGIC         table = Table(displayName=f"{sheet_name}Table", ref=f"A1:{chr(64+df.shape[1])}{df.shape[0]+1}")
-- MAGIC         style = TableStyleInfo(name="TableStyleMedium9", showFirstColumn=False, showLastColumn=False, showRowStripes=True, showColumnStripes=True)
-- MAGIC         table.tableStyleInfo = style
-- MAGIC         sheet.add_table(table)
-- MAGIC     add_sheet_with_table(wb, apc_df, "APC")
-- MAGIC     add_sheet_with_table(wb, op_df, "OP")
-- MAGIC     add_sheet_with_table(wb, ae_df, "AE")
-- MAGIC     wb.save(file_path)
-- MAGIC     print(f"Saved: {file_path}")
-- MAGIC  
-- MAGIC # Save total data in a single file
-- MAGIC save_to_excel_with_tables(df_apc_pandas, df_op_pandas, df_ae_pandas, f"Total_{latest_date}.xlsx")
-- MAGIC  
-- MAGIC print("✅ Single Excel file with reduced size successfully saved in JAR Activity - Productivity Output Files!")
-- MAGIC  

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Contact
-- MAGIC  
-- MAGIC To find out more about the South West Intelligence and Insights Team visit our [South West Intelligence and Insights Team Futures Page](https://future.nhs.uk/SouthWestAnalytics)) or get in touch at [england.southwestanalytics@nhs.net](mailto:england.southwestanalytics@nhs.net). Alternatively, Please feel free to reach out to me directly:
-- MAGIC  
-- MAGIC 📧 Email: [Destiny.Bradley@nhs.net](mailto:Destiny.Bradley@nhs.net)  
-- MAGIC 💬 Teams: [Join my Teams](https://teams.microsoft.com/l/chat/0/0?users=<destiny.bradley@nhs.net)
-- MAGIC  
-- MAGIC ### Acknowledgements
-- MAGIC Thanks to Bernardo Detanico for his ongoing support in applying National Logic and Miles Filton for his support with getting started on Databricks