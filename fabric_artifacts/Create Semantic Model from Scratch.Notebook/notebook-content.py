# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8bbca298-8c74-4e51-8814-48bf61e8c5fe",
# META       "default_lakehouse_name": "DemoLakehouse",
# META       "default_lakehouse_workspace_id": "2e063166-e0f6-4fb8-888f-8ed0f6c1d481",
# META       "known_lakehouses": [
# META         {
# META           "id": "8bbca298-8c74-4e51-8814-48bf61e8c5fe"
# META         }
# META       ]
# META     },
# META     "environment": {}
# META   }
# META }

# CELL ********************

!pip install semantic-link-labs

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Import the required packages and models

# CELL ********************

import sempy.fabric as fabric
import sempy_labs as labs
import sempy_labs.lakehouse as lakehouse
import pandas as pd
from sempy_labs.tom import connect_semantic_model
from datetime import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Define the parameters for the Direct Lake semantic model

# CELL ********************

semantic_model_name = "demo_semantic_model"
feature_workspace = "DevOpsDemo_FeatureBranch"
lakehouse_workspace = "DevOpsDemo_Lakehouse"
tables = lakehouse.get_lakehouse_tables()['Table Name'].tolist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Build the semantic model using the information define in the above cell

# CELL ********************

labs.directlake.generate_direct_lake_semantic_model(
     dataset = semantic_model_name 
    ,lakehouse_tables = tables
    ,workspace = feature_workspace
    ,lakehouse_workspace = lakehouse_workspace
    ,overwrite = True
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Creates the model's table relationships

# MARKDOWN ********************

# Reads in the contents of the ***model_relationships.csv*** file into the ***pdf_relationship_data*** data frame

# CELL ********************

pdf_relationship_data = pd.read_csv("/lakehouse/default/Files/model_information/model_relationships.csv")
display(pdf_relationship_data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Iterates over the ***pdf_relationship_data*** data frame and uses the information in the row to create the relationship using the information in the row

# CELL ********************

with connect_semantic_model(dataset=semantic_model_name, readonly=False, workspace=feature_workspace) as tom:
    for index, row in pdf_relationship_data.iterrows():
        tom.add_relationship(
             from_table = row['from_table']
            ,from_column = row['from_column']
            ,from_cardinality = row['from_cardinality']
            ,to_table = row['to_table']
            ,to_column = row['to_column']
            ,to_cardinality = row['to_cardinality']
            ,is_active = True if row['is_active'] == 1 else False        
        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Configure semantic model's columns

# MARKDOWN ********************

# The ***fabric.list_columns()*** function returns information about each column in the specified semantic model. We will use some of the information provided to determine how we will configure the column.

# CELL ********************

fabric.list_columns(dataset = semantic_model_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# In the following cell, we are subsetting the data frame returned by ***fabric.list_columns()*** to only include the columns specified in the ***columns*** list variable. 

# CELL ********************

columns = ["Table Name", "Column Name", "Hidden", "Is Available in MDX"]
fdf_ColumnInfo = fabric.list_columns(dataset = semantic_model_name, workspace=feature_workspace)[columns]
display(fdf_ColumnInfo)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fabric.refresh_tom_cache(workspace=feature_workspace)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_tables = ['sales']

with connect_semantic_model(dataset=semantic_model_name, workspace=feature_workspace, readonly=False) as tom:
    for index, row in fdf_ColumnInfo.iterrows():
        if (row['Table Name'] in fact_tables) | ((row['Table Name'] not in fact_tables) & (row['Column Name'].endswith('_key'))):
            tom.update_column(
                 table_name=row['Table Name']
                ,column_name=row['Column Name']
                ,is_available_in_mdx=False
                ,hidden=True
            )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fabric.refresh_tom_cache(workspace=feature_workspace)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fdf_ColumnInfo = fabric.list_columns(dataset = semantic_model_name, workspace=feature_workspace)[columns]
display(fdf_ColumnInfo)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Add Measures

# CELL ********************

fdfSalesColumnInfo = fabric.list_columns(dataset=semantic_model_name, table="sales")
fdfSalesColumnInfo["Column Name"].str.startswith("Fake")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fdfSalesColumnInfo = fabric.list_columns(dataset=semantic_model_name, table="sales")
fdfSalesColumnInfo = fdfSalesColumnInfo[fdfSalesColumnInfo["Column Name"].str.startswith("Fake")]
display(fdfSalesColumnInfo)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

with connect_semantic_model(dataset=semantic_model_name, workspace=feature_workspace, readonly=False) as tom:
    for index, row in fdfSalesColumnInfo.iterrows():

        # Define variables
        table_name = row["Table Name"]
        measure_name = f'Total {row["Column Name"].replace("_"," ")}'
        expression = f'=SUM(sales[{row["Column Name"]}])'
        format_string = "#,##0"

        # Configure add_measure()
        tom.add_measure(
             table_name = table_name
            ,measure_name = measure_name
            ,expression = expression
            ,format_string = format_string
        )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fabric.refresh_tom_cache(workspace=feature_workspace)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

columns = ["Table Name", "Measure Name", "Measure Expression", "Measure Display Folder", "Format String"]
fabric.list_measures(dataset = semantic_model_name, workspace=feature_workspace)[columns]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
