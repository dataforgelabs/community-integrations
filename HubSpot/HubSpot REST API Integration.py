# Databricks notebook source
# MAGIC %scala
# MAGIC import com.dataforgelabs.sdk._
# MAGIC import org.apache.spark.sql.DataFrame
# MAGIC import org.apache.spark.sql.functions._
# MAGIC
# MAGIC // Define new DataForge ingestion session
# MAGIC val session = new IngestionSession("HubSpot - Leads","Alec")
# MAGIC
# MAGIC // Fail the SDK session if the hubspot_object custom parameter is missing
# MAGIC val hubspot_object = (session.customParameters \ "hubspot_object").asOpt[String]
# MAGIC if (hubspot_object.isEmpty) {
# MAGIC     session.fail("Missing hubspot_object. Please check custom parameters in source ingestion parameters.")
# MAGIC     throw new Exception("Error: missing hubspot_object in custom parameters")
# MAGIC }
# MAGIC
# MAGIC // Get custom parameters and store them in spark config to pass to python cell
# MAGIC val customParams = session.customParameters.toString
# MAGIC session.log(s"Custom Parameters: ${customParams}")
# MAGIC spark.conf.set("customParams", customParams)

# COMMAND ----------

# customParamsStr = """{"limit":100,"archived":false,"hubspot_object":"contacts","flatten_properties":true,"include_all_properties":true}"""

# COMMAND ----------

import requests
import json
import re
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *

access_token = dbutils.secrets.get(scope="hubspot",key="access_token") # Get HubSpot access token from Databricks secret created in the workspace

# Get custom parameters for request
customParamsStr = spark.conf.get("customParams")
customParams = json.loads(customParamsStr)

# Store each custom parameter in a variable for later use
hubspot_object = customParams.get("hubspot_object")
get_pipelines = customParams.get("get_pipelines", False)
get_properties = customParams.get("get_properties", False)
properties = customParams.get("properties", None)
limit = customParams.get("limit", 50)
archived = customParams.get("archived", None)
properties_with_history = customParams.get("properties_with_history", None)
associations = customParams.get("associations", None) 
include_all_properties = customParams.get("include_all_properties", False)
flatten_properties = customParams.get("flatten_properties", False)
flatten_properties_stage_fields = customParams.get("flatten_properties_stage_fields", False)

# COMMAND ----------

# Function to get data from HubSpot using the REST API
def get_data(url: str, access_token: str, params: dict) -> list[dict]:    

    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    
    all_data = []
    after = None
    
    while True:
        if after:
            params['after'] = after
        print(url)
        response = requests.get(url,headers=headers,params=params)
        
        if response.status_code == 200:
            data = response.json()
            all_data.extend(data['results'])
            
            # Check for more data
            if 'paging' in data and 'next' in data['paging']:
                after = data['paging']['next']['after']
            else:
                break
        else:
            raise Exception(f"Error {response.status_code}: {response.json().get('message')}")

    return all_data

# Function to extract keys and their types from records while preserving order
def extract_object_fields(item: dict) -> tuple[dict[str,str], dict[str,str]]:
    root_fields = {}
    properties_fields = {}

    # Extract fields from the root level of the item
    for key in item.keys():  # Maintain order by iterating over keys directly
        value = item[key]  # Get the value for the key
        if key != "properties":  # Skip properties for now
            root_fields[key] = type(value).__name__  # Get the type of the value
    
    # Now extract fields from the properties struct
    for key, value in item['properties'].items():
        if key not in properties_fields:
            properties_fields[key] = type(value).__name__  # Get the type of the property value

    return root_fields, properties_fields

# Function to get the schema for the properties field
def get_object_properties_schema(object_type: str) -> tuple[list[dict], dict[str,str]]:
    properties_results = get_data(f"https://api.hubapi.com/crm/v3/properties/{object_type}", access_token, params=None)
    schema_map = {}

    # Ensure we handle the structure of properties_schema correctly
    for prop in properties_results:  # Access the results attribute
        schema_map[prop['name']] = prop['type']  # Map property name to its type

    return properties_results, schema_map

# Function to map HubSpot types to Spark types
def map_hubspot_to_spark(hubspot_type: str) -> DataType:
    mapping = {
        "string": StringType(),
        "number": DoubleType(),
        "date": DateType(),
        "datetime": TimestampType(),
        "boolean": BooleanType(),
        "enumeration": StringType(),  # Assuming enum types are treated as strings
        "json": StringType(),  # Assuming JSON data is treated as strings
        # Add more mappings as necessary
    }
    return mapping.get(hubspot_type, StringType())  # Default to StringType

# Function to extract root fields and properties struct fields into two dicts
def extract_object_fields(item: dict) -> tuple[dict[str,str],dict[str,str]]:
    root_fields = {}
    properties_fields = {}

    # Extract fields from the root level of the item
    for key in object_results[0].keys():  # Maintain order by iterating over keys directly
        value = item[key]  # Get the value for the key
        root_fields[key] = type(value).__name__  # Get the type of the value
    
    # Now extract fields from the properties struct
    for key, value in item['properties'].items():
        if key not in properties_fields:
            properties_fields[key] = type(value).__name__  # Get the type of the property value

    return root_fields, properties_fields

# Function to create the spark schema from the list of root fields and properties struct fields, mapping them to spark types and creating the schema
def create_spark_schema(sample_item: dict, object_schema: dict[str,str]) -> StructType:
    #Getting list of root field names and properties struct field names
    root_fields_order, properties_fields = extract_object_fields(sample_item)

    # Create Spark StructType for properties struct field
    properties_struct_fields = []
    for field in properties_fields.keys():
        spark_type = map_hubspot_to_spark(object_schema.get(field, "string"))  # Map HubSpot type to Spark type
        properties_struct_fields.append(StructField(field, spark_type))
    properties_struct_type = StructType(properties_struct_fields)
    
    # Creating root fields as struct fields
    struct_fields = []
    added_keys = set()  # Track added keys to prevent duplicates

    for field, hubspot_type in root_fields_order.items():
        if field not in added_keys:
            if field == 'properties':
                struct_fields.append(StructField("properties", properties_struct_type))
            else:
                spark_type = map_hubspot_to_spark(hubspot_type)  # Map to Spark type
                struct_fields.append(StructField(field, spark_type))
                added_keys.add(field)  # Mark this field as added

    final_schema = StructType(struct_fields)
    return final_schema

# Function to cast dataframe columns based on spark schema recursively
def cast_column(col_name: str, schema_field: StructField) -> Column:
    data_type = schema_field.dataType
    if isinstance(data_type, StructType):
        # For nested fields, recursively cast each subfield
        return struct([
            cast_column(f"{col_name}.{sub_field.name}", sub_field).alias(sub_field.name)
            for sub_field in data_type.fields
        ]).alias(col_name)
    elif isinstance(data_type, ArrayType):
        # For ArrayType, cast the element type
        element_type = data_type.elementType
        return F.col(col_name).cast(ArrayType(element_type)).alias(col_name)
    else:
        # For simple types, cast directly
        return col(col_name).cast(data_type).alias(col_name)
    
# Function to identify base field names that are duplicated with an ID at the end
def identify_stage_field_names(df: DataFrame, struct_field: str) -> list[str]:
    # Define field name pattern with ID of more than one digit at the end
    pattern = re.compile(r"(.+)_\d{2,}$")

    # For storing base field names that match the pattern
    base_field_counts = {}

    # Loop through the fields of the struct
    for field in df.schema[struct_field].dataType.fields:
        match = pattern.match(field.name)
        if match:
            base_name = match.group(1)  # Extract base name (e.g. 'hs_date_entered')
            
            # Count occurrences of the base name
            if base_name in base_field_counts:
                base_field_counts[base_name] += 1
            else:
                base_field_counts[base_name] = 1

    # Return the list of base names that have more than one occurrence of the pattern
    base_field_names = [base_name for base_name, count in base_field_counts.items() if count > 1]

    return base_field_names

# Function to convert fields into an array of structs where the key is the number at the end
def convert_fields_to_array_of_structs(df: DataFrame, struct_field: str, base_field_names: list[str]) -> DataFrame:
    # Define field name pattern with ID of more than one digit at the end
    valid_field_pattern = re.compile(r"(.+)_\d{2,}$")
    
    # For each base field name, create an array of structs with key-value pairs
    for base_field_name in base_field_names:
        key_value_structs = []
        fields_to_drop = []

        # Get all fields from the struct that match the base field name pattern (e.g. 'hs_date_exited_*')
        for field in df.schema[struct_field].dataType.fields:
            # Loop through fields and to get list of fields that start with the base field name
            if field.name.startswith(base_field_name):
                # Check if the field ends with a valid digit ending
                match = valid_field_pattern.match(field.name)
                
                if match:
                    # Extract the numeric suffix (after the last underscore)
                    key = field.name.split("_")[-1]  # Extract '235832088' from 'hs_date_exited_235832088'

                    # Create a struct with key-value pair, where key is the number and value is the field value
                    key_value_struct = struct(lit(key).alias("pipeline_stage_id"), col(f"{struct_field}.{field.name}").alias("value"))

                    # Append the struct to the list
                    key_value_structs.append(key_value_struct)

                    # Add the field name to the list of fields to drop later
                    fields_to_drop.append(field.name)

        # Combine all key-value pairs into an array of structs and create a new column for each base field name. Remove the separated fields from the properties column.
        if key_value_structs:
            df = df.withColumn(f"array_{base_field_name}", array(*key_value_structs))

            # Dropping fields that were separated into columns from the original properties struct
            df = df.withColumn(struct_field, col(struct_field).dropFields(*fields_to_drop))
    
    return df

# Function to check the dataframe for repeated pipeline stage fields, convert them to columns, and return the final dataframe
def map_dataframe(df: DataFrame, struct_field: str) -> DataFrame:
    # Check for any pipeline stage fields in the api resulting dataframe
    if struct_field in df.columns:
        pipeline_stage_fields = identify_stage_field_names(df, struct_field)
    else:
        pipeline_stage_fields = None

    # If pipeline stage fields exist, create new columns as array[struct{}] and map the data in to the new columns
    if pipeline_stage_fields:
        final_df = convert_fields_to_array_of_structs(df, struct_field, pipeline_stage_fields)
    else:
        final_df = df
    
    return final_df

# Helper function to process flattening of dataframe
def process_dataframe(df: DataFrame, flatten_props: bool, flatten_stage_fields: bool) -> DataFrame:
    if flatten_properties:
        print("Run with flatten_properties.")
        # Separate pipeline stage fields into separate columns as array<struct<>> first
        mapped_df = map_dataframe(cast_df, "properties")

        # Get list of all fields from mapped dataframe for flattening
        mapped_df_fields = mapped_df.schema.names

        # Flatten remaining properties fields into final dataframe
        final_df = mapped_df.selectExpr(*mapped_df.schema.names, "properties.*").drop("properties")
            
    # Determine if we should flatten pipeline stage fields into separate columns as ARRAY<STRUCT<>> so they're easier to work with
    elif flatten_properties_stage_fields:
        print("Run with flatten_properties_stage_fields.")
        # Separate pipeline stage fields into separate columns as array<struct<>> for final dataframe
        final_df = map_dataframe(cast_df, "properties")

    # If neither flatten_properties or flatten_properties_stage_fields are true, return the dataframe as is
    else:
        print("Run with no flattening.")
        final_df = cast_df

    return final_df

# COMMAND ----------

# Get all properties of object and the schema map (used for include_all_properties and get_properties)
try:
    all_properties_results, properties_schema = get_object_properties_schema(hubspot_object)
except:
    pass

# Define which properties to include if any
try:
    if include_all_properties:
        properties = [prop['name'] for prop in all_properties_results]
except:
    print(f"properties don't exist for {hubspot_object}")
    pass

# Parameter builder
parameters = {
    "limit": limit,
    "properties": properties,
    "propertiesWithHistory": properties_with_history,
    "associations": associations,
    "archived": archived
}
print(parameters)

# COMMAND ----------

# Define an empty schema to be returned if the object results are empty
schema = StructType([StructField("id", StringType(), True)])
empty_df = spark.createDataFrame([], schema)

# If get_properties then return a dataframe of the object properties rather than the object results
if get_properties:
    try:
        final_df = spark.createDataFrame(all_properties_results)
    except Exception as e:
        print(f"Exception: {e}")
        final_df = empty_df

# If get_pipelines then return a dataframe of the object pipelines rather than the object results
elif get_pipelines:
    pipeline_url = f"https://api.hubapi.com/crm/v3/pipelines/{hubspot_object}"
    object_results = get_data(pipeline_url, access_token, params=None)

    if object_results:
        final_df = spark.createDataFrame(object_results)
    else:
        final_df = empty_df

# If hubspot_object is owners, we need to use a different url
elif hubspot_object == "owners":
    owners_url = f"https://api.hubapi.com/crm/v3/{hubspot_object}"
    object_results = get_data(owners_url, access_token, params=None)

    if object_results:
        final_df = spark.createDataFrame(object_results)
    else:
        final_df = empty_df

# If not get_pipeline or get_properties, get the object results and return a dataframe
else:
    try:
        url = f"https://api.hubapi.com/crm/v3/objects/{hubspot_object}"

        # Get the object results
        object_results = get_data(url, access_token, parameters)

        # Check if the object results have data or if it is an empty list
        if object_results:
            df = spark.createDataFrame(object_results)
            try:
                # Get spark schema from object results
                final_schema = create_spark_schema(object_results[0], properties_schema)
                
                # Cast all columns based on the spark schema
                cast_df = df.select(
                    [cast_column(field.name, field) for field in final_schema.fields]
                )
            except:
                # Return dataframe as is if casting is not possible
                cast_df = df

            # Determine if we should flatten properties struct fields, flatten all properties, or not flatten into separate columns
            final_df = process_dataframe(cast_df, flatten_props=flatten_properties, flatten_stage_fields=flatten_properties_stage_fields)
        
        # If the object doesn't have results, return an empty dataframe
        else:
            print("Empty object results. Returning empty dataframe.")
            final_df = empty_df
    except Exception as e:
        print(f"Exception: {e}")
        print("Returning empty dataframe")
        final_df = empty_df

#Create temp table to pass to scala sdk
final_df.createOrReplaceTempView("df_temp")

display(final_df)

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC // Define and return dataframe for DataForge
# MAGIC def ingestDf(): DataFrame = {
# MAGIC   // Set temp table from Python cell into a Spark DataFrame for the function to return
# MAGIC   val df: DataFrame = spark.table("df_temp")
# MAGIC
# MAGIC   // Log error message if the dataframe is empty
# MAGIC   if (df.isEmpty) {
# MAGIC     session.log("No data in object results", "E")
# MAGIC   }
# MAGIC   
# MAGIC   return df
# MAGIC }
# MAGIC
# MAGIC // Run ingestion
# MAGIC session.ingest(ingestDf)
