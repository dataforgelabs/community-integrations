## Getting started
This community supported integration provides a method to getting CRM data from HubSpot and integrating it with [DataForge](https://dataforgelabs.com) via the Scala SDK.
For more information on the HubSpot API and the DataForge SDK, visit the respective documentation.

[HubSpot API](https://developers.hubspot.com/beta-docs/reference/api)

[DataForge SDK](https://docs.dataforgelabs.com/hc/en-us/articles/18570216887835-Custom-Notebook-SDK)
## Pre-requisites
To run the integration, you need to have a private app in HubSpot and an access token to the app.

[HubSpot Private App Docs](https://developers.hubspot.com/docs/api/private-apps)

## Installation
### Databricks
Set up a git folder in your Databricks workspace attached to your DataForge workspace pointing to this repository. Copy the notebook directory path out to be used in DataForge below.
Install and use the Databricks CLI to create a scope and secret to store your HubSpot access token.

## DataForge setup
### Cluster configuration
Create a cluster configuration in your DataForge workspace as a custom notebook config. Paste the notebook directory path into the cluster configuration "Notebook Path" parameter. We recommend using a single node cluster to start unless you know you are working with large data.

### Source
Create a source for each HubSpot object you will pull data from. For each source, set the connection type to **Custom** and select your custom notebook cluster configuration in the **Custom Ingest Cluster Config*** parameter.

Set your custom parameters in the Parameters->Ingestion section of the source settings. See below for available custom parameters to use.

### Custom parameters
The following custom parameters are available for you to use with this integration. Some objects do not support the use of 
| Custom Parameter | Required | Definition |
|-----|-----|-----|
| hubspot_object | yes | Identifies the object to pull data for. 
| limit | no | Record limit for each page request. Default limit is set to 50.
| include_all_properties | no | Include all properties available for the object type.
| properties | no | Provide a list of property names to return.
| properties_with_history | no | Provide a list of property names to return with history of the property values.
| archived | no | Identify whether to pull only archived records or un-archived records.
| get_pipelines | no | Return all pipelines of the object rather than the object data. Example would be pulling all pipelines of the deals object.
| get_properties | no | Return all properties of the object rather than the object data. Example is pulling all properties of the deals object.
| flatten_properties_stage_fields | no | Convert all pipeline stage fields to separate columns as ARRAY<STRUCT<>>
| flatten_properties | no | Convert all properties fields to separate columns

The HubSpot object data is returned in the form of a struct of Properties with sub-fields in the struct for each property of the object. Utilize the flatten_properties custom parameter to flatten the data into separate columns for every property.

For objects that have pipelines (deals and tickets), the properties fields that are returned are duplicated for every stage of a pipeline (e.g. hs_date_entered shows up with hs_date_entered_203523623 and hs_date_entered_203523624). To make the data easier to work with, utilize the flatten_properties_stage_fields to separate these fields into separate columns as ARRAY<STRUCT<>>. This easily enables you to use the sub-source feature in DataForge.

## Contributing
We've made every effort to support all the HubSpot CRM objects and parameters available. However, contributions are always welcome! 

Please feel free to contribute by submitting an issue or joining the discussion. Each contribution helps us grow and improve.

## Links
- https://dataforgelabs.com
