# CKAN-Publisher-App
Snowflake to CKAN Connector https://medium.com/@gabriel.mullen/connecting-snowflake-to-ckan-for-publishing-to-the-open-data-portal-176384708f8e
The California Department of Technology will use the CKAN Publisher App to easily publish data from Snowflake to the California Open Data Portal.

Maintainers Gabriel Mullen is a Senior Sales Engineer with Snowflake for the State and Local Government Team

This is a community-developed script, not an official Snowflake offering. It comes with no support or warranty. However, feel free to raise a github issue if you find a bug or would like a new feature.

Legal Licensed under the Apache License, Version 2.0 (the "License"); you may not use this tool except in compliance with the License. You may obtain a copy of the License at: http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

Use the following command to get started with this streamlit app:
```
streamlit run main.py
```
The Main page gives simple guidance on how the app works
![Alt text](readme_images/main.png?raw=true "Main Page")
The Connect page sets the conext for where to store the published records. The accompaning SQL scripts must be executed first as they contain the backend pipeline DDL to make the connection to CKAN. The Control table is what tracks the status of the tables which have been published. The context should be set to the database and schema in which you executed the SQL files (i.e. where the Control table is located).
![Alt text](readme_images/connect.png?raw=true "Connect to Snowflake")
The Existing Tables page allows you to delete records from the Control table. There is a daily process that refreshes the presigned URLs to CKAN. Removing records from the Control table stops this refresh process for those resources and packages.
![Alt text](readme_images/Existing.png?raw=true "Review Existing Published Tables")
The Publish page allows the user to insert metadata about the data which will be published to CKAN. These are required fields from CKAN. Upon pressing publish, the external functions will create a package, drop the data to a Snowflake internal stage, and create a resource in the package with a presigned URL back to the data in Snowflake.
![Alt text](readme_images/Publish.png?raw=true "Publish Selected Tables")
![Alt text](readme_images/ckan_pipeline.png?raw=true "pipeline")
The proof is in the pudding. Here's a quick example of the results. But go check it out yourself; after all, it's open!
![Alt text](readme_images/OpenDataPortal.png?raw=true "Final Results")
