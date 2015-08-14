dataset-publisher
=================

Service for exposing datasets as Hive tables
 
## Details
Service creates table (if it does not already exist) named after dataset title inside of database
(which is also created if it does not already exist) named after organization to which dataset
belongs to. In other words full name of the table is:
`<organization_name>.<dataset_title>`

#### Note
Name of the database, table and columns can be transformed to fit database engine and driver
naming scheme.

## Development
To start dataset-publisher run: 
`mvn spring-boot:run`

To run the service locally or in Cloud Foundry, the following environment variables need to be defined:
* `VCAP_SERVICES_SSO_CREDENTIALS_APIENDPOINT` - a Cloud Foundry API endpoint;
* `VCAP_SERVICES_SSO_CREDENTIALS_TOKENKEY` - an UAA endpoint for verifying token signatures;
* `VCAP_SERVICES_ARCADIA_CREDENTIALS_ARCADIAURL` - an Arcadia service address;
* `VCAP_SERVICES_HUE_CREDENTIALS_HUEURL` - a HUE service address;

