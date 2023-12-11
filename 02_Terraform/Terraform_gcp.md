# Building GCP infrastructure using Terraform
 The objective is to set up two gcp dervices using terraform

   1. Google Cloud Storage (GCS): Data Lake
   2. BigQuery: Data Warehouse

Reference video : <https://www.youtube.com/watch?v=dNkEgO-CExg&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=12>


### Files
* main.tf
* variables.tf


### Declarations
* terraform: configure basic Terraform settings to provision your   infrastructure
   * required_version: minimum Terraform version to apply to your configuration
  * backend: stores Terraform's "state" snapshots, to map real-world resources to your configuration.
    * local: stores state file locally as terraform.tfstate
  * required_providers: specifies the providers required by the current module
* provider:adds a set of resource types and/or data sources that Terraform can manage
The Terraform Registry is the main directory of publicly available providers from most major infrastructure platforms.
* resource blocks to define components of your infrastructure
Project modules/resources: google_storage_bucket, google_bigquery_dataset, google_bigquery_table
variable & locals
runtime arguments and constants

### Execution

Refresh service-account's auth-token for this session
``` 
gcloud auth application-default login
```
Open cmd and navigate to the folder containing terraform files then
Initialize state file (.tfstate)

```
terraform init
```
Check changes to new infra plan

```
terraform plan -var="project=<your-gcp-project-id>"
```
Create new infra
```
terraform apply -var="project=<your-gcp-project-id>"
```

Delete infra after your work, to avoid costs on any running services
```
terraform destroy
```