# Google cloud platform
We are going to use GCP as our cloud platform.
We will looka at:

    - creating a free GCP account.
    - Setting up service acount and IAM roles.
    - Installinga,setting up google cli and connecting to cloud via cli
  
  Reference video:
  https://www.youtube.com/watch?v=Hajwnmj0xfQ&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=11
  ## Creating GCP account
   (For free version you will get up to 300 credits)
  
   1. create an account with your google email ID.
   2. Set up your first project e.g "data-engineering-course" and note down the project ID.

## Service account overview
Service account in is a special type of Google account that is used by applications or services to access GCP resources securely. Service accounts are not associated with a specific individual user, but rather with a specific application, service, or resource. 

They are often used when you need to grant permissions to resources within your GCP projects without exposing your own personal credentials.

Here are some key characteristics and uses of service accounts in GCP:

   1. ***Identity***: Each service account is associated with a unique email address in the format <service-account-name>@<project-id>.iam.gserviceaccount.com.

2. ***Permissions:*** Service accounts can be granted specific IAM (Identity and Access Management) roles and permissions within a GCP project. This allows you to control what actions the service account is allowed to perform.

3. ***Keys:*** Service accounts can be associated with one or more cryptographic keys, such as JSON key files or OAuth 2.0 access tokens, which are used for authentication when making API requests.

## Initial setup
1. Setup service account & authentication for this project
   Grant Viewer role to begin with
2. Download service-account-keys (.json) for auth.

## Setup for Access
IAM Roles for Service account:

1. Go to the IAM section of IAM & Admin https://console.cloud.google.com/iam-admin/iam
2. Click the Edit principal icon for your service account.
Add these roles in addition to Viewer : Storage Admin + Storage Object Admin + BigQuery Admin
3. Enable these APIs for your project:
    https://console.cloud.google.com/apis/library/iam.googleapis.com
    https://console.cloud.google.com/apis/library/iamcredentials.googleapis.com

**NB: make sure when you click on the APIs link make sure you choose the specific project you are working on then enable it.**
   
## Google CLI download and setup
Download SDK for local setup here:<https://cloud.google.com/sdk/docs/install-sdk>

Set environment variable to point to your downloaded GCP keys
(for windows users use this)
``` 
set GOOGLE_APPLICATION_CREDENTIALS="<path/to/your/service-account-authkeys>.json" 

```
For Mac and Linux users use
```
EXPORT GOOGLE_APPLICATION_CREDENTIALS="<path/to/your/service-account-authkeys>.json"
```



then efresh token/session, and verify authentication
```
gcloud auth application-default login
```