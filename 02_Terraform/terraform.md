# Terraform and GCP

 In this section we are going to learn how to use terraform to create gcp infrastructure.

 ## what is covered:
    - Terraform overview
    - Installation of terraform
    - Creating GCP infrastructure

## What is terraform
Terraform is an open source infrastructure as code (IaC) tool offered by HarshiCorp that lets you define both cloud and on premise resources in human readable configurations files other than using graphical interface.

### Advantages of using terraform
1.  **State management** - it maintains a state file that keeps track of the current state of your infrastructure.
2. **Multi-cloud and hybrid cloud support** - Terraform supports a wide range of cloud providers including AWS,Azure, Google cloud and many others.It also support on-premises and hybrid cloud environments enabling one to manage infrastructure across diffrent platform with a single tool.
3. **Modularity and reusability** - It allows you to create modules which are templates for provisioning sets of resources.This modularity promotes best practices,consistency and code reuse within an organization.
4.  **Automation**- Terraform can be integrated into continuous integration and continuous deployment (CI/CD) pipelines, enabling automated infrastructure provisioning and updates as part of your software delivery process.

## Terraform installation and setup 
Download terraform here : <https://developer.hashicorp.com/terraform/downloads?product_intent=terraform>

For windows users once you have dowloaded you can follow these steps to add terraform to the system path variable:

1. Unzip the terraform zip file and store "Terraform.exe" file in a folder and run it 
2. Go to edit control panel > systen amd security >system > Advanced > Environent Variables > on system variable click path > Edit > New then copy the path to your unzipped tettaform and paste it here.Click okay
3. Now open command propmt and run this command:     
    `C:\user\user>terraform -version`
If it should return the version of terraform like
```
Terraform v1.5.6
on windows_386
```

#