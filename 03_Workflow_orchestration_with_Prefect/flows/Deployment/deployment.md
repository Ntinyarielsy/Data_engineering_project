# Deployment




```
prefect deployment build parameterization_flow:etl_parent_flow -n "Parameterized ETL"
```
this creates a yaml file with all the details.WE can add the parameters to the yaml file i.e 

```parameters: {"colour":"yellow",months:[1,2,3],"year": 2021}```

then we need to apply the deployment using this command
```
prefect deployment apply etl_parent_flow-deployment.yaml
```