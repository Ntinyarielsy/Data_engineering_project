from prefect.infrastructure.container import DockerContainer

# alternative to creating DockerContainer block in the UI
docker_block = DockerContainer(
    image="gatobu/prefect:prefect-flow",  # insert your image here
    image_pull_policy="ALWAYS",
    #auto_remove=True,
)

docker_block.save("prefect-etl-deploy", overwrite=True)