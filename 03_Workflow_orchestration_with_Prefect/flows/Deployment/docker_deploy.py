from prefect.infrastructure.container import DockerContainer
from prefect.deployments import Deployment
from parameterization_flow import etl_parent_flow

docker_container_block = DockerContainer.load("prefect-etl-deploy")
docker_dep = Deployment.build_from_flow(
    flow=etl_parent_flow,
    name="docker-flow",
    infrastructure=docker_container_block,
)


if __name__ == "__main__":
    docker_dep.apply()
    