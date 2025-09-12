# Quickbooks-Reports source connector


This is the repository for the Quickbooks-Reports source connector, written in Python.
For information about how to use this connector within Airbyte, see [the documentation](https://docs.airbyte.com/integrations/sources/Quickbooks-Reports).

## Local development

### Prerequisites
* Python (~=3.11)
* Poetry (~=1.7) - installation instructions [here](https://python-poetry.org/docs/#installation)


### Installing the connector
From this connector directory, run:
```bash
poetry install --with dev
```


### Create credentials
**If you are a community contributor**, follow the instructions in the [documentation](https://docs.airbyte.com/integrations/sources/Quickbooks-Reports)
to generate the necessary credentials. Then create a file `secrets/config.json` conforming to the `source_youtube_analytics/spec.yaml` file.
Note that any directory named `secrets` is gitignored across the entire Airbyte repo, so there is no danger of accidentally checking in sensitive information.
See `sample_files/sample_config.json` for a sample config file.


### Locally running the connector
```
poetry run source-quickbooks-drivepoint spec
poetry run source-quickbooks-drivepoint check --config secrets/config.json
poetry run source-quickbooks-drivepoint discover --config secrets/config.json
poetry run source-quickbooks-drivepoint read --config secrets/config.json --catalog integration_tests/configured_catalog.json
```

### Running unit tests
To run unit tests locally, from the connector directory run:
```
poetry run pytest unit_tests
```

### Building the docker image
1. Install [`airbyte-ci`](https://github.com/airbytehq/airbyte/blob/master/airbyte-ci/connectors/pipelines/README.md)
2. Run the following command to build the docker image:
```bash
airbyte-ci connectors --name=source-quickbooks-drivepoint build
```
or if you need to build for a different architecture (e.g., to deploy to VM):
```bash 
airbyte-ci connectors --name=source-quickbooks-drivepoint build --architecture=linux/amd64
```

An image will be available on your host with the tag `airbyte/source-quickbooks-drivepoint:dev`.


### Running as a docker container
Then run any of the connector commands as follows:
```
docker run --rm airbyte/source-quickbooks-drivepoint:dev spec
docker run --rm -v $(pwd)/secrets:/secrets airbyte/source-quickbooks-drivepoint:dev check --config /secrets/config.json
docker run --rm -v $(pwd)/secrets:/secrets airbyte/source-quickbooks-drivepoint:dev discover --config /secrets/config.json
docker run --rm -v $(pwd)/secrets:/secrets -v $(pwd)/integration_tests:/integration_tests airbyte/source-quickbooks-drivepoint:dev read --config /secrets/config.json --catalog /integration_tests/configured_catalog.json
```

### Running our CI test suite
You can run our full test suite locally using [`airbyte-ci`](https://github.com/airbytehq/airbyte/blob/master/airbyte-ci/connectors/pipelines/README.md):
```bash
airbyte-ci connectors --name=source-quickbooks-drivepoint test
```

### Customizing acceptance Tests
Customize `acceptance-test-config.yml` file to configure acceptance tests. See [Connector Acceptance Tests](https://docs.airbyte.com/connector-development/testing-connectors/connector-acceptance-tests-reference) for more information.
If your connector requires to create or destroy resources for use during acceptance tests create fixtures for it and place them inside integration_tests/acceptance.py.

### Dependency Management
All of your dependencies should be managed via Poetry. 
To add a new dependency, run:
```bash
poetry add <package-name>
```

Please commit the changes to `pyproject.toml` and `poetry.lock` files.

## Publishing a new version of the connector
You've checked out the repo, implemented a million dollar feature, and you're ready to share your changes with the world. Now what?
1. Make sure your changes are passing our test suite: `airbyte-ci connectors --name=source-quickbooks-drivepoint test`
2. Bump the connector version (please follow [semantic versioning for connectors](https://docs.airbyte.com/contributing-to-airbyte/resources/pull-requests-handbook/#semantic-versioning-for-connectors)): 
    - bump the `dockerImageTag` value in in `metadata.yaml`
    - bump the `version` value in `pyproject.toml`
3. Make sure the `metadata.yaml` content is up to date.
4. Make sure the connector documentation and its changelog is up to date (`docs/integrations/sources/Quickbooks-Reports.md`).
5. Create a Pull Request: use [our PR naming conventions](https://docs.airbyte.com/contributing-to-airbyte/resources/pull-requests-handbook/#pull-request-title-convention).
6. Pat yourself on the back for being an awesome contributor.
7. Someone from Airbyte will take a look at your PR and iterate with you to merge it into master.
8. Once your PR is merged, the new version of the connector will be automatically published to Docker Hub and our connector registry.

## Pushing a new version of the connector to Self-Managed Community Airbyte
If you want to push a new version of the connector to your Self-Managed Community Airbyte instance, follow these steps:
1. Build the docker image locally as described above in "Building the docker image".
2. Tag the image with the version you want to push (the version in `metadata.yaml`):
```bash
docker tag airbyte/source-quickbooks-drivepoint:dev path/to/artifactory:<version>
```
3. Push the image to your Artifactory:
```bash
docker push path/to/artifactory:<version>
```
4. SSH into your Airbyte instance and download + check the image you pushed:
```bash
docker run --rm -i path/to/artifactory:<version> spec
```
5. Load image to airbyte:
```bash
kind load docker-image path/to/artifactory:<version> -n airbyte-abctl
```
6. Load Airbyte UI and add the new sources from your image.