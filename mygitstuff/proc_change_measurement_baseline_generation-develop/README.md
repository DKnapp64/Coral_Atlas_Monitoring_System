## Generate a Baseline Mosaic to support Change Measurement

Given a folder in Google Cloud Storage (GCS) with subfolders, each subfolder containing one or more Planet quads for the same geographic area, this code will generate a baseline quad for each geographic area and upload them back to GCS.

It can run in a Docker container, so the only the only tool that needs to be installed locally is Docker. This code was tested with Docker version `19.03.1, build 74b1e89`, but earlier or later versions might also work.

### Running Locally
If needed, the code can be run locally. The required Python version and libraries are listed in the `Pipfile`. Install them and create your Python virtual environment with [`pipenv`](https://docs.pipenv.org/en/latest/).

```
$ pipenv shell
$ pipenv install
```

#### Running Tests
[`Pytest`](https://docs.pytest.org/en/latest/) is the test harness. Make sure tests are passing by running this or other `pytest` commands:

```
$ pipenv run pytest
```

#### Authentication
In order to authenticate with GCS, this code expects credentials to be provided via the following environment variable.

| Env Var                      | Notes                                      |
| :--------:                   | ------------------------------------------ |
| `SERVICE_ACCOUNT_KEY`        | Google Cloud Platform (GCP) service account key for an account that has the necessary permissions on the source and destination GCS folders (NOTE) |

NOTE: The `workflow-storage-user` GCP service account was created for this purpose, and a corresponding key has been stored in Vault at `coral-atlas/main/coral-atlas-storage-user`. To retrieve it from Vault with whitespace and carriage returns removed, run:

```
$ vault read --field=value coral-atlas/main/coral-atlas-storage-user | jq -c .
```

Remember when setting the `SERVICE_ACCOUNT_KEY` environment variable to surround JSON with single quotes:

```
$ export SERVICE_ACCOUNT_KEY='{"type":"service_account", etc}'
```

#### Running the Code
```
$ baseline [GCP_PROJECT_NAME] [GCS_BUCKET_NAME] [SOURCE_PATH] [DESTINATION_PATH]
```

| Argument                     | Notes                                      |
| :--------:                   | ------------------------------------------ |
| `GCP_PROJECT_NAME` | `REQUIRED` The name of the GCP project where the quads are stored |
| `GCS_BUCKET_NAME` | `REQUIRED` The name of the GCS bucket where the quads are stored |
| `SOURCE_PATH` | `REQUIRED` The path to the folder in the GCS bucket that contains all of the quad subfolders
| `DESTINATION_PATH` | `REQUIRED` The path to the folder in the GCS bucket where the baseline quads will be stored

Sample command:

```
$ baseline coral-atlas coral-atlas-workflow path/to/input/quads path/to/baseline/quads
```

### Running in Docker
If you don't want to bother with installing the dependencies locally and dealing with Python virtual environments, you can run the code in a Docker image.

#### Building the Docker Image
Use this command to build the Docker Image:

```
$ docker build .
```

#### Running the Docker Image
Use this command to run the code using the Docker image if you're providing the boundary GeoJSON directly in the command line (not as a path to a file). Note that it requires the same environment variable described above.

```
$ docker run --rm -e SERVICE_ACCOUNT_KEY [IMAGE] sh -c "baseline [ARGS]"
```

Where `IMAGE` is the `id` of the image you just built (or an image build by continuous integration - see below), and `ARGS` are as defined above. Note that GeoJSON quotes must be escaped.

Sample command:
```
$ docker run --rm -e SERVICE_ACCOUNT_KEY [IMAGE] sh -c "baseline coral-atlas coral-atlas-workflow path/to/input/quads path/to/baseline/quads"
```

### Continuous Integration

[CircleCI continuous integration](https://circleci.com) is configured to build this code into a Docker image, run the tests, and push it to Google Container Registry (GCR) at `gcr.io/coral-atlas/proc-change-measurement-baseline-generation`.

CI is configured in the `.circleci/` directory.

In order to push the Docker image to GCR, CircleCI needs appropriate credentials. The `container-registry-push` GCP service account was created for this purpose, and a corresponding key has been stored in Vault at `coral-atlas/main/coral-atlas-container-registry-push`. To retrieve it from Vault in the base64-encoded format that CircleCI requires, run:

```
$ vault read --field=value coral-atlas/main/coral-atlas-container-registry-push | base64
```

If it doesn't already exist, [create an environment variable](https://circleci.com/docs/2.0/env-vars/) in the CircleCI project named `ENCODED_GCR_PUSH_KEY` and give it the base64-encoded GCR push credentials.

CircleCI simply calls targets in the `Makefile`, but the image can also be built, tested, and pushed locally using the corresponding `make` targets:

```
$ make [build, test, push]
```

NOTE: Pushing the Docker image from your local machine up to GCR requires that you have authenticated to `https://gcr.io` using the `docker login` command.
