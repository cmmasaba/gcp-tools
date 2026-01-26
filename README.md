# Publishing Python Packages to Pip

## Env Vars

The following environment variables need to be set:

```bash

export GCP_PROJECT_ID=""
export GCP_BUCKET_NAME=""
export GCP_DATASET_ID=""
export GCP_LOGGER_NAME=""
export GOOGLE_APPLICATION_CREDENTIALS=""

```

### Challenges

```python

python3 -m build

```

No module named build

The `build` module from PEP 517 is not part of the standard library for Python. This error means it is not installed and should be installed using the following command:

```python

python3 -m pip install build

```

Ensure `__init__.py` is in the folder listed as project name
