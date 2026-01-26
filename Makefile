APP_NAME := gcp-tools
VERSION := 1.0.0

## Update pip
.PHONY: update_pip
update_pip:
	@echo "==> Updating pip..."
	python3 -m pip install --upgrade pip

## Update pip
.PHONY: install_builder
install_builder:
	@echo "==>Installing builder..."
	python3 -m pip install build

## Build distribution archives
.PHONY: build
build: update_pip install_builder
	@echo "==> Building distribution archives..."
	python3 -m build

## Upload to TestPyPi
.PHONY: upload_testpypi
upload_testpypi:
	@echo "==> Uploading distribution archives..."
	python3 -m pip install --upgrade twine
	python3 -m twine upload --repository testpypi dist/*

## Upload to PyPi
.PHONY: upload_pypi
upload_pypi:
	@echo "==> Uploading distribution archives..."
	python3 -m pip install --upgrade twine
	python3 -m twine upload dist/*
