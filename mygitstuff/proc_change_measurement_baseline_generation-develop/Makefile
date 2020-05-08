MAKEFLAGS += --warn-undefined-variables

.POSIX:
SHELL := /bin/sh

.DEFAULT_GOAL := menu

archive_path := $(CURDIR)/.archive
base_image_name := proc-change-measurement-baseline-generation
version := $(shell git rev-parse HEAD)
archive := $(archive_path)/$(base_image_name)-$(version).tar
beta_tag_suffix ?= -$(shell git symbolic-ref --short HEAD)
image_registry := gcr.io/coral-atlas

%:
	@:

.PHONY: menu
menu:
	@ grep -E '^[a-zA-Z_-]+:.*?## .*$$' Makefile | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-35s\033[0m %s\n", $$1, $$2}'

.PHONY: build
build: $(archive) ## Build the Docker image
	@ :

$(archive): $(shell find $(CURDIR) -not -wholename "*/\.*" -type f) $(archive_path)
	docker build --tag $(base_image_name):$(version) --build-arg GITHUB_DEPLOY_KEY="$$GITHUB_DEPLOY_KEY" .
	docker save --output $(archive) $(base_image_name):$(version)

$(archive_path):
	mkdir -p $(archive_path)

.PHONY: test
test:  $(shell find $(CURDIR)/src -type f) ## Run the unit tests
	docker load --input $(archive)
	docker run $(base_image_name):$(version) sh -c "pipenv run pytest"

.PHONY: push
push: $(archive) ## Push the Docker image to Google Container Repository
	docker load --input $(archive)
	docker tag $(base_image_name):$(version) $(image_registry)/$(base_image_name):$(version)$(beta_tag_suffix)
	docker push $(image_registry)/$(base_image_name):$(version)$(beta_tag_suffix)
