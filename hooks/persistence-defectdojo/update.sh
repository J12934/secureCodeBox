#!/bin/bash
# Copyright 2020 The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This script generates the model classes from a released version of cert-manager CRDs
# under src/main/java/io/cert/manager/models.

DEFAULT_IMAGE_NAME=docker.pkg.github.com/kubernetes-client/java/crd-model-gen
DEFAULT_IMAGE_TAG=v1.0.3
IMAGE_NAME=${IMAGE_NAME:=$DEFAULT_IMAGE_NAME}
IMAGE_TAG=${IMAGE_TAG:=$DEFAULT_IMAGE_TAG}

# a crdgen container is run in a way that:
#   1. it has access to the docker daemon on the host so that it is able to create sibling container on the host
#   2. it runs on the host network so that it is able to communicate with the KinD cluster it launched on the host
docker run \
  --rm \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v "$(pwd)":"$(pwd)" \
  --network host \
  ${IMAGE_NAME}:${IMAGE_TAG} \
  /generate.sh \
  -u https://gist.githubusercontent.com/J12934/67722bc1c6bf69dc5d09cb7aed341fac/raw/3fcccc0a145ee7babd64c57a707e4c997fab00ad/scan-crd.yaml \
  -n io.securecodebox \
  -p io.securecodebox \
  -o "$(pwd)"