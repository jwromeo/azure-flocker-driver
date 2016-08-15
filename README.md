# Azure Driver for ClusterHQ/Flocker
====================================

[![Build Status](https://travis-ci.org/CatalystCode/azure-flocker-driver.svg?branch=master)](https://travis-ci.org/CatalystCode/azure-flocker-driver)
[![Code Climate](https://codeclimate.com/github/CatalystCode/azure-flocker-driver/badges/gpa.svg)](https://codeclimate.com/github/CatalystCode/azure-flocker-driver)

*Tested against Flocker 1.14.0*

This block storage driver for [Flocker](https://clusterhq.com/) enables the use of data disks with Azure VMs.

## Overview
Flocker is an open-source Container Data Volume Manager for your Dockerized applications.

Typical Docker data volumes are tied to a single server. With Flocker datasets, the data volume can move with a container between different hosts in your cluster. This flexibility allows stateful container services to access data no matter where the container is placed.

## Prerequisites

The following components are required before using the Azure Driver for Flocker:

* A working Flocker installation on Azure
* Azure VMs with at least 4 data disk slots.

**Flocker**

You must first have Flocker installed on your node. Instructions on getting started with Flocker can be found on the [Flocker](https://clusterhq.com/flocker/getting-started) web site.


## Installation

**Download Driver**

Download the Azure driver to the node on which you want to use Azure storage. This process will need to be performed for each node in your cluster.

```bash
git clone https://github.com/CatalystCode/azure-flocker-driver
cd azure-flocker-driver
sudo /opt/flocker/bin/python setuppython setup.py install
```

**_NOTE:_** Make sure to use the python version installed with Flocker or the driver will not be installed correctly.

**Configure Flocker**

After the Azure Flocker driver is installed on the local node, Flocker must be configured to use that driver. 

Configuration is set in Flocker's agent.yml file. Copy the example agent file installed with the driver to get started:

```bash
sudo cp /etc/flocker/example.azure_agent.yml /etc/flocker/agent.yml
sudo vi /etc/flocker/agent.yml
```

Edit the agent.yml file to include the required Azure configuration settings. Sample placeholder information is included in the example file.

Some descriptions of the values are:

```bash
version: 1
control-server:
  hostname: "<host or IP of the Flocker control server>"
  "port": 4524

dataset:
  backend: "azure_flocker_driver"
  client_id: "<AZURE_CLIENT_ID>"
  tenant_id: "<AZURE_TENANT_ID>"
  client_secret: "<AZURE_CLIENT_SECRET>"
  subscription_id: "<AZURE_SUBSCRIPTION_ID>"
  storage_account_name: "<STORAGE_ACCOUNT_NAME>"
  storage_account_key: "<STORAGE_ACCOUNT_KEY>"
  storage_account_container: "<STORAGE_ACCOUNT_CONTAINER>"
  group_name: "<AZURE_RESOURCE_GROUP_NAME>"
  location: "<AZURE_RESOURCE_GROUP_LOCATION>"
  async_timeout: 100000
  debug: "false"
```

**_NOTE:_** The agent configuration should match between all nodes of the cluster.


**Test Configuration**

To validate agent settings and make sure everything will work as expected, you may run the following tests from the downloaded driver directory.

```bash
cd azure-flocker-driver
export FLOCKER_CONFIG="/etc/flocker/agent.yml"
sudo trial test_azure_driver.py
```

Several tests will be run to verify the functionality of the driver. Test action logging will output to the file driver.log in the local directory.

## Getting Help
For general Flocker issues, you can either contact [Flocker](http://docs.clusterhq.com/en/latest/gettinginvolved/contributing.html#talk-to-us) or file a [GitHub Issue](https://github.com/clusterhq/flocker/issues).

You can also connect with ClusterHQ help on [IRC](https://webchat.freenode.net/) in the \#clusterhq channel.

For specific issues with the Azure Driver for Flocker, file a [GitHub Issue](https://github.com/CatalystCode/azure-flocker-driver/issues).

If you have any suggestions for an improvements, please feel free create a fork in your repository, make any changes, and submit a pull request to have the changes considered for merging. Community collaboration is welcome!

**As a community project, no warranties are provided for the use of this code.**

## License
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
