# Sentient Trading Simulator #
## Software Installation ##

It is assumed that the mandated Python version has already been installed on the target ec2.  What follows are instructions to install the simulation software.

1. Install the requisite Python modules:

    `$ pip install -r Python/requirements.txt`


## Configuration ##

Using whatever tool is available, create a document in the `configurations` collection in the `sim` database (documents must be uniquely named) that describes the details of the planned simulation.

ToDo: Describe the structure of `configurations` documents.

## Running ##

Simulator execution is initiated as follows:

`$ sim-load <config_name>`
`$ sim <config_name>`

where <config_name> refers to the name of a `configurations` document.

Output of a simulation run appears in the following collections:

  * `simulations`: Top-level description of the simulation.  Investigation of the consequences of a simulation run normally will begin with this collection.
  * `partitions`: A partition is effectively a sub-simulation working within a time subframe of a wider simulation.
  * `trades`: Detailed simulation results recorded by the ids of the simulation and partition in which the simulation trade was created.

## Cleaning

The following command clears out the results of all simulations (it does NOT, however, delete documents from the `configurations` collection).

`$ clean`











