# Sentient Trading Simulator #
## AWS ec2 Preparation ##

To the target AWS ec2 apply the following changes:

1. Add the following `export` to ~/.profile:

    `export MONGODB=mongodb://<username>:<password>@<database_host>:<port>/admin?readPreference=primary`

3. After saving the file, ensure that the above exports are applied:

    `$ source ~/.profile`

## Software Installation ##

It is assumed that Python 3.6.5 has already been installed on the target ec2.  What follows are instructions to install the simulation software.

1. Install the software:

    `$ git clone https://github.com/1057405bcltd/simulator.git`
    
2. Create a Python virtual environment in the same folder:

    `$ python3.7 -m venv simulator`

3. Activate the virtual environment:

    `source ./simulator/bin/activate`

4. `$ cd simulator`

5. Prepare the simulator environment:

    `$ source ./Scripts/project`

6. Install the required Python packages:

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











