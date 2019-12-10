# How to run the tool `trade_quality.py` #


1. ssh to the ec2 that has the simulation software installed

2. Carry out the following setup work

    `$ cd simulator`

    `$ source ./scripts/project`

    `$ cd Python/Tools`


3. Checkout out the tools usage:

    `$ python trade_quality.py --help`

4.  The following commanding is possible:
    
    View trade quality statistics for a specific market:

     `$ python trade_quality.py -m usd-bsv`
   
    View trade quality statistics for all markets:

     `$ python trade_quality.py -all`

    Clear trade quality statistics for a specific market:

     `$ python trade_quality.py -m usd-bsv -c`

    Clear trade quality statistics for all markets:

     `$ python trade_quality.py -all -c`











