# Running SoFASTER on CloudLab
This README describes how to run SoFASTER on [CloudLab](https://www.cloudlab.us/).
There are two ways to do so: by joining the sandstorm project, or by creating a
new project.

## Using the sandstorm project
1. Create an account on CloudLab and request to join the "sandstorm" project
2. Create an experiment using the "sofaster" profile
3. Once the experiment has been successfully allocated, ssh into each machine and
   run `/proj/sandstorm-PG0/sofaster/setup.sh`. This command will install
   dependencies, setup vim and git, clone into SoFASTER, and run cmake

## Creating a new project
1. Create a new project on CloudLab
2. Copy `setup.sh` into the `/proj` directory of the project
3. Create a new experimental profile called "sofaster". Upload `sofaster.py` to
   generate the profile
4. Create an experiment using this "sofaster" profile
5. Once the experiment has been successfully allocated, ssh into each machine and
   run the previously copied `setup.sh`. This command will install dependencies,
   setup vim and git, clone into SoFASTER, and run cmake
