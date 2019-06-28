# Multiprocess_program

This project applies multiprocessing technique to solve a task that reading a big twitter file and simply processing it.

## Components of project
task description files  
slurm files  
python files  
test twitter files

## Prerequisites

python3  
mpi4py  
collections  
json  
HPC environment 

## Running the tests
Submit the corresponding slurm file to HPC system.

## Note
The difference between beta1 and beta2 is that: the way for beta1 to get the hashtag is extracting from field "hashtags", for beta 2 is according to the format <space>#XX..X<space> in the "text" field.
