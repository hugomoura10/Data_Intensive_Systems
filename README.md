# Data Intensive Systems Assignment, HTML: Group 01


## Overview

This project aims to find a solution for grouping similar processes together from a given a log. The log contains a sequence of Server Requests and Responses from a certain user with a specific user ID. The log analysis has two steps:

1. First we need to identify different small variations of the a processes recorded in the log file and when we identify processes that are similar, replace them with one specific representation.
2. Identify groups of processes that look similar.


## Table of Contents

- [Requirements](#requirements)
- [Usage](#usage)
- [Files](#files)
- [Contributors](#contributors)


## Requirements
- Python 3.8.8
- Libraries: pyspark, datasketch, numpy, shutil, os, resource, time, psutil, matplotlib
- Other software: Jupyter Notebook, Git


## Usage
Run the experiments.ipynb file to obtain all the outputs and see all the experiments.


## Files

### Synthetic Data Generation
The data_generation.py file contains all the functions used to create the syntheticly generated dataset. To create a dataset run generate_dataset(), which requires the following parameters to be specified:

1. tasks: a dictionary containing the structure of the network (dic);
2. min_datapoints: minimum number of desired datapoints (int);
3. start_time: start of the time window for datapoints' timestamp (datetime.datetime;)
4. end_time: end of the time window for the datapoints timestamp (datetime.datetime);
5. random: whether it is possible to have only a subset of the servers defined in tasks (boolean);
6. connect: whether the tasks dictionary contains connect servers (boolean);
7. file_name: name of the output file (string).

### Solution
The main.py file contains all the functions related to our solution, including some functions used to extract traces, investigate and plot performance metrics, implemente MinHashLSH and bucketing with BFS, and write functions for the outputs.

To obtain the output files, run the function output(), which requires the following parameters to be specified:

1. dataset: contains the name of the file containing the dataset we want to investigate (string);
2. k: value of k for the k-shingles (int);
3. threshold: percentage threshold for the MinHash function from datasketch (float);
4. p1: whether we want to print the output file with the same format then the input file (true->part1Output.txt AND part1Observations.txt), or only the one with the structure of the groups (false->part1Observations.txt) (boolean);

### Experiments
This experiments.ipynb file contains all the experiments performed in this assignment, for both folds.


## Contributors
- t.andradecapeladasilvacarrilho@students.uu.nl
- m.s.matre@students.uu.nl
- h.d.marcosmoura@students.uu.nl

