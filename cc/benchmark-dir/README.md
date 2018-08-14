Setting up YCSB
===============
First, download and install YCSB, from 
https://github.com/brianfrankcooper/YCSB/ . Configure YCSB for your intended
workload, and run the "basic" driver (both "load" and "run," as required),
redirecting the output to a file.

The output of YCSB's "basic" driver is verbose. A typical line looks like:

  INSERT usertable user5575651532496486335 [ field1='...' ... ]

To speed up file ingestion, our basic YCSB benchmark assumes that the input
file consists only of the 8-byte-integer portion of the key--e.g.:

  5575651532496486335

To convert YCSB "basic" output to the format we expect, run "process_ycsb."
