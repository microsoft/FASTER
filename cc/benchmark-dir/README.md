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


Running YCSB
============

1) 50:50 reads:updates, 72 threads, and YCSB zipf distribution files:

```
benchmark.exe 0 72 d:\ycsb_files\load_zipf_250M_raw.dat d:\ycsb_files\run_zipf_250M_1000M_raw.dat
```

2) 50:50 reads:updates, 72 threads, and YCSB uniform distribution files:

```
benchmark.exe 0 72 d:\ycsb_files\load_uniform_250M_raw.dat d:\ycsb_files\run_uniform_250M_1000M_raw.dat
```

3) 100% RMW, 72 threads, and YCSB zipf distribution files:

```
benchmark.exe 1 72 d:\ycsb_files\load_zipf_250M_raw.dat d:\ycsb_files\run_zipf_250M_1000M_raw.dat
```

4) 100% RMW, 72 threads, and YCSB uniform distribution files:

```
benchmark.exe 1 72 d:\ycsb_files\load_uniform_250M_raw.dat d:\ycsb_files\run_uniform_250M_1000M_raw.dat
```

