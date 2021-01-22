Copy-Item Z:\memurai\dump.rdb -Destination D:\
Copy-Item Z:\load_uniform_250M_raw.dat -Destination D:\
Copy-Item Z:\load_zipf_250M_raw.dat -Destination D:\
Copy-Item Z:\run_uniform_250M_1000M_raw.dat -Destination D:\
Copy-Item Z:\run_zipf_250M_1000M_raw.dat -Destination D:\
& 'C:\Program Files\Memurai\memurai-cli.exe' shutdown nosave
& 'C:\Program Files\Memurai\memurai.exe' C:\memurai.conf