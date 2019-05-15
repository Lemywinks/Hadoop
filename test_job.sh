#/bin/bash
cd src/
javac */*.java
./ordo/DaemonImpl Server4 2225
./ordo/DaemonImpl Server5 2226
./ordo/Job