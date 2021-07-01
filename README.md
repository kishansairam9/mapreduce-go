# Map Reduce in Go

- Implemented Map Reduce in Go Lang using RPCs using wrapper code of MIT's [6.824 Distributed Systems Lab 1](https://pdos.csail.mit.edu/6.824/labs/lab-mr.html)
  
## Directory Structure

- src -> Wrapper Code from Lab files
- mr -> Implementation of map reduce logic
  - `coordinator.go` -> Master in Map Reduce
    - Assigns work to worker nodes (both map and reduce tasks)
    - Handles worker faults by reassigning work to different nodes if they don't finish in timeout (10s here)
  - `worker.go` -> Worker in Map Reduce
    - Reads input files performs operation using provided function and writes results to files
    - Coordinates with coordinator to get work
  - `rpc.go` -> RPC definitions

## Running Tests

- cd into src/main
- Run `test-mr.sh` to test using programs in boiler plate testing different scenarios
```
cd src/main
./test-mr.sh
```
- `test-mr-many.sh` takes arugment trails - no of iterations to run tests. More robust testing
