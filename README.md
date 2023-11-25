# CPSC416 Capstone Project

## MapReduceProject

### In the main folder run the following command

go build -buildmode=plugin ../mrapps/wc.go

### Clear output files

run "rm mr-out*"
Our output files starting with name "mr-out-*"

### Put text file(s) in the main folder

Text files should have same prefix name. In our example we use "pg-\*"

### Run sequential word count

go run mrsequential.go wc.so pg-\*.txt

### Run distributed word count

Start server(coordinator):\
go run mrcoordinator.go pg-\*.txt

Call workers to do the map and then reduce:\
go run mrworker.go wc.so

### Check work count results

Work count results are stored in the main folder starting with name "mr-out-\*"
