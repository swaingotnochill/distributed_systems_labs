default: all

build_mrsequential: main/mrsequential.go
	go build -o ./main/mrsequential ./main/mrsequential.go
	codesign --force --deep --sign - ./main/mrsequential

run_mrsequential: build_mrsequential
	./main/mrsequential ./main/wc.so ./main/pg-*.txt

build_mrcoordinator: main/mrcoordinator.go
	go build -o ./main/mrcoordinator ./main/mrcoordinator.go
	codesign --force --deep --sign - ./main/mrcoordinator

run_mrcoordinator: build_mrcoordinator
	./main/mrcoordinator ./main/pg-*.txt

build_mrworker: mrapps/wc.go main/mrworker.go
	go build -o ./main/mrworker ./main/mrworker.go
	codesign --force --deep --sign - ./main/mrworker
	go build -buildmode=plugin -o ./main/wc.so mrapps/wc.go
	codesign --force --deep --sign - ./main/wc.so

run_mrworker: build_mrworker
	./main/mrworker ./main/wc.so
#
# test: main/test-mr.sh build_mrsequential
# 	bash main/test-mr.sh

.PHONY: clean

clean:
	rm -r ./main/mrcoordinator ./main/mrworker ./main/wc.so
	rm mr-out-*






