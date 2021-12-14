error_main: error_main.o hash_file.o
	@echo " Compile error_main ...";
	gcc -L ./lib/ -Wl,-rpath,./lib/ ./build/error_main.o ./build/hash_file.o -lbf -o ./bin/runner

main: main.o hash_file.o
	@echo " Compile main ...";
	gcc -L ./lib/ -Wl,-rpath,./lib/ ./build/main.o ./build/hash_file.o -lbf -o ./bin/runner

main.o:
	@echo " Compile main ...";
	gcc -I ./include/ -c -o ./build/main.o ./src/main.c -O2

error_main.o:
	@echo " Compile main ...";
	gcc -I ./include/ -c -o ./build/error_main.o ./src/error_main.c -O2

hash_file.o:
	@echo " Compile main ...";
	gcc -I ./include/ -c -o ./build/hash_file.o ./src/hash_file.c -O2

ht: 
	@echo " Compile ht_main ...";
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/ht_main.c ./src/hash_file.c -lbf -o ./bin/runner -O2

bf:
	@echo " Compile bf_main ...";
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/bf_main.c -lbf -o ./bin/runner -O2

run: 
	./bin/runner

clean:
	@echo " Delete binary, build and data ...";
	-rm -f ./bin/runner ./build/main.o ./build/error_main.o ./build/hash_file.o ./data.db ./data1.db ./data_1.db ./data_2.db ./data_3.db