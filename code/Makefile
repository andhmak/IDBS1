error_main: 
	@echo " Compile main ...";
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./src/error_main.c ./src/hash_file.c -lbf -o ./build/runner -O2

main: 
	@echo " Compile main ...";
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./src/main.c ./src/hash_file.c -lbf -o ./build/runner -O2

ht: 
	@echo " Compile ht_main ...";
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/ht_main.c ./src/hash_file.c -lbf -o ./build/runner -O2

bf:
	@echo " Compile bf_main ...";
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/bf_main.c -lbf -o ./build/runner -O2

run: 
	./build/runner

clean:
	@echo " Delete /build/runner ...";
	-rm -f ./build/runner ./data.db ./data1.db ./data2.db ./data3.db