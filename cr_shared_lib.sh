gcc -c -fPIC upper.c -o strupper.o
gcc -shared upper.c -o libstrupper.so

cp libstrupper.so $ORACLE_HOME/bin
