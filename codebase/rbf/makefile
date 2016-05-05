include ../makefile.inc

all: librbf.a rbftest8

# c file dependencies
pfm.o: pfm.h
rbfm.o: rbfm.h

# lib file dependencies
librbf.a: librbf.a(pfm.o)  # and possibly other .o files
librbf.a: librbf.a(rbfm.o)

rbftest8.o: pfm.h rbfm.h

# binary dependencies
rbftest8: rbftest8.o librbf.a $(CODEROOT)/rbf/librbf.a

# dependencies to compile used libraries
.PHONY: $(CODEROOT)/rbf/librbf.a
$(CODEROOT)/rbf/librbf.a:
	$(MAKE) -C $(CODEROOT)/rbf librbf.a

.PHONY: clean
clean:
	-rm rbftest1 rbftest2 rbftest3 rbftest4 rbftest5 rbftest6 rbftest7 rbftest8 rbftest8b rbftest9 rbftest10 rbftest11 rbftest12 *.a *.o *~
