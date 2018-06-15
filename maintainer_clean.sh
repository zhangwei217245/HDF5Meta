#!/bin/sh

make clean
make -k maintainer-clean
rm -rf Makefile.in Makefile build-aux/ autom4te.cache/ aclocal.m4 configure.scan configure config.status config.log m4 "test"
find ./ -name "Makefile.in" -exec rm -f {} \;


