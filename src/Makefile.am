AM_CPPFLAGS = -I$(srcdir)/../lib
bin_PROGRAMS = hello hello_mongoc
hello_SOURCES = dira/printa.c dirb/printb.c main.c
hello_LDADD = ../lib/libstring.la ../lib/libquery.la ../lib/libtimer.la

hello_mongoc_SOURCES = hello_mongoc.c
hello_mongoc_LDADD = ../lib/libstring.la ../lib/libquery.la ../lib/libtimer.la /global/homes/w/wzhang5/software/rust_mongo_bench/target/release/librust_mongo_bench.so

