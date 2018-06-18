AM_CPPFLAGS = -I$(srcdir)/../lib
bin_PROGRAMS = hello hello_mongoc hdf5_reader
hello_SOURCES = dira/printa.c dirb/printb.c main.c
hello_LDADD = ../lib/libstring.la ../lib/libquery.la ../lib/libtimer.la

hello_mongoc_SOURCES = hello_mongoc.c
hello_mongoc_LDADD = ../lib/libstring.la ../lib/libquery.la ../lib/libtimer.la ../lib/rust_mongo_bench/target/release/librust_mongo_bench.so

hdf5_reader_SOURCES = HDF5_reader.c
hdf5_reader_LDADD = ../lib/libstring.la ../lib/libquery.la ../lib/libtimer.la ../lib/libthpool.la ../lib/libindexds.la ../lib/libh5_meta.la