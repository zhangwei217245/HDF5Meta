#!/bin/sh

autoreconf --install

./configure --prefix=$(pwd)/test

make && make install


