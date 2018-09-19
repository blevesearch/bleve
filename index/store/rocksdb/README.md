## install rocksDB
wget https://github.com/facebook/rocksdb/archive/v5.11.3.tar.gz
tar xvf v5.11.3.tar.gz
cd rocksdb-5.11.3
mkdir build
cd build
cmake ..
make -j 4
sudo make install

## install gorocksDB

CGO_CFLAGS="-I/path/to/rocksdb/include" \
CGO_LDFLAGS="-L/path/to/rocksdb -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd" \
  go get github.com/tecbot/gorocksdb
  
##### note
 path/to/ must the real rocksdb path for lib and include
 may be you need install snappy and lz4
 
 brew install snappy
 
 brew install lz4
 
 ## build
 when build you must add CGO_CFLAGS and CGO_LDFLAGS before go build

