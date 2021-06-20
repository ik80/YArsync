# YArsync



## Overview

This is reimplementation of librsync/rsync in multithreaded way. It began as takeaway programming test. Then it began getting hotter by the minute, and then it started **burning**

## Design

It will consist of two parts, a library with the common code and client program. Library will contain functionality that implements rsync algorithm and related utility code. Client will make heavy use of [Asio library](https://think-async.com/Asio/) for async operations using coroutines. Client should support both spinning and SSD storage, and will also take care of directory watching functionality in a cross-platform way (via [filesentry](https://github.com/Nelarius/filesentry)). Client performs synchronization of multiple files in parallel.

Every outstanding synchronization request will have a synchronization context associated with it on both client and server. Proccessing will be done as follows:

 1. Client creates context  and requests digest for the server version of the file.
 2. Server calculates digest for the file and sends it to the client, saving digest locally.
 3. Client calculates diff against its version of the file and the digest received, sends it over to the server (async streaming operation)
 4. Server applies diff against its version of file and saved digest to reconstruct clients version of the file locally (also async streaming operation in lockstep with the above)

## Asio implementation detail

Both client and server will use single asio::io_context with a large thread pool.

## TODO

 1. Linux/cmake build
 2. Git submodules for asio/filesentry
 3. Unit tests
 4. Change CLI interface to match that of rsync
 5. implement whole file transfers via sendfile/transmitfile
