# YArsync



## Overview

This began as takeaway programming test. Then it began getting hotter by the minute, and then it started **burning**

## Design

It will consist of three parts, a library with the common code, client and server programs. Library will contain functionality that implements rsync algorithm and related utility code. Both client and server will make heavy use of [Asio library](https://think-async.com/Asio/) for async operations. Both client and server should support both spinning and SSD storage, and will also take care of directory watching functionality in a cross-platform way. Client/server should perform synchronization of multiple files in parallel. Processing of large files should be done in streaming manner. Some error handling should be performed.

Every outstanding synchronization request will have a synchronization context associated with it on both client and server. Proccessing will be done as follows:

 1. Client creates context  and requests digest for the server version of the file.
 2. Server calculates digest for the file and sends it to the client, saving digest locally.
 3. Client calculates diff against its version of the file and the digest received, sends it over to the server (async streaming operation)
 4. Server applies diff against its version of file and saved digest to reconstruct clients version of the file locally (also async streaming operation in lockstep with the above)

## Asio implementation detail

Both client and server will use single asio::io_context with either two threads or large thread pool (in case of HDD and SSD storage respectively). Request context object will have a asio::strand object, a set of buffers used for network and file I/O, and a compound state.
