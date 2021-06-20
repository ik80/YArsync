#pragma once

#include <chrono>

#include "../YAlibrsync/YAlibrsync.h"

struct NetPacketHeaderBase
{
	YAlibrsync::SyncContextState type;
	long long id;
};

struct NetDeleteHeader
{
	long long fileNameLength;
};

struct NetCopyHeader
{
	long long fileSize;
	long long offset;
	long long chunksize;
	long long fileAttributes; // TODO
};

struct NetDigestRequestHeader
{
	long long creationTime; 
	long long fileNameLength;
};

struct NetDigestResponseHeader
{
	int status; // TODO, 0 found, 1 not found
	long long numDigestBlocks;
	long long fileSize;
	long long blockSize;
	long long lastBlockSize;
	long long totalDiffBlocks;
};

struct NetDiffHeader 
{
};

struct NetDiffBlockHeader
{
	int status; // TODO
	long long totalDiffBlocks;
	long long numDiffBlocks;
	long long packageSize; // package is [existingblock][existingblock][newblock[data]]
};

struct NetSyncCompleteHeader {};

struct NetSyncFailedHeader {};

struct NetPacketHeader 
{
	NetPacketHeaderBase base;
	union  
	{
		NetDeleteHeader deleteFile;
		NetCopyHeader copyFile;
		NetDigestRequestHeader digestRequest;
		NetDigestResponseHeader digestResponse;
		NetDiffHeader diff;
		NetDiffBlockHeader diffBlock;
		NetSyncCompleteHeader syncComplete;
		NetSyncFailedHeader syncFailed;
	};
	size_t getDataSize() const;
};

