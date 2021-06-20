#include "../YAlibrsync/YAlibrsync.h"

#include "Network.h"

size_t NetPacketHeader::getDataSize() const
{
	size_t dataSize = 0;
	switch (base.type)
	{
		case YAlibrsync::SyncContextState::NONE:
			abort();
		break;
		case YAlibrsync::SyncContextState::PING:
			dataSize = 0;
			break;
		case YAlibrsync::SyncContextState::PONG:
			dataSize = 0;
			break;
		case YAlibrsync::SyncContextState::COMPLETE:
			dataSize = 0;
			break;
		case YAlibrsync::SyncContextState::DIGEST_REQUEST:
			dataSize = digestRequest.fileNameLength;
			break;
		case YAlibrsync::SyncContextState::DIGEST_RESPONSE:
			dataSize = digestResponse.numDigestBlocks*sizeof(YAlibrsync::BlockDigest);
			break;
		case YAlibrsync::SyncContextState::DIFF_BLOCK:
			dataSize = diffBlock.packageSize;
			break;
		case YAlibrsync::SyncContextState::DIFF_APPLIED:
			dataSize = diffBlock.packageSize;
			break;
		case YAlibrsync::SyncContextState::DELETE_FILE:
			dataSize = deleteFile.fileNameLength;
			break;
		case YAlibrsync::SyncContextState::COPY_FILE:
			dataSize = copyFile.chunksize;
			break;
		default:
			abort();
		break;
	}
	return dataSize;
}