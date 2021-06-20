#include "YAlibrsync.h"

#include <vector>
#include <unordered_map>
#include <iostream>
#include <fstream>
#include <cassert>
#include <filesystem>

#include <stdio.h>
#include <math.h>

#include "YAlibrsync.h"

namespace YAlibrsync
{
    uint32_t adler32(unsigned char* data, long long len)
    {
        uint32_t a = 1, b = 0;
        for (long long index = 0; index < len; ++index)
        {
            a = (a + data[index]) % ADLER_PRIME;
            b = (b + a) % ADLER_PRIME;
        }
        return (b << 16) | a;
    }
    uint32_t rollAdler32(uint32_t lastHash, unsigned char byteOut, unsigned char byteIn)
    {
        // overflows should be just fine
        uint32_t a = ((lastHash & 0xFFFF) + byteIn - byteOut) % ADLER_PRIME;
        uint32_t b = ((lastHash >> 16) + a - byteOut) % ADLER_PRIME;
        return (b << 16) | a;
    }
    uint64_t fnv1a(unsigned char* data, long long len)
    {
        uint64_t hash = FNV_SEED;
        for (long long index = 0; index < len; ++index)
        {
            hash ^= data[index];
            hash *= FNV_PRIME;
        }
        return hash;
    }
    void adler32fnv1a(unsigned char* data, long long len, uint32_t& adlerHash, uint64_t& fnv1aHash)
    {
        fnv1aHash = FNV_SEED;
        uint32_t a = 1, b = 0;
        for (long long index = 0; index < len; ++index)
        {
            a = (a + data[index]) % ADLER_PRIME;
            b = (b + a) % ADLER_PRIME;
            fnv1aHash ^= data[index];
            fnv1aHash *= FNV_PRIME;
        }
        adlerHash = (b << 16) | a;
    }
    void prepareFileDigest(std::string fileName, FileDigest& fileDigest, CountingSemaphore& ioSemaphore)
    {
        std::streampos fsize = 0;
        std::ifstream file(fileName, std::ios::binary);
        fsize = file.tellg();
        file.seekg(0, std::ios::end);
        fsize = file.tellg() - fsize;
        file.seekg(0, std::ios::beg);

        fileDigest.fileSize = fsize;
        // block size is next power of two after sqrt of the fileSize, clamped 
        fileDigest.blockSize = (long long)(pow(2, floor(log(sqrt(fileDigest.fileSize)) / log(2.)) + 1));
        fileDigest.blockSize = (fileDigest.blockSize < MIN_BLOCK_SIZE) ? MIN_BLOCK_SIZE : ((fileDigest.blockSize > MAX_BLOCK_SIZE) ? MAX_BLOCK_SIZE : fileDigest.blockSize);
        fileDigest.lastBlockSize = fileDigest.fileSize % fileDigest.blockSize;

        fileDigest.hashes.resize(fileDigest.fileSize / fileDigest.blockSize + (fileDigest.lastBlockSize && fileDigest.fileSize ? 1 : 0));
        std::vector<char> blockData(fileDigest.blockSize);

        for (size_t i = 0; i < fileDigest.hashes.size(); ++i)
        {
            {
                ScopedSemaphoreAcqRel ioGuard(ioSemaphore);
                file.read(reinterpret_cast<char*>(blockData.data()), fileDigest.blockSize);
            }
            long long bytesRead = file.gcount();
            adler32fnv1a(reinterpret_cast<unsigned char*>(blockData.data()), bytesRead, fileDigest.hashes[i].rollingHash, fileDigest.hashes[i].slowHash);
        }
    }

    void prepareFileDiff(std::string fileName, const FileDigest& fileDigest, std::vector<DiffBlock>& diffBlocks, CountingSemaphore& ioSemaphore)
    {
        // prepare file digest index
        std::unordered_multimap<uint32_t, std::pair<uint64_t, long long>, IntFnvHasher> digestIndex;
        long long counter = 0;
        for (const auto& blockDigest : fileDigest.hashes)
            digestIndex.emplace(std::make_pair(blockDigest.rollingHash, std::make_pair(blockDigest.slowHash, counter++)));

        // lets rock
        std::ifstream file(fileName, std::ios::binary);
        std::vector<char> blockData(2 * MAX_BLOCK_SIZE);
        long long bytesInBuffer = 0;
        while (true)
        {
            long long tail = 0;
            long long windowStart = 0;
            uint32_t currentRollingHash = 0;
            {
                ScopedSemaphoreAcqRel ioGuard(ioSemaphore);
                file.read(reinterpret_cast<char*>(blockData.data() + bytesInBuffer), blockData.size() - bytesInBuffer);
            }
            long long bytesRead = file.gcount();
            bytesInBuffer += bytesRead;
            if (bytesInBuffer >= fileDigest.blockSize)
            {
                // roll the diff
                currentRollingHash = adler32(reinterpret_cast<unsigned char*>(blockData.data()), fileDigest.blockSize);
                while (windowStart + fileDigest.blockSize <= bytesInBuffer)
                {
                    // we got blockSize worth of unmatched data, push it out
                    if (windowStart - tail == fileDigest.blockSize)
                    {
                        diffBlocks.emplace_back(std::vector<char>(blockData.begin() + tail, blockData.begin() + windowStart), 0);
                        tail = windowStart;
                    }
                    bool gotMatch = false;
                    auto candidateBlocks = digestIndex.equal_range(currentRollingHash);
                    for (auto it = candidateBlocks.first; it != candidateBlocks.second; ++it)
                    {
                        uint64_t slowHash = fnv1a(reinterpret_cast<unsigned char*>(blockData.data()) + windowStart, fileDigest.blockSize);
                        // we look for full blocks with matching slow hash
                        if (slowHash == it->second.first && (!fileDigest.lastBlockSize || (it->second.second != fileDigest.hashes.size() - 1)))
                        {
                            gotMatch = true;
                            // anything between the tail and window start is unmatched data, push it out first
                            if (tail < windowStart)
                            {
                                // UNLESS it matches the last small block
                                bool matchedTail = false;
                                if (fileDigest.lastBlockSize && (tail - windowStart == fileDigest.lastBlockSize))
                                {
                                    uint32_t rollingHash = adler32(reinterpret_cast<unsigned char*>(blockData.data()) + tail, fileDigest.lastBlockSize);
                                    if (rollingHash == fileDigest.hashes.rbegin()->rollingHash)
                                    {
                                        uint64_t slowHash = fnv1a(reinterpret_cast<unsigned char*>(blockData.data()) + tail, fileDigest.lastBlockSize);
                                        if (slowHash == fileDigest.hashes.rbegin()->slowHash)
                                        {
                                            matchedTail = true;
                                            diffBlocks.emplace_back(std::vector<char>(), fileDigest.hashes.size() - 1);
                                        }
                                    }
                                }
                                if (!matchedTail)
                                    diffBlocks.emplace_back(std::vector<char>(blockData.begin() + tail, blockData.begin() + windowStart), 0);
                            }
                            diffBlocks.emplace_back(std::vector<char>(), it->second.second);
                            tail = windowStart += fileDigest.blockSize;
                            break; // just in case there are double collisions
                        }
                    }
                    if (gotMatch)
                    {
                        // if we still have block worth of data to check, get new currentRollingHash
                        if (windowStart + fileDigest.blockSize <= bytesInBuffer)
                            currentRollingHash = adler32(reinterpret_cast<unsigned char*>(blockData.data()) + windowStart, fileDigest.blockSize);
                    }
                    else
                    {
                        // shift the window, roll the hash if there's enough data
                        if (windowStart + fileDigest.blockSize < bytesInBuffer) 
                            currentRollingHash = rollAdler32(currentRollingHash, blockData[windowStart], blockData[windowStart + fileDigest.blockSize]);
                        ++windowStart;
                    }
                }
                // anything between the tail and window start is unmatched data, push it out first
                if (tail < windowStart)
                {
                    // UNLESS it matches the last small block
                    bool matchedTail = false;
                    if (fileDigest.lastBlockSize && (tail - windowStart == fileDigest.lastBlockSize))
                    {
                        uint32_t rollingHash = adler32(reinterpret_cast<unsigned char*>(blockData.data()) + tail, fileDigest.lastBlockSize);
                        if (rollingHash == fileDigest.hashes.rbegin()->rollingHash)
                        {
                            uint64_t slowHash = fnv1a(reinterpret_cast<unsigned char*>(blockData.data()) + tail, fileDigest.lastBlockSize);
                            if (slowHash == fileDigest.hashes.rbegin()->slowHash)
                            {
                                matchedTail = true;
                                diffBlocks.emplace_back(std::vector<char>(), fileDigest.hashes.size() - 1);
                            }
                        }
                    }
                    if (!matchedTail)
                        diffBlocks.emplace_back(std::vector<char>(blockData.begin() + tail, blockData.begin() + windowStart), 0);
                    tail = windowStart;
                }
                blockData.erase(blockData.begin(), blockData.begin() + windowStart);
                blockData.resize(2 * MAX_BLOCK_SIZE);
                bytesInBuffer -= windowStart;
            }
            if (fileDigest.lastBlockSize && bytesInBuffer == fileDigest.lastBlockSize)
            {
                // maybe its the last block
                uint32_t currentRollingHash = adler32(reinterpret_cast<unsigned char*>(blockData.data()), fileDigest.lastBlockSize);
                bool matchedTail = false;
                if (currentRollingHash == fileDigest.hashes.rbegin()->rollingHash)
                {
                    uint64_t slowHash = fnv1a(reinterpret_cast<unsigned char*>(blockData.data()), fileDigest.lastBlockSize);
                    if (slowHash == fileDigest.hashes.rbegin()->slowHash)
                    {
                        matchedTail = true;
                        diffBlocks.emplace_back(std::vector<char>(), fileDigest.hashes.size() - 1);
                    }
                }
                if (matchedTail)
                    bytesInBuffer = 0;
            }
            if (!bytesRead)
            {
                // flush the remainder and bail
                if (bytesInBuffer)
                    diffBlocks.emplace_back(std::vector<char>(blockData.begin(), blockData.begin() + (const int)bytesInBuffer), 0);
                break;
            }
            // nothing that matches the digest, but maybe there's more in the file
        }
    }

    void applyFileDiff(std::string srcFileName, std::string dstFileName, const FileDigest& fileDigest, std::vector<DiffBlock>& diffBlocks, CountingSemaphore& ioSemaphore)
    {
        std::ifstream file(srcFileName, std::ios::binary);
        std::ofstream resfile(dstFileName, std::ios::binary);
        std::vector<char> buffer(fileDigest.blockSize); // cant believe there's no zerocopy from ifstream to ofstream
        for (auto& diffBlock : diffBlocks)
        {
            if (!diffBlock.newBlockData.empty())
            {
                ScopedSemaphoreAcqRel ioGuard(ioSemaphore);
                resfile.write(reinterpret_cast<const char*>(diffBlock.newBlockData.data()), diffBlock.newBlockData.size());
            }
            else
            {
                long long bytesToTransfer = fileDigest.blockSize;
                if (fileDigest.lastBlockSize && (diffBlock.existingBlockIndex == fileDigest.hashes.size() - 1))
                    bytesToTransfer = fileDigest.lastBlockSize;
                file.seekg(fileDigest.blockSize * diffBlock.existingBlockIndex, std::ios::beg);
                {
                    ScopedSemaphoreAcqRel ioGuard(ioSemaphore);
                    file.read(buffer.data(), bytesToTransfer);
                    resfile.write(buffer.data(), file.gcount());
                }
            }
        }
    }

    // this one is simpliest, no async, we do file digest in one go, just make sure to read in large enough blocks
    void prepareContextDigest(SyncContext& syncCtx, CountingSemaphore& ioSemaphore)
    {
        std::streampos fsize = 0;
        {
            std::unique_lock<std::mutex> ctxGuard(syncCtx.contextMutex);
            assert((syncCtx.state == SyncContextState::DIGEST_REQUEST_RECEIVED) && !syncCtx.ptrOldFile.get());
            syncCtx.ptrBuffer.reset(new std::array<char, 2 * MAX_BLOCK_SIZE>());
            syncCtx.ptrOldFile.reset(new std::ifstream(syncCtx.fileName, std::ios::binary));
            if (syncCtx.ptrOldFile->good()) 
            {
                fsize = syncCtx.ptrOldFile->tellg();
                syncCtx.ptrOldFile->seekg(0, std::ios::end);
                fsize = syncCtx.ptrOldFile->tellg() - fsize;
                syncCtx.ptrOldFile->seekg(0, std::ios::beg);
                syncCtx.fileDigest.fileSize = fsize;
                // block size is next power of two after sqrt of the fileSize, clamped 
                syncCtx.fileDigest.blockSize = (long long)(pow(2, floor(log(sqrt(syncCtx.fileDigest.fileSize)) / log(2.)) + 1));
                syncCtx.fileDigest.blockSize = (syncCtx.fileDigest.blockSize < MIN_BLOCK_SIZE) ? MIN_BLOCK_SIZE : ((syncCtx.fileDigest.blockSize > MAX_BLOCK_SIZE) ? MAX_BLOCK_SIZE : syncCtx.fileDigest.blockSize);
                syncCtx.fileDigest.lastBlockSize = syncCtx.fileDigest.fileSize % syncCtx.fileDigest.blockSize;
                if (fsize)
                    syncCtx.state = SyncContextState::COMPUTING_DIGEST;
                else 
                {
                    syncCtx.ptrOldFile->close();
                    std::filesystem::remove(syncCtx.fileName);
                }
            }
        }
        if (fsize) 
        {
            syncCtx.fileDigest.hashes.resize(syncCtx.fileDigest.fileSize / syncCtx.fileDigest.blockSize + (syncCtx.fileDigest.lastBlockSize && syncCtx.fileDigest.fileSize ? 1 : 0));
            unsigned long long bytesInBuffer = 0;
            unsigned long long bufferOffset = 0;
            syncCtx.ptrOldFile->read(reinterpret_cast<char*>(syncCtx.ptrBuffer->data()), syncCtx.ptrBuffer->size());
            bytesInBuffer = syncCtx.ptrOldFile->gcount();
            unsigned long long hashIdx = 0;

            do
            {
                if (bytesInBuffer - bufferOffset < syncCtx.fileDigest.getBlockSize(hashIdx))
                {
                    std::memmove(syncCtx.ptrBuffer->data(), syncCtx.ptrBuffer->data() + bufferOffset, bytesInBuffer - bufferOffset);
                    bytesInBuffer = bytesInBuffer - bufferOffset;
                    bufferOffset = 0;
                    syncCtx.ptrOldFile->read(reinterpret_cast<char*>(syncCtx.ptrBuffer->data() + bytesInBuffer), syncCtx.ptrBuffer->size() - bytesInBuffer);
                    bytesInBuffer += syncCtx.ptrOldFile->gcount();
                }
                adler32fnv1a(reinterpret_cast<unsigned char*>(syncCtx.ptrBuffer->data() + bufferOffset), syncCtx.fileDigest.getBlockSize(hashIdx), syncCtx.fileDigest.hashes[hashIdx].rollingHash, syncCtx.fileDigest.hashes[hashIdx].slowHash);
                bufferOffset += syncCtx.fileDigest.getBlockSize(hashIdx);
            }
            while (++hashIdx < syncCtx.fileDigest.hashes.size());
        }
        {
            std::unique_lock<std::mutex> ctxGuard(syncCtx.contextMutex);
            syncCtx.state = SyncContextState::DIGEST_RESPONSE;
			syncCtx.nextState = SyncContextState::DIGEST_RESPONSE_SENT;
            syncCtx.ptrOldFile->close(); // WTF, resetting this should call close but apparently it doesn't??!
            syncCtx.ptrOldFile.reset();
            syncCtx.ptrBuffer.reset();
        }
    }

    // async method that reads in huge page worth of data and fills up outgoing diffblock queue with appropriate diff blocks
    void prepareContextDiff(SyncContext& syncCtx, CountingSemaphore& ioSemaphore)
    {
        {
            std::unique_lock<std::mutex> ctxGuard(syncCtx.contextMutex);
            assert((syncCtx.state == SyncContextState::DIGEST_RESPONSE_RECEIVED) || (syncCtx.state == SyncContextState::COMPUTING_DIFF) || (syncCtx.state == SyncContextState::DIFF_BLOCK) || (syncCtx.state == SyncContextState::DIFF_BLOCK_SENT));
            if (syncCtx.state == SyncContextState::DIGEST_RESPONSE_RECEIVED)
            {
                // prepare file digest index
                long long counter = 0;
                for (const auto& blockDigest : syncCtx.fileDigest.hashes)
                    syncCtx.digestIndex.emplace(std::make_pair(blockDigest.rollingHash, std::make_pair(blockDigest.slowHash, counter++)));

                syncCtx.ptrSourceFile.reset(new std::ifstream(syncCtx.fileName, std::ios::binary));
                syncCtx.state = SyncContextState::COMPUTING_DIFF;
                syncCtx.bytesInBuffer = 0;

                syncCtx.ptrBuffer.reset(new std::array<char, 2 * MAX_BLOCK_SIZE>());
            }
        }

        // easy! just do one iteration and fill up outgoing diffblock queue
        long long tail = 0;
        long long windowStart = 0;
        uint32_t currentRollingHash = 0;
        syncCtx.ptrSourceFile->read(syncCtx.ptrBuffer->data() + syncCtx.bytesInBuffer, syncCtx.ptrBuffer->size() - syncCtx.bytesInBuffer);
        long long bytesRead = syncCtx.ptrSourceFile->gcount();
        syncCtx.bytesInBuffer += bytesRead;
        if (syncCtx.bytesInBuffer >= syncCtx.fileDigest.blockSize)
        {
            // roll the diff
            currentRollingHash = adler32(reinterpret_cast<unsigned char*>(syncCtx.ptrBuffer->data()), syncCtx.fileDigest.blockSize);
            while (windowStart + syncCtx.fileDigest.blockSize <= syncCtx.bytesInBuffer)
            {
                // we got blockSize worth of unmatched data, push it out
                if (windowStart - tail == syncCtx.fileDigest.blockSize)
                {
                    syncCtx.nextDiff.emplace_back(std::vector<char>(syncCtx.ptrBuffer->begin() + tail, syncCtx.ptrBuffer->begin() + windowStart), 0);
                    ++syncCtx.diffBlocksProcessed;
                    tail = windowStart;
                }
                bool gotMatch = false;
                auto candidateBlocks = syncCtx.digestIndex.equal_range(currentRollingHash);
                for (auto it = candidateBlocks.first; it != candidateBlocks.second; ++it)
                {
                    uint64_t slowHash = fnv1a(reinterpret_cast<unsigned char*>(syncCtx.ptrBuffer->data() + windowStart), syncCtx.fileDigest.blockSize);
                    // we look for full blocks with matching slow hash
                    if (slowHash == it->second.first && (!syncCtx.fileDigest.lastBlockSize || (it->second.second != syncCtx.fileDigest.hashes.size() - 1)))
                    {
                        gotMatch = true;
                        // anything between the tail and window start is unmatched data, push it out first
                        if (tail < windowStart)
                        {
                            // UNLESS it matches the last small block
                            bool matchedTail = false;
                            if (syncCtx.fileDigest.lastBlockSize && (tail - windowStart == syncCtx.fileDigest.lastBlockSize))
                            {
                                uint32_t rollingHash = adler32(reinterpret_cast<unsigned char*>(syncCtx.ptrBuffer->data() + tail), syncCtx.fileDigest.lastBlockSize);
                                if (rollingHash == syncCtx.fileDigest.hashes.rbegin()->rollingHash)
                                {
                                    uint64_t slowHash = fnv1a(reinterpret_cast<unsigned char*>(syncCtx.ptrBuffer->data() + tail), syncCtx.fileDigest.lastBlockSize);
                                    if (slowHash == syncCtx.fileDigest.hashes.rbegin()->slowHash)
                                    {
                                        matchedTail = true;
                                        syncCtx.nextDiff.emplace_back(std::vector<char>(), syncCtx.fileDigest.hashes.size() - 1);
                                        ++syncCtx.diffBlocksProcessed;
                                    }
                                }
                            }
                            if (!matchedTail) 
                            {
                                ++syncCtx.diffBlocksProcessed;
                                syncCtx.nextDiff.emplace_back(std::vector<char>(syncCtx.ptrBuffer->begin() + tail, syncCtx.ptrBuffer->begin() + windowStart), 0);
                            }
                        }
                        syncCtx.nextDiff.emplace_back(std::vector<char>(), it->second.second);
                        ++syncCtx.diffBlocksProcessed;
                        tail = windowStart += syncCtx.fileDigest.blockSize;
                        break; // just in case there are double collisions
                    }
                }
                if (gotMatch)
                {
                    // if we still have block worth of data to check, get new currentRollingHash
                    if (windowStart + syncCtx.fileDigest.blockSize <= syncCtx.bytesInBuffer)
                        currentRollingHash = adler32(reinterpret_cast<unsigned char*>(syncCtx.ptrBuffer->data() + windowStart), syncCtx.fileDigest.blockSize);
                }
                else
                {
                    // shift the window, roll the hash if there's enough data
                    if (windowStart + syncCtx.fileDigest.blockSize < syncCtx.bytesInBuffer)
                        currentRollingHash = rollAdler32(currentRollingHash, (*syncCtx.ptrBuffer)[windowStart], (*syncCtx.ptrBuffer)[windowStart + syncCtx.fileDigest.blockSize]);
                    ++windowStart;
                }
            }
            // anything between the tail and window start is unmatched data, push it out first
            if (tail < windowStart)
            {
                // UNLESS it matches the last small block
                bool matchedTail = false;
                if (syncCtx.fileDigest.lastBlockSize && (tail - windowStart == syncCtx.fileDigest.lastBlockSize))
                {
                    uint32_t rollingHash = adler32(reinterpret_cast<unsigned char*>(syncCtx.ptrBuffer->data() + tail), syncCtx.fileDigest.lastBlockSize);
                    if (rollingHash == syncCtx.fileDigest.hashes.rbegin()->rollingHash)
                    {
                        uint64_t slowHash = fnv1a(reinterpret_cast<unsigned char*>(syncCtx.ptrBuffer->data() + tail), syncCtx.fileDigest.lastBlockSize);
                        if (slowHash == syncCtx.fileDigest.hashes.rbegin()->slowHash)
                        {
                            matchedTail = true;
                            syncCtx.nextDiff.emplace_back(std::vector<char>(), syncCtx.fileDigest.hashes.size() - 1);
                            ++syncCtx.diffBlocksProcessed;
                        }
                    }
                }
                if (!matchedTail) 
                {
                    ++syncCtx.diffBlocksProcessed;
                    syncCtx.nextDiff.emplace_back(std::vector<char>(syncCtx.ptrBuffer->begin() + tail, syncCtx.ptrBuffer->begin() + windowStart), 0);
                }
                tail = windowStart;
            }
            std::memmove(syncCtx.ptrBuffer->data(), syncCtx.ptrBuffer->data() + windowStart, syncCtx.bytesInBuffer - windowStart);
            syncCtx.bytesInBuffer -= windowStart;
        }
        if (syncCtx.fileDigest.lastBlockSize && syncCtx.bytesInBuffer == syncCtx.fileDigest.lastBlockSize)
        {
            // maybe its the last block
            uint32_t currentRollingHash = adler32(reinterpret_cast<unsigned char*>(syncCtx.ptrBuffer->data()), syncCtx.fileDigest.lastBlockSize);
            bool matchedTail = false;
            if (currentRollingHash == syncCtx.fileDigest.hashes.rbegin()->rollingHash)
            {
                uint64_t slowHash = fnv1a(reinterpret_cast<unsigned char*>(syncCtx.ptrBuffer->data()), syncCtx.fileDigest.lastBlockSize);
                if (slowHash == syncCtx.fileDigest.hashes.rbegin()->slowHash)
                {
                    matchedTail = true;
                    syncCtx.nextDiff.emplace_back(std::vector<char>(), syncCtx.fileDigest.hashes.size() - 1);
                    ++syncCtx.diffBlocksProcessed;
                }
            }
            if (matchedTail)
                syncCtx.bytesInBuffer = 0;
        }
        if (!bytesRead)
        {
            // flush the remainder and bail
            if (syncCtx.bytesInBuffer)
            {
                syncCtx.nextDiff.emplace_back(std::vector<char>(syncCtx.ptrBuffer->begin(), syncCtx.ptrBuffer->begin() + (const int)syncCtx.bytesInBuffer), 0);
                ++syncCtx.diffBlocksProcessed;
            }
            {
                // TODO: this was the last of it, update totalDiffBlocksCount, so recipient knows when to stop
                std::unique_lock<std::mutex> ctxGuard(syncCtx.contextMutex);
                syncCtx.totalDiffBlocks = syncCtx.diffBlocksProcessed;
                syncCtx.ptrBuffer.reset();
                syncCtx.ptrSourceFile.reset();
            }
        }
    }

    // async method to apply diff blocks as they are coming in from network
    // TODO: handle situation when network is faster than the disk!
    void applyContextDiff(SyncContext& syncCtx, CountingSemaphore& ioSemaphore)
    {
        std::deque<DiffBlock> diffBlocks;
        long long diffBlockAppliedThisTime = 0;
        {
            std::unique_lock<std::mutex> ctxGuard(syncCtx.contextMutex);
            assert((syncCtx.state == SyncContextState::DIGEST_RESPONSE_SENT) && !syncCtx.ptrNewFile.get() ||
                (syncCtx.state == SyncContextState::APPLYING_DIFF) && syncCtx.ptrNewFile.get());
            if (syncCtx.state == SyncContextState::DIGEST_RESPONSE_SENT) 
            {
                syncCtx.ptrBuffer.reset(new std::array<char, 2 * MAX_BLOCK_SIZE>());
                // TODO: randomize name
                syncCtx.ptrOldFile.reset(new std::ifstream(syncCtx.fileName, std::ios::binary));
                syncCtx.ptrNewFile.reset(new std::ofstream(syncCtx.fileName + ".patched", std::ios::binary));
                syncCtx.state = SyncContextState::APPLYING_DIFF;
            }
            std::swap(diffBlocks, syncCtx.fileDiff);
        }

        // we either accumulate context buffers worth of new data before writing it out or
        // batch consecutive existing blocks (prepare consecutive reads into context buffers in one go)
        syncCtx.bytesInBuffer = 0;
        std::streampos oldFileOffset = 0;
        std::streampos oldFileBytesToRead = 0;
        long long lastBlockIdx = 0;

        while (!diffBlocks.empty())
        {
            auto& diffBlock = diffBlocks.front();
            // check whether this is new block that would fill in the buffer, and there're no outstanding reads from old file
            if (!diffBlock.newBlockData.empty() && (syncCtx.ptrBuffer->size() - syncCtx.bytesInBuffer >= diffBlock.newBlockData.size()) && (!oldFileOffset && !oldFileBytesToRead))
            {
                // now get new block data in the buffer
                std::memcpy(syncCtx.ptrBuffer->data() + syncCtx.bytesInBuffer, diffBlock.newBlockData.data(), diffBlock.newBlockData.size());
                syncCtx.bytesInBuffer += diffBlock.newBlockData.size();
                diffBlocks.pop_front();
                ++diffBlockAppliedThisTime;
            }
            // new existing block, start batching if there's enough space in the buffer 
            else if ((!oldFileOffset && !oldFileBytesToRead) && (syncCtx.ptrBuffer->size() - syncCtx.bytesInBuffer >= syncCtx.fileDigest.getBlockSize(diffBlock.existingBlockIndex)))
            {
                // first existing block, start batching
                lastBlockIdx = diffBlock.existingBlockIndex;
                oldFileOffset = syncCtx.fileDigest.blockSize * diffBlock.existingBlockIndex;
                oldFileBytesToRead = syncCtx.fileDigest.getBlockSize(lastBlockIdx);
                diffBlocks.pop_front();
                ++diffBlockAppliedThisTime;
            }
            // this existing block follows the last, and there should be enough space to keep batching
            else if ((oldFileOffset || oldFileBytesToRead) && (lastBlockIdx == diffBlock.existingBlockIndex - 1) && (syncCtx.ptrBuffer->size() - syncCtx.bytesInBuffer - oldFileBytesToRead >= syncCtx.fileDigest.getBlockSize(diffBlock.existingBlockIndex)))
            {
                // this block is exactly one following previous one, keep batching reads
                lastBlockIdx = diffBlock.existingBlockIndex;
                oldFileBytesToRead += syncCtx.fileDigest.getBlockSize(lastBlockIdx);
                diffBlocks.pop_front();
                ++diffBlockAppliedThisTime;
            }
            else
            {
                // ok no more batching, we have to write out whatever data we have before new blocks can be scheduled
                // first write out any new data accumulated in the buffer
                if (syncCtx.bytesInBuffer) 
                {
                    syncCtx.ptrNewFile->write(syncCtx.ptrBuffer->data(), syncCtx.bytesInBuffer);
                }
                // then copy any old file data
                if (oldFileOffset || oldFileBytesToRead)
                {
                    syncCtx.ptrOldFile->seekg(oldFileOffset, std::ios::beg);
                    syncCtx.ptrOldFile->read(syncCtx.ptrBuffer->data() + syncCtx.bytesInBuffer, oldFileBytesToRead);
                    std::streamsize toWrite = syncCtx.ptrOldFile->gcount();
                    syncCtx.ptrNewFile->write(syncCtx.ptrBuffer->data() + syncCtx.bytesInBuffer, toWrite);
                }
                syncCtx.bytesInBuffer = 0;
                lastBlockIdx = 0;
                oldFileBytesToRead = 0;
                oldFileOffset = 0;
            }
            // write out anything left in the buffers
            if (syncCtx.bytesInBuffer)
            {
                syncCtx.ptrNewFile->write(syncCtx.ptrBuffer->data(), syncCtx.bytesInBuffer);
            }
            // then copy any old file data
            if (oldFileOffset || oldFileBytesToRead)
            {
                syncCtx.ptrOldFile->seekg(oldFileOffset, std::ios::beg);
                syncCtx.ptrOldFile->read(syncCtx.ptrBuffer->data() + syncCtx.bytesInBuffer, oldFileBytesToRead);
                std::streamsize toWrite = syncCtx.ptrOldFile->gcount();
                syncCtx.ptrNewFile->write(syncCtx.ptrBuffer->data() + syncCtx.bytesInBuffer, toWrite);
            }
            syncCtx.bytesInBuffer = 0;
            lastBlockIdx = 0;
            oldFileBytesToRead = 0;
            oldFileOffset = 0;
        }
        {
            std::unique_lock<std::mutex> ctxGuard(syncCtx.contextMutex);
            syncCtx.diffBlocksProcessed += diffBlockAppliedThisTime;
            if (syncCtx.totalDiffBlocks && (syncCtx.diffBlocksProcessed == syncCtx.totalDiffBlocks)) 
            {
                syncCtx.state = SyncContextState::DIFF_APPLIED;
                syncCtx.nextState = SyncContextState::COMPLETE;
                syncCtx.ptrBuffer.reset();
                syncCtx.ptrOldFile->close();
                syncCtx.ptrNewFile->flush();
                syncCtx.ptrNewFile->close();
            }
        }
    }
}