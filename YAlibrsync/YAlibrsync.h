#pragma once

#include <vector>
#include <deque>
#include <array>
#include <unordered_map>
#include <iostream>
#include <fstream>
#include <chrono>
#include <thread>
#include <mutex>
#include <condition_variable>

#include <stdio.h>
#include <math.h>

// TODO: split into 3 headers
namespace YAlibrsync 
{
	// very basic semaphore unsafe against overflows etc
	class CountingSemaphore
	{
	public:
		CountingSemaphore(int initialCount = 1) : counter(initialCount) {}
		~CountingSemaphore() = default;
		CountingSemaphore(const CountingSemaphore&) = delete;
		CountingSemaphore(CountingSemaphore&&) = default;
		CountingSemaphore& operator=(const CountingSemaphore&) = delete;
		CountingSemaphore& operator=(CountingSemaphore&&) = default;

		inline void wait()
		{
			std::unique_lock<std::mutex> lock(mtx);
			if (--counter < 0)
				cv.wait(lock, [&]() {return counter >= 0; });
		}

		inline void signal()
		{
			std::unique_lock<std::mutex> lock(mtx);
			if (counter++ < 0)
				cv.notify_one();
		}

	private:
		int counter = 1;
		std::mutex mtx;
		std::condition_variable cv;
	};

	// RAII for above
	struct ScopedSemaphoreAcqRel
	{
	public:

		ScopedSemaphoreAcqRel() = delete;
		ScopedSemaphoreAcqRel(CountingSemaphore& semaphoreToTick) : pSemaphore(&semaphoreToTick) { pSemaphore->wait(); }
		~ScopedSemaphoreAcqRel() { pSemaphore->signal(); };
		ScopedSemaphoreAcqRel(const ScopedSemaphoreAcqRel&) = delete;
		ScopedSemaphoreAcqRel(ScopedSemaphoreAcqRel&&) = default;
		ScopedSemaphoreAcqRel& operator=(const ScopedSemaphoreAcqRel&) = delete;
		ScopedSemaphoreAcqRel& operator=(ScopedSemaphoreAcqRel&&) = default;
	private:
		CountingSemaphore* pSemaphore = nullptr;
	};

	// very basic semaphore unsafe against overflows etc
	class BinarySemaphore
	{
	public:
		BinarySemaphore() = default;
		~BinarySemaphore() = default;
		BinarySemaphore(const BinarySemaphore&) = delete;
		BinarySemaphore(BinarySemaphore&&) = default;
		BinarySemaphore& operator=(const BinarySemaphore&) = delete;
		BinarySemaphore& operator=(BinarySemaphore&&) = default;

		inline void wait() noexcept
		{
			std::unique_lock<std::mutex> lock(mtx);
			if (!signalled)
				cv.wait(lock, [&]() {return signalled; });
			signalled = false;
		}

		template <class REP, class DURATION>
		inline void wait_for(const std::chrono::duration<REP, DURATION>& timePeriod)
		{
			std::unique_lock<std::mutex> lock(mtx);
			if (!signalled)
				cv.wait_for(lock, timePeriod);
			if (signalled)
				signalled = false;
		}

		inline void signal() noexcept
		{
			std::unique_lock<std::mutex> lock(mtx);
			signalled = true;
			cv.notify_one();
		}

	private:
		bool signalled = false;
		std::mutex mtx;
		std::condition_variable cv;
	};
    // stick to the classics
    const uint32_t ADLER_PRIME = 65521;
    uint32_t adler32(unsigned char* data, long long len);
    uint32_t rollAdler32(uint32_t lastHash, unsigned char byteOut, unsigned char byteIn);

    const uint64_t FNV_SEED = 14695981039346656037ULL;
    const uint64_t FNV_PRIME = 1099511628211ULL;
    uint64_t fnv1a(unsigned char* data, long long len);

    void adler32fnv1a(unsigned char* data, long long len, uint32_t& adlerHash, uint64_t& fnv1aHash);

    const long long CACHE_LINE_SIZE = 64;
    const long long MIN_BLOCK_SIZE = CACHE_LINE_SIZE;
    const long long MAX_BLOCK_SIZE = 16 * 1024; // DEBUG
//    const long long MAX_BLOCK_SIZE = 2 * 1024 * 1024; // huge page

    struct IntFnvHasher
    {
        std::size_t operator()(uint32_t const& toHash) const noexcept
        {
            return fnv1a((unsigned char*)(&toHash), sizeof(uint32_t));
        }
    };
    struct BlockDigest
    {
        uint32_t rollingHash = 0;
        uint64_t slowHash = 0;
    };
    struct FileDigest
    {
        long long fileSize = 0;
        long long blockSize = 0;
        long long lastBlockSize = 0;
        std::vector<BlockDigest> hashes;

        inline unsigned long long getBlockSize(long long blockIdx) { return (lastBlockSize && (blockIdx == hashes.size() - 1)) ? lastBlockSize : blockSize; }
    };
    struct DiffBlock
    {
        DiffBlock(std::vector<char>&& inNewBlockData, long long inExistingBlockIndex) : newBlockData(std::move(inNewBlockData)), existingBlockIndex(inExistingBlockIndex), newDataSize(newBlockData.size()){}
		inline bool isExistingBlock() const { return !newDataSize; }
        std::vector<char> newBlockData; // empty data means existing index is valid
        long long existingBlockIndex = 0;
        long long newDataSize = 0;
    };

    enum SyncContextState : int
    {
        NONE,
        DIGEST_REQUEST,
        DIGEST_REQUEST_SENT,
        DIGEST_REQUEST_RECEIVED,
        COMPUTING_DIGEST,
        COMPUTED_DIGEST,
        DIGEST_RESPONSE,
        DIGEST_RESPONSE_SENT,
        DIGEST_RESPONSE_RECEIVED,
        COMPUTING_DIFF,
        DIFF,
        DIFF_SENT,
        DIFF_RECEIVED,
        DIFF_BLOCK,
        DIFF_BLOCK_SENT,
        DIFF_BLOCK_RECEIVED,
        APPLYING_DIFF,
        DIFF_APPLIED,
        DELETE_RECEIVED,
        COMPLETE,
        FAILED,
        PING,
        PING_SENT,
        PONG,
		DELETE_FILE,
        DELETE_SENT,
        COPY_FILE,
        COPY_RECEIVED,
        MAX
    };
    struct SyncContext 
    {
        std::string fileName = "";
        long long id = 0;
        SyncContextState state = SyncContextState::NONE;
        SyncContextState nextState = SyncContextState::NONE;
        long long creationTime;
        std::shared_ptr<std::array<char, 2*MAX_BLOCK_SIZE>> ptrBuffer;
        long long bytesInBuffer = 0;
        FileDigest fileDigest;
        std::mutex contextMutex;
        long long totalDiffBlocks = 0;
        long long diffBlocksProcessed = 0;
        inline bool operator<(const SyncContext& other) { return id < other.id; }

        // Request
        std::unordered_multimap<uint32_t, std::pair<uint64_t, long long>, IntFnvHasher> digestIndex;
        std::deque<DiffBlock> inFlightDiff;
        std::deque<DiffBlock> nextDiff;
        std::shared_ptr<std::ifstream> ptrSourceFile;

        // Response
        std::deque<DiffBlock> fileDiff;
        std::shared_ptr<std::ifstream> ptrOldFile;
        std::shared_ptr<std::ofstream> ptrNewFile;

        // copy
        long long copyOffset = 0;
    };

    void prepareFileDigest(std::string fileName, FileDigest& fileDigest, CountingSemaphore& ioSemaphore);
    void prepareFileDiff(std::string fileName, const FileDigest& fileDigest, std::vector<DiffBlock>& diffBlocks, CountingSemaphore& ioSemaphore);
    void applyFileDiff(std::string srcFileName, std::string dstFileName, const FileDigest& fileDigest, std::vector<DiffBlock>& diffBlocks, CountingSemaphore& ioSemaphore);

    // TODO: use io syncrhonization to follow io factor
    void prepareContextDigest(SyncContext& syncCtx, CountingSemaphore& ioSemaphore);
    void prepareContextDiff(SyncContext& syncCtx, CountingSemaphore& ioSemaphore);
    void applyContextDiff(SyncContext& syncCtx, CountingSemaphore& ioSemaphore);
}