
#include <iostream>
#include <vector>
#include <map>
#include <functional>
#include <algorithm>
#include <sstream>
#include <algorithm>
#include <atomic>
#include <deque>
#include <unordered_set>
#include <cassert>
#include <filesystem>
#include <cstdio>
#include <utility>

#include <asio/co_spawn.hpp>
#include <asio/detached.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/signal_set.hpp>
#include <asio/write.hpp>
#include <asio/require.hpp>
#include <asio/read.hpp>
#include <asio/awaitable.hpp>

#include "../YAlibrsync/YAlibrsync.h"


#include "../filesentry/include/filesentry/FileWatcher.h"

#include "Util.h"

#include "YArsync.h"


int YArsync::run(int ioFactor, bool bWatchDirectory, bool bDaemon, bool bListen, int port, std::string ip, std::string directory, std::string local_directory)
{
	// one network thread + number of cores * (3 threads per sync context + io factor), because traditional synchronization mechanisms are used 
	// in context of coroutines, and thus could block complete executor pool leading to a deadlock.. My home PC having 24 cores means 1.5 gb memory allocated on start
	int numThreads = std::thread::hardware_concurrency() * (3 + ioFactor) + 1;
	maxConcurrentResponses = std::thread::hardware_concurrency() * ioFactor;
	asio::io_context ioContext(numThreads);
	std::vector<std::thread> threadPool;
	std::shared_ptr<std::thread> directoryWatcherThread;
	ioSemaphore.reset(new YAlibrsync::CountingSemaphore(ioFactor));
	dispatchSemaphore.reset(new YAlibrsync::CountingSemaphore(std::thread::hardware_concurrency()));
	requestCounter = 0;
	asio::executor_work_guard work(ioContext.get_executor());

	for (int i = 0; i < numThreads; ++i)
		threadPool.emplace_back([&ioContext]() {ioContext.run(); });

	asio::signal_set signals(ioContext, SIGINT, SIGTERM);
	signals.async_wait([&](auto, auto)
	{
		std::cout << "Exiting..." << std::endl;
		if (bWatchDirectory)
		{
			// TODO: signal watcher to stop
		}
		if (bListen)
		{
			if (pAcceptor)
				pAcceptor->cancel();
			if (pSocket)
				pSocket->close();
		}
		work.reset();
		ioContext.stop();
		gExit = true;
	});

	if (!directory.empty() && !local_directory.empty())
	{
		// single local directory to synchronize
		std::vector<std::string> srcFileList;
		{
			YAlibrsync::ScopedSemaphoreAcqRel ioGuard(*ioSemaphore);
			srcFileList = GetFilenamesRecursive(directory);
		}
		// first recreate directory structure
		{
			YAlibrsync::ScopedSemaphoreAcqRel ioGuard(*ioSemaphore);
			std::filesystem::copy(directory, local_directory, std::filesystem::copy_options::directories_only | std::filesystem::copy_options::recursive);
		}
		for (auto& srcFileName : srcFileList)
		{
			std::string dstFileName = local_directory + srcFileName.substr(directory.size());
			SyncLocalFile(ioContext, dispatchSemaphore, requestCounter, srcFileName, dstFileName);
		}
		if (bWatchDirectory)
		{
			directoryWatcherThread.reset(new std::thread([&]() {localDirectoryWatcher(ioContext, directory); }));
		}
		else
			work.reset();
	}
	else
	{
		if (bListen)
		{
			// start listening
			startServer(ioContext, directory, ip, port);
		}
		else
		{
			// connect to server
			// sync directory
			startClient(ioContext, directory, ip, port);
			std::vector<std::string> srcFileList;
			{
				YAlibrsync::ScopedSemaphoreAcqRel ioGuard(*ioSemaphore);
				srcFileList = GetFilenamesRecursive(directory);
			}
			for (auto& srcFileName : srcFileList)
				SyncNetworkFile(ioContext, srcFileName);

			if (/*source and */bWatchDirectory)
			{
				directoryWatcherThread.reset(new std::thread([&]() {netDirectoryWatcher(ioContext, directory); }));
			}
			else
				work.reset();
		}
	}

	while (work.owns_work())
		std::this_thread::sleep_for(std::chrono::seconds(1));

	for (auto& ioThread : threadPool)
		ioThread.join();
	gExit = true;
	if (directoryWatcherThread.get())
		directoryWatcherThread->join();

	// join directory watcher thread
	return 0;
}

std::string YArsync::getRelativeFileName(std::string fileName)
{
	return fileName.substr(directory.size());
}

struct SyncContextSignals
{
	YAlibrsync::BinarySemaphore dataAvailable;
	YAlibrsync::BinarySemaphore syncComplete;
};
std::unordered_map<long long, std::shared_ptr<SyncContextSignals>> contextsSignals;

asio::awaitable<void> YArsync::prepareContextDigestNetAsync(std::shared_ptr<YAlibrsync::SyncContext> ptrCtx)
{
	YAlibrsync::prepareContextDigest(*ptrCtx, *ioSemaphore);
	co_return;
}

// TODO: proper pumping of the network during diff computation
asio::awaitable<void> YArsync::prepareContextDiffNetAsync(std::shared_ptr < YAlibrsync::SyncContext> ptrCtx)
{
	// ugly.. due to async nature of network/diff calculation sometimes it happens that totalDiffBlocks is not sent in last diff block, 
	// so we`ll have to send last tmpty diff block packet to let server complete the request
	bool needExtraSend = true;
	//{ std::lock_guard<std::mutex> grd(coutMtx); std::cout << ptrCtx->id << " Starting diff calculation." << std::endl; }
	while (!ptrCtx->totalDiffBlocks)
	{
		//{ std::lock_guard<std::mutex> grd(coutMtx); std::cout << ptrCtx->id << " Starting diff block calculation, nextDiffSize == " << ptrCtx->nextDiff.size() << std::endl; }
		prepareContextDiff(*ptrCtx, *ioSemaphore);
		//{ std::lock_guard<std::mutex> grd(coutMtx); std::cout << ptrCtx->id << " Ended diff block calculation, nextDiffSize == " << ptrCtx->nextDiff.size() << std::endl; }
		{
			std::lock_guard<std::mutex> lck(ptrCtx->contextMutex);
			if (ptrCtx->state == YAlibrsync::SyncContextState::COMPUTING_DIFF || ptrCtx->state == YAlibrsync::SyncContextState::DIFF_BLOCK_SENT)
			{
				//{ std::lock_guard<std::mutex> grd(coutMtx); std::cout << ptrCtx->id << " Clearing inFlightDiff, swapping with nextDiff" << std::endl; }
				ptrCtx->inFlightDiff.clear();
				std::swap(ptrCtx->inFlightDiff, ptrCtx->nextDiff);
				ptrCtx->state = YAlibrsync::SyncContextState::DIFF_BLOCK;
				ptrCtx->nextState = YAlibrsync::SyncContextState::DIFF_BLOCK_SENT;
				if ((ptrCtx->diffBlocksProcessed == ptrCtx->totalDiffBlocks) && ptrCtx->totalDiffBlocks)
					needExtraSend = false;
			}
			writeReadyEvent.signal();
		}
	}
	//{ std::lock_guard<std::mutex> grd(coutMtx); std::cout << ptrCtx->id << " Ended diff calculation." << std::endl; }
	while (true)
	{
		{
			std::lock_guard<std::mutex> lck(ptrCtx->contextMutex);
			if (!ptrCtx->nextDiff.empty() || needExtraSend)
			{
				if (ptrCtx->state == YAlibrsync::SyncContextState::DIFF_BLOCK_SENT)
				{
					//{ std::lock_guard<std::mutex> grd(coutMtx); std::cout << ptrCtx->id << " Scheduling diff leftovers!" << std::endl; }
					ptrCtx->inFlightDiff.clear();
					std::swap(ptrCtx->inFlightDiff, ptrCtx->nextDiff);
					ptrCtx->state = YAlibrsync::SyncContextState::DIFF_BLOCK;
					ptrCtx->nextState = YAlibrsync::SyncContextState::DIFF_SENT;
					writeReadyEvent.signal();
					break;
				}
			}
			else
				break;
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(10)); // TODO: UGH! this requires reverse notification mechanism from network thread to context, which is missing entirely atm
	}
	co_return;
}

asio::awaitable<void> YArsync::applyContextDiffNetAsync(std::shared_ptr < YAlibrsync::SyncContext> ptrCtx, YAlibrsync::BinarySemaphore& dataAvailable)
{
	while (true)
	{
		{
			std::lock_guard<std::mutex> lck(ptrCtx->contextMutex);
			if (!((!ptrCtx->totalDiffBlocks && !ptrCtx->diffBlocksProcessed) || (ptrCtx->totalDiffBlocks != ptrCtx->diffBlocksProcessed)))
				break;
		}
		dataAvailable.wait();
		YAlibrsync::applyContextDiff(*ptrCtx, *ioSemaphore);
	}
	{
		std::lock_guard<std::mutex> lck(ptrCtx->contextMutex);
		ptrCtx->ptrSourceFile.reset();
		ptrCtx->ptrNewFile.reset();
		ptrCtx->ptrOldFile.reset();
		std::filesystem::remove(ptrCtx->fileName);
		std::filesystem::rename(ptrCtx->fileName + ".patched", ptrCtx->fileName);
	}
	co_return;
}


// must be called only with ptrCtx mutex locked!
void YArsync::prepareTxBuffers(std::shared_ptr < YAlibrsync::SyncContext > ptrCtx, std::vector<asio::const_buffer>& toWrite, NetPacketHeader& txHeader)
{
	toWrite.emplace_back(asio::const_buffer(&txHeader, sizeof(txHeader)));
	switch (ptrCtx->state)
	{
	case YAlibrsync::SyncContextState::NONE:
		abort();
		break;
	case YAlibrsync::SyncContextState::DIGEST_REQUEST:
		txHeader.digestRequest.fileNameLength = ptrCtx->fileName.size() - directory.size();
		toWrite.emplace_back(asio::const_buffer(ptrCtx->fileName.data() + directory.size(), txHeader.digestRequest.fileNameLength));
		break;
	case YAlibrsync::SyncContextState::DIGEST_RESPONSE:
		txHeader.digestResponse.blockSize = ptrCtx->fileDigest.blockSize;
		txHeader.digestResponse.fileSize = ptrCtx->fileDigest.fileSize;
		txHeader.digestResponse.lastBlockSize = ptrCtx->fileDigest.lastBlockSize;
		txHeader.digestResponse.numDigestBlocks = ptrCtx->fileDigest.hashes.size();
		toWrite.emplace_back(asio::const_buffer(ptrCtx->fileDigest.hashes.data(), ptrCtx->fileDigest.hashes.size() * sizeof(YAlibrsync::BlockDigest)));
		break;
	case YAlibrsync::SyncContextState::DIFF_BLOCK:
	{
		// TODO: hackitty hack: right now i cant be bothered to code up proper serialization for diff blocks, so with some redundant space and and tricks i`ll just package memory for sending
		if (ptrCtx->totalDiffBlocks)
			txHeader.diffBlock.totalDiffBlocks = ptrCtx->totalDiffBlocks;
		txHeader.diffBlock.numDiffBlocks = ptrCtx->inFlightDiff.size();
		size_t packageSize = 0;
		for (auto& diffblock : ptrCtx->inFlightDiff)
		{
			diffblock.newDataSize = diffblock.newBlockData.size();
			packageSize += sizeof(YAlibrsync::DiffBlock);
			toWrite.emplace_back(asio::const_buffer(&diffblock, sizeof(YAlibrsync::DiffBlock)));
			if (!diffblock.isExistingBlock())
			{
				packageSize += diffblock.newBlockData.size();
				diffblock.newDataSize = diffblock.newBlockData.size();
				toWrite.emplace_back(asio::const_buffer(diffblock.newBlockData.data(), diffblock.newBlockData.size()));
			}
		}
		txHeader.diffBlock.packageSize = packageSize;
	}
	break;
	case YAlibrsync::SyncContextState::COPY_FILE:
	{
		long long bytesRead = 0;
		{
			YAlibrsync::ScopedSemaphoreAcqRel ioGuard(*ioSemaphore);
			ptrCtx->ptrOldFile->read(reinterpret_cast<char*>(ptrCtx->ptrBuffer->data()), ptrCtx->ptrBuffer->size());
			bytesRead = ptrCtx->ptrOldFile->gcount();
		}
		toWrite.emplace_back(asio::const_buffer(ptrCtx->ptrBuffer->data(), bytesRead));
		txHeader.copyFile.fileSize = ptrCtx->fileDigest.fileSize;
		txHeader.copyFile.chunksize = bytesRead;
		txHeader.copyFile.offset = ptrCtx->copyOffset;
		ptrCtx->copyOffset += bytesRead;
		if (ptrCtx->copyOffset == ptrCtx->fileDigest.fileSize)
		{
			ptrCtx->nextState = YAlibrsync::SyncContextState::COMPLETE;
			ptrCtx->ptrOldFile->close();
			ptrCtx->ptrOldFile.reset();
		}
	}
	break;

	case YAlibrsync::SyncContextState::DELETE_FILE:
	{
		txHeader.deleteFile.fileNameLength = ptrCtx->fileName.size() - directory.size();
		toWrite.emplace_back(asio::const_buffer(ptrCtx->fileName.data() + directory.size(), txHeader.deleteFile.fileNameLength));
	}
	break;

	default:
		break;
	}
}

bool YArsync::isReadyForWrite(std::shared_ptr < YAlibrsync::SyncContext > ptrCtx)
{
	bool ready = false;
	switch (ptrCtx->state)
	{
		// there's a moment when request just have been received and put onto outstanding contexts queue, but hasnt been processed by network reader thread
	case YAlibrsync::SyncContextState::NONE:
		break;
	case YAlibrsync::SyncContextState::PING:
		ready = true;
		break;
	case YAlibrsync::SyncContextState::PONG:
		ready = true;
		break;
	case YAlibrsync::SyncContextState::PING_SENT:
		ready = false;
		break;
	case YAlibrsync::SyncContextState::DIGEST_REQUEST:
		ready = true;
		break;
	case YAlibrsync::SyncContextState::DIGEST_REQUEST_SENT:
		ready = false;
		break;
	case YAlibrsync::SyncContextState::COMPUTING_DIGEST:
		ready = false;
		break;
	case YAlibrsync::SyncContextState::DIGEST_RESPONSE:
		ready = true;
		break;
	case YAlibrsync::SyncContextState::DIGEST_RESPONSE_SENT:
		ready = false;
		break;
	case YAlibrsync::SyncContextState::COMPUTING_DIFF:
		ready = false;
		break;
	case YAlibrsync::SyncContextState::DIFF_BLOCK:
		ready = true;
		break;
	case YAlibrsync::SyncContextState::DIFF_APPLIED:
		ready = true;
		break;
	case YAlibrsync::SyncContextState::DELETE_FILE:
		ready = true;
		break;
	case YAlibrsync::SyncContextState::COPY_FILE:
		ready = true;
		break;
	case YAlibrsync::SyncContextState::COPY_RECEIVED:
		ready = false;
		break;
	default:
		break;
	}

	return ready;
}


asio::awaitable<void> YArsync::networkReader(std::shared_ptr<asio::ip::tcp::socket> socket)
{
	auto executor = co_await asio::this_coro::executor;
	try
	{
		while (true)
		{
			std::memset(&rxHeader, 0, sizeof(rxHeader));
			//{ std::lock_guard<std::mutex> grd(coutMtx); std::cout << "Net: reading " << sizeof(NetPacketHeader) << " bytes." << std::endl; }
			std::size_t n = co_await asio::async_read(*socket, asio::mutable_buffer(static_cast<void*>(&rxHeader), sizeof(NetPacketHeader)), asio::use_awaitable);
			//{ std::lock_guard<std::mutex> grd(coutMtx); std::cout << "Net: read " << n << " bytes." << std::endl; }
			assert(n == sizeof(rxHeader));
			char* pData = rxData.data();
			std::vector<char> oversizeData;
			if (rxHeader.getDataSize())
			{
				if (rxData.size() < rxHeader.getDataSize())
				{
					oversizeData.resize(rxHeader.getDataSize());
					pData = oversizeData.data();
				}
				//{ std::lock_guard<std::mutex> grd(coutMtx); std::cout << "Net: reading " << rxHeader.getDataSize() << " bytes." << std::endl; }
				n = co_await asio::async_read(*socket, asio::buffer(static_cast<void*>(pData), rxHeader.getDataSize()), asio::use_awaitable);
				//{ std::lock_guard<std::mutex> grd(coutMtx); std::cout << "Net: read " << n << " bytes." << std::endl; }
				assert(n == rxHeader.getDataSize());
			}
			std::shared_ptr<YAlibrsync::SyncContext> ptrCtx;
			{
				std::unique_lock<std::mutex> guard(bigFatMutex);
				auto it = contextsIdx.find(rxHeader.base.id);
				if (it != contextsIdx.end())
					ptrCtx = it->second;
				else
				{
					// TODO: proper place for logic
					ptrCtx.reset(new YAlibrsync::SyncContext());
					ptrCtx->id = rxHeader.base.id;
					outsdandingContexts.push_back(ptrCtx);
					contextsIdx[ptrCtx->id] = ptrCtx;
					std::shared_ptr<SyncContextSignals> ptrRequestSignals(new SyncContextSignals());
					contextsSignals[ptrCtx->id] = ptrRequestSignals;
				}
			}
			// TODO: make sure no processing should be done in this loop!! get the data into context and co_spawn relevant method
			switch (rxHeader.base.type)
			{
			case YAlibrsync::SyncContextState::NONE:
			{
				abort();
			}
			break;
			case YAlibrsync::SyncContextState::PING:
			{
				{
					std::unique_lock<std::mutex> guard(ptrCtx->contextMutex);
					ptrCtx->state = YAlibrsync::SyncContextState::PONG;
					ptrCtx->nextState = YAlibrsync::SyncContextState::COMPLETE;
					//{ std::lock_guard<std::mutex> grd(coutMtx);  std::cout << "Received PING, sending PONG" << std::endl; }
					writeReadyEvent.signal();
				}
			}
			break;
			case YAlibrsync::SyncContextState::PONG:
			{
				{
					std::unique_lock<std::mutex> guard(ptrCtx->contextMutex);
					ptrCtx->state = YAlibrsync::SyncContextState::COMPLETE;
					//{ std::lock_guard<std::mutex> grd(coutMtx); std::cout << ptrCtx->id << " Received PONG" << std::endl; }
				}
			}
			break;
			case YAlibrsync::SyncContextState::DIGEST_REQUEST:
			{
				{
					std::unique_lock<std::mutex> guard(ptrCtx->contextMutex);
					ptrCtx->state = YAlibrsync::SyncContextState::DIGEST_REQUEST_RECEIVED;
					ptrCtx->fileName = directory + std::string(pData, pData + rxHeader.digestRequest.fileNameLength);
					// TODO: check if file exits, send empty digest response immediately otherwise
					ptrCtx->creationTime = rxHeader.digestRequest.creationTime;
					asio::co_spawn(executor, prepareContextDigestNetAsync(ptrCtx), asio::detached);
				}
			}
			break;
			case YAlibrsync::SyncContextState::DIGEST_RESPONSE:
			{
				{
					std::unique_lock<std::mutex> guard(ptrCtx->contextMutex);
					ptrCtx->fileDigest.fileSize = rxHeader.digestResponse.fileSize;
					if (ptrCtx->fileDigest.fileSize)
					{
						ptrCtx->state = YAlibrsync::SyncContextState::DIGEST_RESPONSE_RECEIVED;
						ptrCtx->fileDigest.blockSize = rxHeader.digestResponse.blockSize;
						ptrCtx->fileDigest.lastBlockSize = rxHeader.digestResponse.lastBlockSize;
						ptrCtx->fileDigest.hashes.resize(rxHeader.digestResponse.numDigestBlocks);
						// TODO: WHOOHOOOO, no endianness checks or whatever
						std::memcpy(static_cast<void*>(ptrCtx->fileDigest.hashes.data()), pData, ptrCtx->fileDigest.hashes.size() * sizeof(YAlibrsync::BlockDigest));
						asio::co_spawn(executor, prepareContextDiffNetAsync(ptrCtx), asio::detached);
					}
					else
					{
						// no file or zero size, do a copy
						ptrCtx->state = YAlibrsync::SyncContextState::COPY_FILE;
						ptrCtx->ptrOldFile.reset(new std::ifstream(ptrCtx->fileName, std::ios::binary));
						std::streampos fsize = 0;
						fsize = ptrCtx->ptrOldFile->tellg();
						ptrCtx->ptrOldFile->seekg(0, std::ios::end);
						fsize = ptrCtx->ptrOldFile->tellg() - fsize;
						ptrCtx->ptrOldFile->seekg(0, std::ios::beg);
						ptrCtx->fileDigest.fileSize = fsize;
						ptrCtx->ptrBuffer.reset(new std::array<char, 2 * YAlibrsync::MAX_BLOCK_SIZE>());
					}
				}
			}
			break;
			case YAlibrsync::SyncContextState::DIFF_BLOCK:
			{
				{
					// TODO: WHOOHOOOO, no endianness checks or whatever
					std::unique_lock<std::mutex> guard(ptrCtx->contextMutex);
					if (rxHeader.diffBlock.totalDiffBlocks)
						ptrCtx->totalDiffBlocks = rxHeader.diffBlock.totalDiffBlocks;
					YAlibrsync::DiffBlock* pBlock = (YAlibrsync::DiffBlock*)pData;
					size_t diffBlocks = rxHeader.diffBlock.numDiffBlocks;
					if (diffBlocks)
						do
						{
							std::vector<char> newBlockData;
							if (!pBlock->isExistingBlock())
							{
								newBlockData.reserve(pBlock->newDataSize);
								newBlockData.insert(newBlockData.begin(), ((char*)pBlock) + sizeof(YAlibrsync::DiffBlock), ((char*)pBlock) + sizeof(YAlibrsync::DiffBlock) + pBlock->newDataSize);
							}
							ptrCtx->fileDiff.emplace_back(std::move(newBlockData), pBlock->existingBlockIndex);
							if (pBlock->isExistingBlock())
								++pBlock;
							else
							{
								size_t newDataSize = pBlock->newDataSize;
								++pBlock;
								char* pTemp = (char*)pBlock;
								pTemp += newDataSize;
								pBlock = (YAlibrsync::DiffBlock*)(pTemp);
							}
						} while (--diffBlocks);
						if (ptrCtx->state == YAlibrsync::SyncContextState::DIGEST_RESPONSE_SENT)
							asio::co_spawn(executor, applyContextDiffNetAsync(ptrCtx, contextsSignals[ptrCtx->id]->dataAvailable), asio::detached);
						contextsSignals[ptrCtx->id]->dataAvailable.signal();
				}
			}
			break;
			case YAlibrsync::SyncContextState::DIFF_APPLIED:
			{
				{
					std::unique_lock<std::mutex> guard(ptrCtx->contextMutex);
					ptrCtx->state = YAlibrsync::SyncContextState::COMPLETE;
					contextsSignals[ptrCtx->id]->syncComplete.signal();
				}
			}
			break;
			case YAlibrsync::SyncContextState::DELETE_FILE:
			{
				{
					std::unique_lock<std::mutex> guard(ptrCtx->contextMutex);
					std::filesystem::remove(directory + std::string(pData, pData + rxHeader.deleteFile.fileNameLength));
					contextsSignals[ptrCtx->id]->syncComplete.signal();
				}
			}
			break;
			case YAlibrsync::SyncContextState::COPY_FILE:
			{
				{
					std::unique_lock<std::mutex> guard(ptrCtx->contextMutex);
					if (ptrCtx->state == YAlibrsync::SyncContextState::DIGEST_RESPONSE_SENT)
					{
						ptrCtx->fileDigest.fileSize = rxHeader.copyFile.fileSize;
						std::string pathToCreate = ptrCtx->fileName.substr(0, ptrCtx->fileName.rfind('\\'));
						std::filesystem::create_directories(pathToCreate); // TODO: cross-platform path delimiter throughout the code
						ptrCtx->ptrNewFile.reset(new std::ofstream(ptrCtx->fileName, std::ios::binary));
						ptrCtx->state = YAlibrsync::SyncContextState::COPY_RECEIVED;
					}
					{
						YAlibrsync::ScopedSemaphoreAcqRel ioGuard(*ioSemaphore);
						ptrCtx->ptrNewFile->write(pData, rxHeader.getDataSize());
						ptrCtx->copyOffset += rxHeader.getDataSize();
						if (ptrCtx->copyOffset == ptrCtx->fileDigest.fileSize)
						{
							ptrCtx->ptrNewFile->close();
							ptrCtx->ptrNewFile.reset();
							ptrCtx->state = YAlibrsync::SyncContextState::COMPLETE;
							contextsSignals[ptrCtx->id]->syncComplete.signal();
						}
					}
				}
			}
			break;
			default:
			{
				abort();
			}
			break;
			}
			{
				// need to release context mutex before taking bigFatMutex to avoid deadlocking with writer thread
				// ctx data is not accessed past this scope block, so its fine
				bool shouldComplete = false;
				{
					std::unique_lock<std::mutex> guard(ptrCtx->contextMutex);
					shouldComplete = ptrCtx->state == YAlibrsync::SyncContextState::COMPLETE;
					if (shouldComplete)
						contextsSignals[ptrCtx->id]->syncComplete.signal();
				}
				if (shouldComplete)
				{
					std::unique_lock<std::mutex> guard(bigFatMutex);
					auto itProcessingResponse = std::find(processingContexts.begin(), processingContexts.end(), ptrCtx);
					if (itProcessingResponse != processingContexts.end())
					{
						currentFilenames.erase((*itProcessingResponse)->fileName);
						processingContexts.erase(itProcessingResponse);
						std::make_heap(processingContexts.begin(), processingContexts.end());
					}
					else
					{
						auto itOutstandingResponse = std::find(outsdandingContexts.begin(), outsdandingContexts.end(), ptrCtx);
						if (itOutstandingResponse != outsdandingContexts.end())
							outsdandingContexts.erase(itOutstandingResponse);
						else
							abort(); // completed unknown response
					}
					contextsIdx.erase(ptrCtx->id);
					contextsSignals.erase(ptrCtx->id);
				}
			}
		}
	}
	catch (std::exception& e)
	{
		std::printf("echo Exception: %s\n", e.what());
	}
	co_return;
}

asio::awaitable<void> YArsync::networkWriter(std::shared_ptr<asio::ip::tcp::socket> socket)
{
	try
	{
		while (!gExit)
		{
			std::shared_ptr < YAlibrsync::SyncContext > ptrCtx;
			{
				std::unique_lock<std::mutex> guard(bigFatMutex);
				size_t numProcessingRequests = processingContexts.size();
				size_t numDelayedRequests = 0;

				for (auto it = outsdandingContexts.begin(); it != outsdandingContexts.end(); ++it)
				{
					if ((processingContexts.size() == maxConcurrentResponses) || (numDelayedRequests == outsdandingContexts.size()))
						break;
					if (currentFilenames.contains((*it)->fileName))
					{
						//{ std::lock_guard<std::mutex> grd(coutMtx); std::cout << ptrCtx->id << " Delaying request." << std::endl; }
						++numDelayedRequests;
					}
					else
					{
						processingContexts.push_back(*it);
						it = outsdandingContexts.erase(it);
						if (outsdandingContexts.end() == it)
							break;
					}
				}

				if (numProcessingRequests != processingContexts.size())
					std::make_heap(processingContexts.begin(), processingContexts.end());
				for (auto& itPtrCtx : processingContexts)
				{
					std::unique_lock<std::mutex> guard(itPtrCtx->contextMutex);
					if (isReadyForWrite(itPtrCtx))
					{
						ptrCtx = itPtrCtx;
						break;
					}
				}
			}

			if (ptrCtx.get())
			{
				std::vector<asio::const_buffer> toWrite;
				{
					std::memset(&txHeader, 0, sizeof(txHeader));
					std::unique_lock<std::mutex> guard(ptrCtx->contextMutex);
					txHeader.base.id = ptrCtx->id;
					txHeader.base.type = ptrCtx->state;
					prepareTxBuffers(ptrCtx, toWrite, txHeader);
				}
				size_t bytesToWrite = 0;
				for (auto& buffer : toWrite)
					bytesToWrite += buffer.size();
				//{ std::lock_guard<std::mutex> grd(coutMtx); std::cout << "Net: writing " << bytesToWrite << " bytes." << std::endl; }
				size_t bytesWritten = co_await asio::async_write(*socket, toWrite, asio::use_awaitable);
				//{ std::lock_guard<std::mutex> grd(coutMtx); std::cout << "Net: wrote " << bytesWritten << " bytes." << std::endl; }
				{
					std::unique_lock<std::mutex> guard(ptrCtx->contextMutex);
					if (ptrCtx->nextState != YAlibrsync::SyncContextState::NONE)
					{
						ptrCtx->state = ptrCtx->nextState;
						ptrCtx->nextState = YAlibrsync::SyncContextState::NONE;
					}
					if (ptrCtx->state == YAlibrsync::SyncContextState::COMPLETE)
					{
						contextsSignals[ptrCtx->id]->syncComplete.signal();
						std::unique_lock<std::mutex> guard(bigFatMutex);
						auto itProcessingResponse = std::find(processingContexts.begin(), processingContexts.end(), ptrCtx);
						if (itProcessingResponse != processingContexts.end())
						{
							currentFilenames.erase((*itProcessingResponse)->fileName);
							processingContexts.erase(itProcessingResponse);
							std::make_heap(processingContexts.begin(), processingContexts.end());
						}
						else
						{
							auto itOutstandingResponse = std::find(outsdandingContexts.begin(), outsdandingContexts.end(), ptrCtx);
							if (itOutstandingResponse != outsdandingContexts.end())
								outsdandingContexts.erase(itOutstandingResponse);
							else
								abort(); // completed unknown response
						}
						contextsIdx.erase(ptrCtx->id);
						contextsSignals.erase(ptrCtx->id);
					}
				}
			}
			else
			{
				// TODO: proper synchronization
				// BinarySemaphore outstandingResponses.wait();
				writeReadyEvent.wait_for(std::chrono::milliseconds(10));
			}
		}
	}
	catch (std::exception& e)
	{
		std::printf("echo Exception: %s\n", e.what());
	}
	co_return;
}

asio::awaitable<void> YArsync::connector(std::string address, int port)
{
	auto executor = co_await asio::this_coro::executor;
	std::shared_ptr<asio::ip::tcp::socket> ptrSocket;
	ptrSocket.reset(pSocket = new asio::ip::tcp::socket(executor, asio::ip::tcp::v4()));
	asio::ip::tcp::resolver resolver(executor);
	auto resolvedEndpoints = co_await resolver.async_resolve({ address, std::to_string(port) }, asio::use_awaitable);
	co_await ptrSocket->async_connect(*resolvedEndpoints.begin(), asio::use_awaitable);
	if (ptrSocket->is_open())
	{
		asio::co_spawn(executor, networkReader(ptrSocket), asio::detached);
		asio::co_spawn(executor, networkWriter(ptrSocket), asio::detached);
	}
	co_return;
}

void YArsync::startClient(asio::io_context& context, std::string inDirectory, std::string address, int port)
{
	directory = inDirectory;
	asio::co_spawn(context.get_executor(), connector(address, port), asio::detached);
}

asio::awaitable<void> YArsync::listener(std::string address, int port)
{
	auto executor = co_await asio::this_coro::executor;
	std::shared_ptr<asio::ip::tcp::socket> ptrSocket;
	asio::ip::tcp::endpoint localEndpoint(asio::ip::address_v4::from_string(address), port);
	asio::ip::tcp::acceptor acceptor(executor, localEndpoint, true);
	pAcceptor = &acceptor;
	ptrSocket.reset(pSocket = new asio::ip::tcp::socket(std::move(co_await acceptor.async_accept(asio::use_awaitable))));
	if (ptrSocket->is_open())
	{
		asio::co_spawn(executor, networkReader(ptrSocket), asio::detached);
		asio::co_spawn(executor, networkWriter(ptrSocket), asio::detached);
	}
	pAcceptor = nullptr;
	co_return;
}

void YArsync::startServer(asio::io_context& context, std::string inDirectory, std::string address, int port)
{
	directory = inDirectory;
	asio::co_spawn(context.get_executor(), listener(address, port), asio::detached);
}

asio::awaitable<void> YArsync::SyncNetworkFileAsync(asio::io_context& context, std::string fileName)
{
	std::shared_ptr<YAlibrsync::SyncContext> ptrCtx;
	std::shared_ptr<SyncContextSignals> ptrRequestSignals(new SyncContextSignals());
	ptrCtx.reset(new YAlibrsync::SyncContext());
	ptrCtx->fileName = fileName;
	ptrCtx->state = YAlibrsync::DIGEST_REQUEST;
	ptrCtx->nextState = YAlibrsync::DIGEST_REQUEST_SENT;
	ptrCtx->id = ++requestCounter;
	{
		std::unique_lock<std::mutex> guard(bigFatMutex);
		outsdandingContexts.push_back(ptrCtx);
		contextsIdx[ptrCtx->id] = ptrCtx;
		contextsSignals[ptrCtx->id] = ptrRequestSignals;
		writeReadyEvent.signal();
	}
	contextsSignals[ptrCtx->id]->syncComplete.wait();
	co_return;
}

void YArsync::SyncNetworkFile(asio::io_context& context, std::string fileName)
{
	asio::co_spawn(context.get_executor(), SyncNetworkFileAsync(context, fileName), asio::detached);
}

asio::awaitable<void> YArsync::DeleteNetworkFileAsync(asio::io_context& context, std::string fileName)
{
	std::shared_ptr<YAlibrsync::SyncContext> ptrCtx;
	std::shared_ptr<SyncContextSignals> ptrRequestSignals(new SyncContextSignals());
	ptrCtx.reset(new YAlibrsync::SyncContext());
	ptrCtx->fileName = fileName;
	ptrCtx->state = YAlibrsync::DELETE_FILE;
	ptrCtx->nextState = YAlibrsync::DELETE_SENT;
	ptrCtx->id = ++requestCounter;
	{
		std::unique_lock<std::mutex> guard(bigFatMutex);
		outsdandingContexts.push_back(ptrCtx);
		contextsIdx[ptrCtx->id] = ptrCtx;
		contextsSignals[ptrCtx->id] = ptrRequestSignals;
		writeReadyEvent.signal();
	}
	contextsSignals[ptrCtx->id]->syncComplete.wait();
	co_return;
}

void YArsync::DeleteNetworkFile(asio::io_context& context, std::string fileName)
{
	asio::co_spawn(context.get_executor(), DeleteNetworkFileAsync(context, fileName), asio::detached);
}

int YArsync::netDirectoryWatcher(asio::io_context& context, std::string directory)
{
	fs::FileWatcher fileWatcher;

	fs::WatchID watchID = fileWatcher.addWatch(directory, [&](fs::WatchID watchID, const std::string& dir, const std::string filename, fs::Action action)
	{
		// this code is just opening the can of worms. 
		// Events can come in all kinds of types and quantities. Properly batching them (skipping multple edits when one is already queued, for one)
		// will require much more complex code to work correctly
		bool alreadyQueued = false;
		YAlibrsync::SyncContextState queuedState = YAlibrsync::SyncContextState::NONE;
		{
			std::unique_lock<std::mutex> guard(bigFatMutex);
			for (auto& ptrCtx : outsdandingContexts)
			{
				if (ptrCtx->fileName == dir + "\\" + filename)
				{
					alreadyQueued = true;
					queuedState = ptrCtx->state;
				}
			}
		}
		if (action == fs::Actions::Delete && (!alreadyQueued || queuedState != YAlibrsync::SyncContextState::DELETE_FILE))
			DeleteNetworkFile(context, dir + "\\" + filename);
		else if (action != fs::Actions::Add && (!alreadyQueued || queuedState != YAlibrsync::SyncContextState::DIGEST_REQUEST))
			SyncNetworkFile(context, dir + "\\" + filename);
	}, true);

	while (!gExit)
		fileWatcher.update();

	return 0;
}

asio::awaitable<void> YArsync::prepareContextDiffAsync(std::shared_ptr < YAlibrsync::SyncContext> syncRequest, std::shared_ptr <YAlibrsync::SyncContext> syncResponse, YAlibrsync::CountingSemaphore& dataAvailable)
{
	while (!syncRequest->totalDiffBlocks)
	{
		prepareContextDiff(*syncRequest, *ioSemaphore);
		{
			std::lock_guard<std::mutex> lck(syncResponse->contextMutex);
			if (syncResponse->fileDiff.empty())
				std::swap(syncResponse->fileDiff, syncRequest->nextDiff);
			else
			{
				syncResponse->fileDiff.insert(syncResponse->fileDiff.end(), syncRequest->nextDiff.begin(), syncRequest->nextDiff.end());
				syncRequest->nextDiff.clear();
			}
			if (syncRequest->totalDiffBlocks)
				syncResponse->totalDiffBlocks = syncRequest->totalDiffBlocks;
		}
		dataAvailable.signal();
	}
	co_return;
}

asio::awaitable<void> YArsync::applyContextDiffAsync(std::shared_ptr < YAlibrsync::SyncContext> syncResponse, YAlibrsync::CountingSemaphore& dataAvailable, YAlibrsync::CountingSemaphore& syncComplete)
{
	while ((!syncResponse->totalDiffBlocks && !syncResponse->diffBlocksProcessed) || (syncResponse->totalDiffBlocks != syncResponse->diffBlocksProcessed))
	{
		dataAvailable.wait();
		YAlibrsync::applyContextDiff(*syncResponse, *ioSemaphore);
	}
	syncComplete.signal();

	co_return;
}

asio::awaitable<void> YArsync::prepareContextDigestAsync(std::shared_ptr<YAlibrsync::SyncContext> ptrSyncResponse)
{
	YAlibrsync::prepareContextDigest(*ptrSyncResponse, *ioSemaphore);
	co_return;
}

asio::awaitable<void> YArsync::SyncLocalFileAsync(std::shared_ptr<YAlibrsync::CountingSemaphore> dispatchSemaphore,
	std::shared_ptr<YAlibrsync::SyncContext> ptrRequest,
	std::shared_ptr<YAlibrsync::SyncContext> ptrResponse)
{
	std::string fileName = ptrRequest->fileName;
	auto executor = co_await asio::this_coro::executor;
	co_await prepareContextDigestAsync(ptrResponse);

	ptrRequest->fileDigest = ptrResponse->fileDigest;
	ptrResponse->state = YAlibrsync::SyncContextState::DIGEST_RESPONSE_SENT;
	ptrRequest->state = YAlibrsync::SyncContextState::DIGEST_RESPONSE_RECEIVED;

	YAlibrsync::CountingSemaphore syncComplete(0);
	YAlibrsync::CountingSemaphore dataAvailable(0);

	asio::co_spawn(executor, prepareContextDiffAsync(ptrRequest, ptrResponse, dataAvailable), asio::detached);
	asio::co_spawn(executor, applyContextDiffAsync(ptrResponse, dataAvailable, syncComplete), asio::detached);

	syncComplete.wait();

	ptrRequest->ptrSourceFile.reset();
	ptrResponse->ptrNewFile.reset();
	ptrResponse->ptrOldFile.reset();

	std::filesystem::remove(ptrResponse->fileName);
	std::filesystem::rename(ptrResponse->fileName + ".patched", ptrResponse->fileName);

	dispatchSemaphore->signal();

	co_return;
}

void YArsync::SyncLocalFile(asio::io_context& ioContext,
	std::shared_ptr<YAlibrsync::CountingSemaphore> dispatchSemaphore,
	std::atomic_llong& contextCounter,
	std::string srcFileName,
	std::string dstFileName)
{
	if (!std::filesystem::exists(dstFileName) || !std::filesystem::file_size(dstFileName))
	{
		// simple copy
		YAlibrsync::ScopedSemaphoreAcqRel ioGuard(*ioSemaphore);
		std::filesystem::copy(srcFileName, dstFileName, std::filesystem::copy_options::overwrite_existing);
	}
	else
	{
		std::shared_ptr<YAlibrsync::SyncContext> ptrRequest(new YAlibrsync::SyncContext());
		ptrRequest->fileName = srcFileName;
		ptrRequest->state = YAlibrsync::SyncContextState::NONE;
		ptrRequest->id = contextCounter++;

		std::shared_ptr<YAlibrsync::SyncContext> ptrResponse(new YAlibrsync::SyncContext());
		ptrResponse->state = YAlibrsync::SyncContextState::DIGEST_REQUEST_RECEIVED;
		ptrResponse->fileName = dstFileName;
		ptrResponse->id = ptrRequest->id;

		dispatchSemaphore->wait();
		asio::co_spawn(ioContext.get_executor(), SyncLocalFileAsync(dispatchSemaphore, ptrRequest, ptrResponse), asio::detached);
	}
}


int YArsync::localDirectoryWatcher(asio::io_context& context, std::string directory)
{
	fs::FileWatcher fileWatcher;

	// TODO: this also misses the proper batching logic
	fs::WatchID watchID = fileWatcher.addWatch(directory, [&](fs::WatchID watchID, const std::string& dir, const std::string filename, fs::Action action)
	{
		std::string dstFileName = local_directory + filename.substr(directory.size());
		switch (action)
		{
		case fs::Actions::Delete:
		{
			std::filesystem::remove_all(dstFileName);
		}
		break;
		case fs::Actions::Add:
		{
			YAlibrsync::ScopedSemaphoreAcqRel ioGuard(*ioSemaphore);
			std::string pathToCreate = dstFileName.substr(0, dstFileName.rfind('\\'));
			std::filesystem::create_directories(pathToCreate); // TODO: cross-platform path delimiter throughout the code
			std::filesystem::copy_file(dir + filename, dstFileName);
		}
		break;
		case fs::Actions::Modified:
		{
			SyncLocalFile(context, dispatchSemaphore, requestCounter, dir + filename, dstFileName);
		}
		break;
		default:
			break;
		}

	});

	while (!gExit)
		fileWatcher.update();

	return 0;
}
