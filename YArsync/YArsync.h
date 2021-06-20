#pragma once

#include <string>
#include <memory>
#include <unordered_set>
#include <list>

#include <asio/io_context.hpp>
#include <asio/co_spawn.hpp>
#include <asio/detached.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/signal_set.hpp>
#include <asio/write.hpp>
#include <asio/require.hpp>

#include "../YAlibrsync/YAlibrsync.h"

#include "Network.h"
#include "Util.h"

class YArsync
{
public:
	int run(int ioFactor, bool bWatchDirectory, bool bDaemon, bool bListen, int port, std::string ip, std::string directory, std::string local_directory);
private:

	std::string getRelativeFileName(std::string fileName);

	bool gExit;
	std::shared_ptr<YAlibrsync::CountingSemaphore> ioSemaphore;
	std::shared_ptr<YAlibrsync::CountingSemaphore> dispatchSemaphore;
	std::atomic_llong requestCounter;
	YAlibrsync::BinarySemaphore writeReadyEvent;
	std::string local_directory;
	std::string directory;
	asio::ip::tcp::socket* pSocket = nullptr;
	asio::ip::tcp::acceptor* pAcceptor = nullptr;
	NetPacketHeader rxHeader;
	NetPacketHeader txHeader;
	std::array<char, 4 * YAlibrsync::MAX_BLOCK_SIZE> rxData;
	std::mutex bigFatMutex;
	std::unordered_map<long long, std::shared_ptr<YAlibrsync::SyncContext>> contextsIdx;
	std::list<std::shared_ptr<YAlibrsync::SyncContext>> outsdandingContexts;
	std::vector< std::shared_ptr<YAlibrsync::SyncContext>> processingContexts;
	std::unordered_set<std::string> currentFilenames;
	// std::mutex coutMtx; // TODO: replace with google logging
	size_t maxConcurrentResponses;

	// server
	void startClient(asio::io_context& context, std::string directory, std::string address, int port);
	void startServer(asio::io_context& context, std::string directory, std::string address, int port);
	void DeleteNetworkFile(asio::io_context& context, std::string fileName);
	void SyncNetworkFile(asio::io_context& context, std::string fileName);
	int netDirectoryWatcher(asio::io_context& context, std::string directory);
	asio::awaitable<void> prepareContextDigestNetAsync(std::shared_ptr<YAlibrsync::SyncContext> ptrCtx);
	asio::awaitable<void> prepareContextDiffNetAsync(std::shared_ptr < YAlibrsync::SyncContext> ptrCtx);
	asio::awaitable<void> applyContextDiffNetAsync(std::shared_ptr < YAlibrsync::SyncContext> ptrCtx, YAlibrsync::BinarySemaphore& dataAvailable);
	void prepareTxBuffers(std::shared_ptr < YAlibrsync::SyncContext > ptrCtx, std::vector<asio::const_buffer>& toWrite, NetPacketHeader& txHeader);
	bool isReadyForWrite(std::shared_ptr < YAlibrsync::SyncContext > ptrCtx);
	asio::awaitable<void> networkReader(std::shared_ptr<asio::ip::tcp::socket> socket);
	asio::awaitable<void> networkWriter(std::shared_ptr<asio::ip::tcp::socket> socket);
	asio::awaitable<void> connector(std::string address, int port);
	asio::awaitable<void> listener(std::string address, int port);
	asio::awaitable<void>  SyncNetworkFileAsync(asio::io_context& context, std::string fileName);
	asio::awaitable<void> DeleteNetworkFileAsync(asio::io_context& context, std::string fileName);

	// client
	void SyncLocalFile(asio::io_context& ioContext, std::shared_ptr<YAlibrsync::CountingSemaphore> dispatchSemaphore, std::atomic_llong& contextCounter, std::string srcFileName, std::string dstFileName);
	int localDirectoryWatcher(asio::io_context& context, std::string directory);
	asio::awaitable<void> prepareContextDiffAsync(std::shared_ptr < YAlibrsync::SyncContext> syncRequest, std::shared_ptr <YAlibrsync::SyncContext> syncResponse, YAlibrsync::CountingSemaphore& dataAvailable);
	asio::awaitable<void> applyContextDiffAsync(std::shared_ptr < YAlibrsync::SyncContext> syncResponse, YAlibrsync::CountingSemaphore& dataAvailable, YAlibrsync::CountingSemaphore& syncComplete);
	asio::awaitable<void> prepareContextDigestAsync(std::shared_ptr<YAlibrsync::SyncContext> ptrSyncResponse);
	asio::awaitable<void>  SyncLocalFileAsync(std::shared_ptr<YAlibrsync::CountingSemaphore> dispatchSemaphore, std::shared_ptr<YAlibrsync::SyncContext> ptrRequest, std::shared_ptr<YAlibrsync::SyncContext> ptrResponse);


};

