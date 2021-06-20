#include <iostream>
#include <algorithm>

#include "YArsync.h"

namespace 
{
	char* getCmdOption(char** begin, char** end, const char* option)
	{
		char** itr = std::find_if(begin, end, [option](const char* item) {return !strcmp(item, option); });
		if (itr != end && ++itr != end)
		{
			return *itr;
		}
		return 0;
	}

	bool cmdOptionExists(char** begin, char** end, const char* option)
	{
		return std::find_if(begin, end, [option](const char* item) {return !strcmp(item, option); }) != end;
	}
}

int main(int argc, char* argv[]) 
{
	// TODO: would be nice to support filename wildcards
	// TODO: just convert all these options and they semantics to rsync CL interface
	int ioFactor = 1;
	bool bWatchDirectory = false;
	bool bDaemon = false;
	bool bListen = false;
	int port = 55555;
	std::string ip = "0.0.0.0";
	std::string directory = "";
	std::string local_directory = "";

	if (cmdOptionExists(argv, argv + argc, "--io_factor"))
		ioFactor = atoi(getCmdOption(argv, argv + argc, "--io_factor"));

	if (cmdOptionExists(argv, argv + argc, "--port"))
		port = atoi(getCmdOption(argv, argv + argc, "--port"));

	if (cmdOptionExists(argv, argv + argc, "--ip"))
		ip = getCmdOption(argv, argv + argc, "--ip");

	if (cmdOptionExists(argv, argv + argc, "--daemon"))
		bDaemon = true;

	if (cmdOptionExists(argv, argv + argc, "--listen"))
		bListen = true;

	if (cmdOptionExists(argv, argv + argc, "--watch"))
		bWatchDirectory = true;

	if (cmdOptionExists(argv, argv + argc, "--directory"))
		directory = getCmdOption(argv, argv + argc, "--directory");

	if (cmdOptionExists(argv, argv + argc, "--local_directory"))
		local_directory = getCmdOption(argv, argv + argc, "--local_directory");

	YArsync rsync;
	return rsync.run(ioFactor, bWatchDirectory, bDaemon, bListen, port, ip, directory, local_directory);
}

// --directory D:\Temp\leetcodestruggle\ --local_directory E:\Temp\wtf\ --io_factor 4
// --listen --ip 0.0.0.0 --port 55555 --io_factor 4 --directory E:\Temp\to
// --connect --ip 127.0.0.1 --port 55555 --test --watch --directory E:\Temp\from