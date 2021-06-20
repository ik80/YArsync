#include "Util.h"

#include <iostream>
#include <fstream>
#include <filesystem>

std::vector<std::string> GetFilenamesRecursive(std::string path) 
{
	std::vector<std::string> res;

    auto lastPath = std::filesystem::current_path();
    std::filesystem::current_path(path);
    for (auto& p : std::filesystem::recursive_directory_iterator(path))
    {
        if (p.is_regular_file()) 
        {
            std::string nextPath = p.path().string();
            res.push_back(nextPath);
        }
    }
    std::filesystem::current_path(lastPath);
	return res;
}
