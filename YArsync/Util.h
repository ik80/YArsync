#pragma once

#include <vector>
#include <string>
#include <functional>

#include <mutex>
#include <condition_variable>

std::vector<std::string> GetFilenamesRecursive(std::string path);

