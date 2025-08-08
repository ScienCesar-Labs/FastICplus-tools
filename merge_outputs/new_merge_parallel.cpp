// g++ -std=c++17 -fopenmp -O3 -march=native new_merge_parallel.cpp -o new_merge_parallel
// time ./new_merge_parallel

#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <filesystem>
#include <future>
#include <parallel/algorithm> // <-- replaces <algorithm>

constexpr int TYPE_COLUMN = 2;
constexpr int TIMESTAMP_COLUMN = 3;
constexpr long long MAX_TIMESTAMP = 1e18;
constexpr long long MIN_TIMESTAMP = 1000;
const std::string EXPECTED_HEADER = "ASIC\tCH\tTYPE\tTIMESTAMP_LSB\tPULSE_WIDTH_LSB";

// === Extract timestamp from a line ===
long long extractTimestamp(const std::string& line) {
    std::istringstream ss(line);
    std::string token;
    int colIndex = 0;
    while (std::getline(ss, token, '\t')) {
        if (colIndex == TIMESTAMP_COLUMN) {
            try {
                return std::stoll(token);
            } catch (...) {
                return -1;
            }
        }
        ++colIndex;
    }
    return -1;
}

// === Check if TYPE > 2 ===
bool isValidLine(const std::string& line) {
    std::istringstream ss(line);
    std::string token;
    int colIndex = 0;
    while (std::getline(ss, token, '\t')) {
        if (colIndex == TYPE_COLUMN) {
            try {
                int val = std::stoi(token);
                return val > 2;
            } catch (...) {
                return false;
            }
        }
        ++colIndex;
    }
    return false;
}

// === Process a single file and return valid lines ===
std::vector<std::string> processFile(const std::filesystem::path& filepath) {
    std::vector<std::string> lines;
    std::ifstream infile(filepath);
    if (!infile.is_open()) {
        std::cerr << "Skipping unreadable file: " << filepath << "\n";
        return lines;
    }

    std::string line;
    while (std::getline(infile, line)) {
        if (line == EXPECTED_HEADER) continue;

        long long ts = extractTimestamp(line);
        if ((line.find("ASIC") != std::string::npos || isValidLine(line)) &&
            ts >= MIN_TIMESTAMP && ts <= MAX_TIMESTAMP) {
            lines.push_back(line);
        }
    }
    return lines;
}

// === Merge and sort all valid lines from files with a given prefix ===
void mergeFilesParallel(const std::string& prefix, const std::string& outputFileName) {
    std::vector<std::filesystem::path> files;
    for (const auto& entry : std::filesystem::directory_iterator(".")) {
        if (entry.is_regular_file() && entry.path().filename().string().find(prefix) == 0) {
            files.push_back(entry.path());
        }
    }

    // Launch parallel file processors
    std::vector<std::future<std::vector<std::string>>> futures;
    for (const auto& file : files) {
        futures.push_back(std::async(std::launch::async, processFile, file));
    }

    // Collect results
    std::vector<std::string> allLines;
    for (auto& f : futures) {
        auto partial = f.get();
        allLines.insert(allLines.end(), partial.begin(), partial.end());
    }

    // REAL PARALLEL SORT
    __gnu_parallel::sort(allLines.begin(), allLines.end(),
        [](const std::string& a, const std::string& b) {
            return extractTimestamp(a) < extractTimestamp(b);
        });

    // Write to output
    std::ofstream out(outputFileName);
    out << EXPECTED_HEADER << "\n";
    for (const auto& line : allLines) {
        out << line << "\n";
    }
    out.close();

    std::cout << "Parallel merge done: " << allLines.size()
              << " lines written to '" << outputFileName << "'\n";
}

int main() {
    mergeFilesParallel("Master_", "merged_MASTER.txt");
    mergeFilesParallel("Slave_", "merged_SLAVE.txt");
    std::cout << "All merges complete.\n";
    return 0;
}