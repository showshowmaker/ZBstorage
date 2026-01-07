#pragma once

#include <cstdio>
#include <iostream>
#include <string>

inline bool RedirectLogs(const std::string& path) {
    if (path.empty()) return true;
    if (!std::freopen(path.c_str(), "w", stdout)) {
        return false;
    }
    if (!std::freopen(path.c_str(), "w", stderr)) {
        return false;
    }
    std::ios::sync_with_stdio(true);
    std::cout.setf(std::ios::unitbuf);
    std::cerr.setf(std::ios::unitbuf);
    return true;
}
