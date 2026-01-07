#pragma once
#include <iostream>
#include <ctime>

// 28比特时间戳类
class InodeTimestamp {
public:
    uint32_t year   : 8; // 8比特
    uint32_t month  : 6; // 6比特
    uint32_t day    : 6; // 6比特
    uint32_t hour   : 6; // 6比特
    uint32_t minute : 6; // 6比特

    InodeTimestamp();
    void print() const;
};
