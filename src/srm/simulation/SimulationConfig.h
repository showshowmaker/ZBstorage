#pragma once

struct SimulationConfig {
    int min_latency_ms{5};
    int max_latency_ms{50};
    double failure_rate{0.0}; // 0.0-1.0
    uint64_t default_read_size{4096};
};
