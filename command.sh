#!/bin/bash

PLSDB=/home/ubuntu/rocksdb_limiter_prefetcher
CRUISEDB=/home/ubuntu/CruiseDB
SILK=/home/ubuntu/silk

IO2_1000=/home/ubuntu/gp2_150g_1

clean_cache() {
    sync
    echo 1 >/proc/sys/vm/drop_caches
    sync
    echo 2 >/proc/sys/vm/drop_caches
    sync
    echo 3 >/proc/sys/vm/drop_caches
    free -m
}

# $1 path
load_data() {
    clean_cache
    ./db_bench --benchmarks="fillseq,levelstats,stats" \
        –perf_level=3 \
        -max_background_jobs=8 -subcompactions=1 \
        -use_direct_io_for_flush_and_compaction=true \
        -use_direct_reads=true -cache_size=268435456 \
        -key_size=16 -value_size=256 -num=100000000 \
        -bloom_bits=10 \
        -write_buffer_size=$((16 * 1024 * 1024)) \
        -target_file_size_base=$((8 * 1024 * 1024)) \
        -db=/home/ubuntu/gp2_150g_1
}
# $1 threads  $2 nums
# -mix_get_ratio=0.83 -mix_put_ratio=0.17 -mix_seek_ratio=0.03 \
#         -sine_a=6000 -sine_b=0.035 -sine_c=4.17 -sine_d=15000 \
bench_rocksdb() {
    rm *.log
    clean_cache
    iostat -d /dev/nvme*n1p1 -m 1 >iostat.log &
    ./db_bench --benchmarks="mixgraph2,levelstats,stats" -use_direct_io_for_flush_and_compaction=true -use_direct_reads=true \
        -threads=$1 \
        -histogram=1 --statistics \
        -key_size=16 -value_size=256 \
        -cache_size=$((500 * 1024 * 1024)) \
        -key_dist_a=0.002312 -key_dist_b=0.3467 \
        -keyrange_dist_a=5.18 \
        -keyrange_dist_b=-2.917 \
        -keyrange_dist_c=0.0164 \
        -keyrange_dist_d=-0.08082 \
        -keyrange_num=30 \
        -mix_get_ratio=0.83 -mix_put_ratio=0.17 -mix_seek_ratio=0.03 \
        -sine_mix_rate_interval_milliseconds=50 \
        -sine_a=9000 -sine_b=0.035 -sine_c=4.17 -sine_d=22500 \
        –perf_level=2 -sine_mix_rate_noise=0.3 \
        -reads=$2 -num=100000000 \
        --report_interval_seconds=1 \
        -report_file="./report2.log" \
        -stats_interval_seconds=1 \
        -bloom_bits=10 \
        -target_file_size_base=$((8 * 1024 * 1024)) \
        -sine_mix_rate=true \
        -db=/home/ubuntu/gp2_150g_1 -use_existing_db=true | tee mixgraph.log
    sleep 1
    pkill iostat
}

bench_rocksdb_autotune() {
    rm *.log
    clean_cache
    iostat -d /dev/nvme*n1p1 -m 1 >iostat.log &
    ./db_bench --benchmarks="mixgraph2,levelstats,stats" -use_direct_io_for_flush_and_compaction=true -use_direct_reads=true \
        -threads=$1 \
        -histogram=1 --statistics \
        -key_size=16 -value_size=256 \
        -cache_size=$((500 * 1024 * 1024)) \
        -key_dist_a=0.002312 -key_dist_b=0.3467 \
        -keyrange_dist_a=5.18 \
        -keyrange_dist_b=-2.917 \
        -keyrange_dist_c=0.0164 \
        -keyrange_dist_d=-0.08082 \
        -keyrange_num=30 \
        -mix_get_ratio=0.83 -mix_put_ratio=0.17 -mix_seek_ratio=0.03 \
        -sine_mix_rate_interval_milliseconds=50 \
        -sine_a=9000 -sine_b=0.035 -sine_c=4.17 -sine_d=22500 \
        –perf_level=2 -sine_mix_rate_noise=0.3 \
        -reads=$2 -num=100000000 \
        --report_interval_seconds=1 \
        -report_file="./report2.log" \
        -stats_interval_seconds=1 \
        -bloom_bits=10 \
        -target_file_size_base=$((8 * 1024 * 1024)) \
        -sine_mix_rate=true \
        -rate_limiter_bytes_per_sec=$((250 * 1024 * 1024)) \
        -rate_limiter_auto_tuned=true \
        -db=/home/ubuntu/gp2_150g_1 -use_existing_db=true | tee mixgraph.log
    sleep 1
    pkill iostat
}

bench_tune() {
    rm *.log
    clean_cache
    iostat -d /dev/nvme*n1p1 -m 1 >iostat.log &
    ./db_bench --benchmarks="readwhilewriting,levelstats,stats" -use_direct_io_for_flush_and_compaction=true -use_direct_reads=true \
        -threads=1 \
        -histogram=1 --statistics \
        -key_size=16 -value_size=256 \
        -cache_size=$((500 * 1024 * 1024)) \
        –perf_level=2 \
        -reads=2000000 -num=100000000 \
        --report_interval_seconds=1 \
        -report_file="./report2.log" \
        -stats_interval_seconds=1 \
        -bloom_bits=10 \
        -target_file_size_base=$((8 * 1024 * 1024)) \
        -write_buffer_size=$((32 * 1024 * 1024)) \
        -benchmark_read_rate_limit=$((800)) \
        -benchmark_write_rate_limit=$((2 * (4000 * (16 + 256)))) \
        -db=/home/ubuntu/gp2_150g_1 -use_existing_db=true | tee mixgraph.log
    sleep 1
    pkill iostat
}

# -rate_limiter_bytes_per_sec=$((250 * 1024 * 1024)) \
# -rate_limiter_auto_tuned=true \

# $1 src $2 dst
move_file() {
    cp ./*.log $2
    # for file in test.log test2.log iostat.log mixgraph.log report2.log Monitor.log; do
    #     cp $1/$file $2
    # done
}

ulimit -n 655360

# make db_bench -j 8
# load_data
bench_tune
move_file /home/ubuntu/tests/plsdb/rocksdb_limiter_prefetcher /home/ubuntu/tests/plsdb/rocksdb_limiter_prefetcher/logs/tune/
make db_bench -j 8

load_data
bench_tune
move_file /home/ubuntu/tests/plsdb/rocksdb_limiter_prefetcher /home/ubuntu/tests/plsdb/rocksdb_limiter_prefetcher/logs/notune/


# load_data
# make db_bench -j 8
# bench_tune
# move_file /home/ubuntu/tests/plsdb/rocksdb_limiter_prefetcher /home/ubuntu/tests/plsdb/rocksdb_limiter_prefetcher/logs/tune/



# load_data
# bench_rocksdb 10 2250000
# move_file /home/ubuntu/tests/plsdb/rocksdb_limiter_prefetcher /home/ubuntu/tests/plsdb/rocksdb_limiter_prefetcher/logs/rocksdb/
# bench_rocksdb_autotune 10 2250000
# move_file /home/ubuntu/tests/plsdb/rocksdb_limiter_prefetcher /home/ubuntu/tests/plsdb/rocksdb_limiter_prefetcher/logs/rocksdb_autotune/
# make db_bench -j 8
# # load_data
# bench_rocksdb 10 2250000
# move_file /home/ubuntu/tests/plsdb/rocksdb_limiter_prefetcher /home/ubuntu/tests/plsdb/rocksdb_limiter_prefetcher/logs/plsdb/

# cd /home/ubuntu/tests/CruiseDB
# load_data
# bench_rocksdb 10 2250000
# move_file /home/ubuntu/tests/CruiseDB /home/ubuntu/tests/CruiseDB/logs/cruisedb/

# cd /home/ubuntu/tests/SILK-USENIXATC2019
# load_data
# bench_rocksdb 10 2250000
# move_file /home/ubuntu/tests/SILK-USENIXATC2019 /home/ubuntu/tests/SILK-USENIXATC2019/logs/silk/

# load_data
# bench_rocksdb 1 3000000
# move_file /home/ubuntu/tests/plsdb/rocksdb_limiter_prefetcher /home/ubuntu/tests/plsdb/rocksdb_limiter_prefetcher/logs/rocksdb_autotune/1
# bench_rocksdb 5 3000000
# move_file /home/ubuntu/tests/plsdb/rocksdb_limiter_prefetcher /home/ubuntu/tests/plsdb/rocksdb_limiter_prefetcher/logs/rocksdb_autotune/5

# bench_rocksdb 10 750000
# move_file /home/ubuntu/tests/plsdb/rocksdb_limiter_prefetcher /home/ubuntu/tests/plsdb/rocksdb_limiter_prefetcher/logs/rocksdb/

# bench_rocksdb 20 750000
# move_file /home/ubuntu/tests/plsdb/rocksdb_limiter_prefetcher /home/ubuntu/tests/plsdb/rocksdb_limiter_prefetcher/logs/rocksdb_autotune/20
