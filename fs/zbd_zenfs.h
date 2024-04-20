// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>
#if !defined(ROCKSDB_LITE) && defined(OS_LINUX)

#include <queue>
#include <errno.h>
#include <libzbd/zbd.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "metrics.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/io_status.h"

namespace ROCKSDB_NAMESPACE {
const int INF = 1e9;

const bool MYMODE = true; //true: Prophet false: LIZA
const int ZoneNumber = 100; 

//full compensation: 1 0 0
//full gc: 0 1 0
//gc with compensation: 1 1 3
const int ENABLE_PRECOMPACTION = 1; 
const double GC_THRESHOLD = 1;
const int ENABLE_LIMIT_LEVEL = 3;

const int T = 100;

//const uint64_t GC_START_LEVEL = 60; //micro test
//const uint64_t GC_STOP_LEVEL = 75;

const uint64_t GC_START_LEVEL = 20; //full test
const uint64_t GC_STOP_LEVEL = 45;

const int SHORT_THE = 2; //SHORT_THRESHOLD of level segragation
const int ENABLE_T_SLICE = 1; //ENABLE rounding
const int ENABLE_SHORT_WITH_TYPE0 = 50; //case2B threshold

const int MAX_LIFETIME = 1e9; //deprecate
const int MAX_DIFFTIME = INF; //deprecate
const int MULTI = 1;//deprecate
const int ENABLE_CAZA = 0;//deprecate
const int MODIFY_OFF = 0; //deprecate
const int ENABLE_CASE1 = 0; //deprecate
const int ENABLE_CASE2 = 0; //deprecate
const bool DISABLE_RESET = false; //deprecate
const int ENABLE_T_RANGE = 0; //1 means [-T, T] deprecate


const int CALC_RESET = 1; //default
const int K = 1; //gc top k default
const int MB = 1024 * 1024;
extern int reset_zone_num;



class ZonedBlockDevice;
class ZonedBlockDeviceBackend;
class ZoneSnapshot;
class ZenFSSnapshotOptions;

class ZoneList {
 private:
  void *data_;
  unsigned int zone_count_;

 public:
  ZoneList(void *data, unsigned int zone_count)
      : data_(data), zone_count_(zone_count){};
  void *GetData() { return data_; };
  unsigned int ZoneCount() { return zone_count_; };
  ~ZoneList() { free(data_); };
};

class Zone {
  ZonedBlockDevice *zbd_;
  ZonedBlockDeviceBackend *zbd_be_;
  std::atomic_bool busy_;

 public:
  explicit Zone(ZonedBlockDevice *zbd, ZonedBlockDeviceBackend *zbd_be,
                std::unique_ptr<ZoneList> &zones, unsigned int idx);
  uint64_t id;
  uint64_t start_;
  uint64_t capacity_; /* remaining capacity */
  uint64_t max_capacity_;
  uint64_t wp_;
  Env::WriteLifeTimeHint lifetime_;
  uint64_t min_lifetime;
  uint64_t max_lifetime;
  int lifetime_type; //0 top 1 upper
  int level;
  std::atomic<uint64_t> used_capacity_;
  std::vector<uint64_t> files_id;
  std::vector<uint64_t> lifetime_list;
  std::vector<uint64_t> prediction_lifetime_list;
  std::map<int, int> hint_num;

  IOStatus Reset();
  IOStatus Finish();
  IOStatus Close();

  IOStatus Append(char *data, uint32_t size);
  bool IsUsed();
  bool IsFull();
  bool IsEmpty();
  uint64_t GetZoneNr();
  uint64_t GetCapacityLeft();
  bool IsBusy() { return this->busy_.load(std::memory_order_relaxed); }
  bool Acquire() {
    bool expected = false;
    return this->busy_.compare_exchange_strong(expected, true,
                                               std::memory_order_acq_rel);
  }
  bool Release() {
    bool expected = true;
    return this->busy_.compare_exchange_strong(expected, false,
                                               std::memory_order_acq_rel);
  }

  void EncodeJson(std::ostream &json_stream);

  inline IOStatus CheckRelease();
};

class LogWriter {
public:
  LogWriter(Zone *zone) {
    zone_list.push(zone);
  };
  LogWriter() = default;
  // std::queue<Zone *> *get_zone_list() {
  //   return &zone_list;
  // };
  // bool try_append(uint64_t lifetime) {
  //   if(lifetime >= min_lifetime && lifetime <= max_lifetime) {
      
  //     return true;
  //   }
  //   return false;
  // };
  Zone *get_current_zone() {
    if(active_zone->capacity_ == 0) {
      zone_list.pop();
      if(zone_list.empty()) return nullptr;
      active_zone = zone_list.front();
    }
    return active_zone;
  };
private:
  Zone * active_zone;
  std::queue<Zone *> zone_list;
 
};

class ZonedBlockDeviceBackend {
 public:
  uint32_t block_sz_ = 0;
  uint64_t zone_sz_ = 0;
  uint32_t nr_zones_ = 0;

 public:
  virtual IOStatus Open(bool readonly, bool exclusive,
                        unsigned int *max_active_zones,
                        unsigned int *max_open_zones) = 0;

  virtual std::unique_ptr<ZoneList> ListZones() = 0;
  virtual IOStatus Reset(uint64_t start, bool *offline,
                         uint64_t *max_capacity) = 0;
  virtual IOStatus Finish(uint64_t start) = 0;
  virtual IOStatus Close(uint64_t start) = 0;
  virtual int Read(char *buf, int size, uint64_t pos, bool direct) = 0;
  virtual int Write(char *data, uint32_t size, uint64_t pos) = 0;
  virtual bool ZoneIsSwr(std::unique_ptr<ZoneList> &zones,
                         unsigned int idx) = 0;
  virtual bool ZoneIsOffline(std::unique_ptr<ZoneList> &zones,
                             unsigned int idx) = 0;
  virtual bool ZoneIsWritable(std::unique_ptr<ZoneList> &zones,
                              unsigned int idx) = 0;
  virtual bool ZoneIsActive(std::unique_ptr<ZoneList> &zones,
                            unsigned int idx) = 0;
  virtual bool ZoneIsOpen(std::unique_ptr<ZoneList> &zones,
                          unsigned int idx) = 0;
  virtual uint64_t ZoneStart(std::unique_ptr<ZoneList> &zones,
                             unsigned int idx) = 0;
  virtual uint64_t ZoneMaxCapacity(std::unique_ptr<ZoneList> &zones,
                                   unsigned int idx) = 0;
  virtual uint64_t ZoneWp(std::unique_ptr<ZoneList> &zones,
                          unsigned int idx) = 0;
  virtual std::string GetFilename() = 0;
  uint32_t GetBlockSize() { return block_sz_; };
  uint64_t GetZoneSize() { return zone_sz_; };
  uint32_t GetNrZones() { return nr_zones_; };
  virtual ~ZonedBlockDeviceBackend(){};
};

enum class ZbdBackendType {
  kBlockDev,
  kZoneFS,
};

class ZonedBlockDevice {
 private: 
  std::vector<Zone *> io_zones; 
  std::unique_ptr<ZonedBlockDeviceBackend> zbd_be_;
  std::vector<LogWriter *> log_writer_list;
  std::vector<Zone *> meta_zones;
  time_t start_time_;
  std::shared_ptr<Logger> logger_;
  uint32_t finish_threshold_ = 0;
  std::atomic<uint64_t> bytes_written_{0};
  std::atomic<uint64_t> gc_bytes_written_{0};

  std::atomic<long> active_io_zones_;
  std::atomic<long> open_io_zones_;
  /* Protects zone_resuorces_  condition variable, used
     for notifying changes in open_io_zones_ */
  std::mutex zone_resources_mtx_;
  
  std::condition_variable zone_resources_;
  std::mutex zone_deferred_status_mutex_;
  IOStatus zone_deferred_status_;

  std::condition_variable migrate_resource_;
  std::mutex migrate_zone_mtx_;
  std::atomic<bool> migrating_{false};

  unsigned int max_nr_active_io_zones_;
  unsigned int max_nr_open_io_zones_;

  std::shared_ptr<ZenFSMetrics> metrics_;

  void EncodeJsonZone(std::ostream &json_stream,
                      const std::vector<Zone *> zones);

 public:
  explicit ZonedBlockDevice(std::string path, ZbdBackendType backend,
                            std::shared_ptr<Logger> logger,
                            std::shared_ptr<ZenFSMetrics> metrics =
                                std::make_shared<NoZenFSMetrics>());
  virtual ~ZonedBlockDevice();
  void new_log_writer(Zone *zone) {
    log_writer_list.emplace_back(new LogWriter(zone));
  }
  std::vector<Zone *> &get_io_zones() {
    return io_zones;
  }
  bool remove_log_writer(LogWriter * log_writer) {
    uint32_t pos = -1;
    for(uint32_t i = 0; i < log_writer_list.size(); i++) {
      if(log_writer_list[i] == log_writer) {
        pos = i;
      }
    }
    if(pos == static_cast<uint32_t>(-1)) return false;
    log_writer_list.erase(log_writer_list.begin() + pos);
    delete log_writer;
    return true;
  }

  IOStatus Open(bool readonly, bool exclusive);

  Zone *GetIOZone(uint64_t offset);

  IOStatus AllocateIOZone(Env::WriteLifeTimeHint file_lifetime, IOType io_type, Zone **out_zone, uint64_t new_lifetime, int new_type, std::vector<uint64_t> overlap_zone_list, int level);
  IOStatus AllocateMetaZone(Zone **out_meta_zone);

  uint64_t GetFreeSpace();
  uint64_t GetUsedSpace();
  uint64_t GetReclaimableSpace();

  std::string GetFilename();
  uint32_t GetBlockSize();

  IOStatus ResetUnusedIOZones();
  IOStatus MyResetUnusedIOZones();
  IOStatus ResetTartetUnusedIOZones(uint64_t id);
  void LogZoneStats();
  void LogZoneUsage();
  void LogGarbageInfo();

  uint64_t GetZoneSize();
  uint32_t GetNrZones();
  std::vector<Zone *> GetMetaZones() { return meta_zones; }
  std::vector<Zone *> GetIOZones() { return io_zones; }

  void SetFinishTreshold(uint32_t threshold) { finish_threshold_ = threshold; }

  void PutOpenIOZoneToken();
  void PutActiveIOZoneToken();

  void EncodeJson(std::ostream &json_stream);

  void SetZoneDeferredStatus(IOStatus status);

  std::shared_ptr<ZenFSMetrics> GetMetrics() { return metrics_; }

  void GetZoneSnapshot(std::vector<ZoneSnapshot> &snapshot);

  int Read(char *buf, uint64_t offset, int n, bool direct);

  IOStatus ReleaseMigrateZone(Zone *zone);

  IOStatus TakeMigrateZone(Zone **out_zone, Env::WriteLifeTimeHint lifetime,
                           uint32_t min_capacity, uint64_t new_lifetime, int new_type);

  void AddBytesWritten(uint64_t written) { bytes_written_ += written; };
  void AddGCBytesWritten(uint64_t written) { gc_bytes_written_ += written; };
  uint64_t GetUserBytesWritten() {
    return bytes_written_.load() - gc_bytes_written_.load();
  };
  uint64_t GetTotalBytesWritten() { return bytes_written_.load(); };
 private:
  IOStatus GetZoneDeferredStatus();
  bool GetActiveIOZoneTokenIfAvailable();
  void WaitForOpenIOZoneToken(bool prioritized);
  IOStatus ApplyFinishThreshold();
  IOStatus FinishCheapestIOZone();
  void OpenNewZone(Zone **out_zone, Env::WriteLifeTimeHint file_lifetime, uint64_t new_lifetime, int new_type, int level);
  IOStatus GetBestOpenZoneMatch(Env::WriteLifeTimeHint file_lifetime,
                                unsigned int *best_diff_out, Zone **zone_out,
                                uint32_t min_capacity = 0);
  IOStatus GetBestOpenZoneMatch(uint64_t new_lifetime_, int new_type, Env::WriteLifeTimeHint file_lifetime,
                              unsigned int *best_diff_out, Zone **zone_out, int flag, int flag2, std::vector<uint64_t> overlap_list,
                              uint32_t min_capacity = 0);
  IOStatus AllocateEmptyZone(Zone **zone_out);
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && defined(OS_LINUX)
