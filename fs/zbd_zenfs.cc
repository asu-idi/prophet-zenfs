// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#if !defined(ROCKSDB_LITE) && !defined(OS_WIN)

#include "zbd_zenfs.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <libzbd/zbd.h>
#include <linux/blkzoned.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <unistd.h>

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "fs_zenfs.h"
#include "rocksdb/env.h"
#include "rocksdb/io_status.h"
#include "snapshot.h"
#include "zbdlib_zenfs.h"
#include "zonefs_zenfs.h"

#define KB (1024)
#define MB (1024 * KB)

/* Number of reserved zones for metadata
 * Two non-offline meta zones are needed to be able
 * to roll the metadata log safely. One extra
 * is allocated to cover for one zone going offline.
 */
#define ZENFS_META_ZONES (3)

/* Minimum of number of zones that makes sense */
#define ZENFS_MIN_ZONES (32)

namespace ROCKSDB_NAMESPACE {

static int cnt[111];
static int cnt_zone_hint[111];
/*
flag2: SHORT_THE 
flag: 

0: in [L, R]
1: x < [L, R]
2: [L, R] < x
3: type = 0 -> type 0 or type = 0 -> type 1
4: open new zone when get active zone == true
5: open new zone and finish a zone

*/
void add_allocation(int flag2, int flag, uint64_t lifetime, int new_type, Zone *zone) {
  cnt[flag]++;
  printf("allocation_type:flag2=%d flag=%d lifetime=%ld new_type=%d ", flag2, flag, lifetime, new_type);
  if(zone != nullptr)
    printf("zone_id=%ld zone_l=%ld zone_r=%ld zone_type=%d ", zone->id, zone->min_lifetime, zone->max_lifetime, zone->lifetime_type);
  for(int i = 0; i < 7; i++) printf("type%d=%d ", i, cnt[i]);
  printf("\n");
}
void add_allocation_off(int flag, Env::WriteLifeTimeHint lifetime, Zone *zone) {
  cnt[flag]++;
  printf("allocation_type=%d lifetime=%d ", flag, lifetime);
  if(zone != nullptr)
    printf("zone_id=%ld zone_l=%ld zone_r=%ld ", zone->id, zone->min_lifetime, zone->max_lifetime);
  for(int i = 0; i < 4; i++) printf("type%d=%d ", i, cnt[i]);
  printf("\n");
  if(MYMODE == false && flag == 3) {
    cnt_zone_hint[zone->lifetime_]++;
    for(int i = 0; i < 6; i++) {
      printf("zone_hint%d=%d\n", i, cnt_zone_hint[i]);
    }
  }
}


Zone::Zone(ZonedBlockDevice *zbd, ZonedBlockDeviceBackend *zbd_be,
           std::unique_ptr<ZoneList> &zones, unsigned int idx)
    : zbd_(zbd),
      zbd_be_(zbd_be),
      busy_(false),
      start_(zbd_be->ZoneStart(zones, idx)),
      max_capacity_(zbd_be->ZoneMaxCapacity(zones, idx)),
      wp_(zbd_be->ZoneWp(zones, idx)) {
  lifetime_ = Env::WLTH_NOT_SET;
  used_capacity_ = 0;
  capacity_ = 0;
  id = idx;
  if (zbd_be->ZoneIsWritable(zones, idx))
    capacity_ = max_capacity_ - (wp_ - start_);
}

bool Zone::IsUsed() { return (used_capacity_ > 0); }
uint64_t Zone::GetCapacityLeft() { return capacity_; }
bool Zone::IsFull() { return (capacity_ == 0); }
bool Zone::IsEmpty() { return (wp_ == start_); }
uint64_t Zone::GetZoneNr() { return start_ / zbd_->GetZoneSize(); }

void Zone::EncodeJson(std::ostream &json_stream) {
  json_stream << "{";
  json_stream << "\"start\":" << start_ << ",";
  json_stream << "\"capacity\":" << capacity_ << ",";
  json_stream << "\"max_capacity\":" << max_capacity_ << ",";
  json_stream << "\"wp\":" << wp_ << ",";
  json_stream << "\"lifetime\":" << lifetime_ << ",";
  json_stream << "\"used_capacity\":" << used_capacity_;
  json_stream << "}";
}

IOStatus Zone::Reset() {  

  printf("Zone Reset zone_id=%ld capacity=%ld\n", id, capacity_);
  bool offline;
  uint64_t max_capacity;

  assert(!IsUsed());
  assert(IsBusy());
  if(CALC_RESET) {
    write_size_calc += capacity_;
  }
  IOStatus ios = zbd_be_->Reset(start_, &offline, &max_capacity);
  if (ios != IOStatus::OK()) return ios;

  if (offline)
    capacity_ = 0;
  else
    max_capacity_ = capacity_ = max_capacity;

  files_id.clear();
  wp_ = start_;
  lifetime_ = Env::WLTH_NOT_SET;

  return IOStatus::OK();
}

IOStatus Zone::Finish() {
  assert(IsBusy());

  IOStatus ios = zbd_be_->Finish(start_);
  if (ios != IOStatus::OK()) return ios;
  if(CALC_RESET) {
    write_size_calc += capacity_;
  }
  capacity_ = 0;
  wp_ = start_ + zbd_->GetZoneSize();

  return IOStatus::OK();
}

IOStatus Zone::Close() {
  assert(IsBusy());

  if (!(IsEmpty() || IsFull())) {
    IOStatus ios = zbd_be_->Close(start_);
    if (ios != IOStatus::OK()) return ios;
  }

  return IOStatus::OK();
}

// 一个问题是Zone的append只传入了data，那么如何区分这个data属于哪个ZoneFile呢？
IOStatus Zone::Append(char *data, uint32_t size) {
  ZenFSMetricsLatencyGuard guard(zbd_->GetMetrics(), ZENFS_ZONE_WRITE_LATENCY,
                                 Env::Default());
  zbd_->GetMetrics()->ReportThroughput(ZENFS_ZONE_WRITE_THROUGHPUT, size);
  char *ptr = data;
  uint32_t left = size;
  int ret;

  if (capacity_ < size)
    return IOStatus::NoSpace("Not enough capacity for append");

  assert((size % zbd_->GetBlockSize()) == 0);

  while (left) {
    ret = zbd_be_->Write(ptr, left, wp_);
    if (ret < 0) {
      return IOStatus::IOError(strerror(errno));
    }

    ptr += ret;
    wp_ += ret;
    capacity_ -= ret;
    left -= ret;
    zbd_->AddBytesWritten(ret);
  }

  return IOStatus::OK();
}

inline IOStatus Zone::CheckRelease() {
  if (!Release()) {
    assert(false);
    return IOStatus::Corruption("Failed to unset busy flag of zone " +
                                std::to_string(GetZoneNr()));
  }

  return IOStatus::OK();
}

Zone *ZonedBlockDevice::GetIOZone(uint64_t offset) {
  for (const auto z : io_zones)
    if (z->start_ <= offset && offset < (z->start_ + zbd_be_->GetZoneSize()))
      return z;
  return nullptr;
}

ZonedBlockDevice::ZonedBlockDevice(std::string path, ZbdBackendType backend,
                                   std::shared_ptr<Logger> logger,
                                   std::shared_ptr<ZenFSMetrics> metrics)
    : logger_(logger), metrics_(metrics) {
  if (backend == ZbdBackendType::kBlockDev) {
    zbd_be_ = std::unique_ptr<ZbdlibBackend>(new ZbdlibBackend(path));
    Info(logger_, "New Zoned Block Device: %s", zbd_be_->GetFilename().c_str());
  } else if (backend == ZbdBackendType::kZoneFS) {
    zbd_be_ = std::unique_ptr<ZoneFsBackend>(new ZoneFsBackend(path));
    Info(logger_, "New zonefs backing: %s", zbd_be_->GetFilename().c_str());
  }
}

IOStatus ZonedBlockDevice::Open(bool readonly, bool exclusive) {
  std::unique_ptr<ZoneList> zone_rep;
  unsigned int max_nr_active_zones;
  unsigned int max_nr_open_zones;
  Status s;
  uint64_t i = 0;
  uint64_t m = 0;
  // Reserve one zone for metadata and another one for extent migration
  int reserved_zones = 2;

  if (!readonly && !exclusive)
    return IOStatus::InvalidArgument("Write opens must be exclusive");

  IOStatus ios = zbd_be_->Open(readonly, exclusive, &max_nr_active_zones,
                               &max_nr_open_zones);
  if (ios != IOStatus::OK()) return ios;

  if (zbd_be_->GetNrZones() < ZENFS_MIN_ZONES) {
    return IOStatus::NotSupported("To few zones on zoned backend (" +
                                  std::to_string(ZENFS_MIN_ZONES) +
                                  " required)");
  }

  if (max_nr_active_zones == 0)
    max_nr_active_io_zones_ = zbd_be_->GetNrZones();
  else
    max_nr_active_io_zones_ = max_nr_active_zones - reserved_zones;

  if (max_nr_open_zones == 0)
    max_nr_open_io_zones_ = zbd_be_->GetNrZones();
  else
    max_nr_open_io_zones_ = max_nr_open_zones - reserved_zones;

  Info(logger_, "Zone block device nr zones: %u max active: %u max open: %u \n",
       zbd_be_->GetNrZones(), max_nr_active_zones, max_nr_open_zones);

  zone_rep = zbd_be_->ListZones();
  if (zone_rep == nullptr || zone_rep->ZoneCount() != zbd_be_->GetNrZones()) {
    Error(logger_, "Failed to list zones");
    return IOStatus::IOError("Failed to list zones");
  }

  while (m < ZENFS_META_ZONES && i < zone_rep->ZoneCount()) {
    /* Only use sequential write required zones */
    if (zbd_be_->ZoneIsSwr(zone_rep, i)) {
      if (!zbd_be_->ZoneIsOffline(zone_rep, i)) {
        meta_zones.push_back(new Zone(this, zbd_be_.get(), zone_rep, i));
      }
      m++;
    }
    i++;
  }

  active_io_zones_ = 0;
  open_io_zones_ = 0;

  for (; i < zone_rep->ZoneCount(); i++) {
    /* Only use sequential write required zones */
    if (zbd_be_->ZoneIsSwr(zone_rep, i)) {
      if (!zbd_be_->ZoneIsOffline(zone_rep, i)) {
        Zone *newZone = new Zone(this, zbd_be_.get(), zone_rep, i);
        if (!newZone->Acquire()) {
          assert(false);
          return IOStatus::Corruption("Failed to set busy flag of zone " +
                                      std::to_string(newZone->GetZoneNr()));
        }
        io_zones.push_back(newZone);
        if (zbd_be_->ZoneIsActive(zone_rep, i)) {
          active_io_zones_++;
          if (zbd_be_->ZoneIsOpen(zone_rep, i)) {
            if (!readonly) {
              newZone->Close();
            }
          }
        }
        IOStatus status = newZone->CheckRelease();
        if (!status.ok()) {
          return status;
        }
      }
    }
  }

  start_time_ = time(NULL);

  return IOStatus::OK();
}

uint64_t ZonedBlockDevice::GetFreeSpace() {
  // printf("io_zones_number=%ld\n", io_zones.size());
  uint64_t free = 0;
  for (const auto z : io_zones) {
    free += z->capacity_;
    // printf("z->capacity=%ld\n", z->capacity_);
  }
  return free;
}

uint64_t ZonedBlockDevice::GetUsedSpace() {
  uint64_t used = 0;
  for (const auto z : io_zones) {
    used += z->used_capacity_;
  }
  return used;
}

uint64_t ZonedBlockDevice::GetReclaimableSpace() {
  uint64_t reclaimable = 0;
  for (const auto z : io_zones) {
    if (z->IsFull()) {
      reclaimable += (z->max_capacity_ - z->used_capacity_);
     // printf("GetReclaimableSpace id=%ld max_cap=%ld used=%ld reclaimable=%ld\n", z->id, z->max_capacity_, z->used_capacity_.load(), reclaimable);
    }
  }
  return reclaimable;
}

void ZonedBlockDevice::LogZoneStats() {
  uint64_t used_capacity = 0;
  uint64_t reclaimable_capacity = 0;
  uint64_t reclaimables_max_capacity = 0;
  uint64_t active = 0;

  for (const auto z : io_zones) {
    used_capacity += z->used_capacity_;

    if (z->used_capacity_) {
      reclaimable_capacity += z->max_capacity_ - z->used_capacity_;
      reclaimables_max_capacity += z->max_capacity_;
    }

    if (!(z->IsFull() || z->IsEmpty())) active++;
  }

  if (reclaimables_max_capacity == 0) reclaimables_max_capacity = 1;

  Info(logger_,
       "[Zonestats:time(s),used_cap(MB),reclaimable_cap(MB), "
       "avg_reclaimable(%%), active(#), active_zones(#), open_zones(#)] %ld "
       "%lu %lu %lu %lu %ld %ld\n",
       time(NULL) - start_time_, used_capacity / MB, reclaimable_capacity / MB,
       100 * reclaimable_capacity / reclaimables_max_capacity, active,
       active_io_zones_.load(), open_io_zones_.load());
}

void ZonedBlockDevice::LogZoneUsage() {
  for (const auto z : io_zones) {
    int64_t used = z->used_capacity_;

    if (used > 0) {
      Debug(logger_, "Zone 0x%lX used capacity: %ld bytes (%ld MB)\n",
            z->start_, used, used / MB);
    }
  }
}

// GC过程
void ZonedBlockDevice::LogGarbageInfo() {
  // Log zone garbage stats vector.
  //
  // The values in the vector represents how many zones with target garbage
  // percent. Garbage percent of each index: [0%, <10%, < 20%, ... <100%, 100%]
  // For example `[100, 1, 2, 3....]` means 100 zones are empty, 1 zone has less
  // than 10% garbage, 2 zones have  10% ~ 20% garbage ect.
  //
  // We don't need to lock io_zones since we only read data and we don't need
  // the result to be precise.
  int zone_gc_stat[12] = {0};
  for (auto z : io_zones) {  // 枚举所有的IO zone
    if (!z->Acquire()) {
      continue;
    }

    if (z->IsEmpty()) {
      zone_gc_stat[0]++;
      z->Release();
      continue;
    }

    double garbage_rate =
        double(z->wp_ - z->start_ - z->used_capacity_) /
        z->max_capacity_;  // 这个rate越高，表示未使用的空间越多
    if (garbage_rate < 0) {
      printf(
          "ERROR:garbage_rate<=0 zone_id=%ld wp=%ld start=%ld "
          "used_capacity=%ld max_capacity=%ld\n",
          z->id, z->wp_, z->start_, z->used_capacity_.load(), z->max_capacity_);
    }
    assert(garbage_rate >= 0);
    int idx = int((garbage_rate + 0.1) * 10);
    zone_gc_stat[idx]++;

    z->Release();
  }

  std::stringstream ss;
  ss << "Zone Garbage Stats: [";
  for (int i = 0; i < 12; i++) {
    ss << zone_gc_stat[i] << " ";
  }
  ss << "]";
  Info(logger_, "%s", ss.str().data());
}

ZonedBlockDevice::~ZonedBlockDevice() {
  for (const auto z : meta_zones) {
    delete z;
  }

  for (const auto z : io_zones) {
    delete z;
  }
}

#define LIFETIME_DIFF_NOT_GOOD (100)
#define LIFETIME_DIFF_COULD_BE_WORSE (50)

unsigned int GetLifeTimeDiff(Env::WriteLifeTimeHint zone_lifetime,
                             Env::WriteLifeTimeHint file_lifetime) {
  assert(file_lifetime <= Env::WLTH_EXTREME);

  if ((file_lifetime == Env::WLTH_NOT_SET) ||
      (file_lifetime == Env::WLTH_NONE)) {
    if (file_lifetime == zone_lifetime) {
      return 0;
    } else {
      return LIFETIME_DIFF_NOT_GOOD;
    }
  }

  if (zone_lifetime > file_lifetime) return zone_lifetime - file_lifetime;
  if (zone_lifetime == file_lifetime) return MODIFY_OFF ? 0 : LIFETIME_DIFF_COULD_BE_WORSE;

  return LIFETIME_DIFF_NOT_GOOD;
}

IOStatus ZonedBlockDevice::AllocateMetaZone(Zone **out_meta_zone) {
  assert(out_meta_zone);
  *out_meta_zone = nullptr;
  ZenFSMetricsLatencyGuard guard(metrics_, ZENFS_META_ALLOC_LATENCY,
                                 Env::Default());
  metrics_->ReportQPS(ZENFS_META_ALLOC_QPS, 1);

  for (const auto z : meta_zones) {
    /* If the zone is not used, reset and use it */
    if (z->Acquire()) {
      if (!z->IsUsed()) {
        if (!z->IsEmpty() && !z->Reset().ok()) {
          Warn(logger_, "Failed resetting zone!");
          IOStatus status = z->CheckRelease();
          if (!status.ok()) return status;
          continue;
        }
        *out_meta_zone = z;
        return IOStatus::OK();
      }
    }
  }
  assert(true);
  Error(logger_, "Out of metadata zones, we should go to read only now.");
  return IOStatus::NoSpace("Out of metadata zones");
}

// 注意这个api
IOStatus ZonedBlockDevice::ResetUnusedIOZones() {
  if (DISABLE_RESET == true) return IOStatus::OK();
  for (const auto z : io_zones) {
    if (z->Acquire()) {
      if (!z->IsEmpty() && !z->IsUsed()) {  // used = 0
        printf("Reset zone_id=%ld capacity=%ld used_capacity=%ld HINT=%d level=%d type=%d L=%ld R=%ld\n", 
        z->id, z->capacity_, z->used_capacity_.load(), z->lifetime_, z->level, z->lifetime_type, z->min_lifetime, z->max_lifetime);
        bool full = z->IsFull();
        IOStatus reset_status = z->Reset();
        reset_zone_num++;
        z->prediction_lifetime_list.clear();
        z->lifetime_list.clear();
        z->hint_num.clear();
        IOStatus release_status = z->CheckRelease();
        if (!reset_status.ok()) return reset_status;
        if (!release_status.ok()) return release_status;
        if (!full) PutActiveIOZoneToken();
      } else {
        IOStatus release_status = z->CheckRelease();
        if (!release_status.ok()) return release_status;
      }
    }
  }
  return IOStatus::OK();
}

IOStatus ZonedBlockDevice::MyResetUnusedIOZones() {
  for (const auto z : io_zones) {
    if (z->Acquire()) {
      if (!z->IsEmpty() && !z->IsUsed()) {  // zone is empty
        printf("Reset zone_id=%ld capacity=%ld used_capacity=%ld HINT=%d level=%d type=%d L=%ld R=%ld\n", 
        z->id, z->capacity_, z->used_capacity_.load(), z->lifetime_, z->level, z->lifetime_type, z->min_lifetime, z->max_lifetime);
        bool full = z->IsFull();
        IOStatus reset_status = z->Reset();
        reset_zone_num++;
        z->prediction_lifetime_list.clear();
        z->lifetime_list.clear();
        z->hint_num.clear();
        IOStatus release_status = z->CheckRelease();
        if (!reset_status.ok()) return reset_status;
        if (!release_status.ok()) return release_status;
        if (!full) PutActiveIOZoneToken();
      } else {
        IOStatus release_status = z->CheckRelease();
        if (!release_status.ok()) return release_status;
      }
    }
  }
  return IOStatus::OK();
}

IOStatus ZonedBlockDevice::ResetTartetUnusedIOZones(uint64_t id) {
  for (const auto z : io_zones) {
    if (z->Acquire()) {
      if (!z->IsEmpty() && !z->IsUsed() && z->id == id) {
        printf("Reset zone_id=%ld padding=%ld start=%ld max_capacity=%ld wp=%ld is_empty()=%d capacity=%ld used_capacity=%ld HINT=%d L=%ld R=%ld\n", 
        z->id, z->start_ + z->max_capacity_ - z-> wp_, z->start_, z->max_capacity_, z-> wp_, z->IsEmpty(), z->capacity_, z->used_capacity_.load(), z->lifetime_, z->min_lifetime, z->max_lifetime);
        bool full = z->IsFull();
        IOStatus reset_status = z->Reset();
        z->prediction_lifetime_list.clear();
        z->lifetime_list.clear();
        z->hint_num.clear();
        reset_zone_num++;
        IOStatus release_status = z->CheckRelease();
        if (!reset_status.ok()) return reset_status;
        if (!release_status.ok()) return release_status;
        if (!full) PutActiveIOZoneToken();
      } else {
        IOStatus release_status = z->CheckRelease();
        if(z->id == id) {
          printf("ResetTargetIOZoneFail id=%ld IsEmpty()=%d IsUsed=%ld\n", id, z->IsEmpty(), z->used_capacity_.load());
        }
        if (!release_status.ok()) return release_status;
      }
    }
  }

  return IOStatus::OK();
}

void ZonedBlockDevice::WaitForOpenIOZoneToken(bool prioritized) {
  long allocator_open_limit;

  /* Avoid non-priortized allocators from starving prioritized ones */
  if (prioritized) {
    allocator_open_limit = max_nr_open_io_zones_;
  } else {
    allocator_open_limit = max_nr_open_io_zones_ - 1;
  }

  /* Wait for an open IO Zone token - after this function returns
   * the caller is allowed to write to a closed zone. The callee
   * is responsible for calling a PutOpenIOZoneToken to return the resource
   */
  std::unique_lock<std::mutex> lk(zone_resources_mtx_);
  zone_resources_.wait(lk, [this, allocator_open_limit] {
    if (open_io_zones_.load() < allocator_open_limit) {
      open_io_zones_++;
      return true;
    } else {
      return false;
    }
  });
}

bool ZonedBlockDevice::GetActiveIOZoneTokenIfAvailable() {
  /* Grap an active IO Zone token if available - after this function returns
   * the caller is allowed to write to a closed zone. The callee
   * is responsible for calling a PutActiveIOZoneToken to return the resource
   */
  std::unique_lock<std::mutex> lk(zone_resources_mtx_);
  if (active_io_zones_.load() < max_nr_active_io_zones_) {
    active_io_zones_++;
    return true;
  }
  return false;
}

void ZonedBlockDevice::PutOpenIOZoneToken() {
  {
    std::unique_lock<std::mutex> lk(zone_resources_mtx_);
    open_io_zones_--;
  }
  zone_resources_.notify_one();
}

void ZonedBlockDevice::PutActiveIOZoneToken() {
  {
    std::unique_lock<std::mutex> lk(zone_resources_mtx_);
    active_io_zones_--;
  }
  zone_resources_.notify_one();
}

IOStatus ZonedBlockDevice::ApplyFinishThreshold() {
  IOStatus s;

  if (finish_threshold_ == 0) return IOStatus::OK();

  for (const auto z : io_zones) {
    if (z->Acquire()) {
      bool within_finish_threshold =
          z->capacity_ < (z->max_capacity_ * finish_threshold_ / 100);
      if (!(z->IsEmpty() || z->IsFull()) && within_finish_threshold) {
        /* If there is less than finish_threshold_% remaining capacity in a
         * non-open-zone, finish the zone */
        s = z->Finish();
        if (!s.ok()) {
          z->Release();
          Debug(logger_, "Failed finishing zone");
          return s;
        }
        s = z->CheckRelease();
        if (!s.ok()) return s;
        PutActiveIOZoneToken();
      } else {
        s = z->CheckRelease();
        if (!s.ok()) return s;
      }
    }
  }

  return IOStatus::OK();
}

IOStatus ZonedBlockDevice::FinishCheapestIOZone() {
  IOStatus s;
  Zone *finish_victim = nullptr;

  for (const auto z : io_zones) {
    if (z->Acquire()) {
      if (z->IsEmpty() || z->IsFull()) {
        s = z->CheckRelease();
        if (!s.ok()) return s;
        continue;
      }
      if (finish_victim == nullptr) {
        finish_victim = z;
        continue;
      }
      if (finish_victim->capacity_ > z->capacity_) {
        s = finish_victim->CheckRelease();
        if (!s.ok()) return s;
        finish_victim = z;
      } else {
        s = z->CheckRelease();
        if (!s.ok()) return s;
      }
    }
  }

  // If all non-busy zones are empty or full, we should return success.
  if (finish_victim == nullptr) {
    Info(logger_, "All non-busy zones are empty or full, skip.");
    return IOStatus::OK();
  }
  printf("finish_victim zone_id=%ld zone_capacity=%ld used=%ld\n", finish_victim->id, finish_victim->capacity_, finish_victim->used_capacity_.load());
  s = finish_victim->Finish();
  IOStatus release_status = finish_victim->CheckRelease();

  if (s.ok()) {
    PutActiveIOZoneToken();
  }

  if (!release_status.ok()) {
    return release_status;
  }

  return s;
}

IOStatus ZonedBlockDevice::GetBestOpenZoneMatch(
    Env::WriteLifeTimeHint file_lifetime, unsigned int *best_diff_out,
    Zone **zone_out, uint32_t min_capacity) {
  unsigned int best_diff = LIFETIME_DIFF_NOT_GOOD;
  Zone *allocated_zone = nullptr;
  IOStatus s;

  for (const auto z : io_zones) {  // 枚举所有的IO zone
    if (z->Acquire()) {            // 获得锁
      if ((z->used_capacity_ > 0) && !z->IsFull() &&
          z->capacity_ >= min_capacity) {  // 如果说zone仍然有剩余空间
        unsigned int diff = GetLifeTimeDiff(z->lifetime_, file_lifetime);
         printf(
              "GetBestOpenZoneMatch Off zone_id=%ld "
              "min_lifetime=%ld max_lifetime=%ld zone_hint=%d zone_type=%d global_clock=%d\n",
              z->id, z->min_lifetime, z->max_lifetime, z->lifetime_, z->lifetime_type,
              global_clock);
        if (diff <= best_diff) {  // 如果要比best_diff小
          if (allocated_zone !=
              nullptr) {  // 如果之前已经有过allocate_zone allocated_zone
            s = allocated_zone->CheckRelease();  // check s是否能释放
            if (!s.ok()) {
              IOStatus s_ = z->CheckRelease();
              if (!s_.ok()) return s_;
              return s;
            }
          }
          allocated_zone = z;  // 更新allocated_zone
          best_diff = diff;
        } else {
          s = z->CheckRelease();
          if (!s.ok()) return s;
        }
      } else {
        s = z->CheckRelease();
        if (!s.ok()) return s;
      }
    }
  }

  *best_diff_out = best_diff;
  *zone_out = allocated_zone;

  return IOStatus::OK();
}

int global_clock = 0;
// Allocate只需要传入一个file_lifetime即可
IOStatus ZonedBlockDevice::GetBestOpenZoneMatch(
    uint64_t new_lifetime_, int new_type, Env::WriteLifeTimeHint file_lifetime,
    unsigned int *best_diff_out, Zone **zone_out, int flag, int flag2, std::vector<uint64_t> overlap_zone_list,
    uint32_t min_capacity) {
  unsigned int best_diff = LIFETIME_DIFF_NOT_GOOD;
  Zone *allocated_zone = nullptr;
  IOStatus s;
  uint64_t mx = INF;  // dis最小的zone
  if(new_lifetime_ == 0) {
    new_type = ((SHORT_THE == -1) ? 1 : 0);
  }
    for (const auto z : io_zones) {
      if(z->wp_ != z->start_) {
        printf("zone test zone_id=%ld zone_cap=%ld valid=%ld type=%d min_lifetime=%ld max_lifetime=%ld is_busy=%d\n", z->id, z->capacity_ / MB, z->used_capacity_.load() / MB, z->lifetime_type, z->min_lifetime, z->max_lifetime, z->IsBusy());
      }
      
      if (z->Acquire()) {
        if ((z->used_capacity_ > 0) && !z->IsFull() &&
            z->capacity_ >= min_capacity) {
          //new_type: file type
          //lifetime_type: zone type;
          if( (new_type == 0 && z->lifetime_type == 0 && flag2 == 0) 
              ||
                (
                ((new_type == 0 && z->lifetime_type == 1 && flag2 == 1) || (new_type == 1 && z->lifetime_type == 1 && flag2 == 1)) 
                && 
                (
                  (flag == 0 && (new_lifetime_ >= z->min_lifetime) && (new_lifetime_ <= z->max_lifetime)) ||
                  (flag == 1 && (new_lifetime_  < z->min_lifetime) && (z->min_lifetime - new_lifetime_ < mx)) || 
                  (flag == 2 && (new_lifetime_  > z->max_lifetime) && (new_lifetime_ - z->max_lifetime < mx) && (new_lifetime_ - z->max_lifetime <= MAX_DIFFTIME))
                )
                )
          )
          {
              printf(
              "GetBestOpenZoneMatch Normal zone_id=%ld cap=%ld new_lifetime_=%ld new_type=%d "
              "min_lifetime=%ld max_lifetime=%ld zone_type=%d global_clock=%d flag=%d flag2=%d overlap_list.size()=%ld\n",
              z->id, z->capacity_ / MB, new_lifetime_, new_type, z->min_lifetime, z->max_lifetime, z->lifetime_type,
              global_clock, flag, flag2, overlap_zone_list.size());
            if(flag == 1 && (new_lifetime_ < z->min_lifetime)) 
              mx = z->min_lifetime - new_lifetime_;
            if(flag == 2 && (new_lifetime_ > z->max_lifetime))
              mx = new_lifetime_ - z->max_lifetime;
            if (allocated_zone != nullptr) {  // flag == 1 need to find the maximal max_lifetime
              s = allocated_zone->CheckRelease();
              if (!s.ok()) {
                printf("InRangeButCheckRelease Fail\n");
                IOStatus s_ = z->CheckRelease();
                if (!s_.ok()) return s_;
                return s;
              }
            }
            allocated_zone = z;
            best_diff = 0;  // 把best_diff赋值为一个较小的值
            // if(flag == 0) break; //flag == 0 need to find the first valid zone
          } else {
            s = z->CheckRelease();
            if (!s.ok()) return s;
          }

        } else {
          s = z->CheckRelease();
          if (!s.ok()) return s;
        }
      }
    }
   *best_diff_out = best_diff;
  *zone_out = allocated_zone;
   // if(overlap_zone_list.size() != 0 && ENABLE_CAZA) {
  //   for (const auto z : io_zones) {
  //     if(find(overlap_zone_list.begin(), overlap_zone_list.end(), z->id) == overlap_zone_list.end()) continue; 
  //     if (z->Acquire()) {
  //       if ((z->used_capacity_ > 0) && !z->IsFull() &&
  //           z->capacity_ >= min_capacity) {
  //         printf(
  //             "GetBestOpenZoneMatch Overlap zone_id=%ld new_lifetime_=%ld file_hint=%d new_type=%d "
  //             "min_lifetime=%ld max_lifetime=%ld zone_type=%d global_clock=%d flag=%d flag2=%d overlap_list.size()=%ld\n",
  //             z->id, new_lifetime_, file_lifetime, new_type, z->min_lifetime, z->max_lifetime, z->lifetime_type,
  //             global_clock, flag, flag2, overlap_zone_list.size());
  //           if (allocated_zone != nullptr) {  // flag == 1 need to find the maximal max_lifetime
  //             s = allocated_zone->CheckRelease();
  //             if (!s.ok()) {
  //               printf("InRangeButCheckRelease Fail\n");
  //               IOStatus s_ = z->CheckRelease();
  //               if (!s_.ok()) return s_;
  //               return s;
  //             }
  //           }
  //           allocated_zone = z;
  //           best_diff = 0;  // 把best_diff赋值为一个较小的值
  //       } else {
  //           s = z->CheckRelease();
  //           if (!s.ok()) return s;
  //       }
  //     }
  //   }
  // }
  // else {
 
  //}

  return IOStatus::OK();
}


extern int allocated_zone_num;


// 当Allocate IO zone失效的时候调用此函数来申请一个新的zone
IOStatus ZonedBlockDevice::AllocateEmptyZone(Zone **zone_out) {
  IOStatus s;
  Zone *allocated_zone = nullptr;
  for (const auto z : io_zones) {
    if (z->Acquire()) {
      if (z->IsEmpty()) {
        allocated_zone = z;
        break;
      } else {
        s = z->CheckRelease();
        if (!s.ok()) return s;
      }
    }
  }
  *zone_out = allocated_zone;
  if(allocated_zone != nullptr) {
    allocated_zone_num++;
  }
  new_log_writer(*zone_out);
  printf("io_zones number = %ld and zone_out = %d\n", io_zones.size(),
         zone_out == nullptr ? 0 : 1);
  return IOStatus::OK();
}

int ZonedBlockDevice::Read(char *buf, uint64_t offset, int n, bool direct) {
  int ret = 0;
  int left = n;
  int r = -1;

  while (left) {
    r = zbd_be_->Read(buf, left, offset, direct);
    if (r <= 0) {
      if (r == -1 && errno == EINTR) {
        continue;
      }
      break;
    }
    ret += r;
    buf += r;
    left -= r;
    offset += r;
  }

  if (r < 0) return r;
  return ret;
}

IOStatus ZonedBlockDevice::ReleaseMigrateZone(Zone *zone) {
  IOStatus s = IOStatus::OK();
  {
    std::unique_lock<std::mutex> lock(migrate_zone_mtx_);
    migrating_ = false;
    if (zone != nullptr) {
      s = zone->CheckRelease();
      Info(logger_, "ReleaseMigrateZone: %lu", zone->start_);
    }
  }
  migrate_resource_.notify_one();
  return s;
}

IOStatus ZonedBlockDevice::TakeMigrateZone(Zone **out_zone,
                                           Env::WriteLifeTimeHint file_lifetime,
                                           uint32_t min_capacity, uint64_t new_lifetime, int new_type) {
  std::unique_lock<std::mutex> lock(migrate_zone_mtx_);
  migrate_resource_.wait(lock, [this] { return !migrating_; });

  migrating_ = true;

  unsigned int best_diff = LIFETIME_DIFF_NOT_GOOD;
  Zone *allocated_zone = nullptr;
  IOStatus s;
  if (MYMODE == true) {
         //level segragation
        s = GetBestOpenZoneMatch(new_lifetime, new_type, file_lifetime, &best_diff,
                              &allocated_zone, 0, 0, std::vector<uint64_t>{}, min_capacity);
        if(allocated_zone == nullptr) //L <= x <= R
          s = GetBestOpenZoneMatch(new_lifetime, new_type, file_lifetime, &best_diff,
                                  &allocated_zone, 0, 1, std::vector<uint64_t>{}, min_capacity);
        if (allocated_zone == nullptr)  //x < L
          s = GetBestOpenZoneMatch(new_lifetime, new_type, file_lifetime, &best_diff,
                                    &allocated_zone, 1, 1, std::vector<uint64_t>{}, min_capacity);
        if (allocated_zone == nullptr) //R < x
          s = GetBestOpenZoneMatch(new_lifetime, new_type, file_lifetime, &best_diff,
                                      &allocated_zone, 2, 1, std::vector<uint64_t>{}, min_capacity);
    *out_zone = allocated_zone;
   
  } else if(MYMODE == false) {
      s = GetBestOpenZoneMatch(file_lifetime, &best_diff, out_zone, min_capacity);
  }
  if (s.ok() && (*out_zone) != nullptr) {
     printf("GC Migrate Begin new_lifetime=%ld new_type=%d zone_id=%ld zone_type=%d min_lifetime=%ld max_lifetime=%ld\n", new_lifetime, new_type, (*out_zone)->id, (*out_zone)->lifetime_type, (*out_zone)->min_lifetime, (*out_zone)->max_lifetime);
    Info(logger_, "TakeMigrateZone: %lu", (*out_zone)->start_);
  } else {
    migrating_ = false;
    printf("ERROR GC fail new_lifetime=%ld new_type=%d min_capactiy=%d\n", new_lifetime, new_type, min_capacity);
  } 

  return s;
}

void ZonedBlockDevice::OpenNewZone(Zone **tmp_zone, Env::WriteLifeTimeHint file_lifetime, uint64_t new_lifetime, int new_type, int level) {
    const long long MAX = 1e9;
    assert(allocated_zone->IsBusy());
    Zone *allocated_zone = *tmp_zone;
    allocated_zone->lifetime_ = file_lifetime;
    if (new_lifetime > MAX) new_lifetime = 0;
    //allocated_zone->min_lifetime = std::max(static_cast<uint64_t>(0), new_lifetime - T);
    if(new_lifetime == 0) new_type = ((SHORT_THE == -1) ? 1 : 0);
    allocated_zone->lifetime_type = new_type;
    allocated_zone->level = level;
    //if(new_lifetime < T)

    if(ENABLE_T_SLICE) {
      allocated_zone->min_lifetime = new_lifetime / T * T;
      allocated_zone->max_lifetime = (new_lifetime / T + 1) * T - 1;
    } else {
      if(ENABLE_T_RANGE) {
        allocated_zone->min_lifetime = (new_lifetime < T ? 0: new_lifetime - T);
      } else {
        allocated_zone->min_lifetime = new_lifetime;
      }
      int base = T;
      for(int i = 1; i <= level - 3; i++) base = base * MULTI;
      allocated_zone->max_lifetime = new_lifetime + base;
    }


    printf("OpenNewZone zone_id=%ld l=%ld r=%ld HINT=%d new_type=%d \n", allocated_zone->id, allocated_zone->min_lifetime, allocated_zone->max_lifetime, file_lifetime, new_type);
    
    allocated_zone->hint_num[file_lifetime]++;
}
IOStatus ZonedBlockDevice::AllocateIOZone(Env::WriteLifeTimeHint file_lifetime,
                                          IOType io_type, Zone **out_zone,
                                          uint64_t new_lifetime, int new_type, std::vector<uint64_t> overlap_zone_list, int level) {
  
  printf("AllocateIOZone::Before t_id=%d deletion_timeitZ=%ld new_type=%d active=%ld max_open_zone=%d\n", gettid(),new_lifetime, new_type, active_io_zones_.load(), max_nr_active_io_zones_);
  
  Zone *allocated_zone = nullptr;
  unsigned int best_diff = LIFETIME_DIFF_NOT_GOOD;
  int new_zone = 0;
  IOStatus s;

  auto tag = ZENFS_WAL_IO_ALLOC_LATENCY;
  if (io_type != IOType::kWAL) {
    // L0 flushes have lifetime MEDIUM
    if (file_lifetime == Env::WLTH_MEDIUM) {
      tag = ZENFS_L0_IO_ALLOC_LATENCY;
    } else {
      tag = ZENFS_NON_WAL_IO_ALLOC_LATENCY;
    }
  }

  ZenFSMetricsLatencyGuard guard(metrics_, tag, Env::Default());
  metrics_->ReportQPS(ZENFS_IO_ALLOC_QPS, 1);

  // Check if a deferred IO error was set
  s = GetZoneDeferredStatus();
  if (!s.ok()) {
    return s;
  }

  if (io_type != IOType::kWAL) {
    s = ApplyFinishThreshold();
    if (!s.ok()) {
      return s;
    }
  }

  WaitForOpenIOZoneToken(io_type == IOType::kWAL);

  /* Try to fill an already open zone(with the best life time diff) */
  if (MYMODE == true) {
        //level segragation
        if(new_type == 0) {
          s = GetBestOpenZoneMatch(new_lifetime, new_type, file_lifetime, &best_diff,
                              &allocated_zone, 0, 0, std::vector<uint64_t>{});
        }
        
        if(allocated_zone == nullptr) {
          //L <= x <= R
          s = GetBestOpenZoneMatch(new_lifetime, new_type, file_lifetime, &best_diff,
                                  &allocated_zone, 0, 1, std::vector<uint64_t>{});

          if (allocated_zone == nullptr) {  // try again, find the

            if((max_nr_active_io_zones_ - active_io_zones_.load() >= 3) && GetActiveIOZoneTokenIfAvailable()) {
              printf("GetBestOpenZone when open active=%ld max_open_zone=%d\n", active_io_zones_.load(), max_nr_active_io_zones_);
              s = AllocateEmptyZone(&allocated_zone);
              new_zone = true;
              if (!s.ok()) {
                PutActiveIOZoneToken();
                PutOpenIOZoneToken();
                return s;
              }
              if (allocated_zone != nullptr) {
                OpenNewZone(&allocated_zone, file_lifetime, new_lifetime, new_type, level);
                add_allocation(1, 4, new_lifetime, new_type,allocated_zone);
              } else {
                PutActiveIOZoneToken();
              }
            } else {
              s = GetBestOpenZoneMatch(new_lifetime, new_type, file_lifetime, &best_diff,
                                      &allocated_zone, 1, 1, std::vector<uint64_t>{});
            
              if (allocated_zone == nullptr) {  // try again, find the
                s = GetBestOpenZoneMatch(new_lifetime, new_type, file_lifetime, &best_diff,
                                        &allocated_zone, 2, 1, std::vector<uint64_t>{});
                if(allocated_zone != nullptr) {
                  add_allocation(1, 2, new_lifetime, new_type, allocated_zone);
                }
              } else {
                add_allocation(1, 1, new_lifetime, new_type, allocated_zone);
              }
            }
          } else {
            add_allocation(1, 0, new_lifetime, new_type, allocated_zone);
          }
        } else if(allocated_zone != nullptr) {
          add_allocation(0, 3, new_lifetime, new_type,allocated_zone);
        }
      
 
 
    if(allocated_zone != nullptr) {
      best_diff = 0;
    }
 
  } else if(MYMODE == false) {
    s = GetBestOpenZoneMatch(file_lifetime, &best_diff, &allocated_zone, 0);
    if(allocated_zone != nullptr) {
       add_allocation_off(0, file_lifetime, allocated_zone);
       allocated_zone->hint_num[file_lifetime]++;
    }
  }

  if (!s.ok()) {
    PutOpenIOZoneToken();
    return s;
  }

  if (best_diff >= LIFETIME_DIFF_COULD_BE_WORSE) {

    bool got_token = GetActiveIOZoneTokenIfAvailable();

    /* If we did not get a token, try to use the best match, even if the life
     * time diff not good but a better choice than to finish an existing zone
     * and open a new one
     */
    if (allocated_zone != nullptr) {
      if (!got_token && best_diff == LIFETIME_DIFF_COULD_BE_WORSE) {
        Debug(logger_,
              "Allocator: avoided a finish by relaxing lifetime diff "
              "requirement\n");
      } else {
        s = allocated_zone->CheckRelease();
        if (!s.ok()) {
          PutOpenIOZoneToken();
          if (got_token) PutActiveIOZoneToken();
          return s;
        }
        allocated_zone = nullptr;
      }
    }

    /* If we haven't found an open zone to fill, open a new zone */
    if (allocated_zone == nullptr) {
      /* We have to make sure we can open an empty zone */
      printf("allocated_zone == nulptr But we don't need it best_diff=%d active=%ld max_open_zone=%d\n", best_diff, active_io_zones_.load(), max_nr_active_io_zones_);
      while (!got_token && !GetActiveIOZoneTokenIfAvailable()) {

        s = FinishCheapestIOZone();
        if (!s.ok()) {
          PutOpenIOZoneToken();
          return s;
        }
      }

      s = AllocateEmptyZone(&allocated_zone);
      new_zone = true;
      if (!s.ok()) {
        PutActiveIOZoneToken();
        PutOpenIOZoneToken();
        return s;
      }

      if (allocated_zone != nullptr) {
        OpenNewZone(&allocated_zone, file_lifetime, new_lifetime, new_type, level);
        add_allocation(1, 5, new_lifetime, new_type,allocated_zone);
        add_allocation_off(5, file_lifetime, allocated_zone);
      } else {
        PutActiveIOZoneToken();
      }
    }
  }

  if (allocated_zone) {
    assert(allocated_zone->IsBusy());
    Debug(logger_,
          "Allocating zone(new=%d) start: 0x%lx wp: 0x%lx lt: %d file lt: %d\n",
          new_zone, allocated_zone->start_, allocated_zone->wp_,
          allocated_zone->lifetime_, file_lifetime);
  } else {
    PutOpenIOZoneToken();
  }

  if (io_type != IOType::kWAL) {
    LogZoneStats();
  }

  *out_zone = allocated_zone;
  if (allocated_zone != nullptr)
    allocated_zone->prediction_lifetime_list.emplace_back(new_lifetime);

  metrics_->ReportGeneral(ZENFS_OPEN_ZONES_COUNT, open_io_zones_);
  metrics_->ReportGeneral(ZENFS_ACTIVE_ZONES_COUNT, active_io_zones_);

  return IOStatus::OK();
}

std::string ZonedBlockDevice::GetFilename() { return zbd_be_->GetFilename(); }

uint32_t ZonedBlockDevice::GetBlockSize() { return zbd_be_->GetBlockSize(); }

uint64_t ZonedBlockDevice::GetZoneSize() { return zbd_be_->GetZoneSize(); }

uint32_t ZonedBlockDevice::GetNrZones() { return zbd_be_->GetNrZones(); }

void ZonedBlockDevice::EncodeJsonZone(std::ostream &json_stream,
                                      const std::vector<Zone *> zones) {
  bool first_element = true;
  json_stream << "[";
  for (Zone *zone : zones) {
    if (first_element) {
      first_element = false;
    } else {
      json_stream << ",";
    }
    zone->EncodeJson(json_stream);
  }

  json_stream << "]";
}

void ZonedBlockDevice::EncodeJson(std::ostream &json_stream) {
  json_stream << "{";
  json_stream << "\"meta\":";
  EncodeJsonZone(json_stream, meta_zones);
  json_stream << ",\"io\":";
  EncodeJsonZone(json_stream, io_zones);
  json_stream << "}";
}

IOStatus ZonedBlockDevice::GetZoneDeferredStatus() {
  std::lock_guard<std::mutex> lock(zone_deferred_status_mutex_);
  return zone_deferred_status_;
}

void ZonedBlockDevice::SetZoneDeferredStatus(IOStatus status) {
  std::lock_guard<std::mutex> lk(zone_deferred_status_mutex_);
  if (!zone_deferred_status_.ok()) {
    zone_deferred_status_ = status;
  }
}

void ZonedBlockDevice::GetZoneSnapshot(std::vector<ZoneSnapshot> &snapshot) {
  for (auto *zone : io_zones) {
    snapshot.emplace_back(*zone);
    // printf("zone_information zone_id=%ld zone_capacity=%ld
    // zone_max_capacity=%ld zone_used_capacity=%ld\n",  zone->id,
    // zone->capacity_, zone->max_capacity_, zone->used_capacity_.load());
  }
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && !defined(OS_WIN)
