// Copyright (c) 2011 The TimberSaw Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"

#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/memtable_list.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "TimberSaw/db.h"
#include "TimberSaw/env.h"
#include "TimberSaw/status.h"
#include "TimberSaw/table.h"

#include "port/port.h"
#include "table/block.h"
#include "table/merger.h"
#include "table/table_builder_computeside.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/mutexlock.h"

namespace TimberSaw {


int SuperVersion::dummy = 0;
void* const SuperVersion::kSVInUse = &SuperVersion::dummy;
void* const SuperVersion::kSVObsolete = nullptr;
void SuperVersionUnrefHandle(void* ptr) {
  // UnrefHandle is called when a thread exists or a ThreadLocalPtr gets
  // destroyed. When former happens, the thread shouldn't see kSVInUse.
  // When latter happens, we are in ~ColumnFamilyData(), no get should happen as
  // well.
  SuperVersion* sv = static_cast<SuperVersion*>(ptr);
  bool was_last_ref __attribute__((__unused__));
  was_last_ref = sv->Unref();
  // Thread-local SuperVersions can't outlive ColumnFamilyData::super_version_.
  // This is important because we can't do SuperVersion cleanup here.
  // That would require locking DB mutex, which would deadlock because
  // SuperVersionUnrefHandle is called with locked ThreadLocalPtr mutex.
  assert(!was_last_ref);
}
// Information kept for every waiting writer
struct DBImpl::Writer {
  explicit Writer(port::Mutex* mu)
      : batch(nullptr), sync(false), done(false), cv(mu) {}

  Status status;
  WriteBatch* batch;
  bool sync;
  bool done;
  port::CondVar cv;
};




Options SanitizeOptions(const std::string& dbname,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src) {
  Options result = src;
  result.comparator = icmp;
  result.filter_policy = (src.filter_policy != nullptr) ? ipolicy : nullptr;
  ClipToRange(&result.max_open_files, 64 + kNumNonTableCacheFiles, 50000);
  ClipToRange(&result.write_buffer_size, 64 << 10, 1 << 30);
  ClipToRange(&result.max_file_size, 1 << 20, 1 << 30);
  ClipToRange(&result.block_size, 1 << 10, 4 << 20);
  if (result.info_log == nullptr) {
    // Open a log file in the same directory as the db
    src.env->CreateDir(dbname);  // In case it does not exist
    src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
    Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);
    if (!s.ok()) {
      // No place suitable for logging
      result.info_log = nullptr;
    }
  }
  if (result.block_cache == nullptr) {
    result.block_cache = NewLRUCache(64 << 20);
  }
  return result;
}

static int TableCacheSize(const Options& sanitized_options) {
  // Reserve ten files or so for other uses and give the rest to TableCache.
  return sanitized_options.max_open_files - kNumNonTableCacheFiles;
}
SuperVersion::~SuperVersion() {
  for (auto td : to_delete) {
    delete td;
  }
}

SuperVersion* SuperVersion::Ref() {
  refs.fetch_add(1, std::memory_order_relaxed);
  return this;
}

bool SuperVersion::Unref() {
  // fetch_sub returns the previous value of ref
  uint32_t previous_refs = refs.fetch_sub(1);
  assert(previous_refs > 0);
  // TODO change it back to assert(refs >=0);
  assert(refs >=0);
  return previous_refs == 1;

}

void SuperVersion::Cleanup() {
  assert(refs.load(std::memory_order_relaxed) == 0);
  imm->Unref();
  mem->Unref();
  std::unique_lock<std::mutex> lck(*versionset_mutex);// in case that the version list is messed up
  current->Unref(4);
  lck.unlock();
  delete this;

}
SuperVersion::SuperVersion(MemTable* new_mem, MemTableListVersion* new_imm,
                           Version* new_current, std::mutex* versionset_mtx)
    :refs(0),version_number(0), versionset_mutex(versionset_mtx) {
  //Guarded by superversion_mtx, so those pointer will always be valide
  mem = new_mem;
  imm = new_imm;
  current = new_current;
  mem->Ref();
  imm->Ref();
  current->Ref(4);
}


DBImpl::DBImpl(const Options& raw_options, const std::string& dbname)
    : env_(raw_options.env),
      internal_comparator_(raw_options.comparator),
      internal_filter_policy_(raw_options.filter_policy),
      options_(SanitizeOptions(dbname, &internal_comparator_,
                               &internal_filter_policy_, raw_options)),
      owns_info_log_(options_.info_log != raw_options.info_log),
      owns_cache_(options_.block_cache != raw_options.block_cache),
      dbname_(dbname),
      table_cache_(new TableCache(dbname_, options_, TableCacheSize(options_))),
      db_lock_(nullptr),
      shutting_down_(false),
//      write_stall_cv(&write_stall_mutex_),
      mem_(nullptr),
      imm_(config::Immutable_FlushTrigger, config::Immutable_StopWritesTrigger,
           64 * 1024 * 1024 * config::Immutable_StopWritesTrigger),
      has_imm_(false),
      logfile_(nullptr),
      logfile_number_(0),
      log_(nullptr),
      seed_(0),
      tmp_batch_(new WriteBatch),
      background_compaction_scheduled_(false),
      manual_compaction_(nullptr),
      versions_(new VersionSet(dbname_, &options_, table_cache_,
                               &internal_comparator_, &versionset_mtx)),
      super_version_number_(0),
      super_version(nullptr), local_sv_(new ThreadLocalPtr(&SuperVersionUnrefHandle))
#ifdef PROCESSANALYSIS
      ,Total_time_elapse(0),
      flush_times(0)
#endif
{
  printf("DBImpl start\n");
//        main_comm_threads.emplace_back(Clientmessagehandler());
    std::shared_ptr<RDMA_Manager> rdma_mg = env_->rdma_mg;
    env_->SetBackgroundThreads(options_.max_background_flushes,ThreadPoolType::FlushThreadPool);
    env_->SetBackgroundThreads(options_.max_background_compactions,ThreadPoolType::CompactionThreadPool);
    env_->rdma_mg->Mempool_initialize(std::string("DataBlock"), options_.block_size);

    main_comm_threads.emplace_back(
    &DBImpl::client_message_polling_and_handling_thread, this, "main");
    printf("communication thread created\n");
    //Wait for the clearance of pending receive work request from the last DB open.
    {
      std::unique_lock<std::mutex> lck(superversion_memlist_mtx);
      while (!check_and_clear_pending_recvWR) {
        printf("Start to sleep\n");
        write_stall_cv.wait(lck);
        printf("Waked up\n");
      }
    }


//      rdma_mg->Remote_Memory_Register(1024*1024*1024);
//        std::string trial("trial");
//        rdma_mg->Remote_Query_Pair_Connection(trial);
//        std::shared_lock<std::shared_mutex> l(rdma_mg->qp_cq_map_mutex);
//
//        if (rdma_mg->res->qp_map.find(trial) != rdma_mg->res->qp_map.end()){
//          ibv_qp* qp = rdma_mg->res->qp_map.at(trial);
//          assert(rdma_mg->res->qp_connection_info.find(trial)!= rdma_mg->res->qp_connection_info.end());
//          l.unlock();
//          rdma_mg->modify_qp_to_reset(qp);
//          rdma_mg->connect_qp(qp, trial);
//        }else{
//          l.unlock();
//          rdma_mg->Remote_Query_Pair_Connection(trial);
//          l.lock();
//          ibv_qp* qp = rdma_mg->res->qp_map.at(trial);
//          l.unlock();
//        }
//      main_comm_threads.back().detach();
    printf("DBImpl finished\n");
}

DBImpl::~DBImpl() {
  // Wait for background work to finish.
//  undefine_mutex.Lock();
  printf("DBImpl deallocated\n");
  //TODO: recycle all the
  shutting_down_.store(true);
  env_->JoinAllThreads(false);
  // wait for communicaiton thread to finish
  for(int i = 0; i < main_comm_threads.size(); i++){
    main_comm_threads[i].join();
  }
//  while (background_compaction_scheduled_) {
//    env_->SleepForMicroseconds(10);
//  }
#ifdef PROCESSANALYSIS
  if (RDMA_Manager::ReadCount.load() != 0)
    printf("RDMA read operatoion average time duration: %zu\n", RDMA_Manager::RDMAReadTimeElapseSum.load()/RDMA_Manager::ReadCount.load());
#endif
//  undefine_mutex.Unlock();

  if (db_lock_ != nullptr) {
    env_->UnlockFile(db_lock_);
  }
  delete local_sv_;
  if (super_version!= nullptr && super_version->Unref())
    super_version->Cleanup();
//  if (local_sv_.get()->Get() != nullptr){
//    CleanupSuperVersion(static_cast<SuperVersion*>(local_sv_.get()->Get()));
//  }
//  local_sv_->Reset(nullptr);

  delete versions_;
  if (mem_ != nullptr) mem_.load()->SimpleDelete();
//  if (imm_ != nullptr) imm_.load()->SimpleDelete();
//  if (imm_ != nullptr) delete imm_;
  delete tmp_batch_;
  delete log_;
  delete logfile_;
  delete table_cache_;

  if (owns_info_log_) {
    delete options_.info_log;
  }
  if (owns_cache_) {
    delete options_.block_cache;
  }
#ifdef PROCESSANALYSIS
  if (flush_times.load() >0)
    printf("Memtable total flush time, number of flush, average flush time are %zu, %zu, %zu\n",
           Total_time_elapse.load(), flush_times.load(),
           Total_time_elapse.load()/flush_times.load());
#endif
}

Status DBImpl::NewDB() {
  VersionEdit new_db;
  new_db.SetComparatorName(user_comparator()->Name());
  new_db.SetLogNumber(0);
  new_db.SetNextFile(2);
  new_db.SetLastSequence(0);

  const std::string manifest = DescriptorFileName(dbname_, 1);
  WritableFile* file;
  Status s = env_->NewWritableFile(manifest, &file);
  if (!s.ok()) {
    return s;
  }
  {
    log::Writer log(file);
    std::string record;
    new_db.EncodeTo(&record);
    s = log.AddRecord(record);
    if (s.ok()) {
      s = file->Sync();
    }
    if (s.ok()) {
      s = file->Close();
    }
  }
  delete file;
  if (s.ok()) {
    // Make "CURRENT" file that points to the new manifest file.
    s = SetCurrentFile(env_, dbname_, 1);
  } else {
    env_->RemoveFile(manifest);
  }
  return s;
}

void DBImpl::MaybeIgnoreError(Status* s) const {
  if (s->ok() || options_.paranoid_checks) {
    // No change needed
  } else {
    Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
    *s = Status::OK();
  }
}

void DBImpl::RemoveObsoleteFiles() {
  undefine_mutex.AssertHeld();

  if (!bg_error_.ok()) {
    // After a background error, we don't know whether a new version may
    // or may not have been committed, so we cannot safely garbage collect.
    return;
  }

  // Make a set of all of the live files
//  std::set<uint64_t> live = pending_outputs_;
//  versions_->AddLiveFiles(&live);

  std::vector<std::string> filenames;
  env_->GetChildren(dbname_, &filenames);  // Ignoring errors on purpose
  uint64_t number;
  FileType type;
  std::vector<std::string> files_to_delete;
  for (std::string& filename : filenames) {
    if (ParseFileName(filename, &number, &type)) {
      bool keep = true;
      switch (type) {
        case kLogFile:
          keep = ((number >= versions_->LogNumber()) ||
                  (number == versions_->PrevLogNumber()));
          break;
        case kDescriptorFile:
          // Keep my manifest file, and any newer incarnations'
          // (in case there is a race that allows other incarnations)
          keep = (number >= versions_->ManifestFileNumber());
          break;
//        case kTableFile:
//          keep = (live.find(number) != live.end());
//          break;
//        case kTempFile:
//          // Any temp files that are currently being written to must
//          // be recorded in pending_outputs_, which is inserted into "live"
//          keep = (live.find(number) != live.end());
//          break;
        case kCurrentFile:
        case kDBLockFile:
        case kInfoLogFile:
          keep = true;
          break;
      }

      if (!keep) {
        files_to_delete.push_back(std::move(filename));
        if (type == kTableFile) {
          table_cache_->Evict(number);
        }
        Log(options_.info_log, "Delete type=%d #%lld\n", static_cast<int>(type),
            static_cast<unsigned long long>(number));
      }
    }
  }

  // While deleting all files unblock other threads. All files being deleted
  // have unique names which will not collide with newly created files and
  // are therefore safe to delete while allowing other threads to proceed.
//  undefine_mutex.Unlock();
  for (const std::string& filename : files_to_delete) {
    env_->RemoveFile(dbname_ + "/" + filename);
  }
//  undefine_mutex.Lock();
}

Status DBImpl::Recover(VersionEdit* edit, bool* save_manifest) {
  undefine_mutex.AssertHeld();

  // Ignore error from CreateDir since the creation of the DB is
  // committed only when the descriptor is created, and this directory
  // may already exist from a previous failed creation attempt.
  env_->CreateDir(dbname_);
  assert(db_lock_ == nullptr);
  Status s = env_->LockFile(LockFileName(dbname_), &db_lock_);
  if (!s.ok()) {
    return s;
  }

  if (!env_->FileExists(CurrentFileName(dbname_))) {
    if (options_.create_if_missing) {
      Log(options_.info_log, "Creating DB %s since it was missing.",
          dbname_.c_str());
      s = NewDB();
      if (!s.ok()) {
        return s;
      }
    } else {
      return Status::InvalidArgument(
          dbname_, "does not exist (create_if_missing is false)");
    }
  } else {
    if (options_.error_if_exists) {
      return Status::InvalidArgument(dbname_,
                                     "exists (error_if_exists is true)");
    }
  }

  s = versions_->Recover(save_manifest);
  if (!s.ok()) {
    return s;
  }
  SequenceNumber max_sequence(0);

  // Recover from all newer log files than the ones named in the
  // descriptor (new log files may have been added by the previous
  // incarnation without registering them in the descriptor).
  //
  // Note that PrevLogNumber() is no longer used, but we pay
  // attention to it in case we are recovering a database
  // produced by an older version of TimberSaw.
  const uint64_t min_log = versions_->LogNumber();
  const uint64_t prev_log = versions_->PrevLogNumber();
  std::vector<std::string> filenames;
  s = env_->GetChildren(dbname_, &filenames);
  if (!s.ok()) {
    return s;
  }
  std::set<uint64_t> expected;
  versions_->AddLiveFiles(&expected);
  uint64_t number;
  FileType type;
  std::vector<uint64_t> logs;
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
      expected.erase(number);
      if (type == kLogFile && ((number >= min_log) || (number == prev_log)))
        logs.push_back(number);
    }
  }
  if (!expected.empty()) {
    char buf[50];
    std::snprintf(buf, sizeof(buf), "%d missing files; e.g.",
                  static_cast<int>(expected.size()));
    return Status::Corruption(buf, TableFileName(dbname_, *(expected.begin())));
  }

  // Recover in the order in which the logs were generated
  std::sort(logs.begin(), logs.end());
  for (size_t i = 0; i < logs.size(); i++) {
    s = RecoverLogFile(logs[i], (i == logs.size() - 1), save_manifest, edit,
                       &max_sequence);
    if (!s.ok()) {
      return s;
    }

    // The previous incarnation may not have written any MANIFEST
    // records after allocating this log number.  So we manually
    // update the file number allocation counter in VersionSet.
    versions_->MarkFileNumberUsed(logs[i]);
  }

  if (versions_->LastSequence() < max_sequence) {
    versions_->SetLastSequence(max_sequence);
  }

  return Status::OK();
}

Status DBImpl::RecoverLogFile(uint64_t log_number, bool last_log,
                              bool* save_manifest, VersionEdit* edit,
                              SequenceNumber* max_sequence) {
  struct LogReporter : public log::Reader::Reporter {
    Env* env;
    Logger* info_log;
    const char* fname;
    Status* status;  // null if options_.paranoid_checks==false
    void Corruption(size_t bytes, const Status& s) override {
      Log(info_log, "%s%s: dropping %d bytes; %s",
          (this->status == nullptr ? "(ignoring error) " : ""), fname,
          static_cast<int>(bytes), s.ToString().c_str());
      if (this->status != nullptr && this->status->ok()) *this->status = s;
    }
  };

  undefine_mutex.AssertHeld();

  // Open the log file
  std::string fname = LogFileName(dbname_, log_number);
  SequentialFile* file;
  Status status = env_->NewSequentialFile(fname, &file);
  if (!status.ok()) {
    MaybeIgnoreError(&status);
    return status;
  }

  // Create the log reader.
  LogReporter reporter;
  reporter.env = env_;
  reporter.info_log = options_.info_log;
  reporter.fname = fname.c_str();
  reporter.status = (options_.paranoid_checks ? &status : nullptr);
  // We intentionally make log::Reader do checksumming even if
  // paranoid_checks==false so that corruptions cause entire commits
  // to be skipped instead of propagating bad information (like overly
  // large sequence numbers).
  log::Reader reader(file, &reporter, true /*checksum*/, 0 /*initial_offset*/);
  Log(options_.info_log, "Recovering log #%llu",
      (unsigned long long)log_number);

  // Read all the records and add to a memtable
  std::string scratch;
  Slice record;
  WriteBatch batch;
  int compactions = 0;
  MemTable* mem = nullptr;
  while (reader.ReadRecord(&record, &scratch) && status.ok()) {
    if (record.size() < 12) {
      reporter.Corruption(record.size(),
                          Status::Corruption("log record too small"));
      continue;
    }
    WriteBatchInternal::SetContents(&batch, record);

    if (mem == nullptr) {
      mem = new MemTable(internal_comparator_);
      mem->Ref();
    }
    status = WriteBatchInternal::InsertInto(&batch, mem);
    MaybeIgnoreError(&status);
    if (!status.ok()) {
      break;
    }
    const SequenceNumber last_seq = WriteBatchInternal::Sequence(&batch) +
                                    WriteBatchInternal::Count(&batch) - 1;
    if (last_seq > *max_sequence) {
      *max_sequence = last_seq;
    }

    if (mem->ApproximateMemoryUsage() > options_.write_buffer_size) {
      compactions++;
      *save_manifest = true;
      status = WriteLevel0Table(mem, edit, nullptr);
      mem->Unref();
      mem = nullptr;
      if (!status.ok()) {
        // Reflect errors immediately so that conditions like full
        // file-systems cause the DB::Open() to fail.
        break;
      }
    }
  }

  delete file;

  // See if we should keep reusing the last log file.
  if (status.ok() && options_.reuse_logs && last_log && compactions == 0) {
    assert(logfile_ == nullptr);
    assert(log_ == nullptr);
    assert(mem_ == nullptr);
    uint64_t lfile_size;
    if (env_->GetFileSize(fname, &lfile_size).ok() &&
        env_->NewAppendableFile(fname, &logfile_).ok()) {
      Log(options_.info_log, "Reusing old log %s \n", fname.c_str());
      log_ = new log::Writer(logfile_, lfile_size);
      logfile_number_ = log_number;
      if (mem != nullptr) {
        mem_.store(mem);
        mem = nullptr;
      } else {
        // mem can be nullptr if lognum exists but was empty.
        mem_.store(new MemTable(internal_comparator_));
        mem_.load()->Ref();
      }
    }
  }

  if (mem != nullptr) {
    // mem did not get reused; compact it.
    if (status.ok()) {
      *save_manifest = true;
      status = WriteLevel0Table(mem, edit, nullptr);
    }
    mem->Unref();
  }

  return status;
}

Status DBImpl::WriteLevel0Table(FlushJob* job, VersionEdit* edit) {
//  undefine_mutex.AssertHeld();
  const uint64_t start_micros = env_->NowMicros();
  //Mark all memtable as FLUSHPROCESSING.
  job->SetAllMemStateProcessing();
  std::shared_ptr<RemoteMemTableMetaData> meta = std::make_shared<RemoteMemTableMetaData>(0);
  job->sst = meta;
  meta->number = versions_->NewFileNumber();
//  pending_outputs_.insert(meta->number);
  Iterator* iter = imm_.MakeInputIterator(job);
  Log(options_.info_log, "Level-0 table #%llu: started",
      (unsigned long long)meta->number);
//  printf("now system start to serializae mem %p\n", mem);
  Status s;
  {
//    undefine_mutex.Unlock();
    s = job->BuildTable(dbname_, env_, options_, table_cache_, iter, meta, Flush);
//    undefine_mutex.Lock();
  }
//  printf("remote table use count after building %ld\n", meta.use_count());
//  Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s",
//      (unsigned long long)meta.number, (unsigned long long)meta.file_size,
//      s.ToString().c_str());
  delete iter;
//  pending_outputs_.erase(meta->number);

  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  int level = 0;

  if (s.ok() && meta->file_size > 0) {
//    const Slice min_user_key = meta->smallest.user_key();
//    const Slice max_user_key = meta->largest.user_key();
//    if (base != nullptr) {
//      level = base->PickLevelForMemTableOutput(min_user_key, max_user_key);
//    }
    meta->level = 0;
    edit->AddFile(0, meta);
    assert(edit->GetNewFilesNum()==1);
  }

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros;
  stats.bytes_written = meta->file_size;
  stats_[level].Add(stats);
//  write_stall_mutex_.AssertNotHeld();
  return s;
}
Status DBImpl::WriteLevel0Table(MemTable* job, VersionEdit* edit,
                                Version* base) {
//  undefine_mutex.AssertHeld();
  //The program should never goes here.
  assert(false);
  const uint64_t start_micros = env_->NowMicros();
  std::shared_ptr<RemoteMemTableMetaData> meta = std::make_shared<RemoteMemTableMetaData>(0);
  meta->number = versions_->NewFileNumber();
//  pending_outputs_.insert(meta->number);
  Iterator* iter = job->NewIterator();
  Log(options_.info_log, "Level-0 table #%llu: started",
      (unsigned long long)meta->number);
//  printf("now system start to serializae mem %p\n", mem);
  Status s;
  {
//    undefine_mutex.Unlock();
    s = BuildTable(dbname_, env_, options_, table_cache_, iter, meta, Flush);
//    undefine_mutex.Lock();
  }
//  printf("remote table use count after building %ld\n", meta.use_count());
//  Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s",
//      (unsigned long long)meta.number, (unsigned long long)meta.file_size,
//      s.ToString().c_str());
  delete iter;
//  pending_outputs_.erase(meta->number);

  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  int level = 0;
  if (s.ok() && meta->file_size > 0) {
    const Slice min_user_key = meta->smallest.user_key();
    const Slice max_user_key = meta->largest.user_key();
    if (base != nullptr) {
      level = base->PickLevelForMemTableOutput(min_user_key, max_user_key);
    }
    edit->AddFile(level, meta);
  }

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros;
  stats.bytes_written = meta->file_size;
  stats_[level].Add(stats);
//  write_stall_mutex_.AssertNotHeld();
  return s;
}

//void DBImpl::CompactMemTable() {
////  undefine_mutex.AssertHeld();
//  //TOTHINK What will happen if we remove the mutex in the future?
//  MemTable* mem = mem_.load();
//  MemTable* imm = imm_.load();
//  assert(imm != nullptr);
//  assert(!imm->CheckFlushInProcess());
//
//  // Save the contents of the memtable as a new Table
//  VersionEdit edit;
//  Version* base = versions_->current();
//  // wait for the ongoing writes for 1 millisecond.
//  size_t counter = 0;
//  // wait for the immutable to get ready to flush. the signal here is prepare for
//  // the case that the thread this immutable is under the control of conditional
//  // variable.
////-----------------seldom Lock----------------------------
//  while(!imm->able_to_flush){
//    counter++;
//    if (counter==500){
//      printf("signal all the wait threads\n");
//      usleep(10);
//      write_stall_cv.SignalAll();
//      counter = 0;
//    }
//  }
////---------------Lock free version -----------------------
////  while(!imm->able_to_flush){
////
////    counter++;
////    if (counter==5000){
//////      printf("signal all the wait threads\n");
//////      write_stall_cv.SignalAll();
////      usleep(10);
////      counter = 0;
////    }
////  }
//
//  imm->SetFlushState(MemTable::FLUSH_PROCESSING);
//  base->Ref();
//  Status s = WriteLevel0Table(imm, &edit, base);
//  base->Unref();
//
//  if (s.ok() && shutting_down_.load(std::memory_order_acquire)) {
//    s = Status::IOError("Deleting DB during memtable compaction");
//  }
//
//  // Replace immutable memtable with the generated Table
//  // TODO: use tryinstall flush result here
//  if (s.ok()) {
//    edit.SetPrevLogNumber(0);
//    edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed
//    s = versions_->LogAndApply(&edit);
//  }else{
//    printf("version apply failed");
//  }
////-------------------- seldom Lock version----------------
//  if (s.ok()) {
//    // Commit to the new state
////    printf("imm table head node %p has is still alive\n", mem->GetTable()->GetHeadNode()->Next(0));
//    imm_mtx.lock();
//    MemTable* imm = imm_.load();
//
//    imm->Unref();
////    printf("mem %p has been deleted\n", imm);
////    printf("mem %p has is still alive\n", mem);
////    printf("After mem dereference head node of the imm %p\n", mem->GetTable()->GetHeadNode()->Next(0));
//    imm_.store(nullptr);
//    imm_mtx.unlock();
//
//    write_stall_cv.SignalAll();
//    has_imm_.store(false, std::memory_order_release);
//    // TODO how to remove the obsoleted remote memtable?
////    RemoveObsoleteFiles();
//// ----------------Lock-free version below--------------------
////  if (s.ok()) {
////    // Commit to the new state
//////    printf("imm table head node %p has is still alive\n", mem->GetTable()->GetHeadNode()->Next(0));
//////    undefine_mutex.Lock();
////    MemTable* imm = imm_.load();
////
////    imm->Unref();
//////    printf("mem %p has been deleted\n", imm);
//////    printf("mem %p has is still alive\n", mem);
//////    printf("After mem dereference head node of the imm %p\n", mem->GetTable()->GetHeadNode()->Next(0));
////    imm_.store(nullptr);
//////    undefine_mutex.Unlock();
//////    write_stall_cv.SignalAll();
////    has_imm_.store(false, std::memory_order_release);
////    // TODO how to remove the obsoleted remote memtable?
//////    RemoveObsoleteFiles();
//  } else {
//    RecordBackgroundError(s);
//  }
//}
void DBImpl::CompactMemTable() {
//  undefine_mutex.AssertHeld();
  //TOTHINK What will happen if we remove the mutex in the future?

  // Save the contents of the memtable as a new Table
  VersionEdit edit;
//  Version* base = versions_->current();
  // wait for the ongoing writes for 1 millisecond.
  size_t counter = 0;
  // wait for the immutable to get ready to flush. the signal here is prepare for
  // the case that the thread this immutable is under the control of conditional
  // variable.
  FlushJob f_job(&write_stall_cv, &internal_comparator_);
  { //This code should synchronized outside the superversion mutex, since nothing
    // has been changed for superversion.
    std::unique_lock<std::mutex> l(FlushPickMTX);
    if (imm_.IsFlushPending())
      imm_.PickMemtablesToFlush(&f_job.mem_vec);
    else
      return;
  }
  DEBUG_arg("picked metable number is %lu", f_job.mem_vec.size());
  f_job.Waitforpendingwriter();

//  imm->SetFlushState(MemTable::FLUSH_PROCESSING);
//  base->Ref();
#ifdef PROCESSANALYSIS
  auto start = std::chrono::high_resolution_clock::now();
#endif
  Status s = WriteLevel0Table(&f_job, &edit);
#ifdef PROCESSANALYSIS
  auto stop = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
  Total_time_elapse.fetch_add(duration.count());
  flush_times.fetch_add(1);
#ifndef NDEBUG
      printf("memtable flushing time elapse (%ld) us, immutable num is %lu\n", duration.count(), f_job.mem_vec.size());
#endif
#endif
  //  base->Unref();
  assert(edit.GetNewFilesNum()==1);

  TryInstallMemtableFlushResults(&f_job, versions_, f_job.sst, &edit);
//  MaybeScheduleFlushOrCompaction();
  if (s.ok() && shutting_down_.load(std::memory_order_acquire)) {
    s = Status::IOError("Deleting DB during memtable compaction");
  }
}

//NOte: deprecated function
void DBImpl::CompactRange(const Slice* begin, const Slice* end) {
  int max_level_with_files = 1;
  {
    MutexLock l(&undefine_mutex);
    Version* base = versions_->current();
    for (int level = 1; level < config::kNumLevels; level++) {
      if (base->OverlapInLevel(level, begin, end)) {
        max_level_with_files = level;
      }
    }
  }
//  TEST_CompactMemTable();  // TODO(sanjay): Skip if memtable does not overlap
  for (int level = 0; level < max_level_with_files; level++) {
    TEST_CompactRange(level, begin, end);
  }
}

void DBImpl::TEST_CompactRange(int level, const Slice* begin,
                               const Slice* end) {
  assert(level >= 0);
  assert(level + 1 < config::kNumLevels);

  InternalKey begin_storage, end_storage;

  ManualCompaction manual;
  manual.level = level;
  manual.done = false;
  if (begin == nullptr) {
    manual.begin = nullptr;
  } else {
    begin_storage = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek);
    manual.begin = &begin_storage;
  }
  if (end == nullptr) {
    manual.end = nullptr;
  } else {
    end_storage = InternalKey(*end, 0, static_cast<ValueType>(0));
    manual.end = &end_storage;
  }

//  MutexLock l(&undefine_mutex);
//  while (!manual.done && !shutting_down_.load(std::memory_order_acquire) &&
//         bg_error_.ok()) {
//    if (manual_compaction_ == nullptr) {  // Idle
//      manual_compaction_ = &manual;
//      MaybeScheduleFlushOrCompaction();
//    } else {  // Running either my compaction or another compaction.
//      write_stall_cv.wait(imm_mtx);
//    }
//  }
  if (manual_compaction_ == &manual) {
    // Cancel my manual compaction since we aborted early for some reason.
    manual_compaction_ = nullptr;
  }
}

//Status DBImpl::TEST_CompactMemTable() {
//  // nullptr batch means just wait for earlier writes to be done
//  Status s = Write(WriteOptions(), nullptr);
//  if (s.ok()) {
//    // Wait until the compaction completes
//    MutexLock l(&undefine_mutex);
//    while (imm_ != nullptr && bg_error_.ok()) {
//      write_stall_cv.Wait();
//    }
//    if (imm_ != nullptr) {
//      s = bg_error_;
//    }
//  }
//  return s;
//}

void DBImpl::RecordBackgroundError(const Status& s) {
//  undefine_mutex.AssertHeld();
  if (bg_error_.ok()) {
    bg_error_ = s;
    write_stall_cv.notify_all();
  }
}

void DBImpl::MaybeScheduleFlushOrCompaction() {
//  undefine_mutex.AssertHeld();
// In my implementation the Maybeschedule Compaction will only be triggered once
// by the thread which CAS the memtable successfully.
 if (shutting_down_.load(std::memory_order_acquire)) {
    // DB is being deleted; no more background compactions
    return;
  } else if (!bg_error_.ok()) {
    // Already got an error; no more changes
    return;
  }
  DEBUG("May be schedule a background task! \n");
  if (imm_.IsFlushPending()) {
//    background_compaction_scheduled_ = true;
    void* function_args = nullptr;
    BGThreadMetadata* thread_pool_args = new BGThreadMetadata{.db = this, .func_args = function_args};
    ThreadPoolType type = ThreadPoolType::FlushThreadPool;
    if (env_->Queue_Length_Quiry(type)>256){
      //If there has already be enough compaction scheduled, then drop this one
      return;
    }
    env_->Schedule(BGWork_Flush, static_cast<void*>(thread_pool_args), type);
    DEBUG("Schedule a flushing !\n");
  }
//  if (versions_->NeedsCompaction()) {
////    background_compaction_scheduled_ = true;
//    void* function_args = nullptr;
//    BGThreadMetadata* thread_pool_args = new BGThreadMetadata{.db = this, .func_args = function_args};
//    env_->Schedule(BGWork_Compaction, static_cast<void*>(thread_pool_args), ThreadPoolType::CompactionThreadPool);
//    DEBUG("Schedule a Compaction !\n");
//  }
}

void DBImpl::BGWork_Flush(void* thread_arg) {
  BGThreadMetadata* p = static_cast<BGThreadMetadata*>(thread_arg);
  ((DBImpl*)p->db)->BackgroundFlush(p->func_args);
  delete static_cast<BGThreadMetadata*>(thread_arg);
}
void DBImpl::BGWork_Compaction(void* thread_arg) {
  BGThreadMetadata* p = static_cast<BGThreadMetadata*>(thread_arg);
  ((DBImpl*)p->db)->BackgroundCompaction(p->func_args);
  delete static_cast<BGThreadMetadata*>(thread_arg);
}
void DBImpl::BackgroundCall() {
  //Tothink: why there is a Lock, which data structure is this mutex protecting
//  undefine_mutex.Lock();
//  assert(background_compaction_scheduled_);
  if (shutting_down_.load(std::memory_order_acquire)) {
    // No more background work when shutting down.
  } else if (!bg_error_.ok()) {
    // No more background work after a background error.
  } else {
    void* dummay_p = nullptr;
    BackgroundCompaction(dummay_p);
  }

  background_compaction_scheduled_ = false;

  // Previous compaction may have produced too many files in a level,
  // so reschedule another compaction if needed.
  MaybeScheduleFlushOrCompaction();
//  undefine_mutex.Unlock();
}
void DBImpl::BackgroundFlush(void* p) {
  //Tothink: why there is a Lock, which data structure is this mutex protecting
//  undefine_mutex.Lock();
//  assert(background_compaction_scheduled_);
  if (shutting_down_.load(std::memory_order_acquire)) {
    // No more background work when shutting down.
  } else if (!bg_error_.ok()) {
    // No more background work after a background error.
  } else if (imm_.IsFlushPending()) {

    CompactMemTable();

    DEBUG_arg("First level's file number is %d", versions_->NumLevelFiles(0));
    DEBUG("Memtable flushed\n");
  }

//  background_compaction_scheduled_ = false;

  // Previous compaction may have produced too many files in a level,
  // so reschedule another compaction if needed.
  MaybeScheduleFlushOrCompaction();
//  undefine_mutex.Unlock();
}
void DBImpl::BackgroundCompaction(void* p) {
//  write_stall_mutex_.AssertNotHeld();
  assert(false);
  if (shutting_down_.load(std::memory_order_acquire)) {
    // No more background work when shutting down.
  } else if (!bg_error_.ok()) {
    // No more background work after a background error.
  } else if (versions_->NeedsCompaction()) {
    Compaction* c;
    bool is_manual = (manual_compaction_ != nullptr);
    InternalKey manual_end;
    if (is_manual) {
      ManualCompaction* m = manual_compaction_;
      c = versions_->CompactRange(m->level, m->begin, m->end);
      m->done = (c == nullptr);
      if (c != nullptr) {
        manual_end = c->input(0, c->num_input_files(0) - 1)->largest;
      }
      Log(options_.info_log,
          "Manual compaction at level-%d from %s .. %s; will stop at %s\n",
          m->level, (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
          (m->end ? m->end->DebugString().c_str() : "(end)"),
          (m->done ? "(end)" : manual_end.DebugString().c_str()));
    } else {
      c = versions_->PickCompaction();
      //if there is no task to pick up, just return.
      if (c== nullptr){
        DEBUG("compaction task executed but not found doable task.\n");
        delete c;
        return;
      }

    }
//    write_stall_mutex_.AssertNotHeld();
    Status status;
    if (c == nullptr) {
      // Nothing to do
    } else if (!is_manual && c->IsTrivialMove()) {
      // Move file to next level
      assert(c->num_input_files(0) == 1);
      std::shared_ptr<RemoteMemTableMetaData> f = c->input(0, 0);
      c->edit()->RemoveFile(c->level(), f->number, f->creator_node_id);
      c->edit()->AddFile(c->level() + 1, f);
      {
        std::unique_lock<std::mutex> l(superversion_memlist_mtx);
        c->ReleaseInputs();
        status = versions_->LogAndApply(c->edit(), 0);
        InstallSuperVersion();
      }

      if (!status.ok()) {
        RecordBackgroundError(status);
      }
      VersionSet::LevelSummaryStorage tmp;
      Log(options_.info_log, "Moved #%lld to level-%d %lld bytes %s: %s\n",
          static_cast<unsigned long long>(f->number), c->level() + 1,
          static_cast<unsigned long long>(f->file_size),
          status.ToString().c_str(), versions_->LevelSummary(&tmp));
      DEBUG("Trival compaction\n");
    } else {
      CompactionState* compact = new CompactionState(c);

      auto start = std::chrono::high_resolution_clock::now();
//      write_stall_mutex_.AssertNotHeld();
      // Only when there is enough input level files and output level files will the subcompaction triggered
      if (options_.usesubcompaction && c->num_input_files(0)>=4 && c->num_input_files(1)>1){
        status = DoCompactionWorkWithSubcompaction(compact);
      }else{
        status = DoCompactionWork(compact);
      }

      auto stop = std::chrono::high_resolution_clock::now();
      auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
      printf("Table compaction time elapse (%ld) us, compaction level is %d, first level file number %d, the second level file number %d \n",
             duration.count(), compact->compaction->level(), compact->compaction->num_input_files(0),compact->compaction->num_input_files(1) );
      DEBUG("Non-trivalcompaction!\n");
      std::cout << "compaction task table number in the first level"<<compact->compaction->inputs_[0].size() << std::endl;
      if (!status.ok()) {
        RecordBackgroundError(status);
      }
      CleanupCompaction(compact);
//    RemoveObsoleteFiles();
    }
    delete c;

    if (status.ok()) {
      // Done
    } else if (shutting_down_.load(std::memory_order_acquire)) {
      // Ignore compaction errors found during shutting down
    } else {
      Log(options_.info_log, "Compaction error: %s", status.ToString().c_str());
    }

    if (is_manual) {
      ManualCompaction* m = manual_compaction_;
      if (!status.ok()) {
        m->done = true;
      }
      if (!m->done) {
        // We only compacted part of the requested range.  Update *m
        // to the range that is left to be compacted.
        m->tmp_storage = manual_end;
        m->begin = &m->tmp_storage;
      }
      manual_compaction_ = nullptr;
    }
  }
  MaybeScheduleFlushOrCompaction();

}

void DBImpl::CleanupCompaction(CompactionState* compact) {
//  undefine_mutex.AssertHeld();
  if (compact->builder != nullptr) {
    // May happen if we get a shutdown call in the middle of compaction
    compact->builder->Abandon();
    delete compact->builder;
  } else {
//    assert(compact->outfile == nullptr);
  }
//  delete compact->outfile;
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionOutput& out = compact->outputs[i];
//    pending_outputs_.erase(out.number);
  }
  delete compact;
}
Status DBImpl::OpenCompactionOutputFile(SubcompactionState* compact) {
  assert(compact != nullptr);
  assert(compact->builder == nullptr);
  uint64_t file_number;
  {
//    undefine_mutex.Lock();
    file_number = versions_->NewFileNumber();
//    pending_outputs_.insert(file_number);
    CompactionOutput out;
    out.number = file_number;
    out.smallest.Clear();
    out.largest.Clear();
    compact->outputs.push_back(out);
//    undefine_mutex.Unlock();
  }

  // Make the output file
//  std::string fname = TableFileName(dbname_, file_number);
//  Status s = env_->NewWritableFile(fname, &compact->outfile);
  Status s = Status::OK();
  if (s.ok()) {
    compact->builder = new TableBuilder_ComputeSide(options_, Compact);
  }
  return s;
}
Status DBImpl::OpenCompactionOutputFile(CompactionState* compact) {
  assert(compact != nullptr);
  assert(compact->builder == nullptr);
  uint64_t file_number;
  {
//    undefine_mutex.Lock();
    file_number = versions_->NewFileNumber();
//    pending_outputs_.insert(file_number);
    CompactionOutput out;
    out.number = file_number;
    out.smallest.Clear();
    out.largest.Clear();
    compact->outputs.push_back(out);
//    undefine_mutex.Unlock();
  }

  // Make the output file
//  std::string fname = TableFileName(dbname_, file_number);
//  Status s = env_->NewWritableFile(fname, &compact->outfile);
  Status s = Status::OK();
  if (s.ok()) {
    compact->builder = new TableBuilder_ComputeSide(options_, Compact);
  }
  return s;
}

Status DBImpl::FinishCompactionOutputFile(SubcompactionState* compact,
                                          Iterator* input) {
  assert(compact != nullptr);
//  assert(compact->outfile != nullptr);
  assert(compact->builder != nullptr);

  const uint64_t output_number = compact->current_output()->number;
  assert(output_number != 0);

  // Check for iterator errors
  Status s = input->status();
  const uint64_t current_entries = compact->builder->NumEntries();
  if (s.ok()) {
    s = compact->builder->Finish();
  } else {
    printf("iterator Error!!!!!!!!!!!, Error: %s\n", s.ToString().c_str());
    compact->builder->Abandon();
  }

  compact->builder->get_datablocks_map(compact->current_output()->remote_data_mrs);
  compact->builder->get_dataindexblocks_map(compact->current_output()->remote_dataindex_mrs);
  compact->builder->get_filter_map(compact->current_output()->remote_filter_mrs);
#ifndef NDEBUG
  uint64_t file_size = 0;
  for(auto iter : compact->current_output()->remote_data_mrs){
    file_size += iter.second->length;
  }
#endif
  const uint64_t current_bytes = compact->builder->FileSize();
  compact->current_output()->file_size = current_bytes;
  assert(file_size == current_bytes);
  compact->total_bytes += current_bytes;
  delete compact->builder;
  compact->builder = nullptr;

  // Finish and check for file errors
//  if (s.ok()) {
//    s = compact->outfile->Sync();
//  }
//  if (s.ok()) {
//    s = compact->outfile->Close();
//  }
//  delete compact->outfile;
//  compact->outfile = nullptr;

  if (s.ok() && current_entries > 0) {
    // Verify that the table is usable
//    Iterator* iter =
//        table_cache_->NewIterator(ReadOptions(), output_number, current_bytes);
//    s = iter->status();
//    delete iter;
    if (s.ok()) {
      Log(options_.info_log, "Generated table #%llu@%d: %lld keys, %lld bytes",
          (unsigned long long)output_number, compact->compaction->level(),
          (unsigned long long)current_entries,
          (unsigned long long)current_bytes);
    }
  }
  return s;
}
Status DBImpl::FinishCompactionOutputFile(CompactionState* compact,
                                          Iterator* input) {
  assert(compact != nullptr);
//  assert(compact->outfile != nullptr);
  assert(compact->builder != nullptr);

  const uint64_t output_number = compact->current_output()->number;
  assert(output_number != 0);

  // Check for iterator errors
  Status s = input->status();
  const uint64_t current_entries = compact->builder->NumEntries();
  if (s.ok()) {
    s = compact->builder->Finish();
  } else {
    printf("iterator Error!!!!!!!!!!!, Error: %s\n", s.ToString().c_str());
    compact->builder->Abandon();
  }

  compact->builder->get_datablocks_map(compact->current_output()->remote_data_mrs);
  compact->builder->get_dataindexblocks_map(compact->current_output()->remote_dataindex_mrs);
  compact->builder->get_filter_map(compact->current_output()->remote_filter_mrs);
#ifndef NDEBUG
  uint64_t file_size = 0;
  for(auto iter : compact->current_output()->remote_data_mrs){
    file_size += iter.second->length;
  }
#endif
  const uint64_t current_bytes = compact->builder->FileSize();
  compact->current_output()->file_size = current_bytes;
  assert(file_size == current_bytes);
  compact->total_bytes += current_bytes;
  delete compact->builder;
  compact->builder = nullptr;

  // Finish and check for file errors
//  if (s.ok()) {
//    s = compact->outfile->Sync();
//  }
//  if (s.ok()) {
//    s = compact->outfile->Close();
//  }
//  delete compact->outfile;
//  compact->outfile = nullptr;

  if (s.ok() && current_entries > 0) {
    // Verify that the table is usable
//    Iterator* iter =
//        table_cache_->NewIterator(ReadOptions(), output_number, current_bytes);
//    s = iter->status();
//    delete iter;
    if (s.ok()) {
      Log(options_.info_log, "Generated table #%llu@%d: %lld keys, %lld bytes",
          (unsigned long long)output_number, compact->compaction->level(),
          (unsigned long long)current_entries,
          (unsigned long long)current_bytes);
    }
  }
  return s;
}

Status DBImpl::InstallCompactionResults(CompactionState* compact,
                                        std::unique_lock<std::mutex>* lck_p) {
  assert(false);
  Log(options_.info_log, "Compacted %d@%d + %d@%d files => %lld bytes",
      compact->compaction->num_input_files(0), compact->compaction->level(),
      compact->compaction->num_input_files(1), compact->compaction->level() + 1,
      static_cast<long long>(compact->total_bytes));

  // Add compaction outputs
  compact->compaction->AddInputDeletions(compact->compaction->edit());
  const int level = compact->compaction->level();
  if (compact->sub_compact_states.size() == 0){
    for (size_t i = 0; i < compact->outputs.size(); i++) {
      const CompactionOutput& out = compact->outputs[i];
      std::shared_ptr<RemoteMemTableMetaData> meta = std::make_shared<RemoteMemTableMetaData>(0);
      //TODO make all the metadata written into out
      meta->number = out.number;
      meta->file_size = out.file_size;
      meta->smallest = out.smallest;
      meta->largest = out.largest;
      meta->remote_data_mrs = out.remote_data_mrs;
      meta->remote_dataindex_mrs = out.remote_dataindex_mrs;
      meta->remote_filter_mrs = out.remote_filter_mrs;
      compact->compaction->edit()->AddFile(level + 1, meta);
      assert(!meta->UnderCompaction);
    }
  }else{
    for(auto subcompact : compact->sub_compact_states){
      for (size_t i = 0; i < subcompact.outputs.size(); i++) {
        const CompactionOutput& out = subcompact.outputs[i];
        std::shared_ptr<RemoteMemTableMetaData> meta =
            std::make_shared<RemoteMemTableMetaData>(0);
        // TODO make all the metadata written into out
        meta->number = out.number;
        meta->file_size = out.file_size;
        meta->smallest = out.smallest;
        meta->largest = out.largest;
        meta->remote_data_mrs = out.remote_data_mrs;
        meta->remote_dataindex_mrs = out.remote_dataindex_mrs;
        meta->remote_filter_mrs = out.remote_filter_mrs;
        compact->compaction->edit()->AddFile(level + 1, meta);
        assert(!meta->UnderCompaction);
      }
    }
  }
  assert(compact->compaction->edit()->GetNewFilesNum() > 0 );
  lck_p->lock();
  compact->compaction->ReleaseInputs();
  return versions_->LogAndApply(compact->compaction->edit(), 0);
}
Status DBImpl::TryInstallMemtableFlushResults(
    FlushJob* job, VersionSet* vset,
    std::shared_ptr<RemoteMemTableMetaData>& sstable, VersionEdit* edit) {
  autovector<MemTable*> mems = job->mem_vec;
  assert(mems.size() >0);

//  imm_mtx->lock();
#ifndef NDEBUG
  if (mems.size() == 1)
    DEBUG("check 1 memtable installation\n");
#endif
  // Flush was successful
  // Record the status on the memtable object. Either this call or a call by a
  // concurrent flush thread will read the status and write it to manifest.
  {
    std::unique_lock<std::mutex> lck1(FlushPickMTX);
    for (size_t i = 0; i < mems.size(); ++i) {
      // First mark the flushing is finished in the immutables
      mems[i]->MarkFlushed();
      DEBUG_arg("Memtable %p marked as flushed\n", mems[i]);
      mems[i]->sstable = sstable;
    }
  }


  // if some other thread is already committing, then return
  Status s;

  // Retry until all completed flushes are committed. New flushes can finish
  // while the current thread is writing manifest where mutex is released.
//  while (s.ok()) {
  std::unique_lock<std::mutex> lck2(superversion_memlist_mtx);
  auto& memlist = imm_.current_.load()->memlist_;
  // The back is the oldest; if flush_completed_ is not set to it, it means
  // that we were assigned a more recent memtable. The memtables' flushes must
  // be recorded in manifest in order. A concurrent flush thread, who is
  // assigned to flush the oldest memtable, will later wake up and does all
  // the pending writes to manifest, in order.
  if (memlist.empty() || !memlist.back()->CheckFlushFinished()) {
    //Unlock the spinlock and do not write to the version
//      imm_mtx->unlock();
    return s;
  }
  // scan all memtables from the earliest, and commit those
  // (in that order) that have finished flushing. Memtables
  // are always committed in the order that they were created.
  uint64_t batch_file_number = 0;
  size_t batch_count = 0;
//    autovector<VersionEdit*> edit_list;
//    autovector<MemTable*> memtables_to_flush;
  // enumerate from the last (earliest) element to see how many batch finished
  for (auto it = memlist.rbegin(); it != memlist.rend(); ++it) {
    MemTable* m = *it;
    if (!m->CheckFlushFinished()) {
      break;
    }
    assert(m->sstable != nullptr);
    edit->AddFileIfNotExist(0,m->sstable);
    batch_count++;
  }
  if (batch_count == 0){
    return s;
  }
  //TODO: seperate the logic of find out what file to

  DEBUG("try install inner loop\n");
  // we will be changing the version in the next code path,
  // so we better create a new one, since versions are immutable

  imm_.InstallNewVersion();
  size_t batch_count_for_fetch_sub = batch_count;
  MemTableListVersion* current = imm_.current_.load();
  while (batch_count-- > 0) {
    MemTable* m = current->memlist_.back();

    assert(m->sstable != nullptr);
    autovector<MemTable*> dummy_to_delete = autovector<MemTable*>();
    current->Remove(m);
    imm_.UpdateCachedValuesFromMemTableListVersion();
    imm_.ResetTrimHistoryNeeded();
  }
  imm_.current_memtable_num_.fetch_sub(batch_count_for_fetch_sub);
  DEBUG_arg("Install flushing result, current immutable number is %lu\n", imm_.current_memtable_num_.load());


  // First apply locally then apply remotely.
  {
    std::unique_lock<std::mutex> lck(versionset_mtx);
    s = vset->LogAndApply(edit, 0);
    Edit_sync_to_remote(edit, &lck);
  }

#ifndef NDEBUG
//  std::string temp_buf;
//  edit->EncodeTo(&temp_buf);
//  VersionEdit new_edit;
//  new_edit.DecodeFrom(temp_buf);
//  printf("check\n");
#endif
  InstallSuperVersion();
  lck2.unlock();
  job->write_stall_cv_->notify_all();

//  }

  return s;
}
//SuperVersion* DBImpl::GetReferencedSuperVersion(DBImpl* db) {
//  SuperVersion* sv = GetThreadLocalSuperVersion(db);
//  sv->Ref();
//  if (!ReturnThreadLocalSuperVersion(sv)) {
//    // This Unref() corresponds to the Ref() in GetThreadLocalSuperVersion()
//    // when the thread-local pointer was populated. So, the Ref() earlier in
//    // this function still prevents the returned SuperVersion* from being
//    // deleted out from under the caller.
//    sv->Unref();
//  }
//  return sv;
//}
void DBImpl::CleanupSuperVersion(SuperVersion* sv) {
  // Release SuperVersion
  if (sv->Unref()) {
    {
      std::unique_lock<std::mutex> lck(superversion_memlist_mtx);
      sv->Cleanup();
    }
  }
}
bool DBImpl::ReturnThreadLocalSuperVersion(SuperVersion* sv) {
  assert(sv != nullptr);
  // Put the SuperVersion back
  void* expected = SuperVersion::kSVInUse;
  if (local_sv_->CompareAndSwap(static_cast<void*>(sv), expected)) {
    // When we see kSVInUse in the ThreadLocal, we are sure ThreadLocal
    // storage has not been altered and no Scrape has happened. The
    // SuperVersion is still current.
    return true;
  } else {
    // ThreadLocal scrape happened in the process of this GetImpl call (after
    // thread local Swap() at the beginning and before CompareAndSwap()).
    // This means the SuperVersion it holds is obsolete.
    assert(expected == SuperVersion::kSVObsolete);
  }
  return false;
}

void DBImpl::ReturnAndCleanupSuperVersion(SuperVersion* sv) {
  if (!ReturnThreadLocalSuperVersion(sv)) {
    CleanupSuperVersion(sv);
  }
}
SuperVersion* DBImpl::GetThreadLocalSuperVersion() {
  // The SuperVersion is cached in thread local storage to avoid acquiring
  // mutex when SuperVersion does not change since the last use. When a new
  // SuperVersion is installed, the compaction or flush thread cleans up
  // cached SuperVersion in all existing thread local storage. To avoid
  // acquiring mutex for this operation, we use atomic Swap() on the thread
  // local pointer to guarantee exclusive access. If the thread local pointer
  // is being used while a new SuperVersion is installed, the cached
  // SuperVersion can become stale. In that case, the background thread would
  // have swapped in kSVObsolete. We re-check the value at when returning
  // SuperVersion back to thread local, with an atomic compare and swap.
  // The superversion will need to be released if detected to be stale.
  void* ptr = local_sv_->Swap(SuperVersion::kSVInUse);
  // Invariant:
  // (1) Scrape (always) installs kSVObsolete in ThreadLocal storage
  // (2) the Swap above (always) installs kSVInUse, ThreadLocal storage
  // should only keep kSVInUse before ReturnThreadLocalSuperVersion call
  // (if no Scrape happens).
  assert(ptr != SuperVersion::kSVInUse);
  SuperVersion* sv = static_cast<SuperVersion*>(ptr);
  if (sv == SuperVersion::kSVObsolete ||
      sv->version_number != super_version_number_.load()) {
    SuperVersion* sv_to_delete = nullptr;
    // if there is an old superversion unrefer old one and replace it as a new one

    if (sv && sv->Unref()) {
      std::unique_lock<std::mutex> lck(superversion_memlist_mtx);
      // NOTE: underlying resources held by superversion (sst files) might
      // not be released until the next background job.
      sv->Cleanup();
      sv = super_version->Ref();

    } else {
      std::unique_lock<std::mutex> lck(superversion_memlist_mtx);
      sv = super_version->Ref();
    }

//    lck.unlock();
  }
  assert(sv != nullptr);
  return sv;
}




void DBImpl::InstallSuperVersion() {
  SuperVersion* old_superversion = super_version;
  super_version =
      new SuperVersion(mem_, imm_.current(), versions_->current(), &versionset_mtx);
  super_version->Ref();
  ++super_version_number_;
  super_version->version_number = super_version_number_;
  if (old_superversion != nullptr) {
    // Reset SuperVersions cached in thread local storage.
    // This should be done before old_superversion->Unref(). That's to ensure
    // that local_sv_ never holds the last reference to SuperVersion, since
    // it has no means to safely do SuperVersion cleanup.
    ResetThreadLocalSuperVersions();

    if (old_superversion->Unref()) {
      old_superversion->Cleanup();
    }
  }
}
//void DBImpl::Communication_To_Home_Node() {
//  ibv_wc wc[3] = {};
//  env_->rdma_mg->poll_completion(wc, 1, "main", false);
//}
void DBImpl::sync_option_to_remote() {
  std::shared_ptr<RDMA_Manager> rdma_mg = env_->rdma_mg;
  // register the memory block from the remote memory
  RDMA_Request* send_pointer;
  ibv_mr send_mr = {};
  ibv_mr send_mr_ve = {};
  ibv_mr receive_mr = {};
  rdma_mg->Allocate_Local_RDMA_Slot(send_mr, "message");
  rdma_mg->Allocate_Local_RDMA_Slot(send_mr_ve, "version_edit");
  rdma_mg->Allocate_Local_RDMA_Slot(receive_mr, "message");
  *(Options*)send_mr_ve.addr = options_;
  memset((char*)send_mr_ve.addr + sizeof(options_), 1, 1);
  send_pointer = (RDMA_Request*)send_mr.addr;
  send_pointer->command = sync_option;
  send_pointer->content.ive.buffer_size = sizeof(options_);
  send_pointer->reply_buffer = receive_mr.addr;
  send_pointer->rkey = receive_mr.rkey;

  RDMA_Reply* receive_pointer;
  receive_pointer = (RDMA_Reply*)receive_mr.addr;
  //Clear the reply buffer for the sending the request.
  *receive_pointer = {};
  asm volatile ("sfence\n" : : );
  asm volatile ("lfence\n" : : );
  asm volatile ("mfence\n" : : );
//  sleep(1);
  rdma_mg->post_send<RDMA_Request>(&send_mr, std::string("main"));

  ibv_wc wc[2] = {};
  if (rdma_mg->poll_completion(wc, 1, std::string("main"),true)){
    fprintf(stderr, "failed to poll send for remote memory register\n");
    return;
  }
  asm volatile ("sfence\n" : : );
  asm volatile ("lfence\n" : : );
  asm volatile ("mfence\n" : : );
  if(!rdma_mg->poll_reply_buffer(receive_pointer)) // poll the receive for 2 entires
  {
    printf("Reply buffer is %p", receive_pointer->reply_buffer);
    printf("Received is %d", receive_pointer->received);
    printf("receive structure size is %lu", sizeof(RDMA_Reply));
    exit(0);
  }
  //Note: here multiple threads will RDMA_Write the "main" qp at the same time,
  // which means the polling result may not belongs to this thread, but it does not
  // matter in our case because we do not care when will the message arrive at the other side.
  asm volatile ("sfence\n" : : );
  asm volatile ("lfence\n" : : );
  asm volatile ("mfence\n" : : );
  rdma_mg->RDMA_Write(receive_pointer->reply_buffer, receive_pointer->rkey,
                      &send_mr_ve, sizeof(options_) + 1, "main", IBV_SEND_SIGNALED,1);
  rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr,"message");
  rdma_mg->Deallocate_Local_RDMA_Slot(send_mr_ve.addr,"version_edit");
  rdma_mg->Deallocate_Local_RDMA_Slot(receive_mr.addr,"message");
}
void DBImpl::remote_qp_reset(std::string& q_id){
  std::shared_ptr<RDMA_Manager> rdma_mg = env_->rdma_mg;
  RDMA_Request* send_pointer;
  char temp_receive[2];
  char temp_send[] = "Q";
  ibv_mr send_mr = {};
//  ibv_mr receive_mr = {};
  rdma_mg->Allocate_Local_RDMA_Slot(send_mr, "message");
//  rdma_mg->Allocate_Local_RDMA_Slot(receive_mr, "message");
  send_pointer = (RDMA_Request*)send_mr.addr;
  send_pointer->command = qp_reset_;
//  send_pointer->reply_buffer = receive_mr.addr;
//  send_pointer->rkey = receive_mr.rkey;
//  RDMA_Reply* receive_pointer;
//  receive_pointer = (RDMA_Reply*)receive_mr.addr;
  //Clear the reply buffer for the polling.
//  *receive_pointer = {};
  rdma_mg->post_send<RDMA_Request>(&send_mr, q_id);

  ibv_wc wc[2] = {};
  if (rdma_mg->poll_completion(wc, 1, q_id,true)){
    fprintf(stderr, "failed to poll send for remote memory register\n");
    return;
  }
  // Use out of bind socket to sync two sides
  rdma_mg->sock_sync_data(rdma_mg->res->sock_map[q_id.c_str()], 1, temp_send,temp_receive);
//  rdma_mg->poll_reply_buffer(receive_pointer);
//  printf("polled reply buffer\n");
  rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr,"message");
}
void DBImpl::client_message_polling_and_handling_thread(std::string q_id) {

    ibv_qp* qp;
    int rc = 0;
    //NOte: Re-initialize below is very important because DBImple may be
    // initialize serveral times, every time we need to clear the qp.
    std::shared_ptr<RDMA_Manager> rdma_mg = env_->rdma_mg;

//    assert(!shutting_down_.load());
    if (q_id == "read_local"){
      assert(false);// Never comes to here
      qp = static_cast<ibv_qp*>(rdma_mg->qp_local_read->Get());
      if (qp == NULL) {
        //if qp not exist create a new qp
        rdma_mg->Remote_Query_Pair_Connection(q_id);
        qp = static_cast<ibv_qp*>(rdma_mg->qp_local_read->Get());
      }else{
        // if the qp has already been existed re initialize the qp to clear the old WRQs.
        assert(rdma_mg->local_read_qp_info->Get() != nullptr);
//        rdma_mg->modify_qp_to_reset(qp);
//        rdma_mg->connect_qp(qp, q_id);
      }
    }else if (q_id == "write_local_flush"){
      qp = static_cast<ibv_qp*>(rdma_mg->qp_local_write_flush->Get());
      if (qp == NULL) {
        rdma_mg->Remote_Query_Pair_Connection(q_id);
        qp = static_cast<ibv_qp*>(rdma_mg->qp_local_write_flush->Get());
      }else{
        assert(rdma_mg->local_write_flush_qp_info->Get() != nullptr);
//        rdma_mg->modify_qp_to_reset(qp);
//        rdma_mg->connect_qp(qp, q_id);
      }
    }else if (q_id == "write_local_compact"){
      qp = static_cast<ibv_qp*>(rdma_mg->qp_local_write_compact->Get());
      if (qp == NULL) {
        rdma_mg->Remote_Query_Pair_Connection(q_id);
        qp = static_cast<ibv_qp*>(rdma_mg->qp_local_write_compact->Get());
      }else{
        assert(rdma_mg->local_write_compact_qp_info->Get() != nullptr);
//        rdma_mg->modify_qp_to_reset(qp);
//        rdma_mg->connect_qp(qp, q_id);
      }
    } else {
      std::shared_lock<std::shared_mutex> l(rdma_mg->qp_cq_map_mutex);

      if (rdma_mg->res->qp_map.find(q_id) != rdma_mg->res->qp_map.end()){
//        qp = rdma_mg->res->qp_map.at(q_id);
//        printf("qp number before reset is %d\n", qp->qp_num);
//        assert(rdma_mg->res->qp_connection_info.find(q_id)!= rdma_mg->res->qp_connection_info.end());
//        l.unlock();
//        rdma_mg->modify_qp_to_reset(qp);
//        rdma_mg->connect_qp(qp, q_id);
//        printf("qp number after reset is %d\n", qp->qp_num);
      }else{
        l.unlock();
        rdma_mg->Remote_Query_Pair_Connection(q_id);
        l.lock();
        qp = rdma_mg->res->qp_map.at(q_id);
        l.unlock();
      }

    }

    ibv_mr* recv_mr;
    int buffer_counter;
    //TODO: keep the recv mr in rdma manager so that next time we restart
    // the database we can retrieve from the rdma_mg.
    if (rdma_mg->comm_thread_recv_mrs.find(q_id) != rdma_mg->comm_thread_recv_mrs.end()){
      recv_mr = rdma_mg->comm_thread_recv_mrs.at(q_id);
      buffer_counter = rdma_mg->comm_thread_buffer.at(q_id);
    }else{
      // Some where we need to delete the recv_mr in case of memory leak.
      ibv_mr* recv_mr = new ibv_mr[R_SIZE]();
      for(int i = 0; i<R_SIZE; i++){
        rdma_mg->Allocate_Local_RDMA_Slot(recv_mr[i], "message");
      }

      for(int i = 0; i<R_SIZE; i++) {
        rdma_mg->post_receive<RDMA_Request>(&recv_mr[i], q_id);
      }
      buffer_counter = 0;
      rdma_mg->comm_thread_recv_mrs.insert({q_id, recv_mr});
    }
    printf("Start to sync options\n");
    sync_option_to_remote();
    ibv_wc wc[3] = {};
    RDMA_Request receive_msg_buf;
    {
      std::unique_lock<std::mutex> lck(superversion_memlist_mtx);
      check_and_clear_pending_recvWR = true;

    }
    write_stall_cv.notify_all();
    printf("client handling thread\n");
    while (!shutting_down_.load()) {
      if(rdma_mg->try_poll_this_thread_completions(wc, 1, q_id, false)>0){
        memcpy(&receive_msg_buf, recv_mr[buffer_counter].addr, sizeof(RDMA_Request));
//        printf("Buffer counter %d has been used!\n", buffer_counter);
        // copy the pointer of receive buf to a new place because
        // it is the same with send buff pointer.
        if (receive_msg_buf.command == install_version_edit) {
          ((RDMA_Request*) recv_mr[buffer_counter].addr)->command = invalid_command_;
          rdma_mg->post_receive<RDMA_Request>(&recv_mr[buffer_counter], "main");
          install_version_edit_handler(receive_msg_buf, q_id);
        } else {
          printf("corrupt message from client.");
          break;
        }
        // increase the buffer index
        if (buffer_counter== R_SIZE-1 ){
          buffer_counter = 0;
        } else{
          buffer_counter++;
        }
      }

    }

//    remote_qp_reset(q_id);
    rdma_mg->comm_thread_buffer.insert({q_id, buffer_counter});
//    sleep(1);
//    for (int i = 0; i < R_SIZE; ++i) {
//      rdma_mg->Deallocate_Local_RDMA_Slot(recv_mr[i].addr, "message");
//    }
}
void DBImpl::Edit_sync_to_remote(VersionEdit* edit,
                                 std::unique_lock<std::mutex>* version_mtx) {
  //  std::unique_lock<std::shared_mutex> l(main_qp_mutex);
  auto start = std::chrono::high_resolution_clock::now();
  std::shared_ptr<RDMA_Manager> rdma_mg = env_->rdma_mg;
  // register the memory block from the remote memory
  RDMA_Request* send_pointer;
  ibv_mr send_mr = {};
  ibv_mr send_mr_ve = {};
  ibv_mr receive_mr = {};
  rdma_mg->Allocate_Local_RDMA_Slot(send_mr, "message");
  rdma_mg->Allocate_Local_RDMA_Slot(send_mr_ve, "version_edit");

  rdma_mg->Allocate_Local_RDMA_Slot(receive_mr, "message");
  std::string serilized_ve;
  edit->EncodeTo(&serilized_ve);
  assert(serilized_ve.size() <= send_mr_ve.length);
  memcpy(send_mr_ve.addr, serilized_ve.c_str(), serilized_ve.size());
  memset((char*)send_mr_ve.addr + serilized_ve.size(), 1, 1);

  send_pointer = (RDMA_Request*)send_mr.addr;
  send_pointer->command = install_version_edit;
  send_pointer->content.ive.buffer_size = serilized_ve.size();
  send_pointer->reply_buffer = receive_mr.addr;
  send_pointer->rkey = receive_mr.rkey;
  RDMA_Reply* receive_pointer;
  receive_pointer = (RDMA_Reply*)receive_mr.addr;
  //Clear the reply buffer for the polling.
  *receive_pointer = {};
  asm volatile ("sfence\n" : : );
  asm volatile ("lfence\n" : : );
  asm volatile ("mfence\n" : : );
  rdma_mg->post_send<RDMA_Request>(&send_mr, std::string("main"));
  version_mtx->unlock();
  ibv_wc wc[2] = {};
  if (rdma_mg->poll_completion(wc, 1, std::string("main"),true)){
    fprintf(stderr, "failed to poll send for remote memory register\n");
    return;
  }
  asm volatile ("sfence\n" : : );
  asm volatile ("lfence\n" : : );
  asm volatile ("mfence\n" : : );
  if(!rdma_mg->poll_reply_buffer(receive_pointer)) // poll the receive for 2 entires
  {
    printf("Reply buffer is %p", receive_pointer->reply_buffer);
    printf("Received is %d", receive_pointer->received);
    printf("receive structure size is %lu", sizeof(RDMA_Reply));
    exit(0);
  }
  //Note: here multiple threads will RDMA_Write the "main" qp at the same time,
  // which means the polling result may not belongs to this thread, but it does not
  // matter in our case because we do not care when will the message arrive at the other side.
  asm volatile ("sfence\n" : : );
  asm volatile ("lfence\n" : : );
  asm volatile ("mfence\n" : : );
  rdma_mg->RDMA_Write(receive_pointer->reply_buffer, receive_pointer->rkey,
                      &send_mr_ve, serilized_ve.size() + 1, "main", IBV_SEND_SIGNALED,1);
  rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr,"message");
  rdma_mg->Deallocate_Local_RDMA_Slot(send_mr_ve.addr,"version_edit");
  rdma_mg->Deallocate_Local_RDMA_Slot(receive_mr.addr,"message");
  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
  DEBUG_arg("Sync version edit time elapse: %ld us \n", duration.count());

}
void DBImpl::install_version_edit_handler(RDMA_Request request,
                                          std::string client_ip) {

  auto rdma_mg = env_->rdma_mg;
  if (request.content.ive.trival){
    std::unique_lock<std::mutex> lck(versionset_mtx);
    DEBUG("install trival version\n");
    auto f = versions_->current()->FindFileByNumber(request.content.ive.level, request.content.ive.file_number,
                                   request.content.ive.node_id);
    lck.unlock();
    VersionEdit edit;
    edit.RemoveFile(request.content.ive.level, f->number, f->creator_node_id);
    edit.AddFile(request.content.ive.level + 1, f);
    f->level = f->level +1;
    {
      //first superversion then version set.
      std::unique_lock<std::mutex> l(superversion_memlist_mtx);
      {
        std::unique_lock<std::mutex> l1(versionset_mtx);
        versions_->LogAndApply(&edit, request.content.ive.version_id);
      }


#ifndef NDEBUG
      printf("version edit decoded level is %d file number is %zu\n", edit.compactlevel(), edit.GetNewFilesNum() );
#endif

      InstallSuperVersion();
    }


  }else{
    uint8_t check_byte = request.content.ive.check_byte;
    ibv_mr send_mr;
    rdma_mg->Allocate_Local_RDMA_Slot(send_mr, "message");
    RDMA_Reply* send_pointer = (RDMA_Reply*)send_mr.addr;
    send_pointer->content.ive = {};
    ibv_mr edit_recv_mr;
    rdma_mg->Allocate_Local_RDMA_Slot(edit_recv_mr, "version_edit");
    send_pointer->reply_buffer = edit_recv_mr.addr;
    send_pointer->rkey = edit_recv_mr.rkey;
    send_pointer->received = true;
    //TODO: how to check whether the version edit message is ready, we need to know the size of the
    // version edit in the first REQUEST from compute node.
    volatile char* polling_byte = (char*)edit_recv_mr.addr + request.content.ive.buffer_size;
    memset((void*)polling_byte, 0, 1);
    for (int i = 0; i < 100; ++i) {
      if(*polling_byte != 0){
        printf("polling byte error");
        assert(false);
      }
    }
    asm volatile ("sfence\n" : : );
    asm volatile ("lfence\n" : : );
    asm volatile ("mfence\n" : : );
    rdma_mg->RDMA_Write(request.reply_buffer, request.rkey,
                        &send_mr, sizeof(RDMA_Reply),std::move(client_ip), IBV_SEND_SIGNALED,1);
    DEBUG_arg("install non-trival version, version id is %lu\n", request.content.ive.version_id);
    size_t counter = 0;
    while (*(unsigned char*)polling_byte != check_byte){
      _mm_clflush(polling_byte);
      asm volatile ("sfence\n" : : );
      asm volatile ("lfence\n" : : );
      asm volatile ("mfence\n" : : );
      std::fprintf(stderr, "Polling install version handler\r");
      std::fflush(stderr);
      counter++;
    }
    asm volatile ("sfence\n" : : );
    asm volatile ("lfence\n" : : );
    asm volatile ("mfence\n" : : );
//    assert()
#ifndef NDEBUG
    printf("Get the printed result after %zu iteration, received %d, checkbyte is %d\n", counter, send_pointer->received, check_byte);
#endif
    VersionEdit version_edit;
    version_edit.DecodeFrom(
        Slice((char*)edit_recv_mr.addr, request.content.ive.buffer_size), 0);
//    printf("Marker 1\n");
    std::unique_lock<std::mutex> lck(superversion_memlist_mtx);
//    printf("Marker 2\n");
    std::unique_lock<std::mutex> lck1(versionset_mtx);
    versions_->LogAndApply(&version_edit, request.content.ive.version_id);
    lck1.unlock();
#ifndef NDEBUG
    printf("version edit decoded level is %d file number is %zu\n", version_edit.compactlevel(), version_edit.GetNewFilesNum());
#endif

    InstallSuperVersion();
    lck.unlock();
    write_stall_cv.notify_all();
    rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, "message");
    rdma_mg->Deallocate_Local_RDMA_Slot(edit_recv_mr.addr, "version_edit");
  }

}
void DBImpl::ResetThreadLocalSuperVersions() {
  autovector<void*> sv_ptrs;
  // If there the default pointer for local_sv_ s are nullptr
  local_sv_->Scrape(&sv_ptrs, SuperVersion::kSVObsolete);
  for (auto ptr : sv_ptrs) {
    assert(ptr);
    if (ptr == SuperVersion::kSVInUse || ptr == nullptr) {
      continue;
    }
    auto sv = static_cast<SuperVersion*>(ptr);
    bool was_last_ref __attribute__((__unused__));
    was_last_ref = sv->Unref();
    // sv couldn't have been the last reference because
    // ResetThreadLocalSuperVersions() is called before
    // unref'ing super_version_.
    assert(!was_last_ref);
  }
}

//void DBImpl::InstallSuperVersion() {
//    SuperVersion* old = super_version;
//  super_version.store(new SuperVersion(mem_,imm_.current(), versions_->current()));
//  super_version.load()->Ref();
//#ifndef NDEBUG
//  if (super_version.load()->mem == nullptr){
//    MemTable* mem = mem_.load();
//  }
//#endif
//  if (old != nullptr){
//    old->Unref();//First replace the superverison then Unref().
//  }else{
//    DEBUG("Check not Unref\n");
//  }
//}
Status DBImpl::DoCompactionWorkWithSubcompaction(CompactionState* compact) {
  Compaction* c = compact->compaction;
  c->GenSubcompactionBoundaries();
  auto boundaries = c->GetBoundaries();
  auto sizes = c->GetSizes();
  assert(boundaries->size() == sizes->size() - 1);
//  int subcompaction_num = std::min((int)c->GetBoundariesNum(), config::MaxSubcompaction);
  if (boundaries->size()<=options_.MaxSubcompaction){
    for (size_t i = 0; i <= boundaries->size(); i++) {
      Slice* start = i == 0 ? nullptr : &(*boundaries)[i - 1];
      Slice* end = i == boundaries->size() ? nullptr : &(*boundaries)[i];
      compact->sub_compact_states.emplace_back(c, start, end, (*sizes)[i]);
    }
  }else{
    //Get output level total file size.
    uint64_t sum = c->GetFileSizesForLevel(1);
    std::list<int> small_files{};
    for (int i=0; i< sizes->size(); i++) {
      if ((*sizes)[i] <= options_.max_file_size/4)
        small_files.push_back(i);
    }
    int big_files_num = boundaries->size() - small_files.size();
    int files_per_subcompaction = big_files_num/options_.MaxSubcompaction + 1;//Due to interger round down, we need add 1.
    double mean = sum * 1.0 / options_.MaxSubcompaction;
    for (size_t i = 0; i <= boundaries->size(); i++) {
      size_t range_size = (*sizes)[i];
      Slice* start = i == 0 ? nullptr : &(*boundaries)[i - 1];
      int files_counter = range_size <= options_.max_file_size/4 ? 0 : 1;// count this file.
      // TODO(Ruihong) make a better strategy to group the boundaries.
      //Version 1
//      while (i!=boundaries->size() && range_size < mean &&
//             range_size + (*sizes)[i+1] <= mean + 3*options_.max_file_size/4){
//        i++;
//        range_size += (*sizes)[i];
//      }
      //Version 2
      while (i!=boundaries->size() &&
             (files_counter<files_per_subcompaction ||(*sizes)[i+1] <= options_.max_file_size/4)){
        i++;
        size_t this_file_size = (*sizes)[i];
        range_size += this_file_size;
        // Only increase the file counter when add big file.
        if (this_file_size >= options_.max_file_size/4)
          files_counter++;
      }
      Slice* end = i == boundaries->size() ? nullptr : &(*boundaries)[i];
      compact->sub_compact_states.emplace_back(c, start, end, range_size);
    }

  }
  printf("Subcompaction number is %zu", compact->sub_compact_states.size());
  const size_t num_threads = compact->sub_compact_states.size();
  assert(num_threads > 0);
  const uint64_t start_micros = env_->NowMicros();

  // Launch a thread for each of subcompactions 1...num_threads-1
  std::vector<port::Thread> thread_pool;
  thread_pool.reserve(num_threads - 1);
  for (size_t i = 1; i < compact->sub_compact_states.size(); i++) {
    thread_pool.emplace_back(&DBImpl::ProcessKeyValueCompaction, this,
                             &compact->sub_compact_states[i]);
  }

  // Always schedule the first subcompaction (whether or not there are also
  // others) in the current thread to be efficient with resources
  ProcessKeyValueCompaction(&compact->sub_compact_states[0]);
  for (auto& thread : thread_pool) {
    thread.join();
  }
  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros;
  for (int which = 0; which < 2; which++) {
    for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
      stats.bytes_read += compact->compaction->input(which, i)->file_size;
    }
  }
  for (auto iter : compact->sub_compact_states) {
    for (size_t i = 0; i < iter.outputs.size(); i++) {
      stats.bytes_written += iter.outputs[i].file_size;
    }
  }

// TODO: we can remove this lock.


  Status status;
  {
    std::unique_lock<std::mutex> l(superversion_memlist_mtx, std::defer_lock);
    status = InstallCompactionResults(compact, &l);
    InstallSuperVersion();
  }


  if (!status.ok()) {
    RecordBackgroundError(status);
  }
  VersionSet::LevelSummaryStorage tmp;
  Log(options_.info_log, "compacted to: %s", versions_->LevelSummary(&tmp));
  // NOtifying all the waiting threads.
  write_stall_cv.notify_all();
  return status;
}

void DBImpl::ProcessKeyValueCompaction(SubcompactionState* sub_compact){
  assert(sub_compact->builder == nullptr);
  //Start and End are userkeys.
  Slice* start = sub_compact->start;
  Slice* end = sub_compact->end;
  if (snapshots_.empty()) {
    sub_compact->smallest_snapshot = versions_->LastSequence();
  } else {
    sub_compact->smallest_snapshot = snapshots_.oldest()->sequence_number();
  }

  Iterator* input = versions_->MakeInputIterator(sub_compact->compaction);

  // Release mutex while we're actually doing the compaction work
//  undefine_mutex.Unlock();
  if (start != nullptr) {
    InternalKey start_internal(*start, kMaxSequenceNumber, kValueTypeForSeek);
    //tofix(ruihong): too much data copy for the seek here!
    input->Seek(start_internal.Encode());
    // The sstable range is (start, end]
    input->Next();
  } else {
    input->SeekToFirst();
  }
#ifndef NDEBUG
  int Not_drop_counter = 0;
  int number_of_key = 0;
#endif
  Status status;
  // TODO: try to create two ikey for parsed key, they can in turn represent the current user key
  //  and former one, which can save the data copy overhead.
  ParsedInternalKey ikey;
  std::string current_user_key;
  bool has_current_user_key = false;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
  Slice key;
  assert(input->Valid());
#ifndef NDEBUG
  printf("first key is %s", input->key().ToString().c_str());
#endif
  while (input->Valid() && !shutting_down_.load(std::memory_order_acquire)) {
    key = input->key();

//    assert(key.data()[0] == '0');
    //Check whether the output file have too much overlap with level n + 2
    if (sub_compact->compaction->ShouldStopBefore(key) &&
        sub_compact->builder != nullptr) {
      //TODO: record the largest key as the last ikey, find a more efficient way to record
      // the last key of SSTable.
      sub_compact->current_output()->largest.SetFrom(ikey);
      status = FinishCompactionOutputFile(sub_compact, input);
      if (!status.ok()) {
        break;
      }
    }
    // key merged below!!!
    // Handle key/value, add to state, etc.
    bool drop = false;
    if (!ParseInternalKey(key, &ikey)) {
      // Do not hide error keys
      current_user_key.clear();
      has_current_user_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
    } else {
      if (!has_current_user_key ||
          user_comparator()->Compare(ikey.user_key, Slice(current_user_key)) !=
          0) {
        // First occurrence of this user key
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
        has_current_user_key = true;
        last_sequence_for_key = kMaxSequenceNumber;
        // this will result in the key not drop, next if will always be false because of
        // the last_sequence_for_key.
      }

      if (last_sequence_for_key <= sub_compact->smallest_snapshot) {
        // Hidden by an newer entry for same user key

        drop = true;  // (A)
      }
//      else if (ikey.type == kTypeDeletion &&
//                 ikey.sequence <= sub_compact->smallest_snapshot &&
//                 sub_compact->compaction->IsBaseLevelForKey(ikey.user_key)) {
//        // TOTHINK(0ruihong) :what is this for?
//        //  Generally delete can only be deleted when there is definitely no file contain the
//        //  same key in the upper level.
//        // For this user key:
//        // (1) there is no data in higher levels
//        // (2) data in lower levels will have larger sequence numbers
//        // (3) data in layers that are being compacted here and have
//        //     smaller sequence numbers will be dropped in the next
//        //     few iterations of this loop (by rule (A) above).
//        // Therefore this deletion marker is obsolete and can be dropped.
//        drop = true;
//      }

      last_sequence_for_key = ikey.sequence;

    }
#ifndef NDEBUG
    number_of_key++;
#endif
    if (!drop) {
      // Open output file if necessary
      if (sub_compact->builder == nullptr) {
        status = OpenCompactionOutputFile(sub_compact);
        if (!status.ok()) {
          break;
        }
      }
      if (sub_compact->builder->NumEntries() == 0) {
        sub_compact->current_output()->smallest.DecodeFrom(key);
      }
#ifndef NDEBUG
      Not_drop_counter++;
#endif
      sub_compact->builder->Add(key, input->value());
//      assert(key.data()[0] == '0');
      // Close output file if it is big enough
      if (sub_compact->builder->FileSize() >=
          sub_compact->compaction->MaxOutputFileSize()) {
//        assert(key.data()[0] == '0');
        sub_compact->current_output()->largest.DecodeFrom(key);
        status = FinishCompactionOutputFile(sub_compact, input);
        if (!status.ok()) {
          break;
        }
      }
    }
    if (end != nullptr &&
        user_comparator()->Compare(ExtractUserKey(key), *end) >= 0) {
      break;
    }
//    assert(key.data()[0] == '0');
    input->Next();
    //NOTE(ruihong): When the level iterator is invalid it will be deleted and then the key will
    // be invalid also.
//    assert(key.data()[0] == '0');

  }
//  reinterpret_cast<TimberSaw::MergingIterator>
  // You can not call prev here because the iterator is not valid any more
//  input->Prev();
//  assert(input->Valid());
#ifndef NDEBUG
  printf("For compaction, Total number of key touched is %d, KV left is %d\n", number_of_key,
         Not_drop_counter);
#endif
//  assert(key.data()[0] == '0');
  if (status.ok() && shutting_down_.load(std::memory_order_acquire)) {
    status = Status::IOError("Deleting DB during compaction");
  }
  if (status.ok() && sub_compact->builder != nullptr) {
//    assert(key.data()[0] == '0');

    sub_compact->current_output()->largest.DecodeFrom(key);// The SSTable for subcompaction range will be (start, end]
    status = FinishCompactionOutputFile(sub_compact, input);
  }
  if (status.ok()) {
    status = input->status();
  }
  delete input;
//  input = nullptr;
}
Status DBImpl::DoCompactionWork(CompactionState* compact) {
  const uint64_t start_micros = env_->NowMicros();
  int64_t imm_micros = 0;  // Micros spent doing imm_ compactions

  Log(options_.info_log, "Compacting %d@%d + %d@%d files",
      compact->compaction->num_input_files(0), compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1);

  assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
  assert(compact->builder == nullptr);
//  assert(compact->outfile == nullptr);
  if (snapshots_.empty()) {
    compact->smallest_snapshot = versions_->LastSequence();
  } else {
    compact->smallest_snapshot = snapshots_.oldest()->sequence_number();
  }

  Iterator* input = versions_->MakeInputIterator(compact->compaction);

  // Release mutex while we're actually doing the compaction work
//  undefine_mutex.Unlock();

  input->SeekToFirst();
#ifndef NDEBUG
  int Not_drop_counter = 0;
  int number_of_key = 0;
#endif
  Status status;
  // TODO: try to create two ikey for parsed key, they can in turn represent the current user key
  //  and former one, which can save the data copy overhead.
  ParsedInternalKey ikey;
  std::string current_user_key;
  bool has_current_user_key = false;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
  Slice key;
  assert(input->Valid());
#ifndef NDEBUG
  printf("first key is %s", input->key().ToString().c_str());
#endif
  while (input->Valid() && !shutting_down_.load(std::memory_order_acquire)) {
    key = input->key();
//    assert(key.data()[0] == '0');
    //Check whether the output file have too much overlap with level n + 2
    if (compact->compaction->ShouldStopBefore(key) &&
        compact->builder != nullptr) {
      status = FinishCompactionOutputFile(compact, input);
      if (!status.ok()) {
        break;
      }
    }
    // key merged below!!!
    // Handle key/value, add to state, etc.
    bool drop = false;
    if (!ParseInternalKey(key, &ikey)) {
      // Do not hide error keys
      current_user_key.clear();
      has_current_user_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
    } else {
      if (!has_current_user_key ||
          user_comparator()->Compare(ikey.user_key, Slice(current_user_key)) !=
              0) {
        // First occurrence of this user key
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
        has_current_user_key = true;
        last_sequence_for_key = kMaxSequenceNumber;
        // this will result in the key not drop, next if will always be false because of
        // the last_sequence_for_key.
      }
      //TODO: Can we replace this with if (last_sequence_for_key != kMaxSequenceNumber)
      if (last_sequence_for_key <= compact->smallest_snapshot) {
        // Hidden by an newer entry for same user key

        drop = true;  // (A)
      }
//      else if (ikey.type == kTypeDeletion &&
//                 ikey.sequence <= compact->smallest_snapshot &&
//                 compact->compaction->IsBaseLevelForKey(ikey.user_key)) {
//        // TOTHINK(0ruihong) :what is this for?
//        //  Generally delete can only be deleted when there is definitely no file contain the
//        //  same key in the upper level.
//        // For this user key:
//        // (1) there is no data in higher levels
//        // (2) data in lower levels will have larger sequence numbers
//        // (3) data in layers that are being compacted here and have
//        //     smaller sequence numbers will be dropped in the next
//        //     few iterations of this loop (by rule (A) above).
//        // Therefore this deletion marker is obsolete and can be dropped.
//        drop = true;
//      }

      last_sequence_for_key = ikey.sequence;
    }
#ifndef NDEBUG
    number_of_key++;
#endif
    if (!drop) {
      // Open output file if necessary
      if (compact->builder == nullptr) {
        status = OpenCompactionOutputFile(compact);
        if (!status.ok()) {
          break;
        }
      }
      if (compact->builder->NumEntries() == 0) {
        compact->current_output()->smallest.DecodeFrom(key);
      }
#ifndef NDEBUG
      Not_drop_counter++;
#endif
      compact->builder->Add(key, input->value());
//      assert(key.data()[0] == '0');
      // Close output file if it is big enough
      if (compact->builder->FileSize() >=
          compact->compaction->MaxOutputFileSize()) {
//        assert(key.data()[0] == '0');
        compact->current_output()->largest.DecodeFrom(key);
        status = FinishCompactionOutputFile(compact, input);
        if (!status.ok()) {
          break;
        }
      }
    }
//    assert(key.data()[0] == '0');
    input->Next();
    //NOTE(ruihong): When the level iterator is invalid it will be deleted and then the key will
    // be invalid also.
//    assert(key.data()[0] == '0');
  }
//  reinterpret_cast<TimberSaw::MergingIterator>
  // You can not call prev here because the iterator is not valid any more
//  input->Prev();
//  assert(input->Valid());
#ifndef NDEBUG
  printf("For compaction, Total number of key touched is %d, KV left is %d\n", number_of_key,
         Not_drop_counter);
#endif
//  assert(key.data()[0] == '0');
  if (status.ok() && shutting_down_.load(std::memory_order_acquire)) {
    status = Status::IOError("Deleting DB during compaction");
  }
  if (status.ok() && compact->builder != nullptr) {
//    assert(key.data()[0] == '0');
    compact->current_output()->largest.DecodeFrom(key);
    status = FinishCompactionOutputFile(compact, input);
  }
  if (status.ok()) {
    status = input->status();
  }
  delete input;
  input = nullptr;

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros - imm_micros;
  for (int which = 0; which < 2; which++) {
    for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
      stats.bytes_read += compact->compaction->input(which, i)->file_size;
    }
  }
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    stats.bytes_written += compact->outputs[i].file_size;
  }
  // TODO: we can remove this lock.
  undefine_mutex.Lock();
  stats_[compact->compaction->level() + 1].Add(stats);

  if (status.ok()) {
    std::unique_lock<std::mutex> l(superversion_memlist_mtx, std::defer_lock);
    status = InstallCompactionResults(compact, &l);
    InstallSuperVersion();
  }
  undefine_mutex.Unlock();
  if (!status.ok()) {
    RecordBackgroundError(status);
  }
  VersionSet::LevelSummaryStorage tmp;
  Log(options_.info_log, "compacted to: %s", versions_->LevelSummary(&tmp));
  // NOtifying all the waiting threads.
  write_stall_cv.notify_all();
  return status;
}

namespace {

struct IterState {
  port::Mutex* const mu;
  Version* const version GUARDED_BY(mu);
  MemTable* const mem GUARDED_BY(mu);
//  MemTable* const imm GUARDED_BY(mu);
  MemTableListVersion* imm_version;
  IterState(port::Mutex* mutex, MemTable* mem, MemTableListVersion* imm_version, Version* version)
      : mu(mutex), version(version), mem(mem), imm_version(imm_version) {}
};

static void CleanupIteratorState(void* arg1, void* arg2) {
  IterState* state = reinterpret_cast<IterState*>(arg1);
  state->mu->Lock();
  state->mem->Unref();
  if (state->imm_version != nullptr) state->imm_version->Unref();
  state->version->Unref(0);
  state->mu->Unlock();
  delete state;
}

}  // anonymous namespace

Iterator* DBImpl::NewInternalIterator(const ReadOptions& options,
                                      SequenceNumber* latest_snapshot,
                                      uint32_t* seed) {
  SequenceNumber snapshot;
  if (options.snapshot != nullptr) {
    snapshot =
        static_cast<const SnapshotImpl*>(options.snapshot)->sequence_number();
  } else {
    snapshot = versions_->LastSequence();
  }

  auto sv = GetThreadLocalSuperVersion();

  MemTable* mem = sv->mem;
  MemTableListVersion* imm = sv->imm;
  Version* current = sv->current;

  //  if (sv != nullptr){
  //    sv->Ref();
  //    if (sv == super_version.load()){// there is no superversion change between.
  //      MemTable* mem = sv->mem;
  //      MemTableListVersion* imm = sv->imm;
  //      Version* current = sv->current;
  //      mem->Ref();
  //      if (imm != nullptr) imm->Ref();
  //      current->Ref();
  //    }else{
  //      sv->Unref();
  //    }
  //
  //  }
  bool have_stat_update = false;
  Version::GetStats stats;
  // Collect together all needed child iterators
  std::vector<Iterator*> list;
  list.push_back(mem->NewIterator());
  mem->Ref();
//  imm
  imm->AddIteratorsToList(&list);
  versions_->current()->AddIterators(options, &list);
  Iterator* internal_iter =
      NewMergingIterator(&internal_comparator_, &list[0], list.size());
  versions_->current()->Ref(0);

  IterState* cleanup = new IterState(&undefine_mutex, mem_, imm, versions_->current());
  internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, nullptr);

  *seed = ++seed_;
//  undefine_mutex.Unlock();
  ReturnAndCleanupSuperVersion(sv);
  return internal_iter;
}

Iterator* DBImpl::TEST_NewInternalIterator() {
  SequenceNumber ignored;
  uint32_t ignored_seed;
  return NewInternalIterator(ReadOptions(), &ignored, &ignored_seed);
}

int64_t DBImpl::TEST_MaxNextLevelOverlappingBytes() {
  MutexLock l(&undefine_mutex);
  return versions_->MaxNextLevelOverlappingBytes();
}

Status DBImpl::Get(const ReadOptions& options, const Slice& key,
                   std::string* value) {
  Status s;
  SequenceNumber snapshot;
  if (options.snapshot != nullptr) {
    snapshot =
        static_cast<const SnapshotImpl*>(options.snapshot)->sequence_number();
  } else {
    snapshot = versions_->LastSequence();
  }

  auto sv = GetThreadLocalSuperVersion();

  MemTable* mem = sv->mem;
  MemTableListVersion* imm = sv->imm;
  Version* current = sv->current;

//  if (sv != nullptr){
//    sv->Ref();
//    if (sv == super_version.load()){// there is no superversion change between.
//      MemTable* mem = sv->mem;
//      MemTableListVersion* imm = sv->imm;
//      Version* current = sv->current;
//      mem->Ref();
//      if (imm != nullptr) imm->Ref();
//      current->Ref();
//    }else{
//      sv->Unref();
//    }
//
//  }
  bool have_stat_update = false;
  Version::GetStats stats;

  // Unlock while reading from files and memtables
  {
//    undefine_mutex.Unlock();
    // First look in the memtable, then in the immutable memtable (if any).
    LookupKey lkey(key, snapshot);

    if (mem->Get(lkey, value, &s)) {
      // Done
    } else if (imm != nullptr && imm->Get(lkey, value, &s)) {
      // Done
    } else {
      s = current->Get(options, lkey, value, &stats);
      have_stat_update = true;
    }
//    undefine_mutex.Lock();
  }

  if (have_stat_update && current->UpdateStats(stats)) {
    MaybeScheduleFlushOrCompaction();
  }
  //TOthink: whether we need a lock for the dereference
  ReturnAndCleanupSuperVersion(sv);
  return s;
}

Iterator* DBImpl::NewIterator(const ReadOptions& options) {
  SequenceNumber latest_snapshot;
  uint32_t seed;
  Iterator* iter = NewInternalIterator(options, &latest_snapshot, &seed);
  return NewDBIterator(this, user_comparator(), iter,
                       (options.snapshot != nullptr
                            ? static_cast<const SnapshotImpl*>(options.snapshot)
                                  ->sequence_number()
                            : latest_snapshot),
                       seed);
}

void DBImpl::RecordReadSample(Slice key) {
  MutexLock l(&undefine_mutex);
  if (versions_->current()->RecordReadSample(key)) {
    MaybeScheduleFlushOrCompaction();
  }
}

const Snapshot* DBImpl::GetSnapshot() {
  MutexLock l(&undefine_mutex);
  return snapshots_.New(versions_->LastSequence());
}

void DBImpl::ReleaseSnapshot(const Snapshot* snapshot) {
  MutexLock l(&undefine_mutex);
  snapshots_.Delete(static_cast<const SnapshotImpl*>(snapshot));
}

// Convenience methods
Status DBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val) {
  return DB::Put(o, key, val);
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  return DB::Delete(options, key);
}
//
//Status DBImpl::Write(const WriteOptions& options, WriteBatch* updates) {
//  Writer w(&undefine_mutex);
//  w.batch = updates;
//  w.sync = options.sync;
//  w.done = false;
//
//  MutexLock l(&undefine_mutex);
//  writers_.push_back(&w);
//  while (!w.done && &w != writers_.front()) {
//    w.cv.Wait();
//  }
//  if (w.done) {
//    return w.status;
//  }
//
//  // May temporarily Unlock and wait.
//  Status status = MakeRoomForWrite(updates == nullptr);
//  uint64_t last_sequence = versions_->LastSequence();
//  Writer* last_writer = &w;
//  if (status.ok() && updates != nullptr) {  // nullptr batch is for compactions
//    // TODO: Remove all the Lock, use fettch and add to atomically increase the
//    // TODO: sequence num. Use concurrent write in the Rocks DB to write
//    // TODO:  skiplist concurrently. NO log is needed as well.
//    WriteBatch* write_batch = BuildBatchGroup(&last_writer);
//    WriteBatchInternal::SetSequence(write_batch, last_sequence + 1);
//    last_sequence += WriteBatchInternal::Count(write_batch);
//
//    // Add to log and apply to memtable.  We can release the Lock
//    // during this phase since &w is currently responsible for logging
//    // and protects against concurrent loggers and concurrent writes
//    // into mem_.
//    {
//      undefine_mutex.Unlock();
//      status = log_->AddRecord(WriteBatchInternal::Contents(write_batch));
//      bool sync_error = false;
//      if (status.ok() && options.sync) {
//        status = logfile_->Sync();
//        if (!status.ok()) {
//          sync_error = true;
//        }
//      }
//      if (status.ok()) {
//        status = WriteBatchInternal::InsertInto(write_batch, mem_);
//      }
//      undefine_mutex.Lock();
//      if (sync_error) {
//        // The state of the log file is indeterminate: the log record we
//        // just added may or may not show up when the DB is re-opened.
//        // So we force the DB into a mode where all future writes fail.
//        RecordBackgroundError(status);
//      }
//    }
//    if (write_batch == tmp_batch_) tmp_batch_->Clear();
//
//    versions_->SetLastSequence(last_sequence);
//  }
//
//  while (true) {
//    Writer* ready = writers_.front();
//    writers_.pop_front();
//    if (ready != &w) {
//      ready->status = status;
//      ready->done = true;
//      ready->cv.Signal();
//    }
//    if (ready == last_writer) break;
//  }
//
//  // Notify new head of write queue
//  if (!writers_.empty()) {
//    writers_.front()->cv.Signal();
//  }
//
//  return status;
//}
Status DBImpl::Write(const WriteOptions& options, WriteBatch* updates) {
//  Writer w(&undefine_mutex);
//  w.batch = updates;
//  w.sync = options.sync;
//  w.done = false;



  // May temporarily Unlock and wait.
//  Status status = Status::OK();
#ifdef TIMEPRINT
  auto start = std::chrono::high_resolution_clock::now();
  auto total_start = std::chrono::high_resolution_clock::now();
#endif
  size_t kv_num = WriteBatchInternal::Count(updates);
  assert(kv_num == 1);
  uint64_t sequence = versions_->AssignSequnceNumbers(kv_num);
  //todo: remove
//  kv_counter0.fetch_add(1);
  MemTable* mem;
  Status status = PickupTableToWrite(updates == nullptr, sequence, mem);
#ifdef TIMEPRINT
  auto stop = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
  std::printf("preprocessing, time elapse is %zu\n",  duration.count());
#endif
//    size_t seq_count = 1;
//  spin_mutex.Lock();
//  uint64_t sequence = versions_->LastSequence_nonatomic();
//  versions_->SetLastSequence_nonatomic(sequence+seq_count);
//  spin_mutex.Unlock();
//  uint64_t sequence = 10;
  //TOTHINK: what if a write with a higher seq first go outside MakeRoomForwrite,
  // and it is supposed to write to the new memtable which has not been created yet.
  // hint how about set the metable barrier as seq_num rather than memory size?
#ifdef TIMEPRINT
  start = std::chrono::high_resolution_clock::now();
#endif
  if (status.ok()) {  // nullptr batch is for compactions
    assert(updates != nullptr);
    WriteBatchInternal::SetSequence(updates, sequence);

//    if (sequence >= mem_.load()->GetFirstseq()) {
//      status = WriteBatchInternal::InsertInto(updates, mem_);
//    }else{
//      // TODO: some write may have write to immtable, the immutable need to be notified
//      // when all the outgoing write on this table have finished and flush to storage
//      status = WriteBatchInternal::InsertInto(updates, imm_);
//    }
    assert(sequence <= mem->Getlargest_seq_supposed() && sequence >= mem->GetFirstseq());
    status = WriteBatchInternal::InsertInto(updates, mem);
    mem->increase_seq_count(kv_num);
  }else{
    printf("Weird status not OK");
    assert(0==1);
  }
//  kv_counter1.fetch_add(1);
//  if (mem_switching){}
//  thread_ready_num++;
//  printf("thread ready %d\n", thread_ready_num);
//  if (thread_ready_num >= thread_num) {
//    cv.notify_all();
//  }
#ifdef TIMEPRINT
  stop = std::chrono::high_resolution_clock::now();
  duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
  auto total_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - total_start);
  std::printf("Real insert to memtable, time elapse is %zu\n",  duration.count());
  std::printf("total time, time elapse is %zu\n",  total_duration.count());
#endif
//  printf("A value has been inserted\n");
  return status;
}

// seldom Lock
//// TOTHINK The write batch should not too large. other wise the wait function may
//// memtable could overflow even before the actual write.
Status DBImpl::PickupTableToWrite(bool force, uint64_t seq_num, MemTable*& mem_r) {
  Status s = Status::OK();
  //Get a snapshot it is vital for the CAS but not vital for the wait logic.
  mem_r = mem_.load();
  // First check whether we need to switch the table, we do not Lock here, because
  // most of the time the memtable will not be switched. we will Lock inside and
  // get the table
  bool delayed = false;
  //TODO(RUIHONG): Avoid lock twice when swithing the memtable.
  while(seq_num > mem_r->Getlargest_seq_supposed()){
    //before switch the table we need to check whether there is enough room
    // for a new table.

    size_t level0_filenum = versions_->NumLevelFiles(0);
    if (imm_.current_memtable_num() >= config::Immutable_StopWritesTrigger
        || level0_filenum >= config::kL0_StopWritesTrigger) {
      // We have filled up the current memtable, but the previous
      // one is still being compacted, so we wait.
      // the wait will never get signalled.

      // We check the imm again in the while loop, because the state may have already been changed before you acquire the Lock.
      std::unique_lock<std::mutex> lck(superversion_memlist_mtx);
//      imm_mtx.lock();
      Log(options_.info_log, "Current memtable full; waiting...\n");
      mem_r = mem_.load();
      while ((imm_.current_memtable_num() >= config::Immutable_StopWritesTrigger || versions_->NumLevelFiles(0) >=
             config::kL0_StopWritesTrigger) && seq_num > mem_r->Getlargest_seq_supposed()) {
        assert(seq_num > mem_r->GetFirstseq());
//        std::cout << "Writer is going to wait current immutable number " << (imm_.current_memtable_num()) << " Level 0 file number "
//                  << (versions_->NumLevelFiles(0)) <<std::endl;
        write_stall_cv.wait(lck);
//        printf("thread was waked up\n");
        mem_r = mem_.load();
      }
//      imm_mtx.unlock();
    } else if(level0_filenum > config::kL0_SlowdownWritesTrigger && !delayed){
      env_->SleepForMicroseconds(5);
      delayed = true;
    }else{
      std::unique_lock<std::mutex> l(superversion_memlist_mtx);
//      assert(locked == false);
#ifndef NDEBUG
      while(true){
        if (!locked)
          break;
      }

      locked = true;
#endif
      mem_r = mem_.load();
      //After aquire the lock check the status again
      if (imm_.current_memtable_num() <= config::Immutable_StopWritesTrigger&&
          versions_->NumLevelFiles(0) <= config::kL0_StopWritesTrigger &&
          seq_num > mem_r->Getlargest_seq_supposed()){
        assert(versions_->PrevLogNumber() == 0);
        MemTable* temp_mem = new MemTable(internal_comparator_);
        uint64_t last_mem_seq = mem_r->Getlargest_seq_supposed();
        temp_mem->SetFirstSeq(last_mem_seq+1);
        // starting from this sequenctial number, the data should write the the new memtable
        // set the immutable as seq_num - 1
        temp_mem->SetLargestSeq(last_mem_seq + MEMTABLE_SEQ_SIZE);
        temp_mem->Ref();
        mem_r->SetFlushState(MemTable::FLUSH_REQUESTED);
        mem_.store(temp_mem);
        //set the flush flag for imm
        assert(imm_.current_memtable_num() <= config::Immutable_StopWritesTrigger);
        imm_.Add(mem_r);
        has_imm_.store(true, std::memory_order_release);
        InstallSuperVersion();
        // if we have create a new table then the new table will definite be
        // the table we will write.
        mem_r = temp_mem;
//        imm_mtx.unlock();
        MaybeScheduleFlushOrCompaction();
#ifndef NDEBUG
        locked = false;
#endif
        return s;
      }
#ifndef NDEBUG
      locked = false;
#endif
//      imm_mtx.unlock();
      l.unlock();
    }
    mem_r = mem_.load();
    // For the safety concern (such as the thread get context switch)
    // the mem_ may not be the one this table should write, need to go through
    // the table searching procedure below.
  }
  //if not which table should this writer need to write?
  while(true){
    if (seq_num >= mem_r->GetFirstseq() && seq_num <= mem_r->Getlargest_seq_supposed()){
      return s;
    }else {
      // get the snapshot for imm then check it so that this memtable pointer is guarantee
      // to be the one this thread want.
      // TODO: use imm_mtx to control the access.
      std::unique_lock<std::mutex> l(superversion_memlist_mtx);
      mem_r = imm_.PickMemtablesSeqBelong(seq_num);
      if (mem_r != nullptr)
        return s;
    }
  }
}
// TOTHINK The write batch should not too large. other wise the wait function may
// memtable could overflow even before the actual write.
// ---------------Lock free----------------
//Status DBImpl::PickupTableToWrite(bool force, uint64_t seq_num, MemTable*& mem_r) {
//  Status s = Status::OK();
//  //Get a snapshot it is vital for the CAS but not vital for the wait logic.
//  mem_r = mem_.load();
//  size_t counter = 0;
//  int sleep_counter =0;
//  // First check whether we need to switch the table.
//  while(seq_num > mem_r->Getlargest_seq_supposed()){
//    //before switch the table we need to check whether there is enough room
//    // for a new table.
//    if (imm_.load() != nullptr) {
//      // We have filled up the current memtable, but the previous
//      // one is still being compacted, so we wait.
//      // the wait will never get signalled.
//
//      // We check the imm again in the while loop, because the state may have already been changed before you acquire the Lock.
////      undefine_mutex.Lock();
////      Log(options_.info_log, "Current memtable full; waiting...\n");
////      mem_r = mem_.load();
////      while (imm_.load() != nullptr && seq_num > mem_r->Getlargest_seq_supposed()) {
//        assert(seq_num > mem_r->GetFirstseq());
////        usleep(1);
//        counter++;
//
//        if (counter>200){
//          if (sleep_counter % 1000 == 0)
//            printf("sleep counter for flush waiting is %d\n", sleep_counter);
//          sleep_counter++;
//
//
//          env_->SleepForMicroseconds(10);
////          usleep(10);
//          counter = 0;
//        }
//
////        mem_r = mem_.load();
////      }
////      undefine_mutex.Unlock();
//    }else if(versions_->NumLevelFiles(0) >=
//             config::kL0_StopWritesTrigger){
//      // if level 0 is contain too much files, just wait here.
//      if (counter>200){
//        if (sleep_counter % 1000 == 0)
//          printf("sleep counter for level 0 waiting is %d\n", sleep_counter);
//        sleep_counter++;
//        env_->SleepForMicroseconds(10);// slightly larger than wait for flushing.
////          usleep(10);
//        counter = 0;
//      }
//    }
//    else{
//      assert(versions_->PrevLogNumber() == 0);
//      MemTable* temp_mem = new MemTable(internal_comparator_);
//      uint64_t last_mem_seq = mem_r->Getlargest_seq_supposed();
//      temp_mem->SetFirstSeq(last_mem_seq+1);
//      // starting from this sequenctial number, the data should write the the new memtable
//      // set the immutable as seq_num - 1
//      temp_mem->SetLargestSeq(last_mem_seq + MEMTABLE_SEQ_SIZE);
//      temp_mem->Ref();
//      mem_r->SetFlushState(MemTable::FLUSH_REQUESTED);
//
//      // CAS strong means that one thread will definitedly win under the concurrent,
//      // if it is weak then after the metable is full all the threads may gothrough the while
//      // loop multiple times.
//      if(mem_.compare_exchange_strong(mem_r, temp_mem)){
//        memtable_counter.fetch_add(1);
//        has_imm_.store(true, std::memory_order_release);
//
//        force = false;  // Do not force another compaction if have room
//
//        //set the flush flag for imm
////        assert(imm_.load()->Get_seq_count() == MEMTABLE_SEQ_SIZE);
////        assert(mem_r->Get_seq_count() == MEMTABLE_SEQ_SIZE);
//        assert(imm_.load() == nullptr);
//        imm_.store(mem_r);
//        MaybeScheduleFlushOrCompaction();
//        mem_r = temp_mem;
//        return s;
//      }else{
//        temp_mem->SimpleDelete();
//
//      }
//    }
//    mem_r = mem_.load();
//    // For the safety concern (such as the thread get context switch)
//    // the mem_ may not be the one this table should write, need to go through
//    // the table searching procedure below.
//  }
//  //if not which table should this writer need to write?
//  while(true){
//    if (seq_num >= mem_r->GetFirstseq() && seq_num <= mem_r->Getlargest_seq_supposed()){
//      return s;
//    }else {
//      // get the snapshot for imm then check it so that this memtable pointer is guarantee
//      // to be the one this thread want.
//      mem_r = imm_.load();
//      assert(imm_.load()!= nullptr);
//      assert(MEMTABLE_SEQ_SIZE - mem_r->Get_seq_count() <= 4);
//      printf("write to imm table");
//      if (seq_num >= mem_r->GetFirstseq() && seq_num <= mem_r->Getlargest_seq_supposed()) {
//        return s;
//      }
//    }
//  }
//}
// REQUIRES: undefine_mutex is held
// REQUIRES: this thread is currently at the front of the writer queue
//Status DBImpl::MakeRoomForWrite(bool force, uint64_t seq_num, MemTable*& mem_r) {
////  undefine_mutex.AssertHeld();
////  assert(!writers_.empty());
//  bool allow_delay = !force;
//  Status s = Status::OK();
//
//  while (true) {
//    mem_r = mem_.load();
//    if (!bg_error_.ok()) {
//      // Yield previous error
//      s = bg_error_;
//      break;
////    } else if (allow_delay && versions_->NumLevelFiles(0) >=
////                                  config::kL0_SlowdownWritesTrigger) {
////      // We are getting close to hitting a hard limit on the number of
////      // L0 files.  Rather than delaying a single write by several
////      // seconds when we hit the hard limit, start delaying each
////      // individual write by 1ms to reduce latency variance.  Also,
////      // this delay hands over some CPU to the compaction thread in
////      // case it is sharing the same core as the writer.
//////      undefine_mutex.Unlock();
//////      env_->SleepForMicroseconds(1000);
//////      allow_delay = false;  // Do not delay a single write more than once
//////      undefine_mutex.Lock();
//    } else if (seq_num <= mem_r->GetFirstseq()) {
//      assert(imm_.load()!= nullptr);
//      mem_r = imm_.load();
//      break;
//    } else if (!force &&
//               (seq_num <= mem_r->Getlargest_seq_supposed())) {
//      // There is room in current memtable
//      break;
//
//    } else if (imm_.load() != nullptr) {
//      // We have filled up the current memtable, but the previous
//      // one is still being compacted, so we wait.
//      // the wait will never get signalled.
//
//      // We check the imm again in the while loop, because the state may have already
//      // been changed before you acquire the Lock.
//      undefine_mutex.Lock();
//      Log(options_.info_log, "Current memtable full; waiting...\n");
//      while (imm_.load() != nullptr){
//        background_work_finished_signal_.Wait();
//      }
//      undefine_mutex.Unlock();
//
//
////    } else if (versions_->NumLevelFiles(0) >= config::kL0_StopWritesTrigger) {
////      // There are too many level-0 files.
////      Log(options_.info_log, "Too many L0 files; waiting...\n");
////      background_work_finished_signal_.Wait();
//    } else {
//      // Attempt to switch to a new memtable and trigger compaction of old
//      assert(versions_->PrevLogNumber() == 0);
//
//
////      mem_switching.store(true);
//
//      MemTable* temp_mem = new MemTable(internal_comparator_);
//      uint64_t last_mem_seq = mem_r->Getlargest_seq_supposed();
//      temp_mem->SetFirstSeq(last_mem_seq);
//      // starting from this sequenctial number, the data should write the the new memtable
//      // set the immutable as seq_num - 1
//      temp_mem->SetLargestSeq(last_mem_seq - 1 + MEMTABLE_SEQ_SIZE);
//      temp_mem->Ref();
//      mem_r->SetFlushState(MemTable::FLUSH_REQUESTED);
//
//      //CAS strong means that one thread will definitedly win under the concurrent,
//      // if it is weak then after the metable is full all the threads may gothrough the while
//      // loop multiple times.
//      if(mem_.compare_exchange_strong(mem_r, temp_mem)){
//        has_imm_.store(true, std::memory_order_release);
//
//        force = false;  // Do not force another compaction if have room
//
//        //set the flush flag for imm
//        imm_.store(mem_r);
//        MaybeScheduleFlushOrCompaction();
//      }else{
//        temp_mem->Unref();
//      };
//
//    }
//  }
//  return s;
//}

// REQUIRES: Writer list must be non-empty
// REQUIRES: First writer must have a non-null batch
WriteBatch* DBImpl::BuildBatchGroup(Writer** last_writer) {
//  undefine_mutex.AssertHeld();
  assert(!writers_.empty());
  Writer* first = writers_.front();
  WriteBatch* result = first->batch;
  assert(result != nullptr);

  size_t size = WriteBatchInternal::ByteSize(first->batch);

  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  size_t max_size = 1 << 20;
  if (size <= (128 << 10)) {
    max_size = size + (128 << 10);
  }

  *last_writer = first;
  std::deque<Writer*>::iterator iter = writers_.begin();
  ++iter;  // Advance past "first"
  for (; iter != writers_.end(); ++iter) {
    Writer* w = *iter;
    if (w->sync && !first->sync) {
      // Do not include a sync write into a batch handled by a non-sync write.
      break;
    }

    if (w->batch != nullptr) {
      size += WriteBatchInternal::ByteSize(w->batch);
      if (size > max_size) {
        // Do not make batch too big
        break;
      }

      // Append to *result
      if (result == first->batch) {
        // Switch to temporary batch instead of disturbing caller's batch
        result = tmp_batch_;
        assert(WriteBatchInternal::Count(result) == 0);
        WriteBatchInternal::Append(result, first->batch);
      }
      WriteBatchInternal::Append(result, w->batch);
    }
    *last_writer = w;
  }
  return result;
}



bool DBImpl::GetProperty(const Slice& property, std::string* value) {
  value->clear();

//  MutexLock l(&undefine_mutex);
  MemTable* mem = mem_.load();
//  MemTableListVersion* imm = imm_->current();
  Slice in = property;
  Slice prefix("TimberSaw.");
  if (!in.starts_with(prefix)) return false;
  in.remove_prefix(prefix.size());

  if (in.starts_with("num-files-at-level")) {
    in.remove_prefix(strlen("num-files-at-level"));
    uint64_t level;
    bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
    if (!ok || level >= config::kNumLevels) {
      return false;
    } else {
      char buf[100];
      std::snprintf(buf, sizeof(buf), "%d",
                    versions_->NumLevelFiles(static_cast<int>(level)));
      *value = buf;
      return true;
    }
  } else if (in == "stats") {
    char buf[200];
    std::snprintf(buf, sizeof(buf),
                  "                               Compactions\n"
                  "Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n"
                  "--------------------------------------------------\n");
    value->append(buf);
    for (int level = 0; level < config::kNumLevels; level++) {
      int files = versions_->NumLevelFiles(level);
      if (stats_[level].micros > 0 || files > 0) {
        std::snprintf(buf, sizeof(buf), "%3d %8d %8.0f %9.0f %8.0f %9.0f\n",
                      level, files, versions_->NumLevelBytes(level) / 1048576.0,
                      stats_[level].micros / 1e6,
                      stats_[level].bytes_read / 1048576.0,
                      stats_[level].bytes_written / 1048576.0);
        value->append(buf);
      }
    }
    return true;
  } else if (in == "sstables") {
    *value = versions_->current()->DebugString();
    return true;
  } else if (in == "approximate-memory-usage") {
    size_t total_usage = options_.block_cache->TotalCharge();
    if (mem) {
      total_usage += mem->ApproximateMemoryUsage();
    }

    total_usage += imm_.ApproximateMemoryUsageExcludingLast();

    char buf[50];
    std::snprintf(buf, sizeof(buf), "%llu",
                  static_cast<unsigned long long>(total_usage));
    value->append(buf);
    return true;
  }

  return false;
}

void DBImpl::GetApproximateSizes(const Range* range, int n, uint64_t* sizes) {
  // TODO(opt): better implementation
  MutexLock l(&undefine_mutex);
  Version* v = versions_->current();
  v->Ref(0);

  for (int i = 0; i < n; i++) {
    // Convert user_key into a corresponding internal key.
    InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
    uint64_t start = versions_->ApproximateOffsetOf(v, k1);
    uint64_t limit = versions_->ApproximateOffsetOf(v, k2);
    sizes[i] = (limit >= start ? limit - start : 0);
  }

  v->Unref(0);
}

// Default implementations of convenience methods that subclasses of DB
// can call if they wish
Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
  WriteBatch batch;
  batch.Put(key, value);
  return Write(opt, &batch);
}

Status DB::Delete(const WriteOptions& opt, const Slice& key) {
  WriteBatch batch;
  batch.Delete(key);
  return Write(opt, &batch);
}

DB::~DB() = default;

Status DB::Open(const Options& options, const std::string& dbname, DB** dbptr) {
  *dbptr = nullptr;

  DBImpl* impl = new DBImpl(options, dbname);
  impl->undefine_mutex.Lock();
  VersionEdit edit;
  // Recover handles create_if_missing, error_if_exists
  bool save_manifest = false;
  Status s = impl->Recover(&edit, &save_manifest);
  if (s.ok() && impl->mem_ == nullptr) {
    // Create new log and a corresponding memtable.
    uint64_t new_log_number = impl->versions_->NewFileNumber();
    WritableFile* lfile;
    s = options.env->NewWritableFile(LogFileName(dbname, new_log_number),
                                     &lfile);
    if (s.ok()) {
      edit.SetLogNumber(new_log_number);
      impl->logfile_ = lfile;
      impl->logfile_number_ = new_log_number;
      impl->log_ = new log::Writer(lfile);
      impl->mem_ = new MemTable(impl->internal_comparator_);
      impl->mem_.load()->SetFirstSeq(0);
      impl->mem_.load()->SetLargestSeq(MEMTABLE_SEQ_SIZE-1);
      impl->mem_.load()->Ref();
    }
  }
  if (s.ok() && save_manifest) {
    edit.SetPrevLogNumber(0);  // No older logs needed after recovery.
    edit.SetLogNumber(impl->logfile_number_);
    std::unique_lock<std::mutex> lck(impl->versionset_mtx);
    s = impl->versions_->LogAndApply(&edit, 0);
  }
  if (s.ok()) {
//    impl->RemoveObsoleteFiles();
  impl->MaybeScheduleFlushOrCompaction();
  }
  impl->undefine_mutex.Unlock();
  if (s.ok()) {
    assert(impl->mem_ != nullptr);
    *dbptr = impl;
  } else {
    delete impl;
  }
  return s;
}

Snapshot::~Snapshot() = default;

Status DestroyDB(const std::string& dbname, const Options& options) {
  Env* env = options.env;
  std::vector<std::string> filenames;
  Status result = env->GetChildren(dbname, &filenames);
  if (!result.ok()) {
    // Ignore error in case directory does not exist
    return Status::OK();
  }

  FileLock* lock;
  const std::string lockname = LockFileName(dbname);
  result = env->LockFile(lockname, &lock);
  if (result.ok()) {
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type) &&
          type != kDBLockFile) {  // Lock file will be deleted at end
        Status del = env->RemoveFile(dbname + "/" + filenames[i]);
        if (result.ok() && !del.ok()) {
          result = del;
        }
      }
    }
    env->UnlockFile(lock);  // Ignore error since state is already gone
    env->RemoveFile(lockname);
    env->RemoveDir(dbname);  // Ignore error in case dir contains other files
  }
  return result;
}

}  // namespace TimberSaw
