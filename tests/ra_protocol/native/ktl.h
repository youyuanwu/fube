// Test-only platform support for the unmodified pinned native serializer.
// NOT KTL, not production SF, and contains NO wire serialization algorithm.
#pragma once
#include <algorithm>
#include <array>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <deque>
#include <fstream>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#define __in
#define __out
#define __inout
#define __in_opt
#define __out_opt
#define _In_
#define _Out_
#define _Inout_
#define _In_reads_(x)
#define _In_reads_opt_(x)
#define _Out_writes_(x)
#define _Out_writes_opt_(x)
#define _Inout_updates_(x)
#define __in_ecount(x)
#define __out_bcount(x)
#define _Use_decl_annotations_
#define _Use_decl_annotations_
#define FAILABLE
#define ASSERT(x) assert(x)
#define ASSERT_IFNOT(x, ...) assert(x)
#define TRUE 1
#define FALSE 0
using NTSTATUS = int32_t;
using LONG = int32_t;
using ULONG = uint32_t;
using LONG64 = int64_t;
using ULONG64 = uint64_t;
using UCHAR = unsigned char;
using CHAR = char;
using SHORT = int16_t;
using USHORT = uint16_t;
using WCHAR = wchar_t;
using DOUBLE = double;
using VOID = void;
using byte = unsigned char;
template<class K, class V> struct ArrayPair;
template<class K, class V> class KHashTable;
static_assert(sizeof(WCHAR) == 2, "producer requires -fshort-wchar");
constexpr NTSTATUS STATUS_SUCCESS = 0;
constexpr NTSTATUS STATUS_INSUFFICIENT_RESOURCES = NTSTATUS(0xc000009a);
constexpr NTSTATUS STATUS_INVALID_PARAMETER = NTSTATUS(0xc000000d);
constexpr NTSTATUS STATUS_NOINTERFACE = NTSTATUS(0xc00002b9);
#define NT_SUCCESS(x) ((NTSTATUS)(x) >= 0)
#define NT_ERROR(x) ((NTSTATUS)(x) < 0)
#define CheckStatus(x) do { if (NT_ERROR(x) || (x) == K_STATUS_SCOPE_END) return (x); } while (0)
#include <kstatus.h>

struct GUID { uint32_t Data1; uint16_t Data2, Data3; uint8_t Data4[8]; };
static_assert(sizeof(GUID) == 16);
inline bool operator==(GUID a, GUID b) { return std::memcmp(&a, &b, 16) == 0; }
struct KGuid : GUID { KGuid() : GUID{} {} };
inline void RtlZeroMemory(void* p, size_t n) { std::memset(p, 0, n); }
inline void KMemCpySafe(void* p, size_t cap, const void* src, size_t n) {
    if (n > cap) throw std::runtime_error("platform copy overflow");
    if (n) std::memcpy(p, src, n);
}
inline LONG InterlockedIncrement(volatile LONG* p) { return __sync_add_and_fetch(p, 1); }
inline LONG InterlockedDecrement(volatile LONG* p) { return __sync_sub_and_fetch(p, 1); }
namespace Ktl { template<class T> auto&& Move(T& v) { return std::move(v); } }
struct KAllocator {};
inline void* operator new(size_t size, unsigned, KAllocator&) { return ::operator new(size); }
inline void* operator new[](size_t size, unsigned, KAllocator&) { return ::operator new[](size); }
#define _new(tag, allocator) new(tag, allocator)
#define _delete(p) delete (p)
template<class T> struct KObject {
    NTSTATUS status_ = 0;
    NTSTATUS Status() const { return status_; }
    void SetConstructorStatus(NTSTATUS s) { if (NT_ERROR(s)) status_ = s; }
    void SetStatus(NTSTATUS s) { status_ = s; }
};
template<class T> struct DefaultDeleter { static void Delete(T* p) { delete p; } };
template<class T> struct ArrayDeleter { static void Delete(T* p) { delete[] p; } };
template<class T, class D = DefaultDeleter<T>> class KUniquePtr {
    T* p_ = nullptr;
public:
    KUniquePtr() = default;
    KUniquePtr(T* p) : p_(p) {}
    KUniquePtr(KUniquePtr&& v) : p_(v.p_) { v.p_ = nullptr; }
    KUniquePtr& operator=(KUniquePtr&& v) {
        if (this != &v) { D::Delete(p_); p_ = v.p_; v.p_ = nullptr; } return *this;
    }
    ~KUniquePtr() { D::Delete(p_); }
    operator T*() const { return p_; }
    T* operator->() const { return p_; }
    T& operator*() const { return *p_; }
};
template<class T> class KArray : public KObject<KArray<T>> {
    std::vector<T> v_;
public:
    KArray(KAllocator&) {}
    KArray(const KArray& other) {
        v_.resize(other.v_.size());
        for (size_t i = 0; i < v_.size(); ++i) v_[i] = other.v_[i];
    }
    KArray(KArray&&) = default;
    size_t Count() const { return v_.size(); }
    size_t Max() const { return v_.capacity(); }
    bool IsEmpty() const { return v_.empty(); }
    T& operator[](size_t i) { return v_.at(i); }
    const T& operator[](size_t i) const { return v_.at(i); }
    template<class V> NTSTATUS Append(V&& v) { v_.push_back(std::forward<V>(v)); return 0; }
    void Remove(size_t i) { v_.erase(v_.begin() + i); }
    void Clear() { v_.clear(); }
    T* Data() { return v_.data(); }
    NTSTATUS Reserve(size_t n) { v_.reserve(n); return 0; }
    bool SetCount(size_t n) { v_.resize(n); return true; }
};
struct KMemRef { void* _Address = nullptr; ULONG _Param = 0; };
// Raw seekable storage only. Native FabricStream and FabricSerializableStream
// retain all metadata, compression, framing, scopes and decoding logic.
class KMemChannel : public KObject<KMemChannel> {
    std::vector<UCHAR> bytes_;
    ULONG cursor_ = 0;
public:
    using UPtr = KUniquePtr<KMemChannel>;
    enum { eFromBegin, eBeforeEnd };
    KMemChannel(KAllocator&, int, int = 0, int = 0) {}
    NTSTATUS Write(ULONG n, const void* src) {
        if (n > 4 * 1024 * 1024 || cursor_ > 4 * 1024 * 1024 - n)
            return STATUS_INSUFFICIENT_RESOURCES;
        bytes_.resize(std::max<size_t>(bytes_.size(), cursor_ + n));
        if (n) std::memcpy(bytes_.data() + cursor_, src, n);
        cursor_ += n; return 0;
    }
    NTSTATUS Read(ULONG n, void* dest, ULONG* actual) {
        *actual = std::min<ULONG>(n, bytes_.size() - cursor_);
        if (*actual) std::memcpy(dest, bytes_.data() + cursor_, *actual);
        cursor_ += *actual; return 0;
    }
    NTSTATUS SetCursor(ULONG pos, int origin) {
        size_t target = origin == eBeforeEnd ? bytes_.size() - pos : pos;
        if (target > bytes_.size()) return STATUS_INVALID_PARAMETER;
        cursor_ = target; return 0;
    }
    ULONG GetCursor() const { return cursor_; }
    ULONG Size() const { return bytes_.size(); }
    ULONG GetBlockCount() const { return bytes_.empty() ? 0 : 1; }
    NTSTATUS GetBlock(ULONG i, KMemRef& ref) {
        if (i || bytes_.empty()) return STATUS_INVALID_PARAMETER;
        ref._Address = bytes_.data(); ref._Param = bytes_.size(); return 0;
    }
};
