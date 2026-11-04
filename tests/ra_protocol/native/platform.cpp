#include <Serialization.h>
// The source-stream oracle deliberately compiles with two-byte WCHAR, unlike
// the host libc/libstdc++ four-byte wchar ABI. Native map helpers copy strings;
// char_traits may call these C symbols even when its size/capacity arithmetic
// was instantiated with -fshort-wchar. Supply only that raw-memory ABI boundary.
// These routines contain no Fabric metadata, scopes, lengths or wire codec.
extern "C" wchar_t* wmemcpy(wchar_t* to, const wchar_t* from, size_t count) noexcept {
    std::memcpy(to, from, count * sizeof(wchar_t)); return to;
}
extern "C" wchar_t* wmemmove(wchar_t* to, const wchar_t* from, size_t count) noexcept {
    std::memmove(to, from, count * sizeof(wchar_t)); return to;
}
extern "C" wchar_t* wmemset(wchar_t* to, wchar_t value, size_t count) noexcept {
    for (size_t i = 0; i < count; ++i) to[i] = value;
    return to;
}
namespace Serialization {
KAllocator& FabricSerializationCommon::GetPagedKtlAllocator() {
    static KAllocator allocator; return allocator;
}
KAllocator& FabricSerializationCommon::GetNonPagedKtlAllocator() {
    return GetPagedKtlAllocator();
}
}
