#include <Serialization.h>
namespace Serialization {
KAllocator& FabricSerializationCommon::GetPagedKtlAllocator() {
    static KAllocator allocator; return allocator;
}
KAllocator& FabricSerializationCommon::GetNonPagedKtlAllocator() {
    return GetPagedKtlAllocator();
}
}
