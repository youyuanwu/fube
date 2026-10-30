#pragma once
#include <algorithm>
#include <climits>
#include <clocale>
#include <cstdio>
#include <cwchar>
#include <functional>
#include <map>
#include <stdexcept>
#include <string>
#include <vector>
#include <stdint.h>

// Host libstdc++ wstring has a four-byte ABI. Adapt storage, not comparison.
inline int _wcsicmp(char16_t const *a, char16_t const *b) {
    static_assert(sizeof(wchar_t) == sizeof(char16_t));
    auto wide = [](char16_t const *value) {
        std::vector<wchar_t> result;
        do {
            result.push_back(static_cast<wchar_t>(*value));
        } while (*value++ != 0);
        return result;
    };
    auto aa = wide(a);
    auto bb = wide(b);
    return ::wcscasecmp(aa.data(), bb.data());
}
#include "GeneratedStringUtility.h"

using int64 = int64_t;
using uint64 = uint64_t;
namespace Common {
class TextWriter;
template <typename... Args>
std::string formatString(char const *, Args const &...) { return {}; }
struct LargeInteger {
    uint64 high_, low_;
    LargeInteger();
    LargeInteger(uint64 high, uint64 low);
    LargeInteger(LargeInteger const &);
    ~LargeInteger();
    bool operator < (LargeInteger const &) const;
    bool operator > (LargeInteger const &) const;
    bool operator <= (LargeInteger const &) const;
    bool operator >= (LargeInteger const &) const;
    bool operator == (LargeInteger const &) const;
    bool operator != (LargeInteger const &) const;
};
struct AcquireReadLock {
    explicit AcquireReadLock(int const &) {}
};
}
namespace Federation {
struct NodeId {
    Common::LargeInteger idValue_;
    static NodeId const MinNodeId;
    NodeId();
    NodeId(Common::LargeInteger const &);
    bool operator < (NodeId const &) const;
    bool operator > (NodeId const &) const;
    bool operator <= (NodeId const &) const;
    bool operator >= (NodeId const &) const;
    bool operator == (NodeId const &) const;
    bool operator != (NodeId const &) const;
};
struct NodeInstance {};
}
#define DECLARE_ENUM_STRUCTURED_TRACE(...)
#define TESTASSERT_IF(condition, ...) do { if (condition) throw std::logic_error("native test assert"); } while (0)
#define ASSERT_IF(condition, ...) do { if (condition) throw std::logic_error("native assert"); } while (0)
#include "GeneratedConfig.h"

namespace ServiceModel {
struct ApplicationIdentifier {
    std::u16string applicationTypeName_;
    uint32_t applicationNumber_ = 0;
    bool IsEmpty() const;
    bool operator == (ApplicationIdentifier const &) const;
    bool operator != (ApplicationIdentifier const &) const;
    int compare(ApplicationIdentifier const &) const;
    bool operator < (ApplicationIdentifier const &) const;
};
struct ServicePackageIdentifier {
    ApplicationIdentifier applicationIdentifier_;
    std::u16string servicePackageName_;
    __declspec(property(get=get_ApplicationId)) ApplicationIdentifier const & ApplicationId;
    ApplicationIdentifier const & get_ApplicationId() const { return applicationIdentifier_; }
    bool operator == (ServicePackageIdentifier const &) const;
    bool operator != (ServicePackageIdentifier const &) const;
    int compare(ServicePackageIdentifier const &) const;
    bool operator < (ServicePackageIdentifier const &) const;
};
struct ServiceTypeIdentifier {
    ServicePackageIdentifier packageIdentifier_;
    std::u16string serviceTypeName_;
    bool operator == (ServiceTypeIdentifier const &) const;
    bool operator != (ServiceTypeIdentifier const &) const;
    int compare(ServiceTypeIdentifier const &) const;
    bool operator < (ServiceTypeIdentifier const &) const;
};
}
namespace Reliability {
struct GenerationNumber {
    int64 generation_;
    Federation::NodeId owner_;
    GenerationNumber();
    GenerationNumber(int64, Federation::NodeId const &);
    bool operator < (GenerationNumber const &) const;
    bool operator > (GenerationNumber const &) const;
    bool operator <= (GenerationNumber const &) const;
    bool operator >= (GenerationNumber const &) const;
    bool operator == (GenerationNumber const &) const;
    bool operator != (GenerationNumber const &) const;
};
struct GenerationHeader {
    GenerationNumber Generation;
    bool IsForFMReplica;
};
namespace ReconfigurationAgentComponent {
struct GenerationState {
    GenerationNumber ReceiveGeneration;
};
struct TraceEvents {
    template <typename... Args>
    void GenerationIgnoreMessage(Args const &...) {}
};
struct RAEventSource {
    inline static TraceEvents events;
    inline static TraceEvents *Events = &events;
};
struct GenerationStateManager {
    int lock_ = 0;
    struct { std::string NodeInstanceIdStr; } ra_;
    GenerationState commonGenerations_;
    GenerationState fmReplicaGenerations_;
    GenerationState & GetGenerationState(bool);
    bool CheckGenerationHeader(GenerationHeader const &, std::wstring const &,
                               Federation::NodeInstance const &);
};
}
}
