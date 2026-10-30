#include "Ra.Stdafx.h"
#include "GeneratedModelMethods.h"
#include "GeneratedGenerationMethods.h"
#include <cstring>

using namespace Reliability;
using namespace Reliability::ReconfigurationAgentComponent;
using namespace ServiceModel;

FILE *raw_actions;

char const *consumed(RetryableErrorAction::Enum action) {
    // The enabled Rust handlers consume warning/drop only, not restart/error.
    switch (action) {
    case RetryableErrorAction::ReportHealthWarning: return "Warning";
    case RetryableErrorAction::Drop: return "Drop";
    default: return "None";
    }
}

void failure(char const *family, char const *step, RetryableErrorState &state,
             FailoverConfig const &config) {
    auto action = state.OnFailure(config);
    std::fprintf(raw_actions, "%s %s count=%d raw=%x\n",
                 family, step, state.CurrentFailureCount, action);
    std::printf("failure %s %s count=%d action=%s\n",
                family, step, state.CurrentFailureCount, consumed(action));
}

void clear(char const *family, char const *step, RetryableErrorState &state,
           RetryableErrorStateName::Enum target, FailoverConfig const &config) {
    auto action = state.OnSuccessAndTransitionTo(target, config);
    std::fprintf(raw_actions, "%s %s raw=%x\n", family, step, action);
    std::printf("clear %s %s clear=%d count=%d\n", family, step,
                action == RetryableErrorAction::ClearHealthReport, state.CurrentFailureCount);
}

void flags(char const *family, char const *step, RetryableErrorState const &state,
           FailoverConfig const &config) {
    std::printf("flags %s %s count=%d abort=%d\n", family, step,
                state.CurrentFailureCount, state.IsLastRetryBeforeDrop(config));
}

void policy(char const *family, RetryableErrorStateName::Enum name,
            FailoverConfig config, int FailoverConfig::*warning,
            int FailoverConfig::*drop, int FailoverConfig::*restart,
            int FailoverConfig::*error) {
    std::printf("defaults %s warning=%d drop=%d restart=%d error=%d\n", family,
                config.*warning, config.*drop, config.*restart, config.*error);
    RetryableErrorState state;
    state.EnterState(name);
    failure(family, "default", state, config);
    config.*warning = 2;
    failure(family, "dynamic-warning", state, config);
    config.*drop = 2;
    failure(family, "lowered-drop", state, config);
    config.*drop = 4;
    flags(family, "next-drop", state, config);
    config.*warning = 4;
    failure(family, "warning-precedes-drop", state, config);
    config.*warning = 0;
    config.*drop = -1;
    config.*restart = 5;
    failure(family, "restart-unconsumed", state, config);
    config.*error = 6;
    failure(family, "error-unconsumed", state, config);
    clear(family, "current-zero-warning", state, name, config);
    config.*warning = -2;
    config.*error = -1;
    clear(family, "zero-count-negative", state, name, config);
    config.*warning = config.*drop = config.*restart = config.*error = 0;
    failure(family, "all-zero", state, config);
    flags(family, "zero-drop", state, config);
    state.EnterState(name);
    config.*drop = 1;
    flags(family, "one-drop", state, config);
    config.*warning = config.*error = 100;
    failure(family, "drop", state, config);
    clear(family, "raised-current-threshold", state, name, config);
    config.*warning = config.*error = INT_MIN;
    config.*drop = config.*restart = -1;
    clear(family, "signed-min-clear", state, name, config);
    config.*warning = config.*drop = 1;
    failure(family, "equality-precedence", state, config);
}

void policies() {
    policy("open", RetryableErrorStateName::ReplicaOpen, {},
           &FailoverConfig::ReplicaOpenFailureWarningReportThreshold,
           &FailoverConfig::ReplicaOpenFailureMaxRetryThreshold,
           &FailoverConfig::ReplicaOpenFailureRestartThreshold,
           &FailoverConfig::ReplicaOpenFailureErrorReportThreshold);
    policy("registration", RetryableErrorStateName::FindServiceRegistrationAtOpen, {},
           &FailoverConfig::ServiceTypeRegistrationWarningReportThreshold,
           &FailoverConfig::ServiceTypeRegistrationMaxRetryThreshold,
           &FailoverConfig::ServiceTypeRegistrationRestartThreshold,
           &FailoverConfig::ServiceTypeRegistrationErrorReportThreshold);
    policy("close", RetryableErrorStateName::ReplicaClose, {},
           &FailoverConfig::ReplicaCloseFailureWarningReportThreshold,
           &FailoverConfig::ReplicaCloseFailureMaxRetryThreshold,
           &FailoverConfig::ReplicaCloseFailureRestartThreshold,
           &FailoverConfig::ReplicaCloseFailureErrorReportThreshold);
    policy("delete", RetryableErrorStateName::ReplicaDelete, {},
           &FailoverConfig::ReplicaDeleteFailureWarningReportThreshold,
           &FailoverConfig::ReplicaDeleteFailureMaxRetryThreshold,
           &FailoverConfig::ReplicaDeleteFailureRestartThreshold,
           &FailoverConfig::ReplicaDeleteFailureErrorReportThreshold);
    policy("registration-drop", RetryableErrorStateName::FindServiceRegistrationAtDrop, {},
           &FailoverConfig::ServiceTypeRegistrationWarningReportThresholdAtDrop,
           &FailoverConfig::ServiceTypeRegistrationMaxRetryThresholdAtDrop,
           &FailoverConfig::ServiceTypeRegistrationRestartThreshold,
           &FailoverConfig::ServiceTypeRegistrationErrorReportThreshold);

    FailoverConfig config;
    auto collection = RetryableErrorStateThresholdCollection::CreateDefault();
    config.ServiceTypeRegistrationRestartThreshold = -7;
    config.ServiceTypeRegistrationErrorReportThreshold = -8;
    auto shared = collection.GetThreshold(RetryableErrorStateName::FindServiceRegistrationAtDrop, config);
    std::printf("shared-registration restart=%d error=%d\n", shared.RestartThreshold, shared.ErrorThreshold);
    config.ReplicaOpenFailureWarningReportThreshold = 10;
    collection.AddThreshold(RetryableErrorStateName::ReplicaOpen,
        [&](FailoverConfig const &cfg) {
            int previous = cfg.ReplicaOpenFailureWarningReportThreshold;
            config.ReplicaOpenFailureWarningReportThreshold = 2;
            config.ReplicaOpenFailureMaxRetryThreshold = 2;
            return previous;
        },
        [](FailoverConfig const &cfg) { return cfg.ReplicaOpenFailureMaxRetryThreshold; },
        [](FailoverConfig const &cfg) { return cfg.ReplicaOpenFailureRestartThreshold; },
        [](FailoverConfig const &cfg) { return cfg.ReplicaOpenFailureErrorReportThreshold; });
    auto mixed = collection.GetThreshold(RetryableErrorStateName::ReplicaOpen, config);
    std::printf("mixed-read warning=%d drop=%d restart=%d error=%d\n",
                mixed.WarningThreshold, mixed.DropThreshold, mixed.RestartThreshold, mixed.ErrorThreshold);
    RetryableErrorState state;
    state.Test_Set(RetryableErrorStateName::ReplicaOpen, 1);
    state.Test_SetThresholds(RetryableErrorStateName::ReplicaOpen, mixed.WarningThreshold,
                            mixed.DropThreshold, mixed.RestartThreshold, mixed.ErrorThreshold);
    failure("mixed", "captured", state, config);
    state = RetryableErrorState();
    state.Test_Set(RetryableErrorStateName::ReplicaOpen, 1);
    failure("mixed", "fresh", state, config);
}

void identifiers() {
    std::vector<std::u16string> names = {
        u"Type", u"tYPE", u"\u00e9", u"\u00c9", u"\u03c3", u"\u03c2",
        u"\u00df", u"SS", std::u16string(u"A\0B", 3), std::u16string(u"a\0C", 3),
        u"\U00010428", u"\U00010400", u"\u8000", u"\u7fff"
    };
    for (size_t i = 0; i < names.size(); ++i)
        for (size_t j = 0; j < names.size(); ++j) {
            ServiceTypeIdentifier a{{{}, u"ignored"}, names[i]};
            ServiceTypeIdentifier b{{{}, u"other"}, names[j]};
            int comparison = a.compare(b);
            std::printf("name %zu %zu order=%d equal=%d\n",
                        i, j, (comparison > 0) - (comparison < 0), a == b);
        }
    std::vector<ServiceTypeIdentifier> ids = {
        {{{u"App", 1}, u"Pkg"}, u"Type"},
        {{{u"app", 1}, u"PKG"}, u"tYPE"},
        {{{u"A", 2}, u"Pkg"}, u"Type"},
        {{{u"Z", 0}, u"Pkg"}, u"Type"},
        {{{u"App", 1}, u"Other"}, u"Type"},
        {{{u"App", 1}, u"Pkg"}, u"Other"},
        {{{u"App", UINT32_MAX}, u"Pkg"}, u"Type"},
        {{{}, u"Pkg"}, u"Type"},
        {{{}, u"Other"}, u"Type"},
        {{{std::u16string(u"\0x", 2), 0}, u"Pkg"}, u"Type"},
        {{{u"\u8000", 1}, u"Pkg"}, u"Type"},
        {{{u"\u7fff", 1}, u"Pkg"}, u"Type"},
    };
    for (size_t i = 0; i < ids.size(); ++i)
        for (size_t j = 0; j < ids.size(); ++j) {
            int comparison = ids[i].compare(ids[j]);
            std::printf("identity %zu %zu order=%d equal=%d\n", i, j,
                        (comparison > 0) - (comparison < 0), ids[i] == ids[j]);
        }
}

void generations() {
    std::vector<GenerationNumber> values = {
        {-1, NodeId(Common::LargeInteger(UINT64_MAX, UINT64_MAX))},
        {6, NodeId(Common::LargeInteger(UINT64_MAX, UINT64_MAX))},
        {7, NodeId(Common::LargeInteger(0x8000000000000000ULL, 1))},
        {8, NodeId(Common::LargeInteger(0, 0))},
        {7, NodeId(Common::LargeInteger(0x7fffffffffffffffULL, UINT64_MAX))},
        {7, NodeId(Common::LargeInteger(0x8000000000000000ULL, 0))},
        {7, NodeId(Common::LargeInteger(0x8000000000000000ULL, 2))},
        {7, NodeId(Common::LargeInteger(0x8000000000000001ULL, 0))},
    };
    for (size_t i = 0; i < values.size(); ++i)
        for (size_t j = 0; j < values.size(); ++j)
            std::printf("generation %zu %zu older=%d\n", i, j, values[i] < values[j]);
    GenerationStateManager manager;
    manager.commonGenerations_.ReceiveGeneration = values[2];
    for (size_t i = 0; i < values.size(); ++i)
        std::printf("dispatch %zu accepted=%d\n", i,
                    manager.CheckGenerationHeader({values[i], false}, L"ServiceTypeNotificationReply", {}));
}

int main(int argc, char **argv) {
    if (argc == 2 && !std::strcmp(argv[1], "--identity")) {
        std::puts(FUBE_RA_SOURCE_DIGEST);
        return 0;
    }
    if (argc != 1) throw std::runtime_error("usage: native-helpers [--identity]");
    if (std::strcmp(std::setlocale(LC_CTYPE, nullptr), "C"))
        throw std::runtime_error("helper must start in C locale");
    FILE *table = std::fopen("upper-units.bin", "wb");
    raw_actions = std::fopen("raw-actions.txt", "w");
    if (!table || !raw_actions) throw std::runtime_error("cannot open helper output");
    for (uint32_t unit = 0; unit <= 0xffff; ++unit) {
        std::u16string value(1, char16_t(unit));
        uint16_t folded = static_cast<uint16_t>(
            Common::StringUtility::CompareCaseInsensitive(value, {}));
        std::fputc(folded & 0xff, table);
        std::fputc(folded >> 8, table);
    }
    std::fclose(table);
    identifiers();
    policies();
    generations();
    std::fclose(raw_actions);
}
