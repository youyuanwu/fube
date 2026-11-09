// Source-baseline oracle. Native stream/helper code is compiled unmodified.
// This adapter constructs values independently; it does not construct wire bytes.
#include <Serialization.h>
#include <filesystem>

template<class T> void rich(T&, int&);
inline void rich(LONG64& v,int& c) { v=-0x123456780000LL-++c; }
inline void rich(ULONG64& v,int& c) { v=0xfedcba9800000000ULL+ ++c; }
inline void rich(LONG& v,int& c) { v=1000+ ++c; }
inline void rich(ULONG& v,int& c) { v=4000000000U+ ++c; }
inline void rich(USHORT& v,int& c) { v=60000+ ++c; }
inline void rich(bool& v,int& c) { v=++c%2==0; }
inline void rich(double& v,int& c) { v=1.25+ ++c; }
inline void rich(byte& v,int& c) { v=++c%256; }
inline void rich(GUID& v,int& c) {
    ++c; for(size_t i=0;i<16;++i) reinterpret_cast<UCHAR*>(&v)[i]=(c+i)%256;
}
inline std::wstring text(const std::u16string& value) {
    // Avoid libstdc++'s system-width wchar string functions under -fshort-wchar.
    std::wstring result;
    // Keep adapter-owned strings off the system library's incompatible wchar
    // small-string move path. Storage capacity is not serialized.
    result.reserve(32);
    for(char16_t ch:value) result.push_back(static_cast<wchar_t>(ch));
    return result;
}
inline void rich(std::wstring& v,int& c) {
    std::string number=std::to_string(++c);
    v.clear(); v.reserve(32); v.push_back(L'v');
    for(char ch:number) v.push_back(ch);
    v.push_back(0x03c0); v.push_back(0xd83e); v.push_back(0xdd80);
}
template<class T> void rich(std::vector<T>& values,int& c) {
    values.resize(2); for(auto& value:values) rich(value,c);
}
// A singleton exercises the real native map/pair and set/string-vector helpers
// without libstdc++'s system-width wchar comparators under -fshort-wchar.
template<class K,class V> void rich(std::map<K,V>& values,int& c) {
    values.clear(); K key{}; V value{}; rich(key,c); rich(value,c);
    values.emplace(std::move(key),std::move(value));
}
template<class T> void rich(std::set<T>& values,int& c) {
    values.clear(); T value{}; rich(value,c); values.insert(std::move(value));
}
#include "schema.h"
// Non-template overloads for raw NodeId / enum are declared before generated
// rich functions by an injected support header, below the native enum definition.

inline void require(NTSTATUS status,const char* operation) {
    if(NT_ERROR(status) || status==K_STATUS_SCOPE_END)
        throw std::runtime_error(std::string(operation)+": "+std::to_string(status));
}
inline std::vector<UCHAR> read_file(const std::filesystem::path& path) {
    if(std::filesystem::file_size(path)>4*1024*1024) throw std::runtime_error("oversized input");
    std::ifstream input(path,std::ios::binary);
    if(!input) throw std::runtime_error("missing input");
    return {std::istreambuf_iterator<char>(input),{}};
}
template<class T> std::vector<UCHAR> encode(T& value) {
    Serialization::IFabricSerializableStream* raw=nullptr;
    require(Serialization::FabricSerializableStream::Create(&raw),"create");
    Serialization::FabricSerializableStream::UPtr stream(raw);
    require(stream->WriteSerializable(&value),"native WriteSerializable");
    size_t count=0; raw->GetAllBuffers(nullptr,count);
    std::vector<Serialization::FabricIOBuffer> buffers(count);
    require(raw->GetAllBuffers(buffers.data(),count),"buffers");
    std::vector<UCHAR> output;
    for(auto& buffer:buffers) output.insert(output.end(),buffer.buffer,buffer.buffer+buffer.length);
    if(output.size()!=raw->Size()) throw std::runtime_error("size mismatch");
    return output;
}
template<class T> bool decode(const std::vector<UCHAR>& bytes,T& value) {
    Serialization::IFabricStream* raw_bytes=nullptr;
    require(Serialization::FabricStream::Create(&raw_bytes),"create raw stream");
    Serialization::IFabricStreamUPtr storage(raw_bytes);
    require(storage->WriteBytes(bytes.size(),bytes.data()),"input");
    require(storage->Seek(0),"seek");
    Serialization::IFabricSerializableStream* raw=nullptr;
    require(Serialization::FabricSerializableStream::Create(&raw,Ktl::Move(storage)),"create reader");
    Serialization::FabricSerializableStream::UPtr stream(raw);
    NTSTATUS status=stream->ReadSerializable(&value);
    return NT_SUCCESS(status) && status!=K_STATUS_SCOPE_END && raw->Position()==raw->Size();
}
template<class T> void run(const std::string& mode,const std::filesystem::path& dir,
                         const std::string& name,T value) {
    auto path=dir/(name+".bin");
    if(mode=="produce") {
        auto bytes=encode(value);
        std::ofstream output(path,std::ios::binary);
        output.write(reinterpret_cast<const char*>(bytes.data()),bytes.size());
        if(!output) throw std::runtime_error("write failed");
    } else {
        T received;
        if(!decode(read_file(path),received) || !(received==value))
            throw std::runtime_error("native field comparison failed: "+name);
    }
    std::cout << name << " " << mode << " OK\n";
}
size_t native_truncation_rejections=0;
template<class T> void reject_truncated(T value) {
    auto bytes=encode(value);
    bytes.resize(bytes.size()/2);
    T received;
    if(decode(bytes,received)) throw std::runtime_error("native accepted truncated body");
    ++native_truncation_rejections;
}
inline void bodyless(const std::string& mode,const std::filesystem::path& dir,
                     const std::string& name) {
    auto path=dir/(name+".bin");
    if(mode=="produce") {
        std::ofstream output(path,std::ios::binary);
        if(!output) throw std::runtime_error("bodyless write failed");
    } else if(!read_file(path).empty()) {
        throw std::runtime_error("bodyless proposal has a body");
    }
    std::cout << name << " " << mode << " OK\n";
}
inline FabricVersionInstance fresh_version() {
    FabricVersionInstance value;
    value.version_.codeVersion_.majorVersion_=1;
    value.version_.codeVersion_.minorVersion_=0;
    value.version_.codeVersion_.buildVersion_=960;
    value.version_.codeVersion_.hotfixVersion_=0;
    value.version_.configVersion_.value_=text(u"__default__");
    value.instanceId_=0;
    return value;
}
int main(int argc,char** argv) {
    try {
        if(argc==2 && std::string(argv[1])=="--identity") {
            std::cout << FUBE_RA_SOURCE_DIGEST << "\n"; return 0;
        }
        if(argc!=3) throw std::runtime_error("usage: native-oracle produce|verify DIRECTORY");
        const std::string mode=argv[1];
        if(mode!="produce" && mode!="verify") throw std::runtime_error("unknown mode");
        std::filesystem::path dir=argv[2];
        if(!std::filesystem::is_directory(dir)) throw std::runtime_error("directory required");
        ReplicaMessageBody request;
        run(mode,dir,"default-request",request);
        int c=0; rich(request,c);
        run(mode,dir,"rich-request",request);
        ReplicaReplyMessageBody reply;
        run(mode,dir,"default-reply",reply);
        c=0; rich(reply,c);
        for(LONG error:{0, -2147017398, -2147017441}) {
            reply.errorCode_.value_=Enum(error);
            run(mode,dir,"rich-reply-"+std::to_string(error),reply);
        }
        ReplicaMessageBody primary;
        run(mode,dir,"default-primary",primary);
        c=0; rich(primary,c);
        primary.replica_.currentConfigurationRole_=Enum(4);
        primary.replica_.previousConfigurationRole_=Enum(1);
        primary.replica_.state_=Enum(1);
        primary.replica_.isUp_=true;
        run(mode,dir,"rich-primary",primary);
        ReplicaReplyMessageBody primary_reply;
        run(mode,dir,"default-primary-reply",primary_reply);
        c=0; rich(primary_reply,c);
        primary_reply.replica_.currentConfigurationRole_=Enum(4);
        primary_reply.replica_.previousConfigurationRole_=Enum(1);
        primary_reply.replica_.state_=Enum(2);
        primary_reply.replica_.isUp_=true;
        primary_reply.errorCode_.value_=Enum(0);
        run(mode,dir,"rich-primary-reply",primary_reply);
        GenerationHeader generation;
        run(mode,dir,"default-generation",generation);
        c=0; rich(generation,c); generation.isForFMReplica_=false;
        run(mode,dir,"fm-generation",generation);
        generation.isForFMReplica_=true;
        run(mode,dir,"fmm-generation",generation);
        ActorHeader actor;
        for(LONG destination:{10,11,12,13}) {
            actor.actor_=Enum(destination);
            run(mode,dir,"actor-"+std::to_string(destination),actor);
        }
        ActionHeader action;
        action.action_=text(u"AddInstance"); run(mode,dir,"action-request",action);
        action.action_=text(u"AddInstanceReply"); run(mode,dir,"action-reply",action);
        HighPriorityHeader priority;
        priority.highPriority_=true; run(mode,dir,"priority",priority);
        DeleteReplicaMessageBody removal;
        run(mode,dir,"default-remove",removal);
        c=0; rich(removal,c);
        for(int force:{0,1}) {
            removal.isForce_=force!=0;
            run(mode,dir,"rich-remove-"+std::to_string(force),removal);
        }
        ReplicaReplyMessageBody remove_reply;
        run(mode,dir,"default-remove-reply",remove_reply);
        c=0; rich(remove_reply,c); remove_reply.errorCode_.value_=Enum(0);
        run(mode,dir,"rich-remove-reply",remove_reply);
        DeleteReplicaMessageBody deletion;
        run(mode,dir,"default-delete-replica",deletion);
        c=0; rich(deletion,c);
        deletion.isForce_=false;
        deletion.fudesc_.targetReplicaSetSize_=1;
        deletion.fudesc_.minReplicaSetSize_=1;
        deletion.service_.targetReplicaSetSize_=1;
        deletion.service_.minReplicaSetSize_=1;
        deletion.service_.isStateful_=true;
        deletion.service_.hasPersistedState_=false;
        deletion.replica_.currentConfigurationRole_=Enum(4);
        deletion.replica_.previousConfigurationRole_=Enum(1);
        deletion.replica_.state_=Enum(2);
        deletion.replica_.isUp_=true;
        run(mode,dir,"rich-delete-replica",deletion);
        ReplicaReplyMessageBody deletion_reply;
        run(mode,dir,"default-delete-replica-reply",deletion_reply);
        c=0; rich(deletion_reply,c);
        deletion_reply.fudesc_.targetReplicaSetSize_=1;
        deletion_reply.fudesc_.minReplicaSetSize_=1;
        ReplicaDescription dropped;
        dropped.federationNodeInstance_=deletion_reply.replica_.federationNodeInstance_;
        dropped.replicaId_=deletion_reply.replica_.replicaId_;
        dropped.instanceId_=deletion_reply.replica_.instanceId_;
        dropped.state_=Enum(3);
        dropped.isUp_=false;
        deletion_reply.replica_=std::move(dropped);
        for(LONG error:{0, -2147017398}) {
            deletion_reply.errorCode_.value_=Enum(error);
            run(mode,dir,"rich-delete-replica-reply-"+std::to_string(error),deletion_reply);
        }
        if(CreateReplicaActor!=12 || CreateReplicaReplyActor!=12 ||
           !CreateReplicaPriority || !CreateReplicaReplyPriority)
            throw std::runtime_error("native peer factory metadata mismatch");
        RAReplicaMessageBody peer;
        run(mode,dir,"default-create-replica",peer);
        c=0; rich(peer,c);
        peer.fudesc_.targetReplicaSetSize_=1;
        peer.fudesc_.minReplicaSetSize_=1;
        peer.service_.targetReplicaSetSize_=1;
        peer.service_.minReplicaSetSize_=1;
        peer.service_.isStateful_=true;
        peer.service_.hasPersistedState_=false;
        peer.replica_.currentConfigurationRole_=Enum(2);
        peer.replica_.previousConfigurationRole_=Enum(1);
        peer.replica_.state_=Enum(1);
        peer.replica_.isUp_=true;
        run(mode,dir,"rich-create-replica",peer);
        reject_truncated(peer);
        peer.deactivationInfo_.deactivationEpoch_=peer.fudesc_.ccEpoch_;
        peer.deactivationInfo_.catchupLSN_=0;
        run(mode,dir,"initial-create-replica",peer);
        reject_truncated(peer);
        ReplicaReplyMessageBody peer_reply;
        run(mode,dir,"default-create-replica-reply",peer_reply);
        c=0; rich(peer_reply,c);
        peer_reply.fudesc_.targetReplicaSetSize_=1;
        peer_reply.fudesc_.minReplicaSetSize_=1;
        peer_reply.replica_.currentConfigurationRole_=Enum(2);
        peer_reply.replica_.previousConfigurationRole_=Enum(1);
        peer_reply.replica_.state_=Enum(1);
        peer_reply.replica_.isUp_=true;
        for(LONG error:{0, -2147017398}) {
            peer_reply.errorCode_.value_=Enum(error);
            run(mode,dir,"rich-create-replica-reply-"+std::to_string(error),peer_reply);
        }
        reject_truncated(peer_reply);
        // Native empty ctor leaves bools unspecified: explicitly initialize the fixture.
        ReplicaUpMessageBody up;
        up.isLastReplicaUpMessage_=false; up.isFromFMM_=false;
        run(mode,dir,"explicit-empty-replica-up",up);
        c=0; rich(up,c);
        for(int last:{0,1}) for(int fmm:{0,1}) {
            up.isLastReplicaUpMessage_=last!=0; up.isFromFMM_=fmm!=0;
            run(mode,dir,"rich-replica-up-"+std::to_string(last)+"-"+std::to_string(fmm),up);
        }
        for(auto name:{u"RemoveInstance",u"RemoveInstanceReply",u"ReplicaUp",u"ReplicaUpReply",u"AddPrimary",u"AddPrimaryReply",u"DeleteReplica",u"DeleteReplicaReply",u"CreateReplica",u"CreateReplicaReply"}) {
            action.action_=text(name);
            std::string ascii; for(const auto ch:std::u16string(name)) ascii.push_back(char(ch));
            run(mode,dir,"action-"+ascii,action);
        }
        ServiceTypeNotificationRequestMessageBody notification;
        run(mode,dir,"empty-service-type-notification",notification);
        c=0; rich(notification,c);
        run(mode,dir,"rich-service-type-notification",notification);
        ServiceTypeNotificationReplyMessageBody notification_reply;
        run(mode,dir,"empty-service-type-notification-reply",notification_reply);
        notification_reply.infos_.push_back(notification.infos_[1]);
        run(mode,dir,"partial-service-type-notification-reply",notification_reply);
        for(auto name:{u"ServiceTypeNotification",u"ServiceTypeNotificationReply"}) {
            action.action_=text(name);
            std::string ascii; for(const auto ch:std::u16string(name)) ascii.push_back(char(ch));
            run(mode,dir,"action-"+ascii,action);
        }
        priority.highPriority_=false; run(mode,dir,"priority-false",priority);
        // Explicit fresh inventories/activation, not native empty-ctor guarantees.
        NodeUpMessageBody node_up;
        node_up.nodeDescription_.nodeName_=text(u"node-2");
        node_up.nodeDescription_.nodeType_=text(u"stateless");
        node_up.nodeDescription_.ipAddressOrFQDN_=text(u"127.0.0.2");
        node_up.nodeDescription_.clusterConnectionPort_=45000;
        node_up.nodeDescription_.versionInstance_=fresh_version();
        node_up.anyReplicaFound_=false;
        run(mode,dir,"fresh-node-up",node_up);
        c=0; rich(node_up,c); node_up.anyReplicaFound_=true;
        run(mode,dir,"rich-node-up",node_up);
        NodeUpAckMessageBody ack;
        ack.fabricVersionInstance_=fresh_version();
        ack.isNodeActivated_=true; ack.nodeActivationSequenceNumber_=7;
        run(mode,dir,"fresh-node-up-ack",ack);
        c=0; rich(ack,c); ack.isNodeActivated_=false;
        run(mode,dir,"rich-node-up-ack",ack);
        for(auto name:{u"NodeUp",u"NodeUpAck"}) {
            action.action_=text(name);
            std::string ascii; for(const auto ch:std::u16string(name)) ascii.push_back(char(ch));
            run(mode,dir,"action-"+ascii,action);
        }
        PToPHeader ptop;
        std::memset(&ptop.from_.id_,0x11,sizeof(ptop.from_.id_));
        ptop.from_.instanceId_=0x8000000000000001ULL;
        std::memset(&ptop.to_.id_,0x22,sizeof(ptop.to_.id_));
        ptop.to_.instanceId_=0x8000000000000002ULL;
        ptop.actor_=Enum(PTOP_DIRECT_ACTOR); ptop.exactInstance_=true;
        run(mode,dir,"direct-ptop",ptop);
        c=0; rich(ptop,c); ptop.actor_=Enum(2); ptop.exactInstance_=false;
        run(mode,dir,"rich-ptop",ptop);
        // Initialized empty values, not guarantees about native empty constructors.
        NodeActivateRequestMessageBody activate;
        run(mode,dir,"empty-node-activate-request",activate);
        c=0; rich(activate,c); activate.isFromFmm_=false;
        run(mode,dir,"rich-node-activate-request",activate);
        reject_truncated(activate);
        activate.isFromFmm_=true; activate.sequenceNumber_=INT64_MIN+17;
        run(mode,dir,"fmm-node-activate-request",activate);
        NodeActivateReplyMessageBody activated;
        run(mode,dir,"empty-node-activate-reply",activated);
        c=0; rich(activated,c);
        run(mode,dir,"rich-node-activate-reply",activated);
        reject_truncated(activated);
        NodeDeactivateRequestMessageBody deactivate;
        run(mode,dir,"empty-node-deactivate-request",deactivate);
        c=0; rich(deactivate,c); deactivate.isFromFmm_=false;
        run(mode,dir,"rich-node-deactivate-request",deactivate);
        reject_truncated(deactivate);
        deactivate.isFromFmm_=true; deactivate.sequenceNumber_=INT64_MIN+17;
        run(mode,dir,"fmm-node-deactivate-request",deactivate);
        NodeDeactivateReplyMessageBody deactivated;
        run(mode,dir,"empty-node-deactivate-reply",deactivated);
        c=0; rich(deactivated,c);
        run(mode,dir,"rich-node-deactivate-reply",deactivated);
        reject_truncated(deactivated);
        NodeUpdateServiceRequestMessageBody service_update;
        run(mode,dir,"empty-node-update-service-request",service_update);
        c=0; rich(service_update,c);
        run(mode,dir,"rich-node-update-service-request",service_update);
        reject_truncated(service_update);
        NodeUpdateServiceReplyMessageBody service_updated;
        run(mode,dir,"empty-node-update-service-reply",service_updated);
        c=0; rich(service_updated,c);
        run(mode,dir,"rich-node-update-service-reply",service_updated);
        reject_truncated(service_updated);
        for(auto state:{"enabled","disabled"}) {
            ServiceTypeUpdateMessageBody types;
            run(mode,dir,std::string("empty-service-type-")+state,types);
            c=0; rich(types,c);
            run(mode,dir,std::string("rich-service-type-")+state,types);
            reject_truncated(types);
            ServiceTypeUpdateReplyMessageBody types_reply;
            run(mode,dir,std::string("empty-service-type-")+state+"-reply",types_reply);
            c=0; rich(types_reply,c);
            run(mode,dir,std::string("rich-service-type-")+state+"-reply",types_reply);
            reject_truncated(types_reply);
        }
        ReplicaMessageBody endpoint;
        run(mode,dir,"empty-replica-endpoint-updated",endpoint);
        c=0; rich(endpoint,c);
        run(mode,dir,"rich-replica-endpoint-updated",endpoint);
        reject_truncated(endpoint);
        ReplicaReplyMessageBody endpoint_reply;
        run(mode,dir,"empty-replica-endpoint-updated-reply",endpoint_reply);
        c=0; rich(endpoint_reply,c); endpoint_reply.errorCode_.value_=Enum(-2147017398);
        run(mode,dir,"rich-replica-endpoint-updated-reply",endpoint_reply);
        reject_truncated(endpoint_reply);
        ReportLoadMessageBody load;
        run(mode,dir,"empty-report-load",load);
        c=0; rich(load,c);
        run(mode,dir,"rich-report-load",load);
        reject_truncated(load);
        ReportLoadMessageBody legacy_load;
        c=0; rich(legacy_load.reports_,c); rich(legacy_load.senderTime_,c);
        run(mode,dir,"legacy-report-load",legacy_load);
        reject_truncated(legacy_load);
        ReportLoadMessageBody modern_load;
        c=0; rich(modern_load.reportsNew_,c); rich(modern_load.senderTime_,c);
        run(mode,dir,"modern-report-load",modern_load);
        reject_truncated(modern_load);
        for(auto name:{u"NodeActivateRequest",u"NodeActivateReply",u"NodeDeactivateRequest",
                      u"NodeDeactivateReply",u"NodeUpdateServiceRequest",u"NodeUpdateServiceReply",
                      u"ServiceTypeEnabled",u"ServiceTypeDisabled",u"ServiceTypeEnabledReply",
                      u"ServiceTypeDisabledReply",u"ReplicaEndpointUpdated",
                      u"ReplicaEndpointUpdatedReply",u"ReportLoad"}) {
            action.action_=text(name);
            std::string ascii; for(const auto ch:std::u16string(name)) ascii.push_back(char(ch));
            run(mode,dir,"action-"+ascii,action);
        }
        bodyless(mode,dir,"empty-generation-proposal");
        if(GenerationProposalActor!=12 || GenerationUpdateActor!=12 ||
           GenerationProposalReplyActor!=11 || GenerationUpdateRejectActor!=11 ||
           !GenerationProposalPriority || !GenerationProposalReplyPriority ||
           !GenerationUpdatePriority || !GenerationUpdateRejectPriority)
            throw std::runtime_error("native generation factory metadata mismatch");
        GenerationProposalReplyMessageBody proposal_reply;
        run(mode,dir,"default-generation-proposal-reply",proposal_reply);
        c=0; rich(proposal_reply,c);
        for(LONG error:{0, -2147017398, -2147017441}) {
            proposal_reply.error_.value_=Enum(error);
            run(mode,dir,"rich-generation-proposal-reply-"+std::to_string(error),proposal_reply);
        }
        reject_truncated(proposal_reply);
        proposal_reply.proposedGeneration_.generation_=INT64_MAX;
        proposal_reply.proposedGeneration_.owner_.high_=0x8000000000000001ULL;
        proposal_reply.proposedGeneration_.owner_.low_=UINT64_MAX;
        proposal_reply.currentGeneration_.generation_=INT64_MAX;
        proposal_reply.currentGeneration_.owner_.high_=0x8000000000000002ULL;
        proposal_reply.currentGeneration_.owner_.low_=0;
        proposal_reply.error_.value_=Enum(-2147017398);
        run(mode,dir,"tie-generation-proposal-reply",proposal_reply);
        reject_truncated(proposal_reply);
        GenerationUpdateMessageBody generation_update;
        run(mode,dir,"default-generation-update",generation_update);
        generation_update.fmServiceEpoch_.configurationVersion_=INT64_MIN+17;
        generation_update.fmServiceEpoch_.dataLossVersion_=INT64_MAX-29;
        run(mode,dir,"rich-generation-update",generation_update);
        reject_truncated(generation_update);
        GenerationNumber generation_reject;
        run(mode,dir,"default-generation-update-reject",generation_reject);
        generation_reject=proposal_reply.proposedGeneration_;
        run(mode,dir,"high-generation-update-reject",generation_reject);
        reject_truncated(generation_reject);
        generation_reject.owner_.low_=UINT64_MAX-1;
        run(mode,dir,"tie-generation-update-reject",generation_reject);
        reject_truncated(generation_reject);
        for(auto name:{u"GenerationProposal",u"GenerationProposalReply",
                      u"GenerationUpdate",u"GenerationUpdateReject"}) {
            action.action_=text(name);
            std::string ascii; for(const auto ch:std::u16string(name)) ascii.push_back(char(ch));
            run(mode,dir,"action-"+ascii,action);
        }
        // Native rejection is executed, not inferred from Rust behavior.
        auto bad=encode(request); bad.resize(bad.size()/2);
        ReplicaMessageBody rejected_request;
        if(decode(bad,rejected_request)) throw std::runtime_error("native accepted truncated request");
        ++native_truncation_rejections;
        bad=encode(reply); bad.resize(5);
        ReplicaReplyMessageBody rejected_reply;
        if(decode(bad,rejected_reply)) throw std::runtime_error("native accepted truncated reply");
        ++native_truncation_rejections;
        bad=encode(removal); bad.resize(bad.size()/2);
        DeleteReplicaMessageBody rejected_removal;
        if(decode(bad,rejected_removal)) throw std::runtime_error("native accepted truncated removal");
        ++native_truncation_rejections;
        bad=encode(deletion); bad.resize(bad.size()/2);
        DeleteReplicaMessageBody rejected_deletion;
        if(decode(bad,rejected_deletion)) throw std::runtime_error("native accepted truncated DeleteReplica");
        ++native_truncation_rejections;
        bad=encode(deletion_reply); bad.resize(bad.size()/2);
        ReplicaReplyMessageBody rejected_deletion_reply;
        if(decode(bad,rejected_deletion_reply)) throw std::runtime_error("native accepted truncated DeleteReplicaReply");
        ++native_truncation_rejections;
        bad=encode(up); bad.resize(bad.size()/2);
        ReplicaUpMessageBody rejected_up;
        if(decode(bad,rejected_up)) throw std::runtime_error("native accepted truncated report");
        ++native_truncation_rejections;
        bad=encode(notification); bad.resize(bad.size()/2);
        ServiceTypeNotificationRequestMessageBody rejected_notification;
        if(decode(bad,rejected_notification)) throw std::runtime_error("native accepted truncated notification");
        ++native_truncation_rejections;
        bad=encode(notification_reply); bad.resize(bad.size()/2);
        ServiceTypeNotificationReplyMessageBody rejected_notification_reply;
        if(decode(bad,rejected_notification_reply)) throw std::runtime_error("native accepted truncated notification reply");
        ++native_truncation_rejections;
        bad=encode(node_up); bad.resize(bad.size()/2);
        NodeUpMessageBody rejected_node_up;
        if(decode(bad,rejected_node_up)) throw std::runtime_error("native accepted truncated NodeUp");
        ++native_truncation_rejections;
        bad=encode(ack); bad.resize(bad.size()/2);
        NodeUpAckMessageBody rejected_ack;
        if(decode(bad,rejected_ack)) throw std::runtime_error("native accepted truncated NodeUpAck");
        ++native_truncation_rejections;
        bad=encode(ptop); bad.resize(bad.size()/2);
        PToPHeader rejected_ptop;
        if(decode(bad,rejected_ptop)) throw std::runtime_error("native accepted truncated PToP");
        ++native_truncation_rejections;
        if(native_truncation_rejections!=34)
            throw std::runtime_error("native truncation coverage mismatch");
        std::cout << "native rejects " << native_truncation_rejections << " truncations OK\n";
        return 0;
    } catch(const std::exception& error) {
        std::cerr << error.what() << "\n"; return 1;
    }
}
