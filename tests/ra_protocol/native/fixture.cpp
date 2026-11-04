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
        // Native empty ctor leaves bools unspecified: explicitly initialize the fixture.
        ReplicaUpMessageBody up;
        up.isLastReplicaUpMessage_=false; up.isFromFMM_=false;
        run(mode,dir,"explicit-empty-replica-up",up);
        c=0; rich(up,c);
        for(int last:{0,1}) for(int fmm:{0,1}) {
            up.isLastReplicaUpMessage_=last!=0; up.isFromFMM_=fmm!=0;
            run(mode,dir,"rich-replica-up-"+std::to_string(last)+"-"+std::to_string(fmm),up);
        }
        for(auto name:{u"RemoveInstance",u"RemoveInstanceReply",u"ReplicaUp",u"ReplicaUpReply"}) {
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
        // Native rejection is executed, not inferred from Rust behavior.
        auto bad=encode(request); bad.resize(bad.size()/2);
        ReplicaMessageBody rejected_request;
        if(decode(bad,rejected_request)) throw std::runtime_error("native accepted truncated request");
        bad=encode(reply); bad.resize(5);
        ReplicaReplyMessageBody rejected_reply;
        if(decode(bad,rejected_reply)) throw std::runtime_error("native accepted truncated reply");
        bad=encode(removal); bad.resize(bad.size()/2);
        DeleteReplicaMessageBody rejected_removal;
        if(decode(bad,rejected_removal)) throw std::runtime_error("native accepted truncated removal");
        bad=encode(up); bad.resize(bad.size()/2);
        ReplicaUpMessageBody rejected_up;
        if(decode(bad,rejected_up)) throw std::runtime_error("native accepted truncated report");
        bad=encode(notification); bad.resize(bad.size()/2);
        ServiceTypeNotificationRequestMessageBody rejected_notification;
        if(decode(bad,rejected_notification)) throw std::runtime_error("native accepted truncated notification");
        bad=encode(notification_reply); bad.resize(bad.size()/2);
        ServiceTypeNotificationReplyMessageBody rejected_notification_reply;
        if(decode(bad,rejected_notification_reply)) throw std::runtime_error("native accepted truncated notification reply");
        bad=encode(node_up); bad.resize(bad.size()/2);
        NodeUpMessageBody rejected_node_up;
        if(decode(bad,rejected_node_up)) throw std::runtime_error("native accepted truncated NodeUp");
        bad=encode(ack); bad.resize(bad.size()/2);
        NodeUpAckMessageBody rejected_ack;
        if(decode(bad,rejected_ack)) throw std::runtime_error("native accepted truncated NodeUpAck");
        bad=encode(ptop); bad.resize(bad.size()/2);
        PToPHeader rejected_ptop;
        if(decode(bad,rejected_ptop)) throw std::runtime_error("native accepted truncated PToP");
        std::cout << "native rejects 9 truncations OK\n";
        return 0;
    } catch(const std::exception& error) {
        std::cerr << error.what() << "\n"; return 1;
    }
}
