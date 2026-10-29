// Source-baseline oracle. Native stream/helper code is compiled unmodified.
// This adapter constructs values independently; it does not construct wire bytes.
#include <Serialization.h>
#include <filesystem>

template<class T> void rich(T&, int&);
inline void rich(LONG64& v,int& c) { v=-0x123456780000LL-++c; }
inline void rich(ULONG64& v,int& c) { v=0xfedcba9800000000ULL+ ++c; }
inline void rich(LONG& v,int& c) { v=1000+ ++c; }
inline void rich(ULONG& v,int& c) { v=4000000000U+ ++c; }
inline void rich(bool& v,int& c) { v=++c%2==0; }
inline void rich(double& v,int& c) { v=1.25+ ++c; }
inline void rich(byte& v,int& c) { v=++c%256; }
inline void rich(GUID& v,int& c) {
    ++c; for(size_t i=0;i<16;++i) reinterpret_cast<UCHAR*>(&v)[i]=(c+i)%256;
}
inline std::wstring text(const std::u16string& value) {
    // Avoid libstdc++'s system-width wchar string functions under -fshort-wchar.
    std::wstring result;
    for(char16_t ch:value) result.push_back(static_cast<wchar_t>(ch));
    return result;
}
inline void rich(std::wstring& v,int& c) {
    std::string number=std::to_string(++c);
    v.clear(); v.push_back(L'v');
    for(char ch:number) v.push_back(ch);
    v.push_back(0x03c0); v.push_back(0xd83e); v.push_back(0xdd80);
}
template<class T> void rich(std::vector<T>& values,int& c) {
    values.resize(2); for(auto& value:values) rich(value,c);
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
        for(LONG destination:{10,11,12}) {
            actor.actor_=Enum(destination);
            run(mode,dir,"actor-"+std::to_string(destination),actor);
        }
        ActionHeader action;
        action.action_=text(u"AddInstance"); run(mode,dir,"action-request",action);
        action.action_=text(u"AddInstanceReply"); run(mode,dir,"action-reply",action);
        HighPriorityHeader priority;
        priority.highPriority_=true; run(mode,dir,"priority",priority);
        // Native rejection is executed, not inferred from Rust behavior.
        auto bad=encode(request); bad.resize(bad.size()/2);
        ReplicaMessageBody rejected_request;
        if(decode(bad,rejected_request)) throw std::runtime_error("native accepted truncated request");
        bad=encode(reply); bad.resize(5);
        ReplicaReplyMessageBody rejected_reply;
        if(decode(bad,rejected_reply)) throw std::runtime_error("native accepted truncated reply");
        std::cout << "native rejects 2 truncations OK\n";
        return 0;
    } catch(const std::exception& error) {
        std::cerr << error.what() << "\n"; return 1;
    }
}
