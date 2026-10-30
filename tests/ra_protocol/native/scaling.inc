// Fixture-only polymorphic activation, matching the pinned source kind tags.
// The generator has extracted all base/derived field macros from native headers.
inline Serialization::IFabricSerializable* ScalingTrigger::FabricSerializerActivator(
    Serialization::FabricTypeInformation info) {
    if(info.length != sizeof(Enum)) return nullptr;
    Enum kind; std::memcpy(&kind,info.buffer,sizeof(kind));
    ScalingTrigger* p = nullptr;
    switch(LONG(kind)) {
        case 0: p = new ScalingTrigger; break;
        case 1: p = new AveragePartitionLoadScalingTrigger; break;
        case 2: p = new AverageServiceLoadScalingTrigger; break;
    }
    if(p) p->kind_=kind; return p;
}
inline Serialization::IFabricSerializable* ScalingMechanism::FabricSerializerActivator(
    Serialization::FabricTypeInformation info) {
    if(info.length != sizeof(Enum)) return nullptr;
    Enum kind; std::memcpy(&kind,info.buffer,sizeof(kind));
    ScalingMechanism* p = nullptr;
    switch(LONG(kind)) {
        case 0: p = new ScalingMechanism; break;
        case 1: p = new InstanceCountScalingMechanism; break;
        case 2: p = new AddRemoveIncrementalNamedPartitionScalingMechanism; break;
    }
    if(p) p->kind_=kind; return p;
}
inline void rich_scaling(std::vector<ServiceScalingPolicyDescription>& policies, int& c) {
    policies.resize(4);
    // Both nullptrs; explicit Invalid base types; then each real derived type.
    policies[1].trigger_=std::make_shared<ScalingTrigger>();
    policies[1].mechanism_=std::make_shared<ScalingMechanism>();
    auto trigger1=std::make_shared<AveragePartitionLoadScalingTrigger>();
    trigger1->kind_=Enum(1);
    rich(trigger1->metricName_,c); rich(trigger1->lowerLoadThreshold_,c);
    rich(trigger1->upperLoadThreshold_,c); rich(trigger1->scaleIntervalInSeconds_,c);
    auto mechanism1=std::make_shared<InstanceCountScalingMechanism>();
    mechanism1->kind_=Enum(1);
    rich(mechanism1->maximumInstanceCount_,c); rich(mechanism1->minimumInstanceCount_,c);
    rich(mechanism1->scaleIncrement_,c);
    policies[2].trigger_=trigger1; policies[2].mechanism_=mechanism1;
    auto trigger2=std::make_shared<AverageServiceLoadScalingTrigger>();
    trigger2->kind_=Enum(2);
    rich(trigger2->metricName_,c); rich(trigger2->lowerLoadThreshold_,c);
    rich(trigger2->upperLoadThreshold_,c); rich(trigger2->scaleIntervalInSeconds_,c);
    rich(trigger2->useOnlyPrimaryLoad_,c);
    auto mechanism2=std::make_shared<AddRemoveIncrementalNamedPartitionScalingMechanism>();
    mechanism2->kind_=Enum(2);
    rich(mechanism2->maximumPartitionCount_,c); rich(mechanism2->minimumPartitionCount_,c);
    rich(mechanism2->scaleIncrement_,c);
    policies[3].trigger_=trigger2; policies[3].mechanism_=mechanism2;
}
