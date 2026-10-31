#![allow(dead_code)]
use fube_hosting::*;

pub fn package(name: &str, number: u32) -> PackageId {
    PackageId {
        application: ApplicationId {
            type_name: "App".into(),
            number,
        },
        name: name.into(),
    }
}
pub fn type_id(name: &str) -> TypeInstance {
    TypeInstance {
        service_type: ServiceTypeId {
            package: package("Pkg", 1),
            name: name.into(),
        },
        activation: Activation::SHARED,
        public_activation: String::new(),
    }
}
pub fn context(runtime: &str, host: &str) -> RuntimeContext {
    RuntimeContext {
        runtime_id: runtime.into(),
        host_id: host.into(),
        process_id: 42,
        package: type_id("Widget").package_instance(),
        version: VersionInstance {
            instance: 7,
            ..VersionInstance::default()
        },
        code_package: "Code".into(),
        package_instance_sequence: 123,
    }
}
pub fn hosting() -> Hosting {
    Hosting::new(Limits::default()).unwrap()
}
pub fn completed_host(h: &mut Hosting, host: &str) {
    h.add_host(host.into()).unwrap();
    h.start_host(host, &ManualClock(10), 100).unwrap();
    assert_eq!(h.finish_host(host, true).unwrap().reply, Ok(()));
}
pub fn valid_runtime(h: &mut Hosting, runtime: &str, host: &str) -> RuntimeContext {
    let c = context(runtime, host);
    assert_eq!(h.register_runtime(c.clone()).result, Ok(()));
    c
}
pub fn declared(h: &mut Hosting, name: &str) -> TypeInstance {
    let id = type_id(name);
    h.declare_type(id.clone(), "package-component".into(), Ok(()))
        .unwrap();
    id
}
pub fn registered(h: &mut Hosting, name: &str, c: &RuntimeContext) -> TypeInstance {
    let id = declared(h, name);
    assert_eq!(h.register_type(&id, c).result, Ok(()));
    id
}
pub fn disable(h: &mut Hosting, id: &TypeInstance) -> Timer {
    h.register_failure("failure".into()).unwrap();
    h.disable_type(id, "failure", &ManualClock(0), 30, true)
        .unwrap()
        .unwrap()
}
