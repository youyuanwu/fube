//! Independently construct helper cases using real production functions.
use fube_ra::{entity::*, protocol::*};
use fube_ra_store::{MemoryBackend, Settings as StoreSettings, Store};
use std::{cmp::Ordering, fs, sync::Arc, time::Duration};

const WAIT: Duration = Duration::from_secs(3);

fn order(value: Ordering) -> i32 {
    match value {
        Ordering::Less => -1,
        Ordering::Equal => 0,
        Ordering::Greater => 1,
    }
}

fn service_type(app: &str, number: u32, package: &str, name: &str) -> ServiceType {
    ServiceType {
        package: PackageId {
            application: ApplicationId {
                type_name: app.into(),
                number,
            },
            name: package.into(),
        },
        name: name.into(),
    }
}

fn identifiers() {
    let names = [
        "Type", "tYPE", "é", "É", "σ", "ς", "ß", "SS", "A\0B", "a\0C", "𐐨", "𐐀", "\u{8000}",
        "\u{7fff}",
    ];
    for (i, a) in names.iter().enumerate() {
        for (j, b) in names.iter().enumerate() {
            let result = compare_service_types(
                &service_type("", 0, "ignored", a),
                &service_type("", 0, "other", b),
            );
            println!(
                "name {i} {j} order={} equal={}",
                order(result),
                u8::from(result.is_eq())
            );
        }
    }
    let ids = [
        service_type("App", 1, "Pkg", "Type"),
        service_type("app", 1, "PKG", "tYPE"),
        service_type("A", 2, "Pkg", "Type"),
        service_type("Z", 0, "Pkg", "Type"),
        service_type("App", 1, "Other", "Type"),
        service_type("App", 1, "Pkg", "Other"),
        service_type("App", u32::MAX, "Pkg", "Type"),
        service_type("", 0, "Pkg", "Type"),
        service_type("", 0, "Other", "Type"),
        service_type("\0x", 0, "Pkg", "Type"),
        service_type("\u{8000}", 1, "Pkg", "Type"),
        service_type("\u{7fff}", 1, "Pkg", "Type"),
    ];
    for (i, a) in ids.iter().enumerate() {
        for (j, b) in ids.iter().enumerate() {
            let result = compare_service_types(a, b);
            println!(
                "identity {i} {j} order={} equal={}",
                order(result),
                u8::from(result.is_eq())
            );
        }
    }
}

fn failure(family: &str, step: &str, policy: RetryPolicy, count: &mut u64) {
    let action = match policy.failure(count).unwrap() {
        Action::Warning => "Warning",
        Action::Drop => "Drop",
        Action::None => "None",
    };
    println!("failure {family} {step} count={count} action={action}");
}

fn clear(family: &str, step: &str, policy: RetryPolicy, count: &mut u64) {
    let clear = policy.clear(*count);
    *count = 0;
    println!(
        "clear {family} {step} clear={} count={count}",
        u8::from(clear)
    );
}

fn flags(family: &str, step: &str, policy: RetryPolicy, count: u64) {
    println!(
        "flags {family} {step} count={count} abort={}",
        u8::from(policy.flags(count) == ABORT)
    );
}

fn set_policy(cfg: &NativeConfig, state: RetryState, policy: RetryPolicy) {
    for (field, value) in [
        (Threshold::Warning, policy.warning),
        (Threshold::Drop, policy.drop),
        (Threshold::Restart, policy.restart),
        (Threshold::Error, policy.error),
    ] {
        cfg.set(state, field, value);
    }
}

fn policies(cfg: &NativeConfig) {
    let states = [
        ("open", RetryState::Opening),
        ("registration", RetryState::Registration),
        ("close", RetryState::Closing),
        ("delete", RetryState::Deleting),
        ("registration-drop", RetryState::RegistrationAtDrop),
    ];
    let defaults = states.map(|(_, state)| cfg.policy(state));
    for ((family, state), initial) in states.into_iter().zip(defaults) {
        set_policy(cfg, state, initial);
        println!(
            "defaults {family} warning={} drop={} restart={} error={}",
            initial.warning, initial.drop, initial.restart, initial.error
        );
        let mut count = 0;
        failure(family, "default", cfg.policy(state), &mut count);
        cfg.set(state, Threshold::Warning, 2);
        failure(family, "dynamic-warning", cfg.policy(state), &mut count);
        cfg.set(state, Threshold::Drop, 2);
        failure(family, "lowered-drop", cfg.policy(state), &mut count);
        cfg.set(state, Threshold::Drop, 4);
        flags(family, "next-drop", cfg.policy(state), count);
        cfg.set(state, Threshold::Warning, 4);
        failure(
            family,
            "warning-precedes-drop",
            cfg.policy(state),
            &mut count,
        );
        cfg.set(state, Threshold::Warning, 0);
        cfg.set(state, Threshold::Drop, -1);
        cfg.set(state, Threshold::Restart, 5);
        failure(family, "restart-unconsumed", cfg.policy(state), &mut count);
        cfg.set(state, Threshold::Error, 6);
        failure(family, "error-unconsumed", cfg.policy(state), &mut count);
        clear(
            family,
            "current-zero-warning",
            cfg.policy(state),
            &mut count,
        );
        cfg.set(state, Threshold::Warning, -2);
        cfg.set(state, Threshold::Error, -1);
        clear(family, "zero-count-negative", cfg.policy(state), &mut count);
        set_policy(
            cfg,
            state,
            RetryPolicy {
                warning: 0,
                drop: 0,
                restart: 0,
                error: 0,
            },
        );
        failure(family, "all-zero", cfg.policy(state), &mut count);
        flags(family, "zero-drop", cfg.policy(state), count);
        count = 0;
        cfg.set(state, Threshold::Drop, 1);
        flags(family, "one-drop", cfg.policy(state), count);
        cfg.set(state, Threshold::Warning, 100);
        cfg.set(state, Threshold::Error, 100);
        failure(family, "drop", cfg.policy(state), &mut count);
        clear(
            family,
            "raised-current-threshold",
            cfg.policy(state),
            &mut count,
        );
        cfg.set(state, Threshold::Warning, i32::MIN);
        cfg.set(state, Threshold::Error, i32::MIN);
        cfg.set(state, Threshold::Drop, -1);
        cfg.set(state, Threshold::Restart, -1);
        clear(family, "signed-min-clear", cfg.policy(state), &mut count);
        cfg.set(state, Threshold::Warning, 1);
        cfg.set(state, Threshold::Drop, 1);
        failure(family, "equality-precedence", cfg.policy(state), &mut count);
    }
    cfg.set(RetryState::Registration, Threshold::Restart, -7);
    cfg.set(RetryState::Registration, Threshold::Error, -8);
    let shared = cfg.policy(RetryState::RegistrationAtDrop);
    println!(
        "shared-registration restart={} error={}",
        shared.restart, shared.error
    );
    set_policy(cfg, RetryState::Opening, RetryPolicy::open());
    let mixed = RetryPolicy::read(|field| {
        let value = cfg.get(RetryState::Opening, field);
        if matches!(field, Threshold::Warning) {
            cfg.set(RetryState::Opening, Threshold::Warning, 2);
            cfg.set(RetryState::Opening, Threshold::Drop, 2);
        }
        value
    });
    println!(
        "mixed-read warning={} drop={} restart={} error={}",
        mixed.warning, mixed.drop, mixed.restart, mixed.error
    );
    failure("mixed", "captured", mixed, &mut 1);
    failure("mixed", "fresh", cfg.policy(RetryState::Opening), &mut 1);
}

fn generation(number: i64, high: u64, low: u64) -> GenerationNumber {
    let mut owner = [0; 16];
    owner[..8].copy_from_slice(&high.to_le_bytes());
    owner[8..].copy_from_slice(&low.to_le_bytes());
    GenerationNumber {
        generation: number,
        owner,
    }
}

fn generations() -> [GenerationNumber; 8] {
    let values = [
        generation(-1, u64::MAX, u64::MAX),
        generation(6, u64::MAX, u64::MAX),
        generation(7, 0x8000000000000000, 1),
        generation(8, 0, 0),
        generation(7, 0x7fffffffffffffff, u64::MAX),
        generation(7, 0x8000000000000000, 0),
        generation(7, 0x8000000000000000, 2),
        generation(7, 0x8000000000000001, 0),
    ];
    for (i, a) in values.iter().enumerate() {
        for (j, b) in values.iter().enumerate() {
            println!("generation {i} {j} older={}", u8::from(a.older_than(b)));
        }
    }
    values
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args: Vec<_> = std::env::args_os().collect();
    if args.len() == 2 && args[1] == "--identity" {
        println!(
            "{}",
            option_env!("FUBE_RA_SOURCE_DIGEST").unwrap_or("unbound")
        );
        return;
    }
    assert_eq!(args.len(), 1, "usage: ra_helper_conformance [--identity]");
    let units: Vec<u8> = (0..=u16::MAX)
        .flat_map(|unit| native_upper_unit(unit).to_le_bytes())
        .collect();
    fs::write("upper-units.bin", units).unwrap();
    identifiers();
    let store = Store::new(MemoryBackend::new(), StoreSettings::default())
        .await
        .unwrap();
    let source = NodeInstance {
        id: [8; 16],
        instance: 30,
    };
    let executor = Executor::new(
        Arc::new(store.clone()),
        Admission {
            local_node: NodeInstance {
                id: [4; 16],
                instance: 20,
            },
            fm_source: source.clone(),
            generation: generation(7, 0x8000000000000000, 1),
            owner_ready: true,
            node_open: true,
            node_activated: true,
            fabric_upgrading: false,
        },
        Settings::default(),
    )
    .unwrap();
    policies(&executor.config());
    for (i, number) in generations().into_iter().enumerate() {
        let disposition = executor
            .submit(Command::ServiceTypeReply {
                body: Box::default(),
                source: source.clone(),
                generation: GenerationHeader {
                    number,
                    for_fm_replica: false,
                },
            })
            .unwrap()
            .wait(WAIT)
            .await
            .unwrap();
        assert!(matches!(
            disposition,
            Disposition::Applied | Disposition::IgnoredStale
        ));
        println!(
            "dispatch {i} accepted={}",
            u8::from(disposition == Disposition::Applied)
        );
    }
    executor.close(WAIT).await.unwrap();
    store.close().await.unwrap();
}
