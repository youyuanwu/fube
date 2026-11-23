//! Budgeted peer bodies; placement admission still precedes factory adaptation.
use super::{
    bootstrap::unique, decode, err, one_way, Headers, Properties, Result, GENERATION_HEADER,
    RA_ACTOR,
};
use fube_ra::protocol::{
    Activate, ActivateReply, CreateReplica, CreateReplicaReply, Deactivate, DeactivateReply,
    DoReconfiguration, GenerationHeader, GetLsn, GetLsnReply,
};
use furpc::{header, CodecError, DecodeBuf, Decoder, Error, HeaderId, Message};
use serde::{de::DeserializeOwned, Deserialize};

#[derive(Clone)]
pub(in crate::control) enum Incoming {
    DoReconfiguration(Payload<DoReconfiguration>),
    GetLsn(Payload<GetLsn>),
    GetLsnReply(Payload<GetLsnReply>),
    Deactivate(Payload<Deactivate>),
    DeactivateReply(Payload<DeactivateReply>),
    Activate(Payload<Activate>),
    ActivateReply(Payload<ActivateReply>),
    CreateReplica(Payload<CreateReplica>),
    CreateReplicaReply(Payload<CreateReplicaReply>),
    Unsupported,
}

#[derive(Clone)]
pub(in crate::control) struct Payload<T> {
    headers: Headers,
    action: &'static str,
    body: T,
}

impl Incoming {
    pub(in crate::control) fn decode(message: &Message) -> Result<(String, Self)> {
        one_way::decode_message(message, Self::decode_parts)
    }

    pub(super) fn decode_parts(
        headers: &Headers,
        src: &mut DecodeBuf<'_>,
    ) -> std::result::Result<(String, Self), CodecError> {
        let action = Properties::from_headers(headers)
            .map_err(CodecError::invalid_payload)?
            .action;
        let incoming = match action.as_str() {
            "DoReconfiguration" => {
                Self::DoReconfiguration(Payload::decode_parts(headers, src, "DoReconfiguration")?)
            }
            "GetLSN" => Self::GetLsn(Payload::decode_parts(headers, src, "GetLSN")?),
            "GetLSNReply" => Self::GetLsnReply(Payload::decode_parts(headers, src, "GetLSNReply")?),
            "Deactivate" => Self::Deactivate(Payload::decode_parts(headers, src, "Deactivate")?),
            "DeactivateReply" => {
                Self::DeactivateReply(Payload::decode_parts(headers, src, "DeactivateReply")?)
            }
            "Activate" => Self::Activate(Payload::decode_parts(headers, src, "Activate")?),
            "ActivateReply" => {
                Self::ActivateReply(Payload::decode_parts(headers, src, "ActivateReply")?)
            }
            "CreateReplica" => {
                Self::CreateReplica(Payload::decode_parts(headers, src, "CreateReplica")?)
            }
            "CreateReplicaReply" => {
                Self::CreateReplicaReply(Payload::decode_parts(headers, src, "CreateReplicaReply")?)
            }
            _ => {
                // Preserve foreign/unknown action failures at their original admission points.
                one_way::discard(headers, src)?;
                Self::Unsupported
            }
        };
        Ok((action, incoming))
    }
}

#[derive(Deserialize)]
struct Scalar<T> {
    value: T,
}

impl<T: DeserializeOwned> Payload<T> {
    pub(super) fn decode_parts(
        headers: &Headers,
        src: &mut DecodeBuf<'_>,
        action: &'static str,
    ) -> std::result::Result<Self, CodecError> {
        let body = one_way::Budgeted::<T>::default().decode(src)?;
        Ok(Self {
            headers: headers.clone(),
            action,
            body,
        })
    }
}

impl<T> Payload<T> {
    pub(super) fn action(&self) -> &'static str {
        self.action
    }

    pub(super) fn factory_parts(self, actors: &[i64]) -> Result<(T, Headers, i64)> {
        let headers = &self.headers;
        let actor: Scalar<i64> = decode(unique(headers, header::ACTOR).map_err(err)?.value())?;
        if !actors.contains(&actor.value)
            || decode::<Scalar<String>>(unique(headers, header::ACTION).map_err(err)?.value())?
                .value
                != self.action
        {
            return Err(err(Error::Protocol("wrong reliability actor/action")));
        }
        decode::<Scalar<bool>>(unique(headers, header::HIGH_PRIORITY).map_err(err)?.value())?;
        Ok((self.body, self.headers, actor.value))
    }

    // Header failures remain deferred until the original shutdown/placement gates.
    pub(in crate::control) fn into_parts(self) -> Result<(T, GenerationHeader)> {
        let (body, headers, _) = self.factory_parts(&[RA_ACTOR])?;
        let generation = decode(
            unique(&headers, HeaderId::new(GENERATION_HEADER).map_err(err)?)
                .map_err(err)?
                .value(),
        )?;
        Ok((body, generation))
    }
}

#[cfg(test)]
mod tests {
    use super::super::LIMIT;
    use super::*;
    use fube_ra::protocol::*;
    use fubst::serde::ObjectArray;
    use furpc::Body;

    fn generation() -> GenerationHeader {
        GenerationHeader {
            number: GenerationNumber {
                owner: [9; 16],
                generation: 7,
            },
            for_fm_replica: false,
        }
    }

    fn profiles(generation: &GenerationHeader) -> Vec<Message> {
        let mut base = AddInstance::default();
        base.unit.id.guid.0 = [3; 16];
        base.replica.id = 41;
        base.replica.instance = 52;
        base.service.name = "fabric:/peer-service".into();
        let deactivation = ReplicaDeactivationInfo {
            catchup_lsn: 123,
            ..Default::default()
        };
        let reply = AddInstanceReply {
            unit: base.unit.clone(),
            replica: base.replica.clone(),
            error: -7,
        };
        let configuration = Configuration {
            unit: base.unit.clone(),
            replicas: ObjectArray(vec![base.replica.clone()]),
            service: base.service.clone(),
        };
        vec![
            create_replica_message(
                &CreateReplica::new(base.clone(), deactivation.clone()),
                generation,
            )
            .unwrap(),
            create_replica_reply_message(&reply, generation).unwrap(),
            activate_message(
                &Activate {
                    configuration: configuration.clone(),
                    deactivation: deactivation.clone(),
                },
                generation,
            )
            .unwrap(),
            activate_reply_message(&reply, generation).unwrap(),
            deactivate_message(
                &Deactivate {
                    configuration: configuration.clone(),
                    force: true,
                    deactivation: deactivation.clone(),
                },
                generation,
            )
            .unwrap(),
            deactivate_reply_message(&reply, generation).unwrap(),
            do_reconfiguration_message(
                &DoReconfiguration {
                    configuration,
                    phase0_duration: 987,
                },
                generation,
            )
            .unwrap(),
            get_lsn_message(&base, generation).unwrap(),
            get_lsn_reply_message(
                &GetLsnReply {
                    replica: reply,
                    deactivation,
                },
                generation,
            )
            .unwrap(),
        ]
    }

    fn rebuild(message: &Message, body: Body, keep: impl Fn(HeaderId) -> bool) -> Message {
        let mut builder = Message::builder().body(body);
        for header in message
            .headers()
            .records()
            .filter(|header| keep(header.id()))
        {
            builder = builder.append_header(header.to_owned());
        }
        builder.build().unwrap()
    }

    #[test]
    fn contiguous_and_segmented_peer_profiles_match_native_factory_decoders() {
        for original in profiles(&generation()) {
            let bytes: Vec<_> = original.body().segments().flatten().copied().collect();
            for split in [0, bytes.len() / 2] {
                let body = Body::from_segments(vec![
                    Body::from_bytes(bytes[..split].to_vec()).unwrap(),
                    Body::from_bytes(bytes[split..].to_vec()).unwrap(),
                ])
                .unwrap();
                let message = rebuild(&original, body, |_| true);
                let (_, incoming) = Incoming::decode(&message).unwrap();
                match incoming {
                    Incoming::CreateReplica(payload) => assert_eq!(
                        payload.into_parts().unwrap(),
                        decode_create_replica(&original).unwrap()
                    ),
                    Incoming::CreateReplicaReply(payload) => assert_eq!(
                        payload.into_parts().unwrap(),
                        decode_create_replica_reply(&original).unwrap()
                    ),
                    Incoming::Activate(payload) => assert_eq!(
                        payload.into_parts().unwrap(),
                        decode_activate(&original).unwrap()
                    ),
                    Incoming::ActivateReply(payload) => assert_eq!(
                        payload.into_parts().unwrap(),
                        decode_activate_reply(&original).unwrap()
                    ),
                    Incoming::Deactivate(payload) => assert_eq!(
                        payload.into_parts().unwrap(),
                        decode_deactivate(&original).unwrap()
                    ),
                    Incoming::DeactivateReply(payload) => assert_eq!(
                        payload.into_parts().unwrap(),
                        decode_deactivate_reply(&original).unwrap()
                    ),
                    Incoming::DoReconfiguration(payload) => assert_eq!(
                        payload.into_parts().unwrap(),
                        decode_do_reconfiguration(&original).unwrap()
                    ),
                    Incoming::GetLsn(payload) => assert_eq!(
                        payload.into_parts().unwrap(),
                        decode_get_lsn(&original).unwrap()
                    ),
                    Incoming::GetLsnReply(payload) => assert_eq!(
                        payload.into_parts().unwrap(),
                        decode_get_lsn_reply(&original).unwrap()
                    ),
                    Incoming::Unsupported => panic!("native peer profile was unsupported"),
                }
            }
        }
    }

    #[test]
    fn factory_header_failures_stay_deferred_but_malformed_bodies_fail_first() {
        let original = get_lsn_message(&GetLsn::default(), &generation()).unwrap();
        let id = HeaderId::new(GENERATION_HEADER).unwrap();
        let missing = rebuild(&original, original.body().clone(), |header| header != id);
        let (_, Incoming::GetLsn(payload)) = Incoming::decode(&missing).unwrap() else {
            panic!("expected a decoded GetLSN payload");
        };
        assert_eq!(
            payload.into_parts().unwrap_err(),
            err(decode_get_lsn(&missing).unwrap_err())
        );
        for id in [header::ACTOR, header::ACTION, header::HIGH_PRIORITY, id] {
            let mut builder = Message::builder().body(original.body().clone());
            for header in original.headers().records() {
                builder = builder.append_header(header.to_owned());
            }
            let duplicate = builder
                .append_header(
                    original
                        .headers()
                        .records()
                        .find(|header| header.id() == id)
                        .unwrap()
                        .to_owned(),
                )
                .build()
                .unwrap();
            let (_, Incoming::GetLsn(payload)) = Incoming::decode(&duplicate).unwrap() else {
                panic!("expected a decoded GetLSN payload");
            };
            assert_eq!(
                payload.into_parts().unwrap_err(),
                err(decode_get_lsn(&duplicate).unwrap_err())
            );
        }
        let malformed = rebuild(&missing, Body::from_bytes(vec![0xff]).unwrap(), |_| true);
        assert_eq!(
            Incoming::decode(&malformed).err().unwrap(),
            super::super::predecode(&malformed).unwrap_err()
        );
    }

    #[test]
    fn peer_body_allocation_budget_and_foreign_action_failure_points_are_preserved() {
        let body = Activate {
            configuration: Configuration {
                replicas: ObjectArray(vec![Replica::default(); 256]),
                ..Default::default()
            },
            ..Default::default()
        };
        let oversized = activate_message(&body, &generation()).unwrap();
        assert!(oversized.body().len() <= LIMIT);
        let expected = super::super::predecode(&oversized).unwrap_err();
        assert!(expected.contains("cluster native decode"));
        assert_eq!(Incoming::decode(&oversized).err().unwrap(), expected);

        let foreign = lfum_upload_message(&LfumUpload::default()).unwrap();
        let (action, incoming) = Incoming::decode(&foreign).unwrap();
        assert_eq!(action, "LFUMUpload");
        assert!(matches!(incoming, Incoming::Unsupported));
        let malformed = rebuild(&foreign, Body::from_bytes(vec![0xff]).unwrap(), |_| true);
        assert_eq!(
            Incoming::decode(&malformed).err().unwrap(),
            super::super::predecode(&malformed).unwrap_err()
        );
        let unknown = Message::builder()
            .append_header(header::action("UnknownPeerControl").unwrap())
            .build()
            .unwrap();
        assert_eq!(
            Incoming::decode(&unknown).err().unwrap(),
            super::super::predecode(&unknown).unwrap_err()
        );
    }
}
