//! Shared consuming reply authority for normal actor requests and duplex ingress.

use super::*;

pub(super) struct NativeReplyContext {
    pub(super) shared: Weak<Shared>,
    pub(super) target: Arc<GatewayTarget>,
    pub(super) id: MessageId,
    pub(super) authorize: EnvelopeValidator,
}

impl ReplyContext for NativeReplyContext {
    fn reply(self: Box<Self>, message: Message, deadline: Instant) -> ChannelFuture<'static, ()> {
        self.reply_inner(message, deadline, None)
    }

    fn reply_fenced(
        self: Box<Self>,
        message: Message,
        deadline: Instant,
        valid: WritePredicate,
    ) -> ChannelFuture<'static, ()> {
        self.reply_inner(message, deadline, Some(valid))
    }
}

impl NativeReplyContext {
    fn reply_inner(
        self: Box<Self>,
        message: Message,
        deadline: Instant,
        valid: Option<WritePredicate>,
    ) -> ChannelFuture<'static, ()> {
        Box::pin(async move {
            let shared = self.shared.upgrade().ok_or(Error::Closed)?;
            match submit(
                &shared,
                &self.target,
                message,
                deadline,
                Submission::Reply(self.id, self.authorize),
                None,
                valid,
                false,
            )
            .await?
            {
                Outcome::Written => Ok(()),
                _ => unreachable!(),
            }
        })
    }
}
