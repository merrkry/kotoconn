use super::*;

pub(crate) struct Handler {
    pub policy: Policy,
    pub handler: DnsHandler,
}

impl kotoconn_dns::Handler for Handler {
    fn handle(
        &self,
        request: DnsRequest,
        info: DnsRequestInfo,
    ) -> BoxFuture<'_, Result<DnsHandlerResult>> {
        Box::pin(async move { Ok(self.policy.dns(&self.handler, request, Some(info)).await?) })
    }
}
