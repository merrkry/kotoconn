use super::*;

struct Services(HashMap<DnsServiceId, Arc<kotoconn_dns::Service>>);

impl kotoconn_script::DnsServices for Services {
    fn query(
        &self,
        id: DnsServiceId,
        request: DnsRequest,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<DnsResponse, String>> + Send + 'static>,
    > {
        let service = self.0.get(&id).cloned();
        Box::pin(async move {
            let service = service.ok_or_else(|| "unknown DNS service".to_owned())?;
            service
                .query(request)
                .await
                .map_err(|error| format!("{error:#}"))
        })
    }
}

pub(super) async fn install(
    policy: &Policy,
    clients: &HashMap<DialerId, Arc<Clients>>,
    scope: &Scope,
) -> Result<()> {
    let mut services = HashMap::new();
    for (id, config) in &policy.config().dns_services {
        let carrier = clients
            .get(&config.dialer)
            .context("unknown DNS service dialer")?
            .clone();
        let service = kotoconn_dns::Service::new(config.clone(), carrier, scope.child())?;
        services.insert(*id, Arc::new(service));
    }

    policy
        .install_dns_services(Arc::new(Services(services)))
        .await?;
    Ok(())
}

struct Handler {
    policy: Policy,
    id: DnsHandlerId,
}

impl kotoconn_dns::Handler for Handler {
    fn handle(
        &self,
        request: DnsRequest,
        info: DnsRequestInfo,
    ) -> BoxFuture<'_, Result<DnsHandlerResult>> {
        Box::pin(async move {
            Ok(self
                .policy
                .dns_with_info(self.id, request, Some(info))
                .await?)
        })
    }
}

pub(super) async fn bind(
    id: InboundId,
    config: &DnsListenerConfig,
    policy: &Policy,
    stopping: CancellationToken,
) -> Result<kotoconn_inbounds::BoundInbound> {
    ensure!(
        policy.config().dns_handlers.contains(&config.dns_handler),
        "unknown DNS handler"
    );
    let handler = Arc::new(Handler {
        policy: policy.clone(),
        id: config.dns_handler,
    });
    let bound = kotoconn_dns::bind(id, config, handler, stopping).await?;
    Ok(kotoconn_inbounds::BoundInbound {
        address: kotoconn_inbounds::InboundAddress::Socket(bound.address),
        run: bound.run,
    })
}
