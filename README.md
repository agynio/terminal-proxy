# Terminal Proxy

Terminal Proxy issues short-lived terminal tickets and bridges WebSocket clients
to runner `Exec` streams.

## Configuration

- `TERMINAL_PROXY_TICKET_SIGNING_KEY` is required and must be at least 32 bytes.
- `TERMINAL_PROXY_WEBSOCKET_PING_INTERVAL` defaults to `30s`.
- `TERMINAL_PROXY_WEBSOCKET_PING_TIMEOUT` defaults to `30s`.
- `ZITI_ENABLED=true` loads a pre-provisioned Terminal Proxy OpenZiti identity from
  `ZITI_IDENTITY_FILE` instead of self-enrolling as another service type. The
  identity must have the `terminal-proxy-hosts` role attribute so the existing
  `terminal-proxy-dial-runners` static policy can dial `runner-services`.
