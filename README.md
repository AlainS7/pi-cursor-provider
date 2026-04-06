# pi-cursor-provider

[Pi](https://github.com/badlogic/pi-mono) extension that provides access to [Cursor](https://cursor.com) models via OAuth authentication and a local OpenAI-compatible proxy.

## How it works

```
pi  →  openai-completions  →  localhost:PORT/v1/chat/completions
                                      ↓
                              proxy.ts (HTTP server)
                                      ↓
                              h2-bridge.mjs (Node HTTP/2)
                                      ↓
                              api2.cursor.sh gRPC
```

1. **PKCE OAuth** — browser-based login to Cursor, no client secret needed
2. **Model discovery** — queries Cursor's `GetUsableModels` gRPC endpoint
3. **Local proxy** — translates OpenAI `/v1/chat/completions` to Cursor's protobuf/HTTP2 Connect protocol
4. **Tool routing** — rejects Cursor's native tools, exposes pi's tools via MCP

## Install

```bash
# Clone into pi's extensions directory
git clone https://github.com/ndraiman/pi-cursor-provider ~/.pi/agent/extensions/cursor-provider
cd ~/.pi/agent/extensions/cursor-provider
npm install
```

## Usage

```
/login cursor     # authenticate via browser
/model            # select a Cursor model
```

## Requirements

- [Pi](https://github.com/badlogic/pi-mono)
- [Node.js](https://nodejs.org) >= 18 (for the HTTP/2 bridge)
- Active [Cursor](https://cursor.com) subscription

## Credits

OAuth flow and gRPC proxy adapted from [opencode-cursor](https://github.com/ephraimduncan/opencode-cursor) by Ephraim Duncan.
