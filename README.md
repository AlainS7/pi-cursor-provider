# pi-cursor-provider

[![npm version](https://img.shields.io/npm/v/pi-cursor-provider.svg)](https://www.npmjs.com/package/pi-cursor-provider)

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
5. **Context injection** — forwards `AGENTS.md` / `SKILLS.md` guidance from project, user, and extension locations

## Context injection (AGENTS / SKILLS) + privacy knobs

The proxy merges local guidance into Cursor `requestContext` rules by scanning common locations:

- Workspace: `./AGENTS.md`, `./.cursor/AGENTS.md`, `./.pi/AGENTS.md`, `./.pi/skills/SKILLS.md`, `./.pi/agent/skills/SKILLS.md`
- Home (default on): `~/.pi/AGENTS.md`, `~/.pi/agent/AGENTS.md`, `~/.pi/agent/skills/SKILLS.md`, plus extension-provided `AGENTS.md` / `SKILLS.md` under `~/.pi/agent/extensions/*`

**Home scanning opt-out** (workspace-only):

```bash
PI_CURSOR_INCLUDE_HOME_CONTEXT=0 pi
```

**Rule body dedupe** (drops later rules with identical trimmed `content`, including duplicates between prompt-derived rules and on-disk files):

```bash
PI_CURSOR_DEDUPE_RULE_CONTENT=0 pi
```

## Install

```bash
# Via pi install
pi install npm:pi-cursor-provider

# Or manually
git clone https://github.com/ndraiman/pi-cursor-provider ~/.pi/agent/extensions/cursor-provider
cd ~/.pi/agent/extensions/cursor-provider
npm install
```

## Usage

```
/login cursor     # authenticate via browser
/model            # select a Cursor model
```

## Model Mapping

Cursor exposes many model variants that encode **effort level** (`low`, `medium`, `high`, `xhigh`, `max`, `none`) and **speed** (`-fast`) or **thinking** (`-thinking`) in the model ID. This extension deduplicates them so pi's reasoning effort setting controls the effort level.

### How it works

Each raw Cursor model ID is parsed into components:

```
{base}-{effort}[-fast|-thinking]
```

Examples:

| Raw Cursor ID | Base | Effort | Variant |
|---|---|---|---|
| `gpt-5.4-medium` | `gpt-5.4` | `medium` | — |
| `gpt-5.4-high-fast` | `gpt-5.4` | `high` | `-fast` |
| `claude-4.6-opus-max-thinking` | `claude-4.6-opus` | `max` | `-thinking` |
| `gpt-5.1-codex-max-high` | `gpt-5.1-codex-max` | `high` | — |
| `composer-2` | `composer-2` | — | — |

Models sharing the same `(base, variant)` with **≥2 effort levels** and a sensible default (`medium` or no-suffix) are collapsed into a single entry with `supportsReasoningEffort: true`. Pi's thinking level maps to the effort suffix:

| Pi Level | Cursor Suffix |
|---|---|
| `minimal` | `none` (if available) or `low` |
| `low` | `low` |
| `medium` | `medium` or no suffix (default) |
| `high` | `high` |
| `xhigh` | `max` (Claude) or `xhigh` (GPT) |

The proxy inserts the effort before `-fast`/`-thinking`:

```
pi selects: gpt-5.4-fast  +  effort: high  →  Cursor receives: gpt-5.4-high-fast
pi selects: gpt-5.4       +  effort: medium  →  Cursor receives: gpt-5.4-medium
pi selects: composer-2     +  (no effort)     →  Cursor receives: composer-2
```

When a group is **collapsed**, the proxy registers one model with `supportsReasoningEffort: true` and an internal effort map (see table above).

To make high/low/xhigh selection easier in pi's `/model` picker, the extension also registers fixed-effort aliases (for example `gpt-5.3-codex-high`, `gpt-5.3-codex-low`) alongside the collapsed base row.

**Collapsed** when Cursor returns either:

- **Multiple** effort suffixes for the same `(base, -fast, -thinking)` group, or
- **A single** variant whose parsed effort suffix is **non-empty** (for example only `claude-4.5-opus-high` is listed). The suffix is removed from the displayed ID so Pi's reasoning-effort setting supplies it.

**Left as-is** (raw Cursor ID on that row, `supportsReasoningEffort: false`) when the group has **one** variant and the parsed effort suffix is **empty**—typically IDs with no effort segment, such as `composer-2`, `gemini-3.1-pro`, or `kimi-k2.5`.

### Disabling the mapping

To see all raw Cursor model variants without dedup:

```bash
PI_CURSOR_RAW_MODELS=1 pi
```

## Session Management

The proxy maintains conversation state per pi session, enabling multi-turn conversations with Cursor models while preserving forks, tool continuations, and interruptions correctly.

### How it works

- **Session tracking** — pi's session ID is injected into requests via a `before_provider_request` hook. The proxy keys bridge state and stored conversation state from that real session ID.
- **Checkpoints** — Cursor returns a conversation checkpoint after completed turns. The proxy stores that checkpoint, plus the completed-turn count and a fingerprint of the completed structured history, and reuses it only when the incoming history still matches.
- **Session-scoped state** — real pi session state is kept in memory until explicit cleanup or process restart. Anonymous fallback state can still be TTL-evicted.
- **Lifecycle cleanup** — session state is cleaned up on pi lifecycle events such as session switch, fork, `/tree`, and shutdown.

### Tool continuations

When Cursor pauses for a tool call, the proxy keeps the live upstream bridge open and waits for pi to send the tool result on the next request. That tool result is sent back into the same in-flight Cursor run, so the tool continuation stays part of the original user turn instead of inflating completed history.

### Interruptions

If the client disconnects or interrupts a turn mid-stream, the proxy cancels the upstream Cursor run and does **not** commit the pending checkpoint. Checkpoints are only committed after a turn finishes successfully.

## Default context files

On Cursor `requestContext` requests, the proxy includes guidance from available `AGENTS.md` and `SKILLS.md` files in:

- Project: `./AGENTS.md`, `./.cursor/AGENTS.md`, `./.pi/AGENTS.md`, `./.pi/skills/SKILLS.md`, `./.pi/agent/skills/SKILLS.md`
- User agent home: `~/.pi/AGENTS.md`, `~/.pi/agent/AGENTS.md`, `~/.pi/agent/skills/SKILLS.md`
- Installed extensions: `*/AGENTS.md` and `*/skills/SKILLS.md` under both project and home extension directories

Only existing, non-empty files up to 256KB are included.

## Pi-injected context rules

Besides filesystem context files, the proxy now parses Pi's injected system prompt sections and converts them into Cursor `requestContext.rules`:

- `# Project Context` blocks are converted into **global** rules
- `<available_skills>...</available_skills>` entries are converted into **agent-fetched** rules

When these sections are extracted, they are removed from the system prompt body sent to Cursor to avoid duplicating the same context in two channels.

### Session fork

When you navigate back in pi's session tree and branch from an earlier point, the proxy discards the stored checkpoint whenever the completed history no longer matches the stored checkpoint metadata. That includes both:

- completed turn count mismatches, and
- same-depth branch changes detected via completed-history fingerprint mismatch.

After discarding a stale checkpoint, the proxy reconstructs proper protobuf conversation turns from the message history pi sends, so Cursor sees the actual conversation structure at the fork point.

### Session resume

Conversation state is stored in memory. If the proxy restarts, checkpoints are lost. On the next request, pi sends the full conversation history, and the proxy reconstructs structured protobuf turns from that history instead of relying on an inline plaintext fallback.

That reconstruction preserves:

- assistant messages
- tool calls
- tool results
- final assistant text after tool results

## Requirements

- [Pi](https://github.com/badlogic/pi-mono)
- [Node.js](https://nodejs.org) >= 18
- Active [Cursor](https://cursor.com) subscription

## Development

```bash
npm install
npm test
```

## Debug log timeline

When `PI_CURSOR_PROVIDER_DEBUG=1` is enabled, the proxy writes timestamped JSONL logs to `os.tmpdir()` by default. You can turn a log into a compact human-readable timeline with:

```bash
npm run debug:timeline -- --latest
npm run debug:timeline -- /path/to/pi-cursor-provider-debug-2026-04-08T14-06-07-565Z-41184.log
```

Add `--json` if you want the parsed summary as JSON instead of formatted text.

## Credits

OAuth flow and gRPC proxy adapted from [opencode-cursor](https://github.com/ephraimduncan/opencode-cursor) by Ephraim Duncan.
