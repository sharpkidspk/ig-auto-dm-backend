# IG Auto DM Backend (Render / PostgreSQL)

Production backend for the IG Auto DM Chrome extension.

## What is persisted in PostgreSQL

- one-way HMAC license digests (never raw license keys)
- device activations and session token hashes
- Free-plan daily usage counters
- message history
- monitor configuration
- eligible follower/following audience records

The private `LICENSE_PEPPER` stays only in Render environment variables.

## Render settings

- Runtime: Node
- Build command: `npm install`
- Start command: `npm start`
- Health check: `/api/health`
- Host: `0.0.0.0`

Required environment variables:

- `DATABASE_URL`
- `LICENSE_PEPPER`

For real Instagram sends also configure:

- `GRAPH_VERSION`
- `IG_USER_ID`
- `IG_ACCESS_TOKEN`
- `WEBHOOK_VERIFY_TOKEN`
- `META_APP_SECRET`

## Messaging policy

The service only processes opt-in, messaging-eligible recipient records. The followers/following audience feature is a server-side eligible audience store; it does not scrape instagram.com or bypass Meta messaging restrictions.
