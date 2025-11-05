# Forma TypeScript SDK

Client-side helper for interacting with the Forma service (`cmd/server`).

## Install

```
npm install @forma/sdk
```

## Usage

```ts
import { FormaClient } from "@forma/sdk";

const client = new FormaClient({ baseUrl: "http://localhost:8080" });

const result = await client.advancedQuery({
  schemaName: "lead",
  condition: {
    l: "and",
    c: [
      { a: "status", v: "equals:hot" },
      { a: "personalInfo.age", v: "gte:30" }
    ]
  },
});

console.log(result.data);
```

The SDK targets Node.js ≥ 18 (native `fetch` support) or any environment exposing a `fetch` compatible API.
