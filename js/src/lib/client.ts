import {
  SpanlyPacket,
  SpanlyPacketContext,
  SpanlyPacketTransportContext,
  SpanlyPacketTransportContextHttp,
  SpanlyPacketTransportContextStdio,
  mcpPacketSchema,
} from './spanlyPacket.js';
import { Readable, Writable } from 'node:stream';
import { IncomingMessage, ServerResponse } from 'node:http';
import { z } from 'zod';

export type SpanlyRegion = 'us' | 'eu';

const DEFAULT_INGEST_URLS: Record<SpanlyRegion, string> = {
  us: 'https://ingest.us.spanly.com',
  eu: 'https://ingest.eu.spanly.com',
};

function parseRegionFromApiKey(apiKey: string): SpanlyRegion {
  if (apiKey.startsWith('spanly_us_')) return 'us';
  if (apiKey.startsWith('spanly_eu_')) return 'eu';
  throw new Error('Invalid API key format: must start with spanly_us_ or spanly_eu_');
}

export interface SpanlyClientOptions {
  ingestUrl?: (region: SpanlyRegion) => string;
  apiKey?: string;
}

// 503-only retry config. The ingest server returns 503 + Retry-After when
// the in-flight queue is past its watermark; we back off and try again
// rather than dropping the packet (which used to be the default behavior
// before the batching pipeline went live).
const COLLECT_MAX_ATTEMPTS = Number(process.env['SPANLY_COLLECT_MAX_ATTEMPTS']) || 4;
const COLLECT_BACKOFF_BASE_MS = 500;
const COLLECT_BACKOFF_MAX_MS = 5_000;

// Parse a Retry-After header. Seconds-form first, then HTTP-date fallback.
// Returns ms to wait, or undefined when the header is missing or unparseable.
function parseRetryAfter(value: string | null): number | undefined {
  if (!value) return undefined;
  const seconds = Number(value);
  if (Number.isFinite(seconds) && seconds >= 0) {
    return seconds * 1000;
  }
  const date = Date.parse(value);
  if (!Number.isNaN(date)) {
    return Math.max(0, date - Date.now());
  }
  return undefined;
}

function backoffDelayMs(attempt: number): number {
  const base = Math.min(COLLECT_BACKOFF_BASE_MS * 2 ** attempt, COLLECT_BACKOFF_MAX_MS);
  const jitter = base * 0.2 * (Math.random() * 2 - 1); // ±20%
  return Math.max(0, Math.floor(base + jitter));
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

const collectWarningSchema = z.object({
  code: z.string(),
  message: z.string(),
});

export type CollectWarning = z.infer<typeof collectWarningSchema>;

const collectResultSchema = z.object({
  success: z.boolean(),
  warnings: z.array(collectWarningSchema).optional(),
});

interface StdioServerTransport {
  _stdin: Readable;
  _stdout: Writable;
}

interface StreamableHTTPServerTransport {
  handleRequest(
    req: IncomingMessage,
    res: ServerResponse,
    parsedBody?: unknown
  ): Promise<void>;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type Transport = any;

function isStdioServerTransport(
  transport: Transport
): transport is StdioServerTransport {
  return '_stdin' in transport;
}

function isStreamableHTTPServerTransport(
  transport: Transport
): transport is StreamableHTTPServerTransport {
  return 'handleRequest' in transport;
}

function parseStreamBody(body: string) {
  const lines = body.split('\n');
  const dataLine = lines.find((line) => line.startsWith('data:'));

  if (dataLine === undefined) {
    throw new Error('Invalid body: data line not found');
  }

  return dataLine.slice(5);
}

function bodyToObject(body: unknown) {
  if (Buffer.isBuffer(body) || body instanceof Uint8Array) {
    body = Buffer.from(body).toString('utf-8');
  }

  if (typeof body === 'string') {
    const bodyJson = body.startsWith('event:') ? parseStreamBody(body) : body;
    return JSON.parse(bodyJson);
  } else if (typeof body === 'object') {
    return body;
  }

  throw new Error('Invalid body: not a string or object');
}

function parseBodies(body: unknown): McpPacket[] | null {
  try {
    const obj = bodyToObject(body);
    if (Array.isArray(obj)) {
      return obj.map((o) => mcpPacketSchema.parse(o));
    }
    return [mcpPacketSchema.parse(obj)];
  } catch (error) {
    console.warn('Error parsing MCP packet', body, error);
    return null;
  }
}

function httpMethodFromRequest(req: IncomingMessage) {
  switch (req.method) {
    case 'GET':
      return 'get';
    case 'POST':
      return 'post';
    case 'DELETE':
      return 'delete';
  }

  return 'get';
}

function remoteFromRequest(req: IncomingMessage): {
  remoteAddress?: string;
  remotePort?: number;
} {
  const address = req.socket?.remoteAddress;
  const port = req.socket?.remotePort;
  return {
    ...(address ? { remoteAddress: address } : {}),
    ...(typeof port === 'number' ? { remotePort: port } : {}),
  };
}

// Credential-bearing headers whose values are replaced with [REDACTED]
// before the packet leaves the process. Matched case-insensitively.
export const DEFAULT_REDACTED_HEADERS = [
  'authorization',
  'cookie',
  'set-cookie',
  'proxy-authorization',
  'x-api-key',
] as const;

const defaultRedactedHeaderSet: ReadonlySet<string> = new Set(
  DEFAULT_REDACTED_HEADERS
);

function buildRedactedHeaderSet(extra?: string[]): ReadonlySet<string> {
  if (!extra?.length) return defaultRedactedHeaderSet;
  return new Set([
    ...defaultRedactedHeaderSet,
    ...extra.map((name) => name.toLowerCase()),
  ]);
}

function redactHeader(
  key: string,
  value: string,
  redactedHeaders: ReadonlySet<string>
): string {
  return redactedHeaders.has(key.toLowerCase()) ? '[REDACTED]' : value;
}

export function requestToTransportContext(
  req: IncomingMessage,
  redactedHeaders: ReadonlySet<string> = defaultRedactedHeaderSet
): SpanlyPacketTransportContextHttp {
  return {
    type: 'http',
    httpMethod: httpMethodFromRequest(req),
    path: req.url || '/',
    headers: Object.fromEntries(
      Object.entries(req.headers).map(([key, value]) => [
        key,
        redactHeader(
          key,
          Array.isArray(value) ? value.join(', ') : value || '',
          redactedHeaders
        ),
      ])
    ),
    ...remoteFromRequest(req),
  };
}

export function responseToTransportContext(
  res: ServerResponse,
  req: IncomingMessage,
  redactedHeaders: ReadonlySet<string> = defaultRedactedHeaderSet
): SpanlyPacketTransportContextHttp {
  return {
    type: 'http',
    httpMethod: httpMethodFromRequest(req),
    path: req.url || '/',
    headers: Object.fromEntries(
      Object.entries(res.getHeaders()).map(([key, value]) => [
        key,
        redactHeader(
          key,
          Array.isArray(value) ? value.join(', ') : value?.toString() || '',
          redactedHeaders
        ),
      ])
    ),
    ...remoteFromRequest(req),
  };
}

interface MinimalMcpServer {
  connect(transport: Transport): Promise<void>;
}

export type McpPacket = z.infer<typeof mcpPacketSchema>;

export interface MonitorOptions {
  onError?: (error: Error) => void;
  onWarning?: (warnings: CollectWarning[]) => void;
  onCollect?: (
    direction: SpanlyPacket['direction'],
    context: SpanlyPacketContext,
    mcpPacket: McpPacket
  ) => McpPacket | null;
  /**
   * Additional header names to redact from captured transport context,
   * on top of DEFAULT_REDACTED_HEADERS. Case-insensitive.
   */
  redactHeaders?: string[];
  /**
   * When the server does not assign an MCP session ID (stateless
   * Streamable HTTP transports), set a synthetic `Mcp-Session-Id` header
   * on initialize responses so subsequent requests from the same client
   * are grouped into a session in Spanly. Defaults to true.
   */
  injectSessionId?: boolean;
}

export const SYNTHETIC_SESSION_ID_PREFIX = 'spanly-';

const SESSION_ID_HEADER = 'mcp-session-id';

function newSyntheticSessionId(): string {
  return `${SYNTHETIC_SESSION_ID_PREFIX}${crypto.randomUUID()}`;
}

function containsInitializeRequest(body: unknown): boolean {
  try {
    const obj = bodyToObject(body);
    const items = Array.isArray(obj) ? obj : [obj];
    return items.some(
      (item) =>
        item !== null &&
        typeof item === 'object' &&
        (item as { method?: unknown }).method === 'initialize'
    );
  } catch {
    return false;
  }
}

function headersArgContainsSessionId(arg: unknown): boolean {
  if (Array.isArray(arg)) {
    return arg.some(
      (v) => typeof v === 'string' && v.toLowerCase() === SESSION_ID_HEADER
    );
  }
  if (arg !== null && typeof arg === 'object') {
    return Object.keys(arg).some(
      (key) => key.toLowerCase() === SESSION_ID_HEADER
    );
  }
  return false;
}

function getServerInfo(mcpServer: MinimalMcpServer) {
  type MinimalMcpServerExtended = MinimalMcpServer & {
    server?: {
      _serverInfo?: {
        name: string;
        version: string;
      };
    };
  };

  const mcpServerExtended = mcpServer as MinimalMcpServerExtended;
  return mcpServerExtended.server?._serverInfo;
}

export class SpanlyClient {
  public clientId: string;
  public url: string;
  public apiKey: string;

  constructor(options: SpanlyClientOptions) {
    this.clientId = crypto.randomUUID();

    const apiKey = options.apiKey ?? process.env.SPANLY_API_KEY;
    if (!apiKey) {
      throw new Error(
        'Spanly API key is required. Pass it as `apiKey` in options or set the SPANLY_API_KEY environment variable.'
      );
    }
    this.apiKey = apiKey;

    const region = parseRegionFromApiKey(apiKey);
    this.url = options.ingestUrl
      ? options.ingestUrl(region)
      : DEFAULT_INGEST_URLS[region];
  }

  async _collect(
    direction: SpanlyPacket['direction'],
    context: SpanlyPacketContext,
    transportContext: SpanlyPacketTransportContext,
    body: unknown
  ) {
    if (body === undefined) {
      return null;
    }

    const parsedBodies = parseBodies(body);

    if (parsedBodies === null || parsedBodies.length === 0) {
      return null;
    }

    const timestamp = Date.now();
    const results = await Promise.all(
      parsedBodies.map((mcpPacket) =>
        this._postPacket({
          timestamp,
          direction,
          context,
          transportContext,
          mcpPacket,
        })
      )
    );

    return results[0];
  }

  private async _postPacket(spanlyPacket: SpanlyPacket) {
    const url = new URL(`/collect`, this.url);
    const requestBody = JSON.stringify(spanlyPacket);

    // Retry on 503 only, honoring Retry-After. Other non-2xx statuses and
    // network errors keep the prior behavior (throw immediately) so users
    // who wired `onError` don't see new retry semantics for unrelated
    // failure modes.
    let result: Response | undefined;
    for (let attempt = 0; attempt < COLLECT_MAX_ATTEMPTS; attempt++) {
      result = await fetch(url.toString(), {
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${this.apiKey}`,
        },
        method: 'POST',
        body: requestBody,
      });
      if (result.status !== 503) break;
      if (attempt === COLLECT_MAX_ATTEMPTS - 1) break;
      const retryAfterMs =
        parseRetryAfter(result.headers.get('Retry-After')) ?? backoffDelayMs(attempt);
      await sleep(retryAfterMs);
    }

    if (!result || !result.ok) {
      throw new Error(`Failed to collect MCP packet: ${result?.statusText ?? 'unknown'}`);
    }

    const parsedResult = collectResultSchema.parse(await result.json());

    if (!parsedResult.success) {
      throw new Error(`Failed to collect MCP packet`);
    }

    return parsedResult;
  }

  monitor(mcpServer: MinimalMcpServer, options?: MonitorOptions) {
    const originalConnect = mcpServer.connect.bind(mcpServer);

    mcpServer.connect = async (transport: Transport) => {
      const context: SpanlyPacketContext = {
        spanlyClientId: this.clientId,
        spanlyMonitorId: crypto.randomUUID(),
        mcpServerInfo: getServerInfo(mcpServer),
      };

      const collect = (
        direction: SpanlyPacket['direction'],
        transportContext: SpanlyPacketTransportContext,
        body: unknown,
        contextOverride?: SpanlyPacketContext
      ) => {
        if (body === undefined) return;
        const effectiveContext = contextOverride ?? context;

        if (options?.onCollect) {
          const parsed = parseBodies(body);
          if (parsed === null) return;
          const filtered = parsed
            .map((p) => options.onCollect?.(direction, effectiveContext, p) ?? null)
            .filter((p): p is McpPacket => p !== null);
          if (filtered.length === 0) return;
          body = filtered.length === 1 ? filtered[0] : filtered;
        }

        this._collect(direction, effectiveContext, transportContext, body).then(
          (result) => {
            if (result?.warnings?.length && options?.onWarning) {
              options.onWarning(result.warnings);
            }
          },
          (error) => {
            if (options?.onError) {
              options.onError(
                error instanceof Error ? error : new Error(String(error))
              );
            } else {
              console.warn('Error collecting MCP packet', error);
            }
          }
        );
      };

      if (isStdioServerTransport(transport)) {
        const transportContext: SpanlyPacketTransportContextStdio = {
          type: 'stdio',
        };

        transport._stdin.on('data', (chunk) => {
          collect('from-client', transportContext, chunk);
        });

        transport._stdout.on('data', (chunk) => {
          collect('to-client', transportContext, chunk);
        });
      } else if (isStreamableHTTPServerTransport(transport)) {
        const redactedHeaders = buildRedactedHeaderSet(options?.redactHeaders);
        const injectSessionId = options?.injectSessionId ?? true;
        const originalHandleRequest = transport.handleRequest.bind(transport);

        transport.handleRequest = async (
          req: IncomingMessage,
          res: ServerResponse,
          parsedBody?: unknown
        ) => {
          // Fresh monitorId per HTTP txn → batcher pairs c→s halves on it
          // without colliding on rid across concurrent transactions.
          const txnContext: SpanlyPacketContext = {
            ...context,
            spanlyMonitorId: crypto.randomUUID(),
          };

          let sawInitialize =
            parsedBody !== undefined && containsInitializeRequest(parsedBody);

          // Must run before headers flush and before the to-client collect,
          // so both the client and the captured response carry the ID.
          const maybeInjectSessionId = (statusCode: number) => {
            if (
              !injectSessionId ||
              !sawInitialize ||
              res.headersSent ||
              statusCode < 200 ||
              statusCode >= 300 ||
              res.getHeader(SESSION_ID_HEADER) !== undefined
            ) {
              return;
            }
            res.setHeader(SESSION_ID_HEADER, newSyntheticSessionId());
          };

          if (parsedBody !== undefined) {
            collect(
              'from-client',
              requestToTransportContext(req, redactedHeaders),
              parsedBody,
              txnContext
            );
          }

          req.on('data', (chunk) => {
            sawInitialize = sawInitialize || containsInitializeRequest(chunk);
            collect(
              'from-client',
              requestToTransportContext(req, redactedHeaders),
              chunk,
              txnContext
            );
          });

          const originalWriteHead = res.writeHead;

          res.writeHead = ((...args: Parameters<typeof originalWriteHead>) => {
            const [statusCode, second] = args;
            // writeHead(status, statusMessage?, headers?): headers may sit
            // at index 1 or 2 depending on the overload used.
            const headersArg =
              typeof second === 'string'
                ? (args as readonly unknown[])[2]
                : second;
            if (!headersArgContainsSessionId(headersArg)) {
              maybeInjectSessionId(statusCode);
            }
            return originalWriteHead.apply(res, args);
          }) as typeof originalWriteHead;

          const originalWrite = res.write;

          res.write = ((...args: Parameters<typeof originalWrite>) => {
            maybeInjectSessionId(res.statusCode);
            collect(
              'to-client',
              responseToTransportContext(res, req, redactedHeaders),
              args[0],
              txnContext
            );
            return originalWrite.apply(res, args);
          }) as typeof originalWrite;

          const originalEnd = res.end;

          res.end = ((...args: Parameters<typeof originalEnd>) => {
            maybeInjectSessionId(res.statusCode);
            collect(
              'to-client',
              responseToTransportContext(res, req, redactedHeaders),
              args[0],
              txnContext
            );
            return originalEnd.apply(res, args);
          }) as typeof originalEnd;

          return originalHandleRequest(req, res, parsedBody);
        };
      }

      return await originalConnect(transport);
    };
  }
}
