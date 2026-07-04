import { IncomingMessage, ServerResponse } from 'node:http';
import { Socket } from 'node:net';
import {
  DEFAULT_REDACTED_HEADERS,
  MonitorOptions,
  SpanlyClient,
  SYNTHETIC_SESSION_ID_PREFIX,
  requestToTransportContext,
  responseToTransportContext,
} from './client.js';
import { SESSION_TERMINATED_METHOD } from './spanlyPacket.js';

function makeRequest(headers: Record<string, string>): IncomingMessage {
  const req = new IncomingMessage(new Socket());
  req.method = 'POST';
  req.url = '/mcp';
  req.headers = headers;
  return req;
}

describe('requestToTransportContext', () => {
  it('redacts credential-bearing headers by default', () => {
    const req = makeRequest({
      authorization: 'Bearer super-secret',
      cookie: 'session=abc123',
      'proxy-authorization': 'Basic dXNlcjpwYXNz',
      'x-api-key': 'key-123',
      'content-type': 'application/json',
      'mcp-session-id': 'session-1',
    });

    const context = requestToTransportContext(req);

    expect(context.headers).toEqual({
      authorization: '[REDACTED]',
      cookie: '[REDACTED]',
      'proxy-authorization': '[REDACTED]',
      'x-api-key': '[REDACTED]',
      'content-type': 'application/json',
      'mcp-session-id': 'session-1',
    });
  });

  it('matches header names case-insensitively', () => {
    const req = makeRequest({ Authorization: 'Bearer secret' });

    const context = requestToTransportContext(req);

    expect(context.headers).toEqual({ Authorization: '[REDACTED]' });
  });

  it('redacts additional headers from a custom set', () => {
    const req = makeRequest({
      authorization: 'Bearer secret',
      'x-custom-token': 'secret-token',
      accept: 'application/json',
    });

    const context = requestToTransportContext(
      req,
      new Set([...DEFAULT_REDACTED_HEADERS, 'x-custom-token']),
    );

    expect(context.headers).toEqual({
      authorization: '[REDACTED]',
      'x-custom-token': '[REDACTED]',
      accept: 'application/json',
    });
  });
});

describe('responseToTransportContext', () => {
  it('redacts set-cookie and preserves other headers', () => {
    const req = makeRequest({});
    const res = new ServerResponse(req);
    res.setHeader('Set-Cookie', ['session=abc123; HttpOnly', 'theme=dark']);
    res.setHeader('Content-Type', 'text/event-stream');

    const context = responseToTransportContext(res, req);

    expect(context.headers).toEqual({
      'set-cookie': '[REDACTED]',
      'content-type': 'text/event-stream',
    });
  });

  it('captures the response status code and rate-limit headers', () => {
    const req = makeRequest({});
    const res = new ServerResponse(req);
    res.statusCode = 429;
    res.setHeader('Retry-After', '30');
    res.setHeader('RateLimit-Remaining', '0');

    const context = responseToTransportContext(res, req);

    expect(context.statusCode).toBe(429);
    expect(context.headers['retry-after']).toBe('30');
    expect(context.headers['ratelimit-remaining']).toBe('0');
  });
});

describe('session ID injection', () => {
  const initializeBody = {
    jsonrpc: '2.0',
    id: 1,
    method: 'initialize',
    params: {},
  };
  const toolsCallBody = {
    jsonrpc: '2.0',
    id: 2,
    method: 'tools/call',
    params: {},
  };

  beforeEach(() => {
    jest
      .spyOn(globalThis, 'fetch')
      .mockImplementation(
        async () =>
          new Response(JSON.stringify({ success: true }), { status: 200 }),
      );
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  interface MonitoredTransport {
    handleRequest(
      req: IncomingMessage,
      res: ServerResponse,
      parsedBody?: unknown,
    ): Promise<void>;
  }

  async function monitoredTransport(
    serverBehavior: (res: ServerResponse) => void,
    options?: MonitorOptions,
  ): Promise<MonitoredTransport> {
    const transport: MonitoredTransport = {
      handleRequest: async (req, res) => {
        await new Promise((resolve) => setImmediate(resolve));
        serverBehavior(res);
      },
    };
    const connected: unknown[] = [];
    const mcpServer = {
      connect: async (t: unknown) => {
        connected.push(t);
      },
    };
    const client = new SpanlyClient({ apiKey: 'spanly_us_test' });
    client.monitor(mcpServer, options);
    await mcpServer.connect(transport);
    return transport;
  }

  function jsonResponse(res: ServerResponse) {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ jsonrpc: '2.0', id: 1, result: {} }));
  }

  it('injects a synthetic session ID on initialize responses', async () => {
    const transport = await monitoredTransport(jsonResponse);
    const req = makeRequest({ 'content-type': 'application/json' });
    const res = new ServerResponse(req);

    await transport.handleRequest(req, res, initializeBody);

    expect(String(res.getHeader('mcp-session-id'))).toMatch(
      new RegExp(`^${SYNTHETIC_SESSION_ID_PREFIX}`),
    );
  });

  it('detects initialize requests arriving as stream chunks', async () => {
    const transport = await monitoredTransport(jsonResponse);
    const req = makeRequest({ 'content-type': 'application/json' });
    const res = new ServerResponse(req);

    const pending = transport.handleRequest(req, res);
    req.emit('data', Buffer.from(JSON.stringify(initializeBody)));
    await pending;

    expect(String(res.getHeader('mcp-session-id'))).toMatch(
      new RegExp(`^${SYNTHETIC_SESSION_ID_PREFIX}`),
    );
  });

  it('does not inject when the server assigns its own session ID', async () => {
    const transport = await monitoredTransport((res) => {
      res.writeHead(200, {
        'content-type': 'application/json',
        'mcp-session-id': 'server-session',
      });
      res.end(JSON.stringify({ jsonrpc: '2.0', id: 1, result: {} }));
    });
    const req = makeRequest({ 'content-type': 'application/json' });
    const res = new ServerResponse(req);

    await transport.handleRequest(req, res, initializeBody);

    expect(res.getHeader('mcp-session-id')).toBeUndefined();
  });

  it('does not inject on non-initialize requests', async () => {
    const transport = await monitoredTransport(jsonResponse);
    const req = makeRequest({ 'content-type': 'application/json' });
    const res = new ServerResponse(req);

    await transport.handleRequest(req, res, toolsCallBody);

    expect(res.getHeader('mcp-session-id')).toBeUndefined();
  });

  it('does not inject on error responses', async () => {
    const transport = await monitoredTransport((res) => {
      res.writeHead(400, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ jsonrpc: '2.0', id: 1, error: {} }));
    });
    const req = makeRequest({ 'content-type': 'application/json' });
    const res = new ServerResponse(req);

    await transport.handleRequest(req, res, initializeBody);

    expect(res.getHeader('mcp-session-id')).toBeUndefined();
  });

  it('does not inject when disabled via options', async () => {
    const transport = await monitoredTransport(jsonResponse, {
      injectSessionId: false,
    });
    const req = makeRequest({ 'content-type': 'application/json' });
    const res = new ServerResponse(req);

    await transport.handleRequest(req, res, initializeBody);

    expect(res.getHeader('mcp-session-id')).toBeUndefined();
  });
});

describe('session termination capture', () => {
  beforeEach(() => {
    jest
      .spyOn(globalThis, 'fetch')
      .mockImplementation(
        async () =>
          new Response(JSON.stringify({ success: true }), { status: 200 }),
      );
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  interface MonitoredTransport {
    handleRequest(
      req: IncomingMessage,
      res: ServerResponse,
      parsedBody?: unknown,
    ): Promise<void>;
  }

  async function monitoredTransport(
    serverBehavior: (res: ServerResponse) => void,
    options?: MonitorOptions,
  ): Promise<MonitoredTransport> {
    const transport: MonitoredTransport = {
      handleRequest: async (req, res) => {
        await new Promise((resolve) => setImmediate(resolve));
        serverBehavior(res);
      },
    };
    const connected: unknown[] = [];
    const mcpServer = {
      connect: async (t: unknown) => {
        connected.push(t);
      },
    };
    const client = new SpanlyClient({ apiKey: 'spanly_us_test' });
    client.monitor(mcpServer, options);
    await mcpServer.connect(transport);
    return transport;
  }

  function makeDeleteRequest(headers: Record<string, string>): IncomingMessage {
    const req = makeRequest(headers);
    req.method = 'DELETE';
    return req;
  }

  async function collectedPackets(): Promise<
    {
      direction: string;
      mcpPacket: Record<string, unknown>;
      transportContext: Record<string, unknown>;
    }[]
  > {
    // collect() is fire-and-forget; let the pending posts run.
    await new Promise((resolve) => setImmediate(resolve));
    return (globalThis.fetch as jest.Mock).mock.calls.map(([, init]) =>
      JSON.parse((init as RequestInit).body as string),
    );
  }

  it('records a synthetic notification when a DELETE terminates the session', async () => {
    const transport = await monitoredTransport((res) => {
      res.writeHead(200).end();
    });
    const req = makeDeleteRequest({ 'mcp-session-id': 'session-1' });
    const res = new ServerResponse(req);

    await transport.handleRequest(req, res);

    const packets = await collectedPackets();
    expect(packets).toHaveLength(1);
    expect(packets[0].direction).toBe('from-client');
    expect(packets[0].mcpPacket).toEqual({
      jsonrpc: '2.0',
      method: SESSION_TERMINATED_METHOD,
      params: { sessionId: 'session-1' },
    });
    expect(packets[0].transportContext).toMatchObject({
      type: 'http',
      httpMethod: 'delete',
      headers: { 'mcp-session-id': 'session-1' },
    });
  });

  it('does not record termination when the server rejects the DELETE', async () => {
    const transport = await monitoredTransport((res) => {
      res.writeHead(404).end();
    });
    const req = makeDeleteRequest({ 'mcp-session-id': 'unknown' });
    const res = new ServerResponse(req);

    await transport.handleRequest(req, res);

    expect(await collectedPackets()).toHaveLength(0);
  });

  it('does not record termination for a DELETE without a session ID', async () => {
    const transport = await monitoredTransport((res) => {
      res.writeHead(200).end();
    });
    const req = makeDeleteRequest({});
    const res = new ServerResponse(req);

    await transport.handleRequest(req, res);

    expect(await collectedPackets()).toHaveLength(0);
  });

  it('passes the synthetic packet through onCollect', async () => {
    const seen: { direction: string; method: unknown }[] = [];
    const transport = await monitoredTransport(
      (res) => {
        res.writeHead(200).end();
      },
      {
        onCollect: (direction, _context, mcpPacket) => {
          seen.push({ direction, method: mcpPacket.method });
          return null;
        },
      },
    );
    const req = makeDeleteRequest({ 'mcp-session-id': 'session-2' });
    const res = new ServerResponse(req);

    await transport.handleRequest(req, res);

    expect(seen).toEqual([
      { direction: 'from-client', method: SESSION_TERMINATED_METHOD },
    ]);
    expect(await collectedPackets()).toHaveLength(0);
  });

  it('intercepts a DELETE for a synthetic session without reaching the server', async () => {
    const serverBehavior = jest.fn((res: ServerResponse) => {
      res.writeHead(200).end();
    });
    const transport = await monitoredTransport(serverBehavior);
    const sessionId = `${SYNTHETIC_SESSION_ID_PREFIX}abc123`;
    const req = makeDeleteRequest({ 'mcp-session-id': sessionId });
    const res = new ServerResponse(req);

    await transport.handleRequest(req, res);

    expect(serverBehavior).not.toHaveBeenCalled();
    expect(res.statusCode).toBe(200);

    const packets = await collectedPackets();
    expect(packets).toHaveLength(1);
    expect(packets[0].mcpPacket).toEqual({
      jsonrpc: '2.0',
      method: SESSION_TERMINATED_METHOD,
      params: { sessionId },
    });
  });

  it('forwards a synthetic-looking DELETE to the server when injection is disabled', async () => {
    const serverBehavior = jest.fn((res: ServerResponse) => {
      res.writeHead(200).end();
    });
    const transport = await monitoredTransport(serverBehavior, {
      injectSessionId: false,
    });
    const req = makeDeleteRequest({
      'mcp-session-id': `${SYNTHETIC_SESSION_ID_PREFIX}abc123`,
    });
    const res = new ServerResponse(req);

    await transport.handleRequest(req, res);

    expect(serverBehavior).toHaveBeenCalledTimes(1);
  });
});
