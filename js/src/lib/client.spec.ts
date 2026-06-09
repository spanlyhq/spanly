import { IncomingMessage, ServerResponse } from 'node:http';
import { Socket } from 'node:net';
import {
  DEFAULT_REDACTED_HEADERS,
  requestToTransportContext,
  responseToTransportContext,
} from './client.js';

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
      new Set([...DEFAULT_REDACTED_HEADERS, 'x-custom-token'])
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
});
