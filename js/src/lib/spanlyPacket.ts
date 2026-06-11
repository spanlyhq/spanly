import { z } from 'zod';

// Synthetic method recorded when a client ends its session with an HTTP
// DELETE. Session termination is transport-level in MCP (no JSON-RPC
// message exists on the wire), so Spanly emits this packet itself; the
// `spanly/` prefix marks it as synthesized, like the `spanly-` session IDs.
export const SESSION_TERMINATED_METHOD = 'spanly/session/terminated';

export const mcpPacketSchema = z
  .object({
    jsonrpc: z.literal('2.0'),
  })
  .passthrough();

export const spanlyPacketTransportContextStdioSchema = z.object({
  type: z.literal('stdio'),
});

export type SpanlyPacketTransportContextStdio = z.infer<
  typeof spanlyPacketTransportContextStdioSchema
>;

export const spanlyPacketTransportContextHttpSchema = z.object({
  type: z.literal('http'),
  httpMethod: z.enum(['get', 'post', 'delete']),
  path: z.string(),
  headers: z.record(z.string(), z.string()),
  remoteAddress: z.string().optional(),
  remotePort: z.number().optional(),
});

export type SpanlyPacketTransportContextHttp = z.infer<
  typeof spanlyPacketTransportContextHttpSchema
>;

export const spanlyPacketTransportContextSchema = z.discriminatedUnion(
  'type',
  [
    spanlyPacketTransportContextStdioSchema,
    spanlyPacketTransportContextHttpSchema,
  ]
);

export type SpanlyPacketTransportContext = z.infer<
  typeof spanlyPacketTransportContextSchema
>;

export const spanlyPacketContextSchema = z.object({
  spanlyClientId: z.string(),
  spanlyMonitorId: z.string(),
  projectId: z.string().optional(),
  environmentId: z.string().optional(),
  organisationId: z.string().optional(),
});

export type SpanlyPacketContext = z.infer<typeof spanlyPacketContextSchema>;

export const spanlyPacketSchema = z.object({
  timestamp: z.number(),
  direction: z.enum(['from-client', 'to-client']),
  context: spanlyPacketContextSchema,
  transportContext: spanlyPacketTransportContextSchema,
  mcpPacket: mcpPacketSchema,
});

export type SpanlyPacket = z.infer<typeof spanlyPacketSchema>;
