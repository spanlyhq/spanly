# Security Policy

## Reporting a vulnerability

Please do not report security vulnerabilities through public GitHub issues.

Instead, use GitHub's private vulnerability reporting: go to the
[Security tab](https://github.com/spanlyhq/spanly/security) of this repository
and click "Report a vulnerability". We will respond as quickly as we can,
normally within a few business days.

Please include enough detail to reproduce the issue: affected package
(`@spanly/sdk`, `spanly` on PyPI, or the CLI), version, and a proof of
concept if you have one.

## Supported versions

The SDKs and CLI are pre-1.0. Security fixes are released for the latest
version of each artifact; please upgrade to the most recent release before
reporting.

## Scope

This repository contains the client-side SDKs and CLI. For vulnerabilities
in the Spanly platform itself (dashboard, ingest API), use the same private
reporting channel and we will route it internally.
