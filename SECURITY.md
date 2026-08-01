# Security policy

## Supported versions

Security fixes are currently made on the latest source revision. No released version has a long-term support commitment while the project remains in alpha.

## Reporting a vulnerability

Please report suspected vulnerabilities privately to support@samsarix.com. Include the affected revision, reproduction steps, likely impact, and any known mitigations. Do not include live API keys, private chat content, or other third-party data.

Samsarix LLC will acknowledge a complete report as availability permits, coordinate remediation and disclosure in good faith, and credit reporters who request attribution when doing so is safe. This policy does not promise a bounty or a fixed response deadline.

## Deployment boundary

The engine is not an identity provider. Deployments authenticate users in a host application, issue short-lived room tokens, terminate TLS at a trusted proxy, configure exact browser origins, and protect the SQLite file plus operator/signing secrets. The operator API key and HS256 signing secret grant high-impact access and must never be shipped to ordinary clients. See [Identity and room authorization](docs/AUTHORIZATION.md) for the token profile, rotation impact, and current revocation limitations.
