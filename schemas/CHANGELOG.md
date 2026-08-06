# Economic schema changelog

## 1.0.0 — 2026-08-06

- Initial frozen cross-chain economic observation schema with 85 fields.
- Separates configured, accrued/eligible, paid, returned, forfeited and slashed quantities.
- Represents amount fields as decimal strings to preserve raw integer precision.
- Adds decision-time evidence, forbidden future fields, W3C-style provenance identifiers, finality, cross-chain link grade, coverage status and validation rules.
- Distinguishes independent truth from protocol-only resolution.
- Validated against one real UMA Polygon→Ethereum episode and 13 Tellor Layer dispute episodes.

Future incompatible changes require a major version. New optional fields require a minor version; documentation-only corrections require a patch version.
