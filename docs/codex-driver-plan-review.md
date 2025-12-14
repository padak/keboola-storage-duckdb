# Codex Review: DuckDB Driver Plan

## Shrnutí poznámek
- Protobuf kontrakt je správně akcentovaný, ale kontroluj skutečné `storage-driver-common` definice: v plánu jsou alias handlery, které v seznamu commands/proto nejsou. Vyhni se generování mrtvých handlerů.
- PHP ↔ Connection je protobuf, PHP ↔ Python je REST/JSON. Zvaž sjednocení schémat (protobuf/gRPC i pro Python) nebo aspoň jediný zdroj pravdy pro DTO/Pydantic, jinak hrozí tichý drift.
- Doplň operacionál: auth/TLS mezi PHP a Pythonem, rate limiting a request tracing + structured logging/metrics už ve Fázi 1/2, ať se dá ladit.
- Dev branche kopírují celé `.duckdb` soubory. Pro větší projekty zvaž COW/sparse copy nebo schema-only branch; definuj merge strategii (replace vs upsert) a zamykání, když main zrovna zapisuje.
- Storage files (on-prem FS + metadata) chybí v plánu lifecycle: cleanup stagingu, kvóty per projekt, checksum/encryption a backup/DR – zapoj do Fáze 10/12.
- Write queue: per-project queue je OK, ale chybí specifika – timeouts, retry, co když worker spadne, jak obsloužit dlouhé read-y vs write lock; doplň do Fáze 4/11 jasné chování.
