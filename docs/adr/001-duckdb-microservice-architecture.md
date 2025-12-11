# ADR-001: DuckDB jako Python microservice misto PHP driveru

## Status

Accepted

## Datum

2024-12-11

## Kontext

Potrebujeme pridat podporu DuckDB jako storage backend do Keboola Storage API. Existujici drivery (BigQuery, Snowflake) jsou implementovany jako PHP knihovny, ktere primo komunikuji s databazi.

Pro DuckDB existuje PHP knihovna `satur-io/duckdb-php`, ktera je oficalne doporucena v DuckDB dokumentaci.

## Rozhodnuti

**Nebudeme pouzivat PHP FFI driver pro DuckDB.**

Misto toho vytvorime **samostatnou Python microservice** (`duckdb-api-service`), ktera:
- Bezi jako samostatny proces/container
- Poskytuje REST API pro vsechny storage operace
- Integruje DuckDB primo pres oficialni Python knihovnu

V Connection vytvorime tenky PHP HTTP klient (`Package/StorageDriverDuckdb`), ktery pouze preklada storage commands na REST API volani.

## Duvody

### Problemy s PHP FFI driverem

1. **Nestabilita FFI**: `satur-io/duckdb-php` je wrapper pres PHP FFI (Foreign Function Interface). FFI vola primo C knihovnu DuckDB, coz znamena:
   - Crash v C kodu = crash celeho PHP procesu
   - Nutnost spravovat DuckDB `.so`/`.dll` soubory na serveru
   - Slozite debugovani

2. **Omezena izolace**: V multi-tenant prostredi jako Keboola je riziko, ze problem jednoho tenanta ovlivni ostatni.

3. **Vyzaduje PHP 8.3+**: S povolenym FFI extension, coz nemusi byt vsude dostupne.

4. **Neni nativni PDO/extension**: Na rozdil od MySQL, PostgreSQL ci Snowflake neexistuje stabilni PDO driver.

### Vyhody Python microservice

1. **1st-class DuckDB podpora**: Python `duckdb` knihovna je oficialni, stabilni a plne podporovana.

2. **Izolace**: DuckDB bezi v samostatnem procesu. Crash = restart containeru, ne pad PHP.

3. **Jednoduchy vyvoj**: `pip install duckdb` vs komplikovane FFI setup.

4. **Testovatelnost**: Standardni pytest, zadne FFI hacky.

5. **Flexibilita**: Moznost pridat caching, connection pooling, metriky nezavisle na PHP.

6. **Technologicka nezavislost**: Muzeme pouzit nejlepsi nastroje pro DuckDB ekosystem.

## Dusledky

### Pozitivni

- Stabilnejsi a spolehlivejsi reseni
- Jednodussi vyvoj a udrzba
- Lepsi izolace v produkci
- Moznost nezavisleho skalovani DuckDB sluzby

### Negativni

- Dalsi sluzba k provozovani (container, monitoring)
- HTTP latence mezi PHP a Python sluzbou
- Slozitejsi deployment (2 komponenty misto 1)

### Neutralni

- REST API je self-documenting a testovatelne nezavisle na PHP
- PHP klient v Connection zustava tenky a jednoduchy

## Alternativy (zamitnuty)

1. **PHP FFI driver** - zamitnut kvuli nestabilite a rizikum popsanym vyse

2. **gRPC misto REST** - odlozeno, REST je jednodussi pro zacatek, gRPC muzeme pridat pozdeji pokud bude potreba vykon

3. **MotherDuck (managed DuckDB)** - neodpovida pozadavku na self-hosted reseni

## Reference

- [DuckDB PHP dokumentace](https://duckdb.org/docs/stable/clients/php.html)
- [satur-io/duckdb-php](https://github.com/satur-io/duckdb-php)
- [DuckDB Python dokumentace](https://duckdb.org/docs/api/python/overview)
