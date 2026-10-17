# Meta DWH — lokalna demonstracija

Rešenje selekcionog zadatka: istorijska analitika korišćenja tabela i prijavljene potrošnje Trino upita. DuckDB čuva DWH, Python i SQL ga pune, a dva kumulativna paketa JSONL/CSV fajlova simuliraju izvore. Nisu potrebni Trino klaster, AWS nalog, SQLite ni poseban servis registra.

**DuckDB baza `warehouse/meta_dwh.duckdb` generiše se lokalno i ne čuva se u Gitu.** Repozitorijum sadrži kod, oba paketa sintetičkih podataka i primere rezultata u `outputs/`. Dokumentacija dizajna i pretpostavki napisana je na engleskom da može poslužiti kao deo predaje zadatka.

[Key assumptions](docs/DESIGN.md#key-assumptions) izdvaja pretpostavke uvedene radi jednostavnije demonstracije, posebno istorijski registar i izvorna vremena ingestije/izmene. Njihovo postojanje u stvarnom sistemu nije potvrđeno.

[Design rationale](docs/DESIGN.md#design-rationale) kratko objašnjava zašto su izabrani ovaj model, granularnost, metrike i način punjenja.

## Pokretanje

Python 3.10+; implementacija je proverena na Pythonu 3.12 i DuckDB-u 1.4.3. Iz ovog foldera, u PowerShell-u:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install --disable-pip-version-check -r requirements.txt
```

Posle kloniranja napravi i napuni bazu, proveri ponovno pokretanje i generiši izveštaje:

```powershell
.\.venv\Scripts\python.exe pipeline.py init
.\.venv\Scripts\python.exe pipeline.py incremental
.\.venv\Scripts\python.exe pipeline.py incremental
.\.venv\Scripts\python.exe pipeline.py report
```

Treće pokretanje vraća `already_processed` i ne menja redove. `init` odbija da prepiše postojeću bazu. Na Linuxu/macOS-u zameni `.\.venv\Scripts\python.exe` sa `.venv/bin/python`.

Ako baza već postoji, možeš je koristiti sa `incremental` i `report`. Za novu demonstraciju dodaj novu putanju, na primer `--db warehouse/demo.duckdb`, svim komandama za punjenje i izveštaje.

Pri ručnom otvaranju baze koristi `SET TimeZone = 'UTC';` za isti prikaz vremena kao u pipeline-u. DuckDB klijent inače može prikazati isti trenutak u lokalnoj vremenskoj zoni.

Paketi su već generisani. Možeš ih deterministički ponovo napraviti:

```powershell
.\.venv\Scripts\python.exe generate_data.py
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

Regenerisanje vraća originalne sintetičke fajlove; ne menja bazu. Za novu demonstraciju zatim izaberi novi DB fajl.

## Gde je šta

| Putanja | Sadržaj |
|---|---|
| [docs/DESIGN.md](docs/DESIGN.md) | Model, granularnost, veze, mere, pretpostavke i pravila punjenja |
| [docs/SYNTHETIC_DATA.md](docs/SYNTHETIC_DATA.md) | Dva paketa, objašnjenje primera i očekivani rezultati |
| [sql/schema.sql](sql/schema.sql) | Kreiranje dimenzija, fact-a, bridge-a i tehničkih tabela |
| [pipeline.py](pipeline.py) | Normalizacija, obogaćivanje, init/incremental i transakcioni upis |
| [sql/reports/](sql/reports/) | Izvršivi primeri upita; parametri su na početku prvih upita |
| [generate_data.py](generate_data.py) | Generator oba paketa |
| [data/init/](data/init/) i [data/incremental/](data/incremental/) | Simulirani izvori, uključujući spoljni registar |
| [tests/test_pipeline.py](tests/test_pipeline.py) | Provere poslovnih rezultata, ponavljanja i prekida |
| `warehouse/meta_dwh.duckdb` | Lokalno generisan DWH; nije verzionisan |
| `outputs/*.csv` | Rezultati pet SQL izveštaja |
| [outputs/validation.txt](outputs/validation.txt) | Rezultat završnog pokretanja testova |

## Očekivani rezultat posle oba paketa

- 14 izvršavanja, 19 bridge veza, 5 naloga, 3 tima, 4 tabele i 9 SQL tekstova.
- Ukupno 175001 ms poznatog CPU vremena i 11000 prijavljenih ulaznih bajtova. Dva izvršavanja imaju nepoznat CPU i ulazne bajtove.
- Za `feature_events`: 4 različita naloga sa INPUT vezom, uključujući neuspešna izvršavanja i ZERO/UNKNOWN čitanje.
- Tim `T_PLATFORM`: 7 upita, uključujući 1 neuspešan, 90 CPU sekundi i 5900 ulaznih bajtova.
- `etl_issue`: 3 konfliktna zapisa, 2 odbijena zapisa i 2 neispravne metrike. Prihvaćeni `q001` ostaje sa 60000 ms CPU-a i oznakom konflikta.

Poređenje rezultata radi se nad prijavljenim poznatim metrikama; `NULL` nije nula. Ne postoji `Unknown` tim: kompletnost i ispravnost registra su dokumentovana pretpostavka.
