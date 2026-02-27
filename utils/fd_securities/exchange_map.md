# Exchange Mapping: FinancialData.net → dim_exchanges

Maps values returned by FD `/international-company-information` (`exchange` field)
and FD ticker suffixes (e.g. `SHEL.L` → suffix `L`) to canonical `dim_exchanges.name`
values, plus expected country and currency.

Lookup order in `enrich_company_info.py`:
1. Full API name (e.g. `"London Stock Exchange"`)
2. Ticker suffix extracted from `Ticker` column (e.g. `"L"` from `SHEL.L`)
3. Neither matched → emit error, keep `FD_INTL` placeholder

Exchanges marked **NEW** do not yet exist in `dim_exchanges` and will be
auto-created by `POST /admin/load_securities` on first load.

| FD full name (API response)       | Ticker suffix | dim_exchanges.name | Country     | Currency |        |
|-----------------------------------|---------------|--------------------|-------------|----------|--------|
| London Stock Exchange             | L             | LSE                | UK          | GBP      |        |
| Toronto Stock Exchange            | TO            | TO                 | Canada      | CAD      |        |
| TSX Venture Exchange              | V             | V                  | Canada      | CAD      |        |
| Frankfurt Stock Exchange          | F             | F                  | Germany     | EUR      |        |
| XETRA                             | DE            | XETRA              | Germany     | EUR      |        |
| Euronext Paris                    | PA            | PA                 | France      | EUR      |        |
| Euronext Amsterdam                | AS            | AS                 | Netherlands | EUR      |        |
| Tokyo Stock Exchange              | T             | T                  | Japan       | JPY      | **NEW**|
| Hong Kong Stock Exchange          | HK            | HK                 | Hong Kong   | HKD      | **NEW**|
| Singapore Exchange                | SI            | SI                 | Singapore   | SGD      | **NEW**|
| Indonesia Stock Exchange          | JK            | JK                 | Indonesia   | IDR      |        |
| Bursa Malaysia                    | KL            | KLSE               | Malaysia    | MYR      |        |
| Korea Exchange                    | KS            | KO                 | Korea       | KRW      |        |
| Korea KOSDAQ                      | KQ            | KQ                 | Korea       | KRW      |        |
| B3 Brasil Bolsa Balcao            | SA            | SA                 | Brazil      | BRL      |        |
| Bolsa Mexicana de Valores         | MX            | MX                 | Mexico      | MXN      |        |
| National Stock Exchange India     | NS            | NS                 | India       | INR      | **NEW**|
| Bombay Stock Exchange             | BO            | BO                 | India       | INR      | **NEW**|
| Shanghai Stock Exchange           | SS            | SHG                | China       | CNY      |        |
| Shenzhen Stock Exchange           | SZ            | SHE                | China       | CNY      |        |
