## Install Claude

```
curl -fsSL https://claude.ai/install.sh | bash
claude setup-token
```

## Install the latest version of go & set your path to it
* https://go.dev/doc/install
* https://github.com/go-delve/delve/tree/master/Documentation/installation

```
wget https://go.dev/dl/go1.25.0.src.tar.gz
rm -rf /usr/local/go && tar -C /usr/local -xzf go1.25.0.linux-amd64.tar.gz
```
Then update your path and re-source your .bash_profile:
```
vi ~/.bash_profile
>>> export GOROOT=/usr/local/go
>>> export PATH=${GOROOT}/bin:${PATH}
source ~/.bash_profile
```

Then install your debugger:
```
go install github.com/go-delve/delve/cmd/dlv@latest
```

## Install and Run Postgres
```
sudo apt-get install postgresql postgresql-client
sudo service postgres start
sudo -u postgres psql
>>>CREATE USER your_new_username WITH ENCRYPTED PASSWORD 'your_password';

createdb securities

```

## Use .env to store your environment variables and secrets

`vi .env`
```
# if these got out, the Empire and Darth Vader could win.
PG_URL=postgres://epeers:NOPEONAROPE@localhost:5432/securities
AV_KEY=SERIOUSLY_WHY_WOULD_YOU_PUT_THE_SECRET_IN_A_README
FD_KEY=NEVER_EVER_SECRETS
EODHD_KEY
JWT_SECRET=a-long-random-secret-string

# slightly less secret controls. Don't tell Jabba the Hut though.
LOGLEVEL=DEBUG
ENABLE_SWAGGER=true
# number of concurrent threads (set to slightly less than CPU) - doubles for outbound requests that are waiting and not high CPU usage
CONCURRENCY=10
```

Make sure you link .env to your tests directory so you can run from there!
```
cd tests
ln -s ../.env ./
```

`PG_URL_RO` should match `PG_URL` unless you have a read replica — it is used by `bin/psql_ro`
to open a session-level read-only connection (no separate Postgres role needed):

```bash
bin/psql_ro                          # interactive read-only psql
bin/psql_ro -c "SELECT COUNT(*) FROM fact_price"
```

## Authentication

All routes except `/auth/register` and `/auth/login` require a `Bearer` token.
Add these to `.env` (in addition to the other vars above):

```
ADMIN_EMAIL=you@example.com
ADMIN_PASS=yourpassword
```

`bin/login` handles token acquisition and caching — it reads those credentials from `.env`,
calls `/auth/login`, and caches the 24-hour JWT at `~/.cache/portfolio_token`.
Subsequent calls reuse the cached token until it has less than 60 seconds remaining.

### Scripts

All admin scripts (`fetch_historic.sh`, `load_all.sh`, etc.) call `bin/login` automatically.
Just run them — no token copy-paste needed:

```bash
utils/fetch_historic.sh 2024-01-01 2024-12-31
utils/eodhd_securities/load_all.sh
```

### Direct curl

Embed `$(bin/login)` in the header:

```bash
curl -H "Authorization: Bearer $(bin/login)" http://localhost:8080/admin/export-prices
```

### Talend API Tester (and other browser-based tools)

Get a token and paste it as the Bearer value — it's good for 24 hours:

```bash
bin/login
# WSL: bin/login | clip.exe     copies directly to clipboard
```

In Talend, set the Authorization header to `Bearer <paste token here>`.

### Swagger

Enable Swagger in `.env`:
```
ENABLE_SWAGGER=true
```

Open http://localhost:8080/swagger/index.html, click **Authorize**, and enter
`Bearer <token>` (include the `Bearer ` prefix — Swagger sets the header verbatim):

```bash
bin/login    # copy the output, then paste as: Bearer eyJ...
```


## Create a script for your environment variables (or put them in .env)
Get a key from Alphavantage here: https://www.alphavantage.co/support/#api-key

`vi exports_no_commit.bash`
```
export PG_URL=postgres://USERNAME:PASSWORD@localhost:5432/securities
export AV_KEY=GETONEFROMALPHAVANTAGE
```

```
source exports_no_commit.bash
```


### To install github tools under ubuntu
```
sudo apt-get install gh
gh auth login
```

## To fetch libraries, and then run this code:
```
cd ~epeers/portfolio

go mod init .
go get .
go install github.com/swaggo/swag/cmd/swag@latest
go install golang.org/x/tools/cmd/deadcode@latest
go install honnef.co/go/tools/cmd/staticcheck@latest
go install github.com/securego/gosec/v2/cmd/gosec@latest


# go get github.com/gin-gonic/gin
# go get github.com/jackc/pgx/v5
#  go get -u github.com/swaggo/gin-swagger
#  go get -u github.com/swaggo/files

export AV_KEY=XXXXX
export PG_URL=YYYYY
# you may want to URL encode special characters with a % for the password. Especially for subshell invocation by claude.
go run .
```

The server should be running on port 8080. You can invoke functions:
http://localhost:8080/users/1/portfolios : list the portfolios for user ID 1
http://localhost:8080/portfolios/2 : List portfolio #2



### Running tests
Tests are aggregated in a central directory since we have lots of integration tests. This strategy probably needs to change to include unit tests at some point but... And it doesn't follow the go convention of embedding tests right next to your source code.

```
cd tests
ln -s ../.env ./
go test -v .
```
### Making your App visible on other computers aka Punching holes in firewalls
* Run windows powershell: extract the IP address for your Windows instance: ```ipconfig```
* Run your linut terminal. Get the IP address for that: ```ifconfig```  Notice the "if", not "ip"
* Make your windows computer, port 5173 visible to the outside world. Open your firewall to your computer: Start Menu -> Windows Defender Firewall, Add new Rule, select TCP Port (5173), For all profiles, Name it "React app"
* Run your react app to listen on all Linux/WSL interfaces: ```VITE_USE_MOCK=false npm run dev -- --host 0.0.0.0```
* Map your windows IP address+port to the linux address+port. Run Windows PowerShell as admin: substitute the XXX address with your WSL address (ifconfig)
```
netsh interface portproxy add v4tov4 listenport=5173 listenaddress=0.0.0.0 connectport=5173 connectaddress=XXX.XXX.XXX.XXX
```
### Generating Coverage
```
go test ./tests/ -cover -coverprofile=coverage.out -coverpkg=./internal/... -timeout 180s
go tool cover -func=coverage.out | sort -k3 -rn
```

### Saving and restoring 
Via postgres:
```
pg_dump -Fc -d securities -f securities_db.dump
pg_restore -cd securities ./securities_db.dump 
```
Via admin interface:
```bash
curl -H "Authorization: Bearer $(bin/login)" http://localhost:8080/admin/export-prices > prices.csv
curl -X POST http://localhost:8080/admin/import-prices \
     -H "Authorization: Bearer $(bin/login)" \
     -F 'file=@prices.csv;type=text/csv'
```

### Removed providers / resurrectable code

| What | Last commit with the code | Notes |
|------|--------------------------|-------|
| FinancialData.net pricing provider (`internal/providers/financialdata/`) | `37f9dcf` | Full implementation of `GetDailyPrices`, `GetStockEvents`, splits, dividends. Removed because it was never wired into the pricing service. To restore: `git show 37f9dcf:internal/providers/financialdata/client.go` |

### Looking for gaps in pricing data
There are a lot of microcap stocks (low volume, low price, low market cap) with infrequent days of price data. There are also delisted stocks and mutual funds that are not in our symbol table. So as we go backwards in time to fill data, more "skipped" securities will happen. 
Run the script like this to avoid all the microcaps:
```
python3 check_price_gaps.py --skip-head --low-dollar-cap 100000 --chronic-max-gap 10 --low-volume-cap 100000 --low-volume-min 60 > gaps.log
```

then look at the bottom for days where we might have a provider or data problem.

### Generating Documentation

We are using swagger which will auto parse headers next to the routes and then 
```
~/go/bin/swag init --parseDependency --parseInternal
```

### Size of project
```
find . -type f \( -name "*.go" -o -name "create_tables.sql" -o -name "*.py" -not -path "*/venv/*" \) | xargs wc
```

