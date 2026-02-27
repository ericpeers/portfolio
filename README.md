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

# slightly less secret controls. Don't tell Jabba the Hut though.
LOGLEVEL=DEBUG
ENABLE_SWAGGER=true
```

Make sure you link .env to your tests directory so you can run from there!
```
cd tests
ln -s ../.env ./
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
# go get github.com/gin-gonic/gin
# go get github.com/jackc/pgx/v5
# go install github.com/swaggo/swag/cmd/swag@latest
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

### Generating Documentation

We are using swagger which will auto parse headers next to the routes and then 
```
~/go/bin/swag init --parseDependency --parseInternal
```

### Size of project
```
find . -type f \( -name "*.go" -o -name "create_tables.sql" -o -name "*.py" -not -path "*/venv/*" \) | xargs wc
```

