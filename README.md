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

## Create a script for your environment variables
Get a key from Alphavantage here: https://www.alphavantage.co/support/#api-key

`vi exports_no_commit.bash`
```
export PG_URL=postgres://USERNAME:PASSWORD@localhost:5432/securities
export AV_KEY=GETONEFROMALPHAVANTAGE
```


## To fetch libraries, and then run this code:
```
cd ~epeers/portfolio

go mod init .
go get .
# go get github.com/gin-gonic/gin
# go get github.com/jackc/pgx/v5
go run .
```

### To install github tools under ubuntu
```
sudo apt-get install gh
gh auth login
```
