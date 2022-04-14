[![Build Status](https://travis-ci.org/tuneinc/fasts3.svg?branch=master)](https://travis-ci.org/tuneinc/fasts3) [![Go Report Card](https://goreportcard.com/badge/github.com/tuneinc/fasts3)](https://goreportcard.com/report/github.com/tuneinc/fasts3)
![FastS3](http://i.imgur.com/A42azaA.png)
---

Fast s3 utility is a faster version of s3cmd's ls, get, and cp functions ideal for buckets containing millions of keys.

![autocomplete demo for zsh](autocomplete_demo.gif)

# Installation

```bash
go get -u github.com/metaverse/fasts3
```
This should install the binary under `$GOPATH/bin/`

# Configuration

Use `aws configure` command from the aws cli tool (https://aws.amazon.com/cli/) which will create the necessary config files in `~/.aws/credentials`.

add your region to your credentials file to support tab-completion of buckets:
```ini
[default]
aws_access_key_id=xxxx
aws_secret_access_key=xxxx
region=us-east-1
```

Alternatively you can set these environment variables which will take precedence over the credentials file:
```bash
export AWS_ACCESS_KEY_ID=<access_key>
export AWS_SECRET_ACCESS_KEY=<secret_key>
export AWS_REGION=us-east-1
```

# Usage
Use:
```
fasts3 --help
fasts3 <cmd> --help
```

### Using search depth to *go* faster
Many times you know the structure of your s3 bucket, and this can be used to optimize listings. Say you have a structure like so:
```bash
fasts3 ls s3://mybuck/logs/

DIR s3://mybuck/logs/2011/
DIR s3://mybuck/logs/2012/
DIR s3://mybuck/logs/2013/
DIR s3://mybuck/logs/2014/
DIR s3://mybuck/logs/2015/
```

Doing a `fasts3 ls -r s3://mybuck/logs/` will read all keys under `logs` sequentially. We can make this faster by adding a `--search-depth 1` flag to the command which gives each of the underlying directories its own thread, increasing throughput.

### Concurrency
The concurrency level of s3 command execution can be tweaked based on your usage needs. By default, `4*NumCPU` s3 commands will be executed concurrently, which is ideal based on our benchmarks. If you want to override this value, set `GOMAXPROCS` in your environment to set the concurrency level: `GOMAXPROCS=64 fasts3 ls -r s3://mybuck/logs/` will execute 64 s3 commands concurrently.

### Examples
```bash
# ls
fasts3 ls s3://mybucket/ # lists top level directories and keys
fasts3 ls -r s3://mybucket/ # lists all keys in the bucket
fasts3 ls -r --search-depth 1 s3://mybucket/ # lists all keys in the bucket using the directories 1 level down to thread
fasts3 ls -r s3://mybucket/ | awk '{s += $1}END{print s}' # sum sizes of all objects in the bucket

# get
fasts3 get s3://mybuck/logs/ # fetches all logs in the prefix

# stream
fasts3 stream s3://mybuck/logs/ # streams all logs under prefix to stdout
fasts3 stream --key-regex ".*2015-01-01" s3://mybuck/logs/ # streams all logs with 2015-01-01 in the key name stdout

# cp
fasts3 cp -r s3://mybuck/logs/ s3://otherbuck/ # copies all subdirectories to another bucket
fasts3 cp -r -f s3://mybuck/logs/ s3://otherbuck/all-logs/ # copies all source files into the same destination directory
```

### Completion
Bash and ZSH completion are available.

To install for bash:
```
source completion.sh
```

For zsh:
```
source completion.zsh
```
