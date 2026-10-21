package main

import (
	"flag"
	"fmt"
	"path/filepath"
	"strings"
)

type config struct {
	port           int
	isReplica      bool
	masterHost     string
	masterPort     string
	dir            string
	dbFilename     string
	appendOnly     bool
	aofDir         string // dir joined with appenddirname
	appendFilename string
	appendFsync    string // "always", "everysec" or "no"
	appenddirname  string
}

func parseFlags() (config, error) {
	port := flag.Int("port", 6379, "port to listen on")
	replicaOf := flag.String("replicaof", "", `master to replicate, as "<host> <port>"`)
	dir := flag.String("dir", "/app", "directory for persistence files")
	dbFilename := flag.String("dbfilename", "", "RDB file name")
	appendOnly := flag.String("appendonly", "no", "enable AOF: yes or no")
	appendDirname := flag.String("appenddirname", "appendonlydir", "AOF directory name")
	appendFilename := flag.String("appendfilename", "appendonly.aof", "AOF base file name")
	appendFsync := flag.String("appendfsync", "everysec", "always, everysec or no")
	flag.Parse()

	cfg := config{
		port:           *port,
		dir:            *dir,
		dbFilename:     *dbFilename,
		appendOnly:     *appendOnly == "yes",
		aofDir:         filepath.Join(*dir, *appendDirname),
		appendFilename: *appendFilename,
		appendFsync:    *appendFsync,
		appenddirname:  *appendDirname,
	}

	if *replicaOf != "" {
		host, p, ok := strings.Cut(*replicaOf, " ")
		if !ok {
			return config{}, fmt.Errorf(`--replicaof must be "<host> <port>", got %q`, *replicaOf)
		}
		cfg.isReplica = true
		cfg.masterHost = host
		cfg.masterPort = p
	}

	switch cfg.appendFsync {
	case "always", "everysec", "no":
	default:
		return config{}, fmt.Errorf("--appendfsync must be always, everysec or no, got %q", cfg.appendFsync)
	}

	return cfg, nil
}
