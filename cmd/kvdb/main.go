package main

import (
	"bufio"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	killitdb "killitdb/engine"
)

const (
	defaultMaxBytes int64 = 1024 * 1024 * 1024 // 1 GB default headroom
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	cmd := os.Args[1]

	switch cmd {
	case "open":
		runOpen(os.Args[2:])
	case "check":
		runCheck(os.Args[2:])
	case "dump":
		runDump(os.Args[2:])
	case "get":
		runGet(os.Args[2:])
	case "put":
		runPut(os.Args[2:])
	case "del":
		runDel(os.Args[2:])
	case "batch":
		runBatch(os.Args[2:])
	case "compact":
		runCompact(os.Args[2:])
	case "serve":
		runServe(os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", cmd)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintf(os.Stderr, "Usage: kvdb <command> [arguments]\n\n")
	fmt.Fprintf(os.Stderr, "Commands:\n")
	fmt.Fprintf(os.Stderr, "  open <db>                     Open database, report recovery and key count\n")
	fmt.Fprintf(os.Stderr, "  check <db>                    Verify structural database file integrity (outputs OK)\n")
	fmt.Fprintf(os.Stderr, "  dump <db>                     Dump all live key-values sorted alphabetically\n")
	fmt.Fprintf(os.Stderr, "  get <db> <key>                Get value for key (or NOTFOUND)\n")
	fmt.Fprintf(os.Stderr, "  put <db> <key> <value>        Insert or update a key-value pair\n")
	fmt.Fprintf(os.Stderr, "  del <db> <key>                Delete a key\n")
	fmt.Fprintf(os.Stderr, "  batch <db>                    Apply batch operations from stdin\n")
	fmt.Fprintf(os.Stderr, "  compact <db>                  Compact database file\n")
	fmt.Fprintf(os.Stderr, "  serve <db> [--max-bytes <n>]  Start daemon interactive protocol server\n")
}

func runOpen(args []string) {
	if len(args) < 1 {
		fmt.Fprintf(os.Stderr, "Usage: kvdb open <db_path>\n")
		os.Exit(1)
	}
	dbPath := args[0]
	fmt.Fprintln(os.Stderr, "RECOVERY_SCAN_START")

	start := time.Now()
	db, err := killitdb.Open(dbPath, defaultMaxBytes)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open error: %v\n", err)
		os.Exit(1)
	}
	elapsed := time.Since(start)

	count, _ := db.KeyCount()
	_ = db.Close()

	fmt.Printf("keys=%d recovery=%s\n", count, elapsed)
}

func runCheck(args []string) {
	if len(args) < 1 {
		fmt.Fprintf(os.Stderr, "Usage: kvdb check <db_path>\n")
		os.Exit(1)
	}
	dbPath := args[0]

	db, err := killitdb.Open(dbPath, defaultMaxBytes)
	if err != nil {
		fmt.Fprintf(os.Stderr, "CORRUPT: %v\n", err)
		os.Exit(1)
	}
	_ = db.Close()

	fmt.Println("OK")
}

func runDump(args []string) {
	if len(args) < 1 {
		fmt.Fprintf(os.Stderr, "Usage: kvdb dump <db_path>\n")
		os.Exit(1)
	}
	dbPath := args[0]

	db, err := killitdb.Open(dbPath, defaultMaxBytes)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dump error: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	writer := bufio.NewWriter(os.Stdout)
	err = db.ForEach(func(k string, v []byte) error {
		if len(v) == 0 {
			_, err := fmt.Fprintf(writer, "%s\n", k)
			return err
		}
		_, err := fmt.Fprintf(writer, "%s %s\n", k, string(v))
		return err
	})
	_ = writer.Flush()

	if err != nil {
		fmt.Fprintf(os.Stderr, "dump iteration error: %v\n", err)
		os.Exit(1)
	}
}

func runGet(args []string) {
	if len(args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: kvdb get <db_path> <key>\n")
		os.Exit(1)
	}
	dbPath := args[0]
	key := args[1]

	db, err := killitdb.Open(dbPath, defaultMaxBytes)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open error: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	val, err := db.Get([]byte(key))
	if err != nil {
		if errors.Is(err, killitdb.ErrKeyNotFound) {
			fmt.Println("NOTFOUND")
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "get error: %v\n", err)
		os.Exit(1)
	}

	fmt.Println(string(val))
}

func runPut(args []string) {
	if len(args) < 3 {
		fmt.Fprintf(os.Stderr, "Usage: kvdb put <db_path> <key> <val>\n")
		os.Exit(1)
	}
	dbPath := args[0]
	key := args[1]
	val := args[2]

	db, err := killitdb.Open(dbPath, defaultMaxBytes)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open error: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	if err := db.Put([]byte(key), []byte(val)); err != nil {
		fmt.Fprintf(os.Stderr, "put error: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("OK")
}

func runDel(args []string) {
	if len(args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: kvdb del <db_path> <key>\n")
		os.Exit(1)
	}
	dbPath := args[0]
	key := args[1]

	db, err := killitdb.Open(dbPath, defaultMaxBytes)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open error: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	if err := db.Delete([]byte(key)); err != nil {
		fmt.Fprintf(os.Stderr, "del error: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("OK")
}

func runBatch(args []string) {
	if len(args) < 1 {
		fmt.Fprintf(os.Stderr, "Usage: kvdb batch <db_path>\n")
		os.Exit(1)
	}
	dbPath := args[0]

	db, err := killitdb.Open(dbPath, defaultMaxBytes)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open error: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	reader := bufio.NewReader(os.Stdin)
	var ops []killitdb.Operation

	for {
		line, err := reader.ReadString('\n')
		line = strings.TrimSpace(line)
		if line != "" {
			parts := strings.SplitN(line, " ", 3)
			switch strings.ToLower(parts[0]) {
			case "put":
				if len(parts) < 2 {
					continue
				}
				val := ""
				if len(parts) == 3 {
					val = parts[2]
				}
				ops = append(ops, killitdb.Operation{
					Type:  killitdb.OpPut,
					Key:   []byte(parts[1]),
					Value: []byte(val),
				})
			case "del":
				if len(parts) < 2 {
					continue
				}
				ops = append(ops, killitdb.Operation{
					Type:  killitdb.OpDelete,
					Key:   []byte(parts[1]),
					Value: nil,
				})
			}
		}
		if err != nil {
			break
		}
	}

	if len(ops) == 0 {
		fmt.Println("OK")
		return
	}

	if err := db.Batch(ops); err != nil {
		fmt.Fprintf(os.Stderr, "batch error: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("OK")
}

func runCompact(args []string) {
	if len(args) < 1 {
		fmt.Fprintf(os.Stderr, "Usage: kvdb compact <db_path>\n")
		os.Exit(1)
	}
	dbPath := args[0]

	db, err := killitdb.Open(dbPath, defaultMaxBytes)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open error: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	if err := db.Compact(); err != nil {
		fmt.Fprintf(os.Stderr, "compact error: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("OK")
}

func runServe(args []string) {
	if len(args) < 1 {
		fmt.Fprintf(os.Stderr, "Usage: kvdb serve <db_path> [--max-bytes <n>]\n")
		os.Exit(1)
	}

	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	maxBytesFlag := fs.Int64("max-bytes", defaultMaxBytes, "Maximum allowable database file size in bytes")
	if err := fs.Parse(args[1:]); err != nil {
		os.Exit(1)
	}

	dbPath := args[0]
	maxBytes := *maxBytesFlag

	db, err := killitdb.Open(dbPath, maxBytes)
	if err != nil {
		fmt.Fprintf(os.Stderr, "serve open error: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	stdinReader := bufio.NewReader(os.Stdin)
	stdoutWriter := bufio.NewWriter(os.Stdout)

	sendACK := func(id string, extra ...string) {
		if len(extra) > 0 && extra[0] != "" {
			_, _ = fmt.Fprintf(stdoutWriter, "ACK %s %s\n", id, extra[0])
		} else {
			_, _ = fmt.Fprintf(stdoutWriter, "ACK %s\n", id)
		}
		_ = stdoutWriter.Flush()
	}

	sendERR := func(id string, msg string) {
		_, _ = fmt.Fprintf(stdoutWriter, "ERR %s %s\n", id, msg)
		_ = stdoutWriter.Flush()
	}

	for {
		line, err := stdinReader.ReadString('\n')
		if err != nil && len(line) == 0 {
			if errors.Is(err, io.EOF) {
				break
			}
			break
		}

		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			continue
		}

		parts := strings.Split(line, " ")
		if len(parts) < 2 {
			continue
		}

		cmdType := strings.ToUpper(parts[0])
		opID := parts[1]

		switch cmdType {
		case "PUT":
			if len(parts) < 4 {
				sendERR(opID, "PUT requires <id> <hexkey> <hexvalue>")
				continue
			}
			keyBytes, err := hex.DecodeString(parts[2])
			if err != nil {
				sendERR(opID, fmt.Sprintf("invalid hex key: %v", err))
				continue
			}
			valBytes, err := hex.DecodeString(parts[3])
			if err != nil {
				sendERR(opID, fmt.Sprintf("invalid hex value: %v", err))
				continue
			}

			if err := db.Put(keyBytes, valBytes); err != nil {
				sendERR(opID, err.Error())
			} else {
				sendACK(opID)
			}

		case "DEL":
			if len(parts) < 3 {
				sendERR(opID, "DEL requires <id> <hexkey>")
				continue
			}
			keyBytes, err := hex.DecodeString(parts[2])
			if err != nil {
				sendERR(opID, fmt.Sprintf("invalid hex key: %v", err))
				continue
			}

			if err := db.Delete(keyBytes); err != nil {
				sendERR(opID, err.Error())
			} else {
				sendACK(opID)
			}

		case "GET":
			if len(parts) < 3 {
				sendERR(opID, "GET requires <id> <hexkey>")
				continue
			}
			keyBytes, err := hex.DecodeString(parts[2])
			if err != nil {
				sendERR(opID, fmt.Sprintf("invalid hex key: %v", err))
				continue
			}

			val, err := db.Get(keyBytes)
			if err != nil {
				sendERR(opID, err.Error())
			} else {
				sendACK(opID, hex.EncodeToString(val))
			}

		case "BATCH":
			if len(parts) < 3 {
				sendERR(opID, "BATCH requires at least one operation")
				continue
			}

			var batchOps []killitdb.Operation
			var parseErr error

			for i := 2; i < len(parts); i++ {
				item := parts[i]
				if item == "" {
					continue
				}

				if strings.HasPrefix(item, "P:") {
					// P:<hexkey>:<hexvalue>
					subParts := strings.SplitN(item[2:], ":", 2)
					if len(subParts) != 2 {
						parseErr = fmt.Errorf("malformed PUT item in batch: %s", item)
						break
					}
					k, err := hex.DecodeString(subParts[0])
					if err != nil {
						parseErr = fmt.Errorf("invalid hex key in batch: %w", err)
						break
					}
					v, err := hex.DecodeString(subParts[1])
					if err != nil {
						parseErr = fmt.Errorf("invalid hex value in batch: %w", err)
						break
					}
					batchOps = append(batchOps, killitdb.Operation{
						Type:  killitdb.OpPut,
						Key:   k,
						Value: v,
					})

				} else if strings.HasPrefix(item, "D:") {
					// D:<hexkey>
					k, err := hex.DecodeString(item[2:])
					if err != nil {
						parseErr = fmt.Errorf("invalid hex key in batch: %w", err)
						break
					}
					batchOps = append(batchOps, killitdb.Operation{
						Type:  killitdb.OpDelete,
						Key:   k,
						Value: nil,
					})
				} else {
					parseErr = fmt.Errorf("unknown batch item prefix: %s", item)
					break
				}
			}

			if parseErr != nil {
				sendERR(opID, parseErr.Error())
				continue
			}

			if err := db.Batch(batchOps); err != nil {
				sendERR(opID, err.Error())
			} else {
				sendACK(opID)
			}

		case "COMPACT":
			if err := db.Compact(); err != nil {
				sendERR(opID, err.Error())
			} else {
				sendACK(opID)
			}

		default:
			sendERR(opID, fmt.Sprintf("unknown command '%s'", cmdType))
		}

		if errors.Is(err, io.EOF) {
			break
		}
	}

	_ = db.Close()
}

// Suppress unused strconv import
var _ = strconv.Itoa
