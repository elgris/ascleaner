package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aerospike/aerospike-client-go"
)

type Config struct {
	Hosts      []string
	Namespace  string
	Sets       []string
	BufferSize int
	Timeout    time.Duration
}

const defaultPort = 3000

func main() {
	c := getConfig()
	client, err := connectToAerospike(c.Hosts)
	if err != nil {
		log.Fatal(err.Error())
	}
	client.DefaultScanPolicy = aerospike.NewScanPolicy()
	client.DefaultScanPolicy.Timeout = c.Timeout

	client.DefaultWritePolicy = aerospike.NewWritePolicy(0, 0)
	client.DefaultWritePolicy.Timeout = c.Timeout

	nodes := client.GetNodes()
	if len(nodes) == 0 {
		log.Fatal("no aerospike nodes found")
	}

	log.Printf("received '%d' nodes, will be using first one to get lists of sets", len(nodes))
	conn, err := nodes[0].GetConnection(c.Timeout)
	if err != nil {
		log.Fatal(err.Error())
	}

	infoMap, err := aerospike.RequestInfo(conn, "sets")
	if err != nil {
		log.Fatal(err.Error())
	}

	sets := extractSets(infoMap["sets"])
	if len(c.Sets) > 0 {
		sets = intersectStrings(sets, c.Sets)
	}

	if len(sets) == 0 {
		log.Println("no sets to clean")
		return
	}

	var wg sync.WaitGroup

	for _, set := range sets {
		wg.Add(1)

		go func(wg *sync.WaitGroup, set string) {
			defer wg.Done()
			log.Printf("starting removing data from set %s", set)
			if err := clearSet(client, c.Namespace, set, c.BufferSize); err != nil {
				log.Printf("clearing set %s failed: %s", set, err.Error())
			}
		}(&wg, set)
	}

	wg.Wait()
}

func getConfig() (c Config) {
	var hostsStr string
	var setsStr string

	flag.StringVar(&hostsStr, "h", "localhost:3000", "Comma-separated list of aerospike hosts")
	flag.StringVar(&c.Namespace, "n", "", "Data namespace")
	flag.StringVar(&setsStr, "s", "", "Comma-separated list of set names to erase")
	flag.IntVar(&c.BufferSize, "b", 25000, "Size of the buffer to pre-load keys before deleting")
	flag.DurationVar(&c.Timeout, "t", 5*time.Second, "Connection timeout")

	flag.Parse()

	c.Hosts = strings.Split(hostsStr, ",")
	if setsStr != "" {
		c.Sets = strings.Split(setsStr, ",")
	}

	return
}

func connectToAerospike(hostStrings []string) (*aerospike.Client, error) {
	if len(hostStrings) == 0 {
		return nil, errors.New("no aerospike host provided")
	}

	hosts := make([]*aerospike.Host, len(hostStrings))
	for i, connStr := range hostStrings {
		hostStr, portStr, err := net.SplitHostPort(connStr)
		if err != nil {
			if isErrNoPort(err) {
				hosts[i] = aerospike.NewHost(connStr, defaultPort)
				continue
			}
			return nil, fmt.Errorf("could not split host string '%s' by host and port: %s", connStr, err.Error())
		}

		port, err := strconv.Atoi(portStr)
		if err != nil {
			return nil, fmt.Errorf("could determine port number from string '%s': %s", connStr, err.Error())
		}
		hosts[i] = aerospike.NewHost(hostStr, port)
	}

	return aerospike.NewClientWithPolicyAndHost(nil, hosts...)
}

func isErrNoPort(err error) bool {
	if addrErr, ok := err.(*net.AddrError); ok {
		return addrErr.Err == "missing port in address"
	}

	return false
}

func extractSets(infoStr string) []string {
	opts := strings.Split(infoStr, ":")
	sets := []string{}
	for _, opt := range opts {
		if len(opt) > 4 && opt[:4] == "set=" {
			// works for aerospike info from v.3.9.0
			sets = append(sets, opt[4:])
		} else if len(opt) > 9 && opt[:9] == "set_name=" {
			// works for legacy versions (below 3.9.0)
			sets = append(sets, opt[9:])
		}
	}

	return sets
}

func intersectStrings(sl1, sl2 []string) []string {
	l1 := len(sl1)
	l2 := len(sl2)
	if l1 == 0 || l2 == 0 {
		return []string{}
	}

	minLen := l1
	if l1 > l2 {
		minLen = l2
	}

	res := make([]string, 0, minLen)

	for _, s1 := range sl1 {
		for _, s2 := range sl2 {
			if s1 == s2 {
				res = append(res, s1)
				break
			}
		}
	}

	return res
}

func clearSet(client *aerospike.Client, ns, set string, bufferSize int) error {
	rs, err := client.ScanAll(nil, ns, set)
	if err != nil {
		return err
	}

	buffer := make([]aerospike.Key, 0, bufferSize)
	var wg sync.WaitGroup
	for record := range rs.Results() {
		if record.Err != nil {
			return err
		}

		buffer = append(buffer, *record.Record.Key)

		if len(buffer) == bufferSize {
			wg.Add(1)
			go removeKeys(client, buffer, set, &wg)

			buffer = make([]aerospike.Key, 0, bufferSize)
		}
	}

	if len(buffer) > 0 {
		wg.Add(1)
		go removeKeys(client, buffer, set, &wg)
	}

	wg.Wait()
	return nil
}

func removeKeys(client *aerospike.Client, keys []aerospike.Key, set string, wg *sync.WaitGroup) {
	defer wg.Done()

	log.Printf("removing %d keys in set %s...", len(keys), set)
	removed := 0
	for i := range keys {
		if ok, err := client.Delete(nil, &keys[i]); err != nil {
			log.Printf("removing keys from set %s failed: %s", set, err.Error())
			return
		} else if ok {
			removed++
		}
	}
	log.Printf("removed %d of %d keys in set %s", removed, len(keys), set)
}
