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

type ProcConfig struct {
	Client    *aerospike.Client
	Namespace string
	Set       string
	Done      chan<- error
}

type Processor func()

const defaultPort = 3000

type ErrBroadcaster struct {
	listeners []chan<- error
	input     chan error
}

func NewErrBroadcaster() *ErrBroadcaster {
	return &ErrBroadcaster{
		listeners: []chan<- error{},
		input:     make(chan error),
	}
}

func (this *ErrBroadcaster) AddListener(ch chan<- error) { this.listeners = append(this.listeners, ch) }
func (this *ErrBroadcaster) Input() chan<- error         { return this.input }

func (this *ErrBroadcaster) Listen() {
	for err := range this.input {
		for i := range this.listeners {
			go func(listener chan<- error) {
				listener <- err
			}(this.listeners[i])
		}
	}
}

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
	conn, err := nodes[0].GetConnection(5 * time.Second)
	if err != nil {
		log.Fatal(err.Error())
	}

	infoMap, err := aerospike.RequestInfo(conn, "sets")
	if err != nil {
		log.Fatal(err.Error())
	}

	sets := extractSets(infoMap["sets"])

	var wg sync.WaitGroup

	for _, set := range sets {
		log.Printf("Starting removing data from set %s", set)
		wg.Add(1)
		func(set string) {
			defer wg.Done()
			broadcaster := NewErrBroadcaster()

			pc := &ProcConfig{
				Client:    client,
				Namespace: c.Namespace,
				Set:       set,
				Done:      broadcaster.Input(),
			}

			kg, keyChan, kgErrChan := getKeysGetter(pc)
			kr, finishChan, krErrChan := getKeysRemover(pc, c.BufferSize, keyChan)
			output := make(chan error)

			broadcaster.AddListener(kgErrChan)
			broadcaster.AddListener(krErrChan)
			broadcaster.AddListener(output)
			go broadcaster.Listen()

			go kg()
			go kr()

			select {
			case <-output:
			case <-finishChan:
				log.Printf("Cleaning set %s completed\n\n", set)
			}
		}(set)
	}

	wg.Wait()
}

func getKeysGetter(config *ProcConfig) (Processor, <-chan aerospike.Key, chan<- error) {
	keyChan := make(chan aerospike.Key)
	errChan := make(chan error)

	var errBroadcast error
	go func() {
		errBroadcast = <-errChan
	}()

	var p Processor = func() {
		defer close(keyChan)

		rs, err := config.Client.ScanAll(nil, config.Namespace, config.Set)
		if err != nil {
			config.Done <- err
			return
		}
		for record := range rs.Results() {
			if errBroadcast != nil {
				log.Printf("Getting keys aborted. Reason: %s", errBroadcast.Error())
				return
			}
			if record.Err != nil {
				config.Done <- record.Err
				return
			}

			keyChan <- *record.Record.Key
		}
	}

	return p, keyChan, errChan
}

func getKeysRemover(config *ProcConfig, bufferSize int, keyChan <-chan aerospike.Key) (Processor, <-chan string, chan<- error) {
	errChan := make(chan error)
	doneChan := make(chan string)

	var errBroadcast error
	go func() {
		errBroadcast = <-errChan
	}()

	var wg sync.WaitGroup

	var p Processor = func() {
		buffer := make([]aerospike.Key, 0, bufferSize)

		for key := range keyChan {
			if len(buffer) == bufferSize {
				wg.Add(1)
				go removeKeys(config.Client, buffer, config.Done, &wg)

				buffer = make([]aerospike.Key, 0, bufferSize)
			}

			if errBroadcast != nil {
				log.Printf("removing keys aborted. Reason: %s", errBroadcast.Error())
				return
			}

			buffer = append(buffer, key)
		}

		if len(buffer) > 0 {
			wg.Add(1)
			go removeKeys(config.Client, buffer, config.Done, &wg)
		}

		wg.Wait()

		doneChan <- config.Set
	}

	return p, doneChan, errChan
}

func removeKeys(client *aerospike.Client, keys []aerospike.Key, errChan chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()

	log.Printf("Removing %d keys...", len(keys))
	removed := 0
	for i := range keys {
		if ok, err := client.Delete(nil, &keys[i]); err != nil {
			errChan <- err
			return
		} else if ok {
			removed++
		}
	}
	log.Printf("Removed %d of %d keys...", removed, len(keys))
}

func extractSets(infoStr string) []string {
	opts := strings.Split(infoStr, ":")
	sets := []string{}
	for _, opt := range opts {
		if opt[:9] == "set_name=" {
			sets = append(sets, opt[9:])
		}
	}

	return sets
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

func getConfig() (c Config) {
	var hostsStr string
	var setsStr string

	flag.StringVar(&hostsStr, "h", "localhost:3000", "Comma-separated list of aerospike hosts")
	flag.StringVar(&c.Namespace, "n", "", "Data namespace")
	flag.StringVar(&setsStr, "s", "", "Comma-separated list of set names to erase")
	flag.IntVar(&c.BufferSize, "buffer", 25000, "Size of the buffer to pre-load keys before deleting")
	flag.DurationVar(&c.Timeout, "timeout", 5*time.Second, "Connection timeout")

	flag.Parse()

	c.Hosts = strings.Split(hostsStr, ",")
	c.Sets = strings.Split(setsStr, ",")

	return
}
