package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/aerospike/aerospike-client-go"
)

type Config struct {
	Hosts     []string
	Namespace string
	Sets      []string
	Timeout   time.Duration
}

type ProcConfig struct {
	Client    *aerospike.Client
	Namespace string
	Set       string
	Done      <-chan error
}

const defaultPort = 3000

type ErrBroadcaster struct {
	listeners []chan error
	input     chan error
}

func (this *ErrBroadcaster) AddListener(ch chan error) { this.listeners = append(this.listeners, ch) }
func (this *ErrBroadcaster) Input() chan<- error       { return this.input }

func (this *ErrBroadcaster) Listen() {
	for err := range this.input {
		for i := range this.listeners {
			go func(listener chan error) {
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

	client.DefaultWritePolicy = aerospike.NewWritePolicy()
	client.DefaultWritePolicy.Timeout = c.Timeout

	nodes := client.GetNodes()
	if len(nodes) == 0 {
		log.Fatal("no aerospike nodes found")
	}

	log.Printf("received '%d' nodes, will be using first one to get lists of sets\n", len(nodes))
	conn, err := nodes[0].GetConnection(5 * time.Second)
	if err != nil {
		log.Fatal(err.Error())
	}

	infoMap, err := aerospike.RequestInfo(conn, "sets")
	if err != nil {
		log.Fatal(err.Error())
	}

	sets := extractSets(infoMap["sets"])

}

func getKeys(config *ProcConfig) (keyChan <-chan aerospike.Key) {
	errChan = make(chan error)
	keyChan = make(chan aerospike.Key)

	rs, err := client.ScanAll(nil, namespace, setName)
	if err != nil {
		errChan <- err
		return
	}

	var errOccurred error

	go func() {
		err <- errChan

	}()

	go func() {
		defer close(keyChan)
		for record := range rs.Results() {
			if record.Err != nil {
				errChan <- record.Err
				return
			}

			keyChan <- *record.Record.Key
		}
		return
	}()

	return
}

func deleteKeys(client *aerospike.Client, ns, set string, bufferSize int, keyChan <-chan aerospike.Key, errChan <-chan error) {

	buffer := make([]aerospike.Key, bufferSize, bufferSize)
	i := 0
	for {
		if i >= bufferSize {
			go func(buf []aerospike.Key, errChan chan error) {
				log.Printf("Trying to delete %d records...", len(buffer))
				deleted := 0
				for i := range buf {
					ok, err := client.Delete(policy, key)
					if err != nil {
						errChan <- err
						return
					}
					if ok {
						deleted++
					}
				}
				log.Printf("Delete %d of %d records...", deleted, len(buffer))

			}(buffer)

			buffer = make([]aerospike.Key, bufferSize, bufferSize)
		}
		select {
		case key <- keyChan:
			buffer = append(buffer, keyChan)
		case err <- errChan:
		}

		i++

	}

	for key := range keyChan {

	}

	// when keyChan is closed, that means we have something in errChan

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

	flag.Parse()

	c.Hosts = strings.Split(hostsStr, ",")
	c.Sets = strings.Split(setsStr, ",")

	return
}
