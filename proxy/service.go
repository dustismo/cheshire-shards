package proxy

import (
	"fmt"
	"github.com/trendrr/goshire-shards/shards"
	"github.com/trendrr/goshire/client"
)

// Handles the connections for a single service.

type Service struct {
	connections *shards.Connections
	hasher      Hasher
}

// creates a new client from seed urls.
func NewService(rt *shards.RouterTable) (*Service, error) {
	service := &Service{}

	connections := &shards.Connections{}
	connections.SetClientCreator(service)
	connections.SetRouterTable(rt)

	service.connections = connections
	service.hasher = &DefaultHasher{}
	return service, nil
}

func (this *Service) RouterTable() *shards.RouterTable {
	return this.connections.RouterTable()
}

func (this *Service) Partition(key string) (int, error) {
	partition, err := this.hasher.Hash(key, this.connections.RouterTable().TotalPartitions)
	return partition, err
}

//gets the entries for a partition.
func (this *Service) Entries(partition int) ([]*shards.EntryClient, error) {
	v, err := this.connections.Entries(partition)
	return v, err
}

func (this *Service) Close() {
	this.connections.Close()
}

// to satisify the clientcreator interface
func (this *Service) Create(entry *shards.RouterEntry) (client.Client, error) {
	return client.NewHttp(fmt.Sprintf("http://%s:%d", entry.Address, entry.HttpPort)), nil
}
