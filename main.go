package main

import (
	"fmt"
	"log"

	"fsedano.net/neo/dbdriver"
	"github.com/google/uuid"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
)

func main() {

	drv := dbdriver.NewNeo4jDriver("neo4j", dbdriver.Neo4jDriverParams{
		Host:     "localhost",
		User:     "neo4j",
		Password: "C1$c01234",
		Port:     "7687",
	})
	err := drv.VerifyConnectivity()
	if err != nil {
		log.Printf("err: %s", err)
	}

	//for i := 0; i < 1000; i++ {
	createDevices(drv, 50, 20)
	//}

}

func queryCypher(drv dbdriver.DbDriver) error {
	res, err := drv.ReadSingleTx(dbdriver.DbQueryParams{
		Cypher: "MATCH (n:Device) RETURN n",
	})
	if err != nil {
		log.Printf("err: %s", err)
	}
	record, ok := res.([]*db.Record)
	if !ok {
		log.Printf("err: %s", err)
		return err
	}
	for _, r := range record {
		//log.Printf("record: %#v\n", r)
		results := r.AsMap()
		for _, v := range results {
			//log.Printf("key: %s, value: %#v\n", k, v)
			node := v.(dbtype.Node)
			log.Printf("\n***\nnode labels:\n")
			for _, l := range node.Labels {
				log.Printf("  %s\n", l)
			}
			log.Printf("node properties:\n")
			for k, v := range node.Props {
				log.Printf("  %s: %s\n", k, v)
			}
		}
	}
	return err
}

// create a hub and 100 devices with 100 interfaces each connected to it
func createDevices(drv dbdriver.DbDriver, deviceCount int, ifCount int) error {
	params := make(map[string]interface{})
	cypher := "create (h:Hub {hub_id: $hub_id})\n with h "
	params["hub_id"] = uuid.NewString()
	for i := 0; i < deviceCount; i++ {
		cypher += fmt.Sprintf("create (d%d:Device {device_id: $device%d_id, name: $device%d_name})\n with * \n", i, i, i)
		params[fmt.Sprintf("device%d_id", i)] = uuid.NewString()
		params[fmt.Sprintf("device%d_name", i)] = fmt.Sprintf("device%d", i)
		for j := 0; j < ifCount; j++ {
			cypher += fmt.Sprintf("create(d%d)-[:IF]->(:Interface {interface_id: $interface%d%d_id, name: $interface%d%d_name})-[:CONN]->(:InterfaceHub)<-[:IFH]-(h)\n", i, i, j, i, j)
			params[fmt.Sprintf("interface%d%d_id", i, j)] = uuid.NewString()
			params[fmt.Sprintf("interface%d%d_name", i, j)] = fmt.Sprintf("interface%d", j)
		}

		if i < deviceCount-1 {
			cypher += "with h\n"
		}

	}

	_, err := drv.ReadSingleTx(dbdriver.DbQueryParams{
		Cypher: cypher,
		Params: params,
	})

	if err != nil {
		log.Printf("err: %s", err)
	}
	return err
}
