package main

import (
	"flag"
	"fmt"
	"github.com/gocql/gocql"
	"log"
	"strings"
	"time"
)

func main() {
       var query string;
	partitionNumber := flag.Int("partition_number", 1, "define no. of parition")
	dataCount := flag.Int("data_count", 100, "define data count per parition")
	var hosts string
	flag.StringVar(&hosts, "hosts", "localhost", "hosts to connect ( in format host1,host2,host3 )")

	var replicaFactorCnt int
       flag.IntVar(&replicaFactorCnt, "rf", 1, "Keyspace Replica Factor (1 will be simpleTopology , else will be NetworkTopology)")
       var dc1 string
       flag.StringVar(&dc1, "dc1","scb", "Regoin 1 name")
       var dc2 string
       flag.StringVar(&dc2, "dc2","bna", "Region 2 name")

       var clWrite string;
       flag.StringVar(&clWrite, "write-cl","One","Write Consistency Level");

       var clRead string;
       flag.StringVar(&clRead, "read-cl","One","Read Consistency Level");

       var username string;
       flag.StringVar(&username, "u","","Username");

       var password string;
       flag.StringVar(&password, "p","","Username");
       
 

	flag.Parse()

	// connect to the cluster

	//  cluster := gocql.NewCluster("172.16.3.89", "172.16.3.90", "172.16.3.91") //replace PublicIP with the IP addresses used by your cluster.
	hostsSpace := strings.Replace(hosts, ",", " ", -1)
	cluster := gocql.NewCluster(strings.Fields(hostsSpace)...) //replace PublicIP with the IP addresses used by your cluster.

       var consistency gocql.Consistency;       
       consistency=gocql.ParseConsistency(clWrite)
	cluster.Consistency = consistency
	cluster.ProtoVersion = 4
	cluster.ConnectTimeout = time.Second * 10
	cluster.Timeout = time.Second * 10

       if username !=""{
        cluster.Authenticator = gocql.PasswordAuthenticator{Username: username , Password: password} //replace the username and password fields with their real settings.
       }
	session, err := cluster.CreateSession()
	if err != nil {
		log.Println(err)
		return
	}
	defer session.Close()

	// create keyspaces
       //err = session.Query("CREATE  KEYSPACE  IF NOT EXISTS   test_keyspace   WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'datacenter1' : 1};").Exec()
       query="drop    table IF   EXISTS  test_keyspace.test_table";
       log.Println(query);
	err = session.Query(query).Exec()
	if err != nil {
		log.Println(err)
		return
	}

       query="drop    KEYSPACE IF   EXISTS  test_keyspace";
       log.Println(query);
	err = session.Query(query).Exec()
	if err != nil {
		log.Println(err)
		return
	}
	var classRF = "'class' : 'SimpleStrategy' ,'replication_factor':1"
	if replicaFactorCnt != 1 {
              var dcRFCnt int;
              dcRFCnt=replicaFactorCnt/2;
		classRF = fmt.Sprintf("'class' : 'NetworkTopologyStrategy' ,'%s':%d,'%s':%d", dc1,dcRFCnt,dc2,dcRFCnt)
	}
       query=fmt.Sprintf("CREATE  KEYSPACE    test_keyspace   WITH REPLICATION = { %s };", classRF);
       log.Println(query)
	err = session.Query(query).Exec()

	//'class' : 'NetworkTopologyStrategy'
	if err != nil {
		log.Println(err)
		return
	}

	// create table
	err = session.Query("Create table  IF NOT EXISTS  test_keyspace.test_table(pk text ,ck text ,data text ,primary key(pk,ck)  ) ").Exec()
	if err != nil {
		log.Println(err)
		return
       }
       
       start:=time.Now();
	// insert some practice data
	for i := 0; i < *partitionNumber; i++ {

		for j := 0; j < *dataCount; j++ {

			query := fmt.Sprintf(" insert into  test_keyspace.test_table (pk,ck,data)  values('%d','%d','hello world!' );", i, j)
			//   log.Printf(query)
			err = session.Query(query).Exec()

			if err != nil {
				log.Println(err)
				return
			}
		}
       }
       elapsed := time.Since(start)
       log.Printf("took %s", elapsed)

       consistency=gocql.ParseConsistency(clRead)
       var allparCnt int
	for i := 0; i < *partitionNumber; i++ {

		var total int
		query := fmt.Sprintf("select count(*) from test_keyspace.test_table where pk='%d';", i)
		output := session.Query(query).Iter()
		output.Scan(&total)
		log.Println (fmt.Sprintf("Partition %d Total Row %d\n", i, total))
		allparCnt += total

	}
	log.Println (fmt.Sprintf("All Row %d\n", allparCnt))

	// Return average sleep time for James

} 