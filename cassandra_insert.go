package main

import (
	"fmt"
	"log"
       "time"
       "flag"
       "strings"
	"github.com/gocql/gocql"
)

func main() {
       partitionNumber:=flag.Int("partition_number",1,"define no. of parition");
       dataCount:=flag.Int("data_count",100,"define data count per parition");       
       var hosts string;
       flag.StringVar(&hosts,"hosts","localhost","hosts to connect ( in format host1,host2,host3 )")
       flag.Parse()
             
	// connect to the cluster
       //  cluster := gocql.NewCluster("172.16.3.89", "172.16.3.90", "172.16.3.91") //replace PublicIP with the IP addresses used by your cluster.
       hostsSpace:=strings.Replace(hosts,","," ",-1)
       cluster := gocql.NewCluster(strings.Fields(hostsSpace)... ) //replace PublicIP with the IP addresses used by your cluster.
       
       
       // cluster.Consistency = gocql.Quorum
       cluster.Consistency = gocql.One
	cluster.ProtoVersion = 4
       cluster.ConnectTimeout = time.Second * 10
       cluster.Timeout=time.Second * 2
       
	//  cluster.Authenticator = gocql.PasswordAuthenticator{Username: "cassandra", Password: "Welcome.2019"} //replace the username and password fields with their real settings.
	session, err := cluster.CreateSession()
	if err != nil {
		log.Println(err)
		return
	}
	defer session.Close()

	// create keyspaces
       //err = session.Query("CREATE  KEYSPACE  IF NOT EXISTS   test_keyspace   WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'datacenter1' : 1};").Exec()
       err = session.Query("drop    KEYSPACE IF   EXISTS  test_keyspace").Exec()
       if err != nil {
		log.Println(err)
		return
	}
       err = session.Query("CREATE  KEYSPACE    test_keyspace   WITH REPLICATION = { 'class' : 'SimpleStrategy' ,'replication_factor':1 };").Exec()
       
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

       // insert some practice data
       for i:=0;i<*partitionNumber;i++ {

              for j :=0;j<*dataCount;j++{

                     query:=fmt.Sprintf(" insert into  test_keyspace.test_table (pk,ck,data)  values('%d','%d','hello world!' );",i,j)
                  //   log.Printf(query)
                     err = session.Query(query).Exec()
                     
                     if err != nil {
                            log.Println(err)
                            return
                     }
              }
       }
       var allparCnt int ;
       for i:=0;i<*partitionNumber;i++ {
              
              var total int
              query:=fmt.Sprintf("select count(*) from test_keyspace.test_table where pk='%d';",i)                     
              output := session.Query(query).Iter()
              output.Scan(&total)
              fmt.Printf("Partition %d Total Row %d\n",i ,total)
              allparCnt+=total;
         
       }
       fmt.Printf("All Row %d\n"  ,allparCnt)

	// Return average sleep time for James

}
