package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/gocql/gocql"
	scyllacdc "github.com/scylladb/scylla-cdc-go"
)

func main() {
	cluster := gocql.NewCluster("<DB_IP>")
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.DCAwareRoundRobinPolicy("local-dc"))
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	cfg := &scyllacdc.ReaderConfig{
		Session:               session,
		TableNames:            []string{"ks.<key_space_name>"},
		ChangeConsumerFactory: changeConsumerFactory,
		Logger:                log.New(os.Stderr, "", log.Ldate|log.Lshortfile),
		Advanced: scyllacdc.AdvancedReaderConfig{
		ConfidenceWindowSize:   30 * time.Millisecond,//second
		ChangeAgeLimit:         15 * time.Minute,
		QueryTimeWindowSize:    60 * time.Second,
		PostEmptyQueryDelay:    3 * time.Second,//change 30
		PostNonEmptyQueryDelay: 1 * time.Second,//10 sec
		PostFailedQueryDelay:   1 * time.Hour,//sec
	},
}



	reader, err := scyllacdc.NewReader(context.Background(), cfg)
	if err != nil {
		log.Fatal(err)
	}

	if err := reader.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func consumeChange(ctx context.Context, tableName string, c scyllacdc.Change) error {
	for _, changeRow := range c.Delta {
		pkRaw, _ := changeRow.GetValue("id")
		ckRaw, _ := changeRow.GetValue("balance")
		dkRaw,_:=changeRow.GetValue("date")
		v := changeRow.GetAtomicChange("v")

		pk := pkRaw.(*int)
		ck := ckRaw.(*float64)
		dk:=dkRaw.(*string)

		fmt.Printf("Operation: %s, ID: %s, Balance: %s , Date: %s\n", changeRow.GetOperation(),
			nullableIntToStr(pk), nullableFloatToStr(ck),nullableStr(dk))

		if v.IsDeleted {
			fmt.Printf("  Column v was set to null/deleted\n")
		} else {
			vInt := v.Value
			if vInt != nil {
				fmt.Printf("  Column v was set to %d\n", vInt)
			} else {
				fmt.Print("  Column v was not changed\n")
			}
		}
	}

	return nil
}

func nullableIntToStr(i *int) string {
	if i == nil {
		return "null"
	}
	return fmt.Sprintf("%d", *i)
}
func nullableFloatToStr(i *float64) string {
	if i == nil {
		return "null"
	}
	return fmt.Sprintf("%f", *i)
}

func nullableStr(i *string) string {
	if i == nil {
		return "null"
	}
	return *i
}

var changeConsumerFactory = scyllacdc.MakeChangeConsumerFactoryFromFunc(consumeChange)
