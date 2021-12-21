package cmd

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/gorilla/mux"
	"github.com/mtps/ktab/pkg/checkpoint"
	"github.com/mtps/ktab/pkg/db"
	_ "github.com/mtps/ktab/pkg/db/pebble" // Load the pebble backend.
	"github.com/mtps/ktab/pkg/types"
	"github.com/spf13/cobra"
	"log"
	"net/http"
	"sync"
	"time"
)

var cmdServe = &cobra.Command{
	Use: "serve <topic> [topic...]",
	Short: "Pull a kafka topic into a table",
	Args: cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		if err := serve(args); err != nil {
			panic(err)
		}
	},
}

var serveArgs struct {
	laddr string
	broker string
	topics string
	path string
	db string
}


func init() {
	cmdServe.Flags().StringVar(&serveArgs.laddr, "laddr", "localhost:8089", "Listen address")
	cmdServe.Flags().StringVar(&serveArgs.broker, "broker", "localhost:9092", "Broker to connect to")
	cmdServe.Flags().StringVar(&serveArgs.path, "path", "/tmp", "Directory for db cache")
	cmdServe.Flags().StringVar(&serveArgs.db, "db", "pebble", "DB backend to use (pebble|level) [default:pebble]")

	cmdRoot.AddCommand(cmdServe)
}

const (
	CheckpointFilename = "checkpoint"
)

var (
	start = time.Now()
)

func receiverLoop(dbs map[string]db.DB, pop <-chan *sarama.ConsumerMessage, chk *checkpoint.Checkpoint, done <-chan struct{}) {
	timer := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-timer.C:
			err := chk.Write()
			if err != nil {
				panic(err)
			}

		case m := <-pop:
			js, err := json.Marshal(types.KafkaToRecord(m))
			if err != nil {
				panic(err)
			}

			kdb, ok := dbs[m.Topic]
			if !ok {
				panic(err)
			}

			kdbKey := make([]byte, 4)
			binary.LittleEndian.PutUint32(kdbKey, uint32(m.Partition))
			kdbKey = append(kdbKey, m.Key...)
			err = kdb.Put(kdbKey, js)
			if err != nil {
				panic(err)
			}
			chk.PutOffset(m.Topic, m.Partition, m.Offset)

		case <-done:
			log.Printf("Done!")
			return
		}
	}
}

func serve(topics []string) error {
	cfg := sarama.NewConfig()
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	cfg.Consumer.Return.Errors = true
	cfg.Version = sarama.V2_0_0_0

	brokers := []string{serveArgs.broker}

	client, err := sarama.NewClient(brokers, cfg)
	if err != nil {
		return err
	}

	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return err
	}

	topicPartitions := make(map[string][]int32)
	kdbs := make(map[string]db.DB)

	pop := make(chan *sarama.ConsumerMessage)
	for _, topic := range topics {
		partitions, err := consumer.Partitions(topic)
		if err != nil {
			return err
		}

		topicPartitions[topic] = partitions

		kdb, err := db.NewDB(serveArgs.db, fmt.Sprintf("%s/%s", serveArgs.path, topic))
		if err != nil {
			return err
		}

		kdbs[topic] = kdb
	}

	defer func() {
		for _, db := range kdbs {
			db.Close()
		}
	}()

	totalPartitions := 0
	for _, partitions := range topicPartitions {
		totalPartitions += len(partitions)
	}

	wg := &sync.WaitGroup{}
	log.Printf("Adding wait latch for %d topic-partitions", totalPartitions)
	wg.Add(totalPartitions)

	// Load checkpoints.
	checkpointFilePath := fmt.Sprintf("%s/%s", serveArgs.path, CheckpointFilename)
	chk, err := checkpoint.LoadCheckpoint(checkpointFilePath)
	if err != nil {
		return err
	}

	for topic, partitions := range topicPartitions {
		for _, partition := range partitions {
			go consumePartition(topic, partition, chk, client, consumer, wg, pop)
		}
	}

	done := make(chan struct{})
	go receiverLoop(kdbs, pop, chk, done)

	// Wait for load.
	wg.Wait()
	size := uint64(0)
	for _, v := range kdbs {
		size += v.Size()
	}
	log.Printf("Load complete! Records:%d. Starting http server %s\n", size, serveArgs.laddr)

	log.Printf("Example query:\n")
	log.Printf("   # curl -i '%s/topics", serveArgs.laddr)
	log.Printf("   # curl -i '%s/topics/test'", serveArgs.laddr)

	r := mux.NewRouter()
	r.HandleFunc("/topics", httpListTopics(kdbs))
	r.HandleFunc("/topics/{topic}", httpQueryTopic(kdbs))

	http.ListenAndServe(serveArgs.laddr, r)

	consumer.Close()
	client.Close()

	done <- struct{}{}
	return nil
}

func consumePartition(
	topic string,
	partition int32,
	chk *checkpoint.Checkpoint,
	client sarama.Client,
	consumer sarama.Consumer,
	wg *sync.WaitGroup,
	pop chan *sarama.ConsumerMessage,
) {
	log.Printf("Assigning partition (%s-%d)\n", topic, partition)
	var err error

	currentOffset := chk.GetOffset(topic, partition)
	if currentOffset == -1 {
		currentOffset, err = client.GetOffset(topic, partition, sarama.OffsetOldest)
		if err != nil {
			log.Printf("get start offset: %s", err)
			return
		}
	}

	nextOffset, err := client.GetOffset(topic, partition, sarama.OffsetNewest)
	if err != nil {
		log.Printf("get end offset:  %s", err)
		return
	}

	// Next produced offset is returned from GetOffset, so back up one.
	endOffset := nextOffset - 1

	log.Printf("Loading from current:%d to end:%d (%s-%d)", currentOffset, endOffset, topic, partition)

	pconsumer, err := consumer.ConsumePartition(topic, partition, currentOffset)
	if err != nil {
		log.Printf("consumer (%s-%d):  %s", topic, partition, err)
		return
	}

	chk.PutOffset(topic, partition, currentOffset)

	loading := true
	count := 0
	for {
		if currentOffset >= endOffset && loading {
			log.Printf("Completed load. Read in %d unprocessed records. elapsed:%dms (%s-%d)", count, time.Now().Sub(start).Milliseconds(), topic, partition)
			loading = false
			wg.Done()
			continue
		}

		select {
		case err := <-pconsumer.Errors():
			log.Printf("recv:  %s", err)
			break
		case msg := <-pconsumer.Messages():
			pop <- msg
			currentOffset = msg.Offset

			if loading {
				count++

				if count%1000 == 0 {
					pct := float64(currentOffset) / float64(endOffset) * 100
					log.Printf("%s-%d@(%d / %d) [%0.2f%%]", msg.Topic, msg.Partition, currentOffset, endOffset+1, pct)
				}
			}
		}
	}
}

func httpListTopics(kdbs map[string]db.DB) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, req *http.Request) {
		topics := []string{}
		for k, _ := range kdbs {
			topics = append(topics, k)
		}

		list, err := json.Marshal(topics)
		if err != nil {
			w.WriteHeader(500)
			w.Write([]byte(err.Error()))
			return
		}
		w.Write(list)
		w.Write([]byte("\n"))
		return
	}
}

func httpQueryTopic(kdbs map[string]db.DB) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, req *http.Request) {
		vars := mux.Vars(req)
		topic, ok := vars["topic"]
		if !ok {
			// error: missing topic
		}

		log.Printf("List all on topic:%s\n", topic)
		kdb, ok := kdbs[topic]
		if !ok {
			w.WriteHeader(400)
			w.Write([]byte("topic not found"))
			return
		}

		ms := make([]types.Message, 0)
		it := kdb.NewIterator(nil, nil)
		defer it.Close()
		for ; it.Valid(); it.Next() {
			if it.Error() != nil {
				w.WriteHeader(400)
				w.Write([]byte(it.Error().Error()))
				return
			}

			m := types.Message{}
			bytes := it.Value()
			err := json.Unmarshal(bytes, &m)
			if err != nil {
				w.WriteHeader(400)
				w.Write([]byte(err.Error()))
				return
			}

			ms = append(ms, m)
		}

		list, err := json.Marshal(ms)
		if err != nil {
			w.WriteHeader(500)
			w.Write([]byte(err.Error()))
			return
		}
		w.Write(list)
		w.Write([]byte("\n"))
		return
	}
}
