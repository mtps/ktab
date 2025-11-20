package cmd

import (
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/mtps/ktab/internal/checkpoint"
	routes "github.com/mtps/ktab/internal/http"
	"github.com/mtps/ktab/pkg/db"
	"github.com/mtps/ktab/pkg/db/bolt"
	"github.com/mtps/ktab/pkg/types"
	"github.com/spf13/cobra"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var cmdServe = &cobra.Command{
	Use:   "serve",
	Short: "Serve a kafka topic via http",
	Run: func(cmd *cobra.Command, args []string) {
		if err := serve(); err != nil {
			fmt.Printf("Failed to start: %v\n", err)
			return
		}
	},
}

var serveArgs struct {
	laddr  string
	broker []string
	topics []string
	kafka  struct {
		sasl struct {
			username  string
			password  string
			mechanism string
			enable    bool
		}
		tls struct {
			enable bool
		}
	}
	dataDir string
}

func init() {
	cmdServe.Flags().StringVar(&serveArgs.laddr, "laddr", "localhost:8089", "Listen address")
	cmdServe.Flags().StringArrayVar(&serveArgs.topics, "topic", []string{}, "Topics to read and serve")
	cmdServe.Flags().StringVar(&serveArgs.dataDir, "data-dir", "./data", "Directory for data storage")
	cmdServe.Flags().StringArrayVar(&serveArgs.broker, "broker", []string{"localhost:9092"}, "Broker to connect to")
	cmdServe.Flags().BoolVar(&serveArgs.kafka.sasl.enable, "sasl-enable", false, "Enable sasl connection")
	cmdServe.Flags().StringVar(&serveArgs.kafka.sasl.username, "sasl-username", "", "SASL username")
	cmdServe.Flags().StringVar(&serveArgs.kafka.sasl.password, "sasl-password", "", "SASL password")
	cmdServe.Flags().StringVar(&serveArgs.kafka.sasl.mechanism, "sasl-mechanism", "plain", "SASL mechanism")
	cmdServe.Flags().BoolVar(&serveArgs.kafka.tls.enable, "tls-enable", false, "Enable TLS")

	cmdRoot.AddCommand(cmdServe)
}

var strToBool = func(s string) bool {
	b, err := strconv.ParseBool(s)
	if err != nil {
		return false
	}
	return b
}

var strToStr = func(s string) string {
	return s
}

func setIfExists[K string | bool](p *K, env string, conv func(string) K) {
	if s := os.Getenv(env); s != "" {
		*p = conv(s)
	}
}

func setIfExistsList(p *[]string, env string) {
	if s := os.Getenv(env); s != "" {
		*p = strings.Split(s, ",")
	}
}

func loadCfgFromEnv() {
	setIfExists(&serveArgs.laddr, "LADDR", strToStr)
	setIfExists(&serveArgs.dataDir, "DATA_DIR", strToStr)

	setIfExistsList(&serveArgs.topics, "KAFKA_TOPICS")
	setIfExistsList(&serveArgs.broker, "KAFKA_BROKERS")
	setIfExistsList(&serveArgs.broker, "CONFLUENT_BROKER_ENDPOINT")

	setIfExists(&serveArgs.kafka.sasl.username, "CONFLUENT_CLUSTER_API_KEY", strToStr)
	setIfExists(&serveArgs.kafka.sasl.password, "CONFLUENT_CLUSTER_API_SECRET", strToStr)
	setIfExists(&serveArgs.kafka.sasl.mechanism, "KAFKA_SASL_MECHANISM", strToStr)
	setIfExists(&serveArgs.kafka.sasl.enable, "KAFKA_SASL_ENABLE", strToBool)
	setIfExists(&serveArgs.kafka.tls.enable, "KAFKA_TLS_ENABLE", strToBool)
}

const (
	CheckpointFilename = "checkpoint"
)

var (
	start = time.Now()
)

func receiverLoop(rockses map[string]db.DB, pop <-chan *sarama.ConsumerMessage, chk *checkpoint.Checkpoint, done <-chan struct{}) {
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
			// log.Printf("Recv: key:%v val:%v\n", string(m.Key), string(js))
			err = rockses[m.Topic].Put(m.Key, js)
			if err != nil {
				panic(err)
			}
			log.Printf("Setting offset %s-%d: %d", m.Topic, m.Partition, m.Offset)
			chk.PutOffset(m.Topic, m.Partition, m.Offset)

		case <-done:
			log.Printf("Done!")
			return
		}
	}
}

func serve() error {
	kcfg := sarama.NewConfig()
	kcfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	kcfg.Consumer.Return.Errors = true

	// Pull in any env vars.
	loadCfgFromEnv()

	if len(serveArgs.topics) == 0 {
		return fmt.Errorf("at least one --topic is required")
	}

	// Sasl config
	if serveArgs.kafka.sasl.enable {
		kcfg.Net.SASL.Enable = true
		kcfg.Net.SASL.User = serveArgs.kafka.sasl.username
		kcfg.Net.SASL.Password = serveArgs.kafka.sasl.password
		kcfg.Net.SASL.Mechanism = sarama.SASLMechanism(strings.ToUpper(serveArgs.kafka.sasl.mechanism))
	}
	// Tls config
	if serveArgs.kafka.tls.enable {
		kcfg.Net.TLS.Enable = true
	}

	client, err := sarama.NewClient(serveArgs.broker, kcfg)
	if err != nil {
		return err
	}

	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return err
	}

	topicPartitions := make(map[string][]int32)
	stores := make(map[string]db.DB)
	topics := serveArgs.topics

	// Make sure the data dir exists, if not, make it.
	err = os.MkdirAll(serveArgs.dataDir, os.ModePerm)

	for _, topic := range topics {
		partitions, err := consumer.Partitions(topic)
		if err != nil {
			return err
		}

		topicPartitions[topic] = partitions

		dbFile := fmt.Sprintf("%s/%s", serveArgs.dataDir, topic)
		db, err := bolt.NewBoltDB(dbFile)
		if err != nil {
			return err
		}
		stores[topic] = db
	}

	defer func() {
		for _, db := range stores {
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
	checkpointFilePath := fmt.Sprintf("%s/%s", serveArgs.dataDir, CheckpointFilename)
	ckp, err := checkpoint.LoadCheckpoint(checkpointFilePath)
	if err != nil {
		return err
	}

	done := make(chan struct{})
        var pops []chan *sarama.ConsumerMessage
	for topic, partitions := range topicPartitions {
		for _, partition := range partitions {
                        pop := make(chan *sarama.ConsumerMessage)
                        pops = append(pops, pop)
			go consumePartition(topic, partition, ckp, client, consumer, wg, pop, done)
		}
	}

        for _, pop := range pops {
	    go receiverLoop(stores, pop, ckp, done)
        }

	// Wait for load.
	wg.Wait()
	log.Printf("Load complete! Starting http server %s\n", serveArgs.laddr)

	topic := serveArgs.topics[0]
	log.Printf("Example queries:\n")
	log.Printf(" # List topics")
	log.Printf(" $ curl -i '%s/topics'", serveArgs.laddr)
	log.Printf(" # Dump topic %s", topic)
	log.Printf(" $ curl -i '%s/topics/%s'", serveArgs.laddr, topic)
	log.Printf(" # Find a key in topic %s", topic)
	log.Printf(" $ curl -i '%s/topics/%s/keys/$(echo $key | base64)'", serveArgs.laddr, topic)
	log.Printf(" # Find a message in topic %s by partition and offset", topic)
	log.Printf(" $ curl -i '%s/topics/topic/%s?offset=n&partition=p'", serveArgs.laddr, topic)

	mux := http.NewServeMux()
	mux.HandleFunc("/topics", routes.ListTopic(stores))
	mux.HandleFunc("/topics/{topic}", routes.QueryTopic(stores))
	mux.HandleFunc("/topics/{topic}/seek", routes.QueryTopicOffsetPartition(stores))
	mux.HandleFunc("/topics/{topic}/keys/{key}", routes.QueryTopicKey(stores))

	http.ListenAndServe(serveArgs.laddr, mux)

	consumer.Close()
	client.Close()

	done <- struct{}{}
	return nil
}

func consumePartition(
	t string,
	part int32,
	chk *checkpoint.Checkpoint,
	client sarama.Client,
	consumer sarama.Consumer,
	wg *sync.WaitGroup,
	pop chan *sarama.ConsumerMessage,
        done chan struct{},
) {
	log.Printf("Assigning partition (%s-%d)\n", t, part)
	var err error

	currentOffset := chk.GetOffset(t, part)
	log.Printf("current offset %s-%d: %d", t, part, currentOffset)
	if currentOffset == -1 {
		currentOffset, err = client.GetOffset(t, part, sarama.OffsetOldest)
		if err != nil {
			log.Printf("get start offset: %s", err)
			return
		}
	}

	nextOffset, err := client.GetOffset(t, part, sarama.OffsetNewest)
	if err != nil {
		log.Printf("get end offset:  %s", err)
		return
	}
	log.Printf("next offset %s-%d: %d", t, part, nextOffset)

	// Next produced offset is returned from GetOffset, so back up one if gt 0.
	var endOffset int64 = 0
	if nextOffset > 0 {
		endOffset = nextOffset - 1
	}
	log.Printf("Loading from current:%d to end:%d (%s-%d)", currentOffset, endOffset, t, part)

	pconsumer, err := consumer.ConsumePartition(t, part, currentOffset)
	if err != nil {
		log.Printf("consumer (%s-%d):  %s", t, part, err)
		return
	}

	chk.PutOffset(t, part, currentOffset)

	loading := true
	count := 0
	for {
		if currentOffset >= endOffset && loading {
			log.Printf("Completed load. Read in %d unprocessed records. elapsed:%dms (%s-%d)", count, time.Now().Sub(start).Milliseconds(), t, part)
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
                case <-done:
                    log.Printf("Shutting down consumer for %s-%d", t, part)
                    break
		}
	}
}
