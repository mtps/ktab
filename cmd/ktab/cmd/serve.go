package cmd

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/mtps/ktab/pkg/db"
	"github.com/mtps/ktab/pkg/db/bolt"
	"github.com/mtps/ktab/pkg/types"
	"github.com/spf13/cobra"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sort"
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
			panic(err)
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

type TopicCheckpoint struct {
	Topic     string
	Partition int32
	Offset    int64
}

type Checkpoint struct {
	File             string
	Version          int
	TopicCheckpoints []TopicCheckpoint
}

// file contents:
// $version
// $numEntries
// $topic $partition $lastReadOffset
// $topic $partition $lastReadOffset
// ...
// $topic $partition $lastReadOffset
func (t Checkpoint) Write() error {
	buffer := bytes.Buffer{}
	buffer.WriteString(fmt.Sprintf("%d\n", t.Version))
	buffer.WriteString(fmt.Sprintf("%d\n", len(t.TopicCheckpoints)))
	for _, tc := range t.TopicCheckpoints {
		buffer.WriteString(fmt.Sprintf("%s %d %d\n", tc.Topic, tc.Partition, tc.Offset))
	}

	f, err := os.Create(t.File)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err = f.WriteString(buffer.String()); err != nil {
		return err
	}

	return nil
}

func (t *Checkpoint) PutOffset(topic string, partition int32, offset int64) {
	for idx, tc := range t.TopicCheckpoints {
		if tc.Topic == topic && tc.Partition == partition {
			tc.Offset = offset
			t.TopicCheckpoints[idx] = tc
			return
		}
	}

	t.TopicCheckpoints = append(t.TopicCheckpoints, TopicCheckpoint{
		Topic:     topic,
		Partition: partition,
		Offset:    offset + 1,
	})
}

func (t Checkpoint) GetOffset(topic string, partition int32) int64 {
	for _, tc := range t.TopicCheckpoints {
		if tc.Topic == topic && tc.Partition == partition {
			return tc.Offset
		}
	}
	return -1
}

func LoadCheckpoint(filename string) (t *Checkpoint, err error) {
	t = &Checkpoint{File: filename}

	checkpointBytes, err := ioutil.ReadFile(filename)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("No checkpoint file found, using empty")
			err = nil
			return
		}
		return
	}

	lines := strings.Split(string(checkpointBytes), "\n")
	if len(lines) < 2 {
		err = fmt.Errorf("invalid checkpoint file format")
		return
	}

	if t.Version, err = strconv.Atoi(lines[0]); err != nil {
		return
	}

	count, err := strconv.Atoi(lines[1])
	if err != nil {
		return
	}

	for i := 2; i < 2+count; i++ {
		parts := strings.Split(lines[i], " ")
		if len(parts) != 3 {
			err = fmt.Errorf("invalid checkpoint for topic: parity mismatch: line %d: %w", i+1, err)
			return
		}

		var p int64
		p, err = strconv.ParseInt(parts[1], 10, 32)
		if err != nil {
			err = fmt.Errorf("invalid checkpoint for topic: partition parse error: line %d: %w", i+1, err)
			return
		}

		var o int64
		o, err = strconv.ParseInt(parts[2], 10, 64)
		if err != nil {
			err = fmt.Errorf("invalid checkpoint for topic: offset parse error: line %d: %w", i+1, err)
			return
		}

		t.TopicCheckpoints = append(t.TopicCheckpoints, TopicCheckpoint{
			Topic:     parts[0],
			Partition: int32(p),
			Offset:    o,
		})
	}
	return
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

func receiverLoop(rockses map[string]db.DB, pop <-chan *sarama.ConsumerMessage, chk *Checkpoint, done <-chan struct{}) {
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

	pop := make(chan *sarama.ConsumerMessage)
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
	checkpoint, err := LoadCheckpoint(checkpointFilePath)
	if err != nil {
		return err
	}

	for topic, partitions := range topicPartitions {
		for _, partition := range partitions {
			go consumePartition(topic, partition, checkpoint, client, consumer, wg, pop)
		}
	}

	done := make(chan struct{})
	go receiverLoop(stores, pop, checkpoint, done)

	// Wait for load.
	wg.Wait()
	log.Printf("Load complete! Starting http server %s\n", serveArgs.laddr)

	log.Printf("Example query:\n")
	log.Printf("   # curl -i '%s/topics?topic=test&key=$(echo $key | base64)'", serveArgs.laddr)
	log.Printf("   # curl -i '%s/topics?topic=test&offset=n&partition=p'", serveArgs.laddr)

	mux := http.NewServeMux()
	mux.HandleFunc("/topics", httpListTopic(stores))
	mux.HandleFunc("/topics/{topic}", httpQueryTopic(stores))
	mux.HandleFunc("/topics/{topic}/seek", httpQueryTopicOffsetPartition(stores))
	mux.HandleFunc("/topics/{topic}/keys/{key}", httpQueryTopicKey(stores))

	http.ListenAndServe(serveArgs.laddr, mux)

	consumer.Close()
	client.Close()

	done <- struct{}{}
	return nil
}

func consumePartition(
	t string,
	part int32,
	chk *Checkpoint,
	client sarama.Client,
	consumer sarama.Consumer,
	wg *sync.WaitGroup,
	pop chan *sarama.ConsumerMessage,
) {
	log.Printf("Assigning partition (%s-%d)\n", t, part)
	var err error

	currentOffset := chk.GetOffset(t, part)
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

	// Next produced offset is returned from GetOffset, so back up one.
	endOffset := nextOffset - 1

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
		}
	}
}

func httpQueryTopicOffsetPartition(rockses map[string]db.DB) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, req *http.Request) {
		// Pull url and query params.
		qq := req.URL.Query()
		topic := req.PathValue("topic")
		offsetP := qq.Get("offset")
		partitionP := qq.Get("partition")
		// Parse the partition and offset.
		off, err := strconv.ParseInt(offsetP, 10, 64)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)

		}
		part, err := strconv.ParseInt(partitionP, 10, 32)
		log.Printf("Scanning [%s] for part:%d off:%d\n", topic, part, off)
		// Fetch the db for the requested topic
		db, ok := rockses[topic]
		if !ok {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("topic not found"))
			return
		}
		// Scan for a matching item.
		var bz []byte
		db.NewIterator(func(k, v []byte) error {
			m := types.Message{}
			err := json.Unmarshal(v, &m)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte(err.Error()))
				return nil
			}
			// Found it!
			if m.Partition == int32(part) && m.Offset == off {
				bz = append(v[:0:0], v...)
				return nil
			}
			return nil
		})
		// Not found
		if bz == nil {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte("partition offset not found"))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write(bz)
		w.Write([]byte{'\n'})
	}
}

func httpListTopic(rockses map[string]db.DB) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, req *http.Request) {
		var topics []string
		for topic, _ := range rockses {
			topics = append(topics, topic)
		}
		sort.Strings(topics)
		bz, err := json.Marshal(topics)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(err.Error()))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write(bz)
		w.Write([]byte{'\n'})
	}
}

func httpQueryTopicKey(rockses map[string]db.DB) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, req *http.Request) {
		qq := req.URL.Query()
		topic := req.PathValue("topic")
		key := qq.Get("key")
		keyBytes, err := base64.URLEncoding.DecodeString(key)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("invalid key"))
			return
		}
		// Fetch the db for the requested topic
		db, ok := rockses[topic]
		if !ok {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("topic not found"))
			return
		}
		// Pull the key from the store.
		bz, err := db.Get(keyBytes)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write(bz)
		w.Write([]byte{'\n'})
		return
	}
}

func httpQueryTopic(rockses map[string]db.DB) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, req *http.Request) {
		topic := req.PathValue("topic")
		// Fetch the db for the requested topic
		db, ok := rockses[topic]
		if !ok {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("topic not found"))
			return
		}
		// Start the json array.
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("["))
		db.NewIterator(func(k, v []byte) error {
			w.Write(v)
			return nil
		})
		w.Write([]byte("]"))
		w.Write([]byte{'\n'})
	}
}
