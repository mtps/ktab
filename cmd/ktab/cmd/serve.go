package cmd

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/mtps/ktab/pkg/rocks"
	"github.com/mtps/ktab/pkg/types"
	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"
	"github.com/tecbot/gorocksdb"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var cmdServe = &cobra.Command{
	Use: "serve",
	Short: "Pull a kafka topic into a table",
	Run: func(cmd *cobra.Command, args []string) {
		if err := serve(); err != nil {
			panic(err)
		}
	},
}

var serveArgs struct {
	laddr string
	broker string
	topics string

	rocksDir string
}

type TopicCheckpoint struct {
	Topic string
	Partition int32
	Offset int64
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
		Topic: topic,
		Partition: partition,
		Offset: offset,
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
	cmdServe.Flags().StringVar(&serveArgs.broker, "broker", "localhost:9092", "Broker to connect to")
	cmdServe.Flags().StringVar(&serveArgs.topics, "topics", "", "Topics to read and serve")
	cmdServe.Flags().StringVar(&serveArgs.rocksDir, "rocks-dir", "/tmp", "Directory for rocksDbs")

	cmdRoot.AddCommand(cmdServe)
}

const (
	CheckpointFilename = "checkpoint"
)

var (
	start = time.Now()
)

func receiverLoop(rockses map[string]*gorocksdb.DB, pop <-chan *sarama.ConsumerMessage, chk *Checkpoint, done <-chan struct{}) {
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
			err = rockses[m.Topic].Put(gorocksdb.NewDefaultWriteOptions(), m.Key, js)
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
	rockses := make(map[string]*gorocksdb.DB)

	pop := make(chan *sarama.ConsumerMessage)
	topics := strings.Split(serveArgs.topics, ",")
	for _, topic := range topics {
		partitions, err := consumer.Partitions(topic)
		if err != nil {
			return err
		}

		topicPartitions[topic] = partitions

		db, err := rocks.NewRocksDB(fmt.Sprintf("%s/%s", serveArgs.rocksDir, topic))
		if err != nil {
			return err
		}

		rockses[topic] = db
	}

	defer func() {
		for _, db := range rockses {
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
	checkpointFilePath := fmt.Sprintf("%s/%s", serveArgs.rocksDir, CheckpointFilename)
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
	go receiverLoop(rockses, pop, checkpoint, done)

	// Wait for load.
	wg.Wait()
	log.Printf("Load complete! Starting http server %s\n", serveArgs.laddr)

	log.Printf("Example query:\n")
	log.Printf("   # curl -i '%s/topics?topic=test&key=$(echo $key | base64)'", serveArgs.laddr)
	log.Printf("   # curl -i '%s/topics?topic=test&offset=n&partition=p'", serveArgs.laddr)
	http.HandleFunc("/topics", httpQueryTopic(rockses))

	http.ListenAndServe(serveArgs.laddr, nil)

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

func httpQueryTopic(rockses map[string]*gorocksdb.DB) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, req *http.Request) {
		qq := req.URL.Query()
		topic := qq.Get("topic")
		key := qq.Get("key")
		offset := qq.Get("offset")
		partition := qq.Get("partition")

		db, ok := rockses[topic]
		if !ok {
			w.WriteHeader(400)
			w.Write([]byte("topic not found"))
			return
		}

		var msgBytes []byte
		if key != "" {
			keyBytes, err := base64.URLEncoding.DecodeString(key)
			if err != nil {
				w.WriteHeader(400)
				return
			}

			slice, err := db.Get(gorocksdb.NewDefaultReadOptions(), keyBytes)
			if err != nil || !slice.Exists() {
				w.WriteHeader(404)
				return
			}

			msgBytes = slice.Data()

		} else if offset != "" && partition != "" {
			off, err := strconv.ParseInt(offset, 10, 64)
			part, err := strconv.ParseInt(partition, 10, 32)
			log.Printf("Scanning for part:%d off:%d\n", part, off)

			it := db.NewIterator(gorocksdb.NewDefaultReadOptions())
			defer it.Close()
			for ; it.Valid(); it.Next() {
				if it.Err() != nil {
					w.WriteHeader(400)
					w.Write([]byte(err.Error()))
					return
				}

				m := types.Message{}
				bytes := it.Value().Data()
				err := json.Unmarshal(bytes, &m)
				if err != nil {
					w.WriteHeader(400)
					w.Write([]byte(err.Error()))
					return
				}

				if m.Partition == int32(part) && m.Offset == off {
					msgBytes = bytes
					break
				}
			}
		} else {
			w.WriteHeader(401)
			w.Write([]byte("unknown search type"))
			return
		}

		if len(msgBytes) == 0 {
			w.WriteHeader(404)
			return
		}

		w.WriteHeader(200)
		w.Write(msgBytes)
	}
}
