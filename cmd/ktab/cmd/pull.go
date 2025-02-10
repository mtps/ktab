package cmd

import (
	"encoding/json"
	"github.com/mtps/ktab/pkg/compact"
	"github.com/mtps/ktab/pkg/types"
	"github.com/IBM/sarama"
	"github.com/spf13/cobra"
	"io/ioutil"
	"log"
	"strconv"
	"sync"
)


var cmdPull = &cobra.Command{
	Use: "pull",
	Short: "Pull a kafka topic into a table",
	Run: func(cmd *cobra.Command, args []string) {
		if err := runPull(); err != nil {
			panic(err)
		}
	},
}

var pullArgs struct {
	broker  string
	topic   string
	out     string
	compact bool
}

func init() {
	cmdPull.Flags().StringVar(&pullArgs.broker, "broker", "", "Broker to connect to")
	cmdPull.Flags().StringVar(&pullArgs.topic, "topic", "", "Topic to clone data from")
	cmdPull.Flags().StringVar(&pullArgs.out, "out", "", "File to output to")
	cmdPull.Flags().BoolVar(&pullArgs.compact, "compact", true, "Compact the records to guarantee uniqueness per key")

	cmdRoot.AddCommand(cmdPull)
}

func runPull() error {
	cfg := sarama.NewConfig()
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	cfg.Consumer.Return.Errors = true
	cfg.Version = sarama.V2_0_0_0

	brokers := []string{pullArgs.broker}

	client, err := sarama.NewClient(brokers, cfg)
	if err != nil {
		return err
	}


	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return err
	}

	partitions, err := consumer.Partitions(pullArgs.topic)
	if err != nil {
		return err
	}

	pop := make(chan *sarama.ConsumerMessage)

	wg := &sync.WaitGroup{}
	wg.Add(len(partitions))

	for _, partition := range partitions {
		go func(part int32) {
			defer wg.Done()

			log.Printf("Assigning partition %d\n", part)
			pconsumer, err := consumer.ConsumePartition(pullArgs.topic, part, sarama.OffsetOldest)
			if err != nil {
				log.Printf("consumer:  %w\n", err)
				return
			}

			startOffset, err := client.GetOffset(pullArgs.topic, part, sarama.OffsetOldest)
			if err != nil {
				log.Printf("get start offset:  %w\n", err)
				return
			}
			startOffset--

			endOffset, err := client.GetOffset(pullArgs.topic, part, sarama.OffsetNewest)
			if err != nil {
				log.Printf("get end offset:  %w\n", err)
				return
			}

			log.Printf("part:%d startOffset:%d endOffset:%d\n", part, startOffset, endOffset)
			endOffset--

			if startOffset == endOffset {
				log.Printf("part:%d no data to read\n", part)
				return
			}

			for {
				select {
				case err := <-pconsumer.Errors():
					log.Printf("recv:  %s\n", err)
					break
				case msg := <-pconsumer.Messages():
					log.Printf("part:%d read message %d / %d\n", part, msg.Offset, endOffset)
					pop <- msg

					if msg.Offset >= endOffset {
						log.Printf("part:%d done!\n", part)
						return
					}
				}
			}
		}(partition)
	}

	done := make(chan struct{})
	var msgs []*sarama.ConsumerMessage
	go func() {
		for {
			select {
				case m := <-pop:
					msgs = append(msgs, m)
				case <-done:
					return
			}
		}
	}()

	wg.Wait()
	consumer.Close()
	client.Close()

	done <- struct{}{}

	if pullArgs.compact {
		msgs = compact.Compact(msgs)
	}

	log.Printf("found %d records. writing to %s\n", len(msgs), pullArgs.out)

	splits := [10][]types.Message{}

	// split into 10
	for i := 0; i < len(msgs); i++ {
		splits[i % 10] = append(splits[i % 10],	types.KafkaToRecord(msgs[i]))
	}

	for idx, mms := range splits {
		data, err := json.Marshal(mms)
		if err != nil {
			return err
		}

		fname := pullArgs.out + "." + strconv.Itoa(idx)
		log.Printf("Writing out file %s\n", fname)
		if err := ioutil.WriteFile(fname, data, 0644); err != nil {
			return err
		}
	}

	return nil
}
