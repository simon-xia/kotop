package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	ui "github.com/gizak/termui"
	"github.com/simon-xia/kotop/kotop"
)

var (
	confFile      = flag.String("f", "", "config file name")
	consumerGroup = flag.String("group", "", "consumer group name")
	topic         = flag.String("topic", "", "topic name")
	version       = flag.String("version", "", fmt.Sprintf("kafka version, supported values (%s)", printAllSupportedVersions()))
	verbose       = flag.Bool("v", false, "verbose info")
	brokerHosts   = flag.String("broker", "", fmt.Sprintf("broker list, split by '%s' e.g: \"192.168.0.1:9092,192.168.0.2:9092,192.168.0.3:9092\"", kotop.BrokerListSpliter))
	zkHosts       = flag.String("zk", "", fmt.Sprintf("zookeeper hosts and root, e.g: \"192.168.0.1:2181,192.168.0.2:2181/kafka_root_dir\""))
	dataRefresh   = flag.Int("refresh", 2, "time to refresh data, default is 2s ")

	errInvalidKafkaVersion = errors.New("invalid kafka version")
)

func main() {
	flag.Parse()
	var conf kotop.KOTopConf
	v := sarama.MinVersion

	if len(*confFile) > 0 {
		raw, err := ioutil.ReadFile(*confFile)
		if err != nil {
			panic(err)
		}
		err = json.Unmarshal(raw, &conf)
		if err != nil {
			panic(err)
		}
		conf.Verbose = *verbose
		if len(conf.KafkaVersion) > 0 {
			kv, ok := supportedVersions[conf.KafkaVersion]
			if !ok {
				flag.Usage()
				panic(errInvalidKafkaVersion)
			}
			v = kv
		}
	}

	if len(*brokerHosts) > 0 {
		conf.Brokers = *brokerHosts
	}
	if len(*zkHosts) > 0 {
		conf.ZKHosts = *zkHosts
	}
	if len(*version) > 0 {
		kv, ok := supportedVersions[*version]
		if !ok {
			flag.Usage()
			panic(errInvalidKafkaVersion)
		}
		v = kv
	}
	if *verbose {
		sarama.Logger = log.New(os.Stderr, "[Sarama] ", log.LstdFlags)
	} else {
		// discard log when verbose print is off
		log.SetOutput(ioutil.Discard)
	}

	top, err := kotop.NewKOTop(&conf, *topic, *consumerGroup, v)
	if err != nil {
		panic(err)
	}

	data, err := top.Check(*topic)
	if err != nil {
		panic(err)
	}

	err = ui.Init()
	if err != nil {
		panic(err)
	}
	defer ui.Close()

	var mutex sync.Mutex
	canvas := kotop.NewCanvas()

	mutex.Lock()
	canvas.LoadData(data)
	canvas.Render()
	mutex.Unlock()

	ui.Handle("/sys/wnd/resize", func(e ui.Event) {
		mutex.Lock()
		ui.Body.Width = ui.TermWidth()
		ui.Body.Align()
		ui.Clear()
		ui.Render(ui.Body)
		mutex.Unlock()
	})
	ui.Handle("/sys/kbd/q", func(ui.Event) {
		ui.StopLoop()
	})

	ui.Handle("/sys/kbd/1", func(ui.Event) {
		mutex.Lock()
		canvas.SetSort(kotop.FieldNamePid)
		ui.Clear()
		canvas.Render()
		mutex.Unlock()
	})
	ui.Handle("/sys/kbd/2", func(ui.Event) {
		mutex.Lock()
		canvas.SetSort(kotop.FieldNameSize)
		ui.Clear()
		canvas.Render()
		mutex.Unlock()
	})
	ui.Handle("/sys/kbd/3", func(ui.Event) {
		mutex.Lock()
		canvas.SetSort(kotop.FieldNameHighWaterLag)
		ui.Clear()
		canvas.Render()
		mutex.Unlock()
	})
	ui.Handle("/sys/kbd/4", func(ui.Event) {
		mutex.Lock()
		canvas.SetSort(kotop.FieldNameOffset)
		ui.Clear()
		canvas.Render()
		mutex.Unlock()
	})
	ui.Handle("/sys/kbd/5", func(ui.Event) {
		mutex.Lock()
		canvas.SetSort(kotop.FieldNameOffsetLag)
		ui.Clear()
		canvas.Render()
		mutex.Unlock()
	})
	ui.Handle("/sys/kbd/6", func(ui.Event) {
		mutex.Lock()
		canvas.SetSort(kotop.FieldNameProduceSpeed)
		ui.Clear()
		canvas.Render()
		mutex.Unlock()
	})
	ui.Handle("/sys/kbd/7", func(ui.Event) {
		mutex.Lock()
		canvas.SetSort(kotop.FieldNameConsumeSpeed)
		ui.Clear()
		canvas.Render()
		mutex.Unlock()
	})
	ui.Handle("/sys/kbd/8", func(ui.Event) {
		mutex.Lock()
		canvas.SetSort(kotop.FieldNameLeader)
		ui.Clear()
		canvas.Render()
		mutex.Unlock()
	})

	ui.Handle("/sys/kbd", func(e ui.Event) {
		mutex.Lock()
		ev := e.Data.(ui.EvtKbd)
		switch ev.KeyStr {
		case "<up>":
			canvas.PageUp()
		case "<down>":
			canvas.PageDown()
		}
		ui.Clear()
		canvas.Render()
		mutex.Unlock()
	})

	ticker := time.Tick(time.Duration(*dataRefresh) * time.Second)
	go func() {
		for {
			select {
			case <-ticker:
				data, err := top.Check(*topic)
				if err != nil {
					panic(err)
				}

				mutex.Lock()
				canvas.LoadData(data)
				ui.Clear()
				canvas.Render()
				mutex.Unlock()
			}
		}
	}()

	ui.Loop()
}

func printAllSupportedVersions() string {
	vs := make([]string, 0, len(supportedVersions))
	for v := range supportedVersions {
		vs = append(vs, v)
	}
	return strings.Join(vs, ", ")
}

var supportedVersions = map[string]sarama.KafkaVersion{
	"0.8.2.0":  sarama.V0_8_2_0,
	"0.8.2.1":  sarama.V0_8_2_1,
	"0.8.2.2":  sarama.V0_8_2_2,
	"0.9.0.0":  sarama.V0_9_0_0,
	"0.9.0.1":  sarama.V0_9_0_1,
	"0.10.0.0": sarama.V0_10_0_0,
	"0.10.0.1": sarama.V0_10_0_1,
	"0.10.1.0": sarama.V0_10_1_0,
	"0.10.1.1": sarama.V0_10_1_1,
	"0.10.2.0": sarama.V0_10_2_0,
	"0.10.2.1": sarama.V0_10_2_1,
	"0.11.0.0": sarama.V0_11_0_0,
	"0.11.0.1": sarama.V0_11_0_1,
	"0.11.0.2": sarama.V0_11_0_2,
	"1.0.0.0":  sarama.V1_0_0_0,
	"1.1.0.0":  sarama.V1_1_0_0,
}
