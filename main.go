package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"sync"
	"time"

	ui "github.com/gizak/termui"
	"github.com/simon-xia/kotop/kotop"
)

var (
	confFile      = flag.String("f", "~/.kotop.conf", "config file name")
	consumerGroup = flag.String("group", "", "consumer group name")
	topic         = flag.String("topic", "", "topic name")
	brokerHosts   = flag.String("broker", "", fmt.Sprintf("broker list, split by '%s' e.g: \"192.168.0.1:9092,192.168.0.2:9092,192.168.0.3:9092\"", kotop.BrokerListSpliter))
	zkHosts       = flag.String("zk", "", fmt.Sprintf("zookeeper hosts and root, e.g: \"192.168.0.1:2181,192.168.0.2:2181/kafka_root_dir\""))
	dataRefresh   = flag.Int("refresh", 2, "time to refresh data, default is 1s ")
)

func main() {
	flag.Parse()

	raw, err := ioutil.ReadFile(*confFile)
	if err != nil {
		panic(err)
	}

	var conf kotop.KOTopConf
	err = json.Unmarshal(raw, &conf)
	if err != nil {
		panic(err)
	}

	if len(*brokerHosts) > 0 {
		conf.Brokers = *brokerHosts
	}
	if len(*zkHosts) > 0 {
		conf.ZKHosts = *zkHosts
	}

	err = ui.Init()
	if err != nil {
		panic(err)
	}
	defer ui.Close()

	top, err := kotop.NewKOTop(&conf, *topic, *consumerGroup)
	if err != nil {
		panic(err)
	}

	data, err := top.Check(*topic)
	if err != nil {
		panic(err)
	}

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
		canvas.SetSort(kotop.FieldNameOffset)
		ui.Clear()
		canvas.Render()
		mutex.Unlock()
	})
	ui.Handle("/sys/kbd/3", func(ui.Event) {
		mutex.Lock()
		canvas.SetSort(kotop.FieldNameSize)
		ui.Clear()
		canvas.Render()
		mutex.Unlock()
	})
	ui.Handle("/sys/kbd/4", func(ui.Event) {
		mutex.Lock()
		canvas.SetSort(kotop.FieldNameProduceSpeed)
		ui.Clear()
		canvas.Render()
		mutex.Unlock()
	})
	ui.Handle("/sys/kbd/5", func(ui.Event) {
		mutex.Lock()
		canvas.SetSort(kotop.FieldNameConsumeSpeed)
		ui.Clear()
		canvas.Render()
		mutex.Unlock()
	})
	ui.Handle("/sys/kbd/6", func(ui.Event) {
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
