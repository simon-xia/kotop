package kotop

import (
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/wvanbergen/kazoo-go"
)

const BrokerListSpliter = ","

type KOTopConf struct {
	Brokers string `json:"brokers"`
	ZKHosts string `json:"zkhosts"`
}

type KOTop struct {
	hosts      []string
	group      string
	offsetReq  *sarama.OffsetFetchRequest
	cli        sarama.Client
	consumer   sarama.Consumer
	broker     *sarama.Broker
	pm         map[int32]partitionMeta
	ps         []int32
	cg         *kazoo.Consumergroup
	lastResult map[int32]partitionInfo
	lastCheck  time.Time
}

func NewKOTop(conf *KOTopConf, topic, cg string) (k *KOTop, err error) {
	client, err := sarama.NewClient(strings.Split(conf.Brokers, BrokerListSpliter), nil)
	if err != nil {
		return
	}

	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return
	}

	pids, err := consumer.Partitions(topic)
	if err != nil {
		return
	}
	for _, id := range pids {
		_, err = consumer.ConsumePartition(topic, id, sarama.OffsetNewest)
		if err != nil {
			return
		}
	}

	zk, err := kazoo.NewKazooFromConnectionString(conf.ZKHosts, kazoo.NewConfig())
	if err != nil {
		return
	}

	k = &KOTop{
		cli:      client,
		group:    cg,
		pm:       make(map[int32]partitionMeta),
		cg:       zk.Consumergroup(cg),
		consumer: consumer,
	}
	err = k.refreshMeta(topic)
	return
}

type partitionMeta struct {
	Pid      int32
	Leader   int32
	Replicas []int32
	Isr      []int32
}

type partitionInfo struct {
	partitionMeta
	Offset        int64
	Size          int64
	HighWatermark int64
}

type resultEntry struct {
	partitionInfo
	ConsumeRate    float64
	ProduceRate    float64
	ProducePercent int
	ConsumePercent int
}

type canvasData struct {
	Brokers []int32
	Data    []resultEntry
}

func diffPartionInfo(old, new partitionInfo, dur time.Duration) resultEntry {
	return resultEntry{
		partitionInfo: new,
		ConsumeRate:   float64(new.Offset-old.Offset) / dur.Seconds(), //TODO if offset = -1, sec -> ms
		ProduceRate:   float64(new.Size-old.Size) / dur.Seconds(),
	}
}

func diffPartionInfos(old, new map[int32]partitionInfo, dur time.Duration) (res []resultEntry) {
	pmax, cmax := math.SmallestNonzeroFloat64, math.SmallestNonzeroFloat64
	pmin, cmin := math.MaxFloat64, math.MaxFloat64

	for pid, o := range old {
		if n, ok := new[pid]; ok {
			r := diffPartionInfo(o, n, dur)
			res = append(res, r)

			if r.ProduceRate > pmax {
				pmax = r.ProduceRate
			}

			if r.ProduceRate < pmin {
				pmin = r.ProduceRate
			}

			if r.ConsumeRate > cmax {
				cmax = r.ConsumeRate
			}

			if r.ConsumeRate < cmin {
				cmin = r.ConsumeRate
			}
		}
	}

	pmax, cmax, pmin, cmin = pmax*rateDisplayScaleMax, cmax*rateDisplayScaleMax, pmin*rateDisplayScaleMin, cmin*rateDisplayScaleMin
	for i := range res {
		res[i].ConsumePercent = int((res[i].ConsumeRate - cmin) * 100 / (cmax - cmin))
		res[i].ProducePercent = int((res[i].ProduceRate - pmin) * 100 / (pmax - pmin))
	}

	return
}

func (k *KOTop) brokers() []int32 {
	brokers := make([]int32, 0, 20)
	for _, b := range k.cli.Brokers() {
		brokers = append(brokers, b.ID())
	}

	sort.Slice(brokers, func(i, j int) bool {
		return brokers[i] < brokers[j]
	})

	return brokers
}

func (k *KOTop) Check(topic string) (data canvasData, err error) {
	data.Brokers = k.brokers()
	logsizes, err := k.logSize(topic)
	if err != nil {
		return
	}

	offsets, get, err := k.offsetFromKafka(topic)
	if err != nil {
		return
	}

	if !get {
		offsets, err = k.offsetFromZK(topic)
		if err != nil {
			return
		}
	}

	hws := k.highWaterMarks(topic)

	now := time.Now()
	current := k.marshalResult(logsizes, offsets, hws)
	if k.lastResult != nil {
		data.Data = diffPartionInfos(k.lastResult, current, now.Sub(k.lastCheck))
	} else {
		for _, info := range current {
			data.Data = append(data.Data, resultEntry{
				partitionInfo: info,
			})
		}
	}

	k.lastCheck = now
	k.lastResult = current
	return
}

func (k *KOTop) marshalResult(sizes, offs, hws map[int32]int64) (results map[int32]partitionInfo) {
	results = make(map[int32]partitionInfo, len(k.ps))
	for _, pid := range k.ps {
		off, ok := offs[pid]
		if !ok {
			off = -1
		}

		s, ok := sizes[pid]
		if !ok {
			s = -1
		}

		hw, ok := hws[pid]
		if !ok {
			hw = -1
		}
		results[pid] = partitionInfo{
			partitionMeta: k.pm[pid],
			Offset:        off,
			Size:          s,
			HighWatermark: hw,
		}
	}
	return
}

func (k *KOTop) logSize(topic string) (offsets map[int32]int64, err error) {
	offsets = make(map[int32]int64, len(k.ps))
	var (
		mux sync.Mutex
		wg  sync.WaitGroup
	)

	for _, pid := range k.ps {
		wg.Add(1)
		go func(pid int32) {
			defer wg.Done()
			off, er := k.cli.GetOffset(topic, pid, sarama.OffsetNewest)
			if er != nil {
				return
			}
			mux.Lock()
			offsets[pid] = off
			mux.Unlock()
		}(pid)
	}
	wg.Wait()
	return
}

func (k *KOTop) highWaterMarks(topic string) (hw map[int32]int64) {
	hws := k.consumer.HighWaterMarks()
	hw = hws[topic]
	return
}

func (k *KOTop) offsetFromZK(topic string) (offsets map[int32]int64, err error) {
	offs, err := k.cg.FetchAllOffsets()
	if err != nil {
		return
	}

	offsets = offs[topic]
	return
}

func (k *KOTop) offsetFromKafka(topic string) (offsets map[int32]int64, get bool, err error) {
	offsets = make(map[int32]int64, len(k.ps))

	resp, err := k.broker.FetchOffset(k.offsetReq)
	if err != nil {
		return
	}
	for _, pid := range k.ps {
		block := resp.GetBlock(topic, pid)
		if block == nil || block.Err != sarama.ErrNoError {
			continue
		}
		if block.Offset != -1 {
			get = true
		}
		offsets[pid] = block.Offset
	}
	return
}

func (k *KOTop) refreshMeta(topic string) (err error) {
	err = k.cli.RefreshMetadata()
	if err != nil {
		return
	}

	err = k.cli.RefreshCoordinator(k.group)
	if err != nil {
		return
	}

	b, err := k.cli.Coordinator(k.group)
	if err != nil {
		return
	}
	k.broker = b

	ps, err := k.cli.Partitions(topic)
	if err != nil {
		return
	}
	k.ps = ps

	err = k.refreshPartitionMeta(topic)
	if err != nil {
		return
	}

	err = k.updateOffsetReq(topic)
	return
}

func (k *KOTop) refreshPartitionMeta(topic string) (err error) {
	for _, pid := range k.ps {
		b, er := k.cli.Leader(topic, pid)
		if er != nil {
			err = er
			return
		}

		replicas, er := k.cli.Replicas(topic, pid)
		if er != nil {
			err = er
			return
		}

		isr, er := k.cli.InSyncReplicas(topic, pid)
		if er != nil {
			err = er
			return
		}
		k.pm[pid] = partitionMeta{
			Pid:      pid,
			Replicas: replicas,
			Isr:      isr,
			Leader:   b.ID(),
		}
	}
	return
}

func (k *KOTop) updateOffsetReq(topic string) (err error) {

	req := &sarama.OffsetFetchRequest{
		Version:       1,
		ConsumerGroup: k.group,
	}

	for _, pid := range k.ps {
		req.AddPartition(topic, pid)
	}
	k.offsetReq = req
	return
}
