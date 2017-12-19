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

const Broker_list_spliter = ","

type KOTopConf struct {
	Brokers string `json:"brokers"`
	ZKHosts string `json:"zkhosts"`
}

type KOTop struct {
	hosts      []string
	group      string
	offsetReq  *sarama.OffsetFetchRequest
	cli        sarama.Client
	broker     *sarama.Broker
	pm         map[int32]PartitionMeta
	ps         []int32
	cg         *kazoo.Consumergroup
	lastResult map[int32]PartitionInfo
	lastCheck  time.Time
}

func NewKOTop(conf *KOTopConf, topic, cg string) (k *KOTop, err error) {
	c, err := sarama.NewClient(strings.Split(conf.Brokers, Broker_list_spliter), nil)
	if err != nil {
		return
	}

	zk, err := kazoo.NewKazooFromConnectionString(conf.ZKHosts, kazoo.NewConfig())
	if err != nil {
		return
	}

	k = &KOTop{
		cli:   c,
		group: cg,
		pm:    make(map[int32]PartitionMeta),
		cg:    zk.Consumergroup(cg),
	}
	err = k.RefreshMeta(topic)
	return
}

type PartitionMeta struct {
	Pid      int32
	Leader   int32
	Replicas []int32
	Isr      []int32
}

type PartitionInfo struct {
	PartitionMeta
	Offset int64
	Size   int64
	//highwatermark int64
}

type ResultEntry struct {
	PartitionInfo
	ConsumeRate    float64
	ProduceRate    float64
	ProducePercent int
	ConsumePercent int
}

type CanvasData struct {
	Brokers []int32
	Data    []ResultEntry
}

func diffPartionInfo(old, new PartitionInfo, dur time.Duration) ResultEntry {
	return ResultEntry{
		PartitionInfo: new,
		ConsumeRate:   float64(new.Offset-old.Offset) / dur.Seconds(), //TODO if offset = -1, sec -> ms
		ProduceRate:   float64(new.Size-old.Size) / dur.Seconds(),
	}
}

func diffPartionInfos(old, new map[int32]PartitionInfo, dur time.Duration) (res []ResultEntry) {
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

	pmax, cmax, pmin, cmin = pmax*rate_display_scale_max, cmax*rate_display_scale_max, pmin*rate_display_scale_min, cmin*rate_display_scale_min
	for i := range res {
		res[i].ConsumePercent = int((res[i].ConsumeRate - cmin) * 100 / (cmax - cmin))
		res[i].ProducePercent = int((res[i].ProduceRate - pmin) * 100 / (pmax - pmin))
	}

	return
}

func (k *KOTop) Brokers() []int32 {
	brokers := make([]int32, 0, 20)
	for _, b := range k.cli.Brokers() {
		brokers = append(brokers, b.ID())
	}

	sort.Slice(brokers, func(i, j int) bool {
		return brokers[i] < brokers[j]
	})

	return brokers
}

func (k *KOTop) Check(topic string) (data CanvasData, err error) {
	data.Brokers = k.Brokers()
	logsizes, err := k.LogSize(topic)
	if err != nil {
		return
	}

	offsets, get, err := k.OffsetFromKafka(topic)
	if err != nil {
		return
	}

	if !get {
		offsets, err = k.OffsetFromZK(topic)
		if err != nil {
			return
		}
	}

	now := time.Now()
	current := k.marshalResult(logsizes, offsets)
	if k.lastResult != nil {
		data.Data = diffPartionInfos(k.lastResult, current, now.Sub(k.lastCheck))
	} else {
		for _, info := range current {
			data.Data = append(data.Data, ResultEntry{
				PartitionInfo: info,
			})
		}
	}

	k.lastCheck = now
	k.lastResult = current
	return
}

func (k *KOTop) marshalResult(sizes, offs map[int32]int64) (results map[int32]PartitionInfo) {
	results = make(map[int32]PartitionInfo, len(k.ps))
	for _, pid := range k.ps {
		off, ok := offs[pid]
		if !ok {
			off = -1
		}

		s, ok := sizes[pid]
		if !ok {
			s = -1
		}
		results[pid] = PartitionInfo{
			PartitionMeta: k.pm[pid],
			Offset:        off,
			Size:          s,
		}
	}
	return
}

func (k *KOTop) LogSize(topic string) (offsets map[int32]int64, err error) {
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

func (k *KOTop) OffsetFromZK(topic string) (offsets map[int32]int64, err error) {
	offs, err := k.cg.FetchAllOffsets()
	if err != nil {
		return
	}

	offsets = offs[topic]
	return
}

func (k *KOTop) OffsetFromKafka(topic string) (offsets map[int32]int64, get bool, err error) {
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

func (k *KOTop) RefreshMeta(topic string) (err error) {
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

	err = k.RefreshPartitionMeta(topic)
	if err != nil {
		return
	}

	err = k.UpdateOffsetReq(topic)
	return
}

func (k *KOTop) RefreshPartitionMeta(topic string) (err error) {
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
		k.pm[pid] = PartitionMeta{
			Pid:      pid,
			Replicas: replicas,
			Isr:      isr,
			Leader:   b.ID(),
		}
	}
	return
}

func (k *KOTop) UpdateOffsetReq(topic string) (err error) {

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
