package kotop

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"

	ui "github.com/gizak/termui"
)

const (
	rateDisplayScaleMax = 1.2
	rateDisplayScaleMin = 0.8

	sparklineHeight  = 2
	sparklinesHeight = 5
	oneLineHeight    = 1
	barchartHeight   = 10

	barchartWidth   = 10
	sparklinesWidth = 2
	gaugeWidth      = 2
	oneColumnWidth  = 1
	canvasWidth     = 12

	defaultRingSize = 100
)

type FieldName string

const (
	FieldNamePid          FieldName = "pid"
	FieldNameOffset       FieldName = "offset"
	FieldNameSize         FieldName = "size"
	FieldNameOffsetLag    FieldName = "oslag"
	FieldNameHighWaterLag FieldName = "hwlag"
	FieldNameProduceSpeed FieldName = "produce_speed"
	FieldNameConsumeSpeed FieldName = "consume_speed"
	FieldNameLeader       FieldName = "leader"
)

type Canvas struct {
	sortby                          FieldName
	produceRate                     []*ui.Gauge
	consumeRate                     []*ui.Gauge
	pid, offset, size, hwlag, oslag []*ui.Par
	leader, replicas, isr           []*ui.Par
	leaderDistribute                *ui.BarChart
	consumeSpeed                    *ui.Sparklines
	produceSpeed                    *ui.Sparklines
	totalProduce, totalConsume      *ring
	page                            int
	data                            []resultEntry
	header, footer                  *ui.Row
}

func NewCanvas() *Canvas {
	return &Canvas{
		sortby:           FieldNamePid,
		leaderDistribute: newBarChart(),
		header:           newHeader(),
		footer:           newFooter(),
		produceSpeed:     newSparkLines(" " + string(FieldNameProduceSpeed) + " "),
		consumeSpeed:     newSparkLines(" " + string(FieldNameConsumeSpeed) + " "),
		totalConsume:     newRing(defaultRingSize),
		totalProduce:     newRing(defaultRingSize),
	}
}

func (c *Canvas) PageUp() {
	if c.page > 0 {
		c.page--
	}
}

func (c *Canvas) PageDown() {
	if c.pageSize()*(c.page+1) <= len(c.data) {
		c.page++
	}
}

func (c *Canvas) pageSize() int {
	return ui.TermHeight() - (barchartHeight + oneLineHeight*2)
}

func (c *Canvas) refresh() {
	n := c.pageSize()
	start := n * c.page
	end := n * (c.page + 1)
	if end > len(c.data) {
		end = len(c.data)
		start = end - n
		if start < 0 {
			start = 0
		}
	}

	display := c.data[start:end]
	c.fillData(display, n)
	return
}

func (c *Canvas) fillData(data []resultEntry, pagesize int) {

	w := ui.TermWidth()
	c.leaderDistribute.BarWidth = (w / canvasWidth * barchartWidth / len(c.leaderDistribute.Data)) - 1

	c.pid = newParList(pagesize)
	c.offset = newParList(pagesize)
	c.size = newParList(pagesize)
	c.hwlag = newParList(pagesize)
	c.oslag = newParList(pagesize)
	c.leader = newParList(pagesize)
	c.replicas = newParList(pagesize)
	c.isr = newParList(pagesize)
	c.produceRate = newGaugeList(len(data))
	c.consumeRate = newGaugeList(len(data))

	var totalProduce, totalConsume float64
	for i, r := range data {
		c.pid[i].Text = strconv.Itoa(int(r.Pid))
		c.offset[i].Text = strconv.FormatInt(r.Offset, 10)
		c.size[i].Text = strconv.FormatInt(r.Size, 10)
		c.oslag[i].Text = strconv.FormatInt(r.Size-r.Offset, 10)
		c.hwlag[i].Text = strconv.FormatInt(r.Size-r.HighWatermark, 10)
		c.leader[i].Text = strconv.Itoa(int(r.Leader))
		c.replicas[i].Text = formatIntSlice(r.Replicas)
		c.isr[i].Text = formatIntSlice(r.Isr)
		c.produceRate[i].Percent = r.ProducePercent
		c.produceRate[i].Label = rateStr(r.ProduceRate)
		c.consumeRate[i].Percent = r.ConsumePercent
		c.consumeRate[i].Label = rateStr(r.ConsumeRate)
		totalProduce += r.ProduceRate
		totalConsume += r.ConsumeRate
	}
	c.totalProduce.add(int(totalProduce))
	c.totalConsume.add(int(totalConsume))

	c.produceSpeed.Lines[0].Data = c.totalProduce.dump(w / 6)
	c.produceSpeed.Lines[0].Title = rateStr(totalProduce)
	c.consumeSpeed.Lines[0].Data = c.totalConsume.dump(w / 6)
	c.consumeSpeed.Lines[0].Title = rateStr(totalConsume)
}

func (c *Canvas) LoadData(data canvasData) {

	leaders := make(map[int32]int, len(data.Brokers))
	for _, d := range data.Data {
		leaders[d.Leader]++
	}
	ld := make([]int, len(data.Brokers))
	label := make([]string, len(data.Brokers))
	for i, id := range data.Brokers {
		ld[i] = leaders[id]
		label[i] = "B" + strconv.Itoa(int(id))
	}
	c.leaderDistribute.Data = ld
	c.leaderDistribute.DataLabels = label

	c.data = data.Data
}

func (c *Canvas) SetSort(field FieldName) {
	c.sortby = field
}

func (c *Canvas) sort() {
	switch c.sortby {
	case FieldNamePid:
		sort.Slice(c.data, func(i, j int) bool {
			return c.data[i].Pid < c.data[j].Pid
		})
	case FieldNameOffset:
		sort.Slice(c.data, func(i, j int) bool {
			return c.data[i].Offset < c.data[j].Offset
		})
	case FieldNameSize:
		sort.Slice(c.data, func(i, j int) bool {
			return c.data[i].Size < c.data[j].Size
		})
	case FieldNameOffsetLag:
		sort.Slice(c.data, func(i, j int) bool {
			return (c.data[i].Size - c.data[i].Offset) < (c.data[j].Size - c.data[j].Offset)
		})
	case FieldNameHighWaterLag:
		sort.Slice(c.data, func(i, j int) bool {
			return (c.data[i].Size - c.data[i].HighWatermark) < (c.data[j].Size - c.data[j].HighWatermark)
		})
	case FieldNameProduceSpeed:
		sort.Slice(c.data, func(i, j int) bool {
			return c.data[i].ProduceRate < c.data[j].ProduceRate
		})
	case FieldNameConsumeSpeed:
		sort.Slice(c.data, func(i, j int) bool {
			return c.data[i].ConsumeRate < c.data[j].ConsumeRate
		})
	case FieldNameLeader:
		sort.Slice(c.data, func(i, j int) bool {
			return c.data[i].Leader < c.data[j].Leader
		})
	}
}

func gauge2GridBufferSlice(s []*ui.Gauge) []ui.GridBufferer {
	res := make([]ui.GridBufferer, len(s))
	for i := range res {
		res[i] = s[i]
	}
	return res
}

func par2GridBufferSlice(s []*ui.Par) []ui.GridBufferer {
	res := make([]ui.GridBufferer, len(s))
	for i := range res {
		res[i] = s[i]
	}
	return res
}

func (c *Canvas) Render() {
	c.sort()
	c.refresh()

	ui.Body.Rows = nil
	ui.Body.AddRows(
		ui.NewRow(
			ui.NewCol(barchartWidth, 0, c.leaderDistribute),
			ui.NewCol(sparklinesWidth, 0, c.produceSpeed, c.consumeSpeed),
		),
		c.header,
		ui.NewRow(
			ui.NewCol(oneColumnWidth, 0, par2GridBufferSlice(c.pid)...),
			ui.NewCol(oneColumnWidth, 0, par2GridBufferSlice(c.size)...),
			ui.NewCol(oneColumnWidth, 0, par2GridBufferSlice(c.hwlag)...),
			ui.NewCol(oneColumnWidth, 0, par2GridBufferSlice(c.offset)...),
			ui.NewCol(oneColumnWidth, 0, par2GridBufferSlice(c.oslag)...),
			ui.NewCol(gaugeWidth, 0, gauge2GridBufferSlice(c.produceRate)...),
			ui.NewCol(gaugeWidth, 0, gauge2GridBufferSlice(c.consumeRate)...),
			ui.NewCol(oneColumnWidth, 0, par2GridBufferSlice(c.leader)...),
			ui.NewCol(oneColumnWidth, 0, par2GridBufferSlice(c.replicas)...),
			ui.NewCol(oneColumnWidth, 0, par2GridBufferSlice(c.isr)...),
		),
		c.footer,
	)
	ui.Body.Align()
	ui.Render(ui.Body)
}

// ----- init widgets func

var headerText = []string{"pid", "size", "high watermark lag", "offset", "offset lag", "size increment speed", "consume speed", "leader", "replicas", "isr"}

func footerText() string {
	buf := bytes.NewBufferString("[q] quit, [up] page up, [down] page down, sort by ")
	for i, h := range headerText {
		if i == 8 {
			break
		}
		buf.WriteString(fmt.Sprintf("[%d] %s ", i+1, h))
	}
	return buf.String()
}

func newFooter() *ui.Row {
	p := ui.NewPar(footerText())
	p.TextFgColor = ui.ColorWhite
	p.TextBgColor = ui.ColorCyan
	p.Bg = ui.ColorCyan
	p.Border = false
	p.Height = oneLineHeight

	return ui.NewRow(
		ui.NewCol(canvasWidth, 0, p),
	)
}

func newSparkLines(label string) *ui.Sparklines {
	sl := ui.NewSparkline()
	sl.LineColor = ui.ColorGreen
	sl.Height = sparklineHeight
	sls := ui.NewSparklines(sl)
	sls.Height = sparklinesHeight
	sls.BorderFg = ui.ColorCyan
	sls.BorderLabel = label
	sls.BorderLeft = false
	sls.BorderRight = false
	sls.BorderBottom = false
	return sls
}

func newHeader() *ui.Row {
	header := make([]*ui.Par, len(headerText))
	for i := range header {
		p := ui.NewPar(headerText[i])
		p.TextFgColor = ui.ColorWhite
		p.TextBgColor = ui.ColorCyan
		p.Bg = ui.ColorCyan
		p.Border = false
		p.BorderLeft = false
		p.BorderRight = false
		p.BorderTop = false
		p.BorderBottom = false
		p.Height = oneLineHeight
		header[i] = p
	}

	return ui.NewRow(
		ui.NewCol(oneColumnWidth, 0, header[0]),
		ui.NewCol(oneColumnWidth, 0, header[1]),
		ui.NewCol(oneColumnWidth, 0, header[2]),
		ui.NewCol(oneColumnWidth, 0, header[3]),
		ui.NewCol(oneColumnWidth, 0, header[4]),
		ui.NewCol(gaugeWidth, 0, header[5]),
		ui.NewCol(gaugeWidth, 0, header[6]),
		ui.NewCol(oneColumnWidth, 0, header[7]),
		ui.NewCol(oneColumnWidth, 0, header[8]),
		ui.NewCol(oneColumnWidth, 0, header[9]),
	)
}

func newBarChart() *ui.BarChart {
	bc := ui.NewBarChart()
	bc.BorderLabel = " Leader Distributition "
	bc.Height = barchartHeight
	bc.BorderLeft = false
	bc.BorderRight = false
	bc.BorderBottom = false
	bc.TextColor = ui.ColorGreen
	bc.BarColor = ui.ColorRed
	bc.NumColor = ui.ColorYellow
	return bc
}

func newParList(size int) []*ui.Par {
	pars := make([]*ui.Par, size)
	for i := range pars {
		p := ui.NewPar("")
		p.TextFgColor = ui.ColorWhite
		p.BorderFg = ui.ColorCyan
		p.Border = false
		p.BorderLeft = false
		p.BorderRight = false
		p.BorderTop = false
		p.BorderBottom = false
		p.Height = oneLineHeight
		pars[i] = p
	}
	return pars
}

func newGaugeList(size int) (gs []*ui.Gauge) {
	for i := 0; i < size; i++ {
		g := ui.NewGauge()
		g.BarColor = ui.ColorGreen
		g.Border = false
		g.BorderTop = false
		g.BorderBottom = false
		g.Height = oneLineHeight
		g.LabelAlign = ui.AlignCenter
		gs = append(gs, g)
	}
	return
}

// ----- helper func

func formatIntSlice(s []int32) (str string) {
	for i, ss := range s {
		if i != 0 {
			str += ", "
		}
		str += strconv.Itoa(int(ss))
	}
	return
}

func rateStr(r float64) string {
	return fmt.Sprintf("%.1f/s", r)
}
