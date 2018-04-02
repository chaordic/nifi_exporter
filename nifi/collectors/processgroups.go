package collectors

import (
	"strconv"
	"container/list"
	"github.com/chaordic/nifi_exporter/nifi/client"
	"github.com/prometheus/client_golang/prometheus"
)

const rootProcessGroupID = "root"

type ProcessGroupsCollector struct {
	api *client.Client
	maxDeep int

	bulletin5mCount *prometheus.Desc
	componentCount  *prometheus.Desc

	inFlowFiles5mCount          *prometheus.Desc
	inBytes5mCount              *prometheus.Desc
	queuedFlowFilesCount        *prometheus.Desc
	queuedBytes                 *prometheus.Desc
	readBytes5mCount            *prometheus.Desc
	writtenBytes5mCount         *prometheus.Desc
	outFlowFiles5mCount         *prometheus.Desc
	outBytes5mCount             *prometheus.Desc
	transferredFlowFiles5mCount *prometheus.Desc
	transferredBytes5mCount     *prometheus.Desc
	receivedBytes5mCount        *prometheus.Desc
	receivedFlowFiles5mCount    *prometheus.Desc
	sentBytes5mCount            *prometheus.Desc
	sentFlowFiles5mCount        *prometheus.Desc
	activeThreadCount           *prometheus.Desc
}

func NewProcessGroupsCollector(api *client.Client, labels map[string]string, maxDeep int) *ProcessGroupsCollector {

	// Just to avoid overkill configurations
	if(maxDeep > 5) {
		maxDeep = 5;
	}

	prefix := MetricNamePrefix + "pg_"
	statLabels := []string{"node_id", "group"}
	return &ProcessGroupsCollector{
		api: api,

		maxDeep: maxDeep,

		bulletin5mCount: prometheus.NewDesc(
			prefix+"bulletin_5m_count",
			"Number of bulletins posted during last 5 minutes.",
			[]string{"group", "level"},
			labels,
		),
		componentCount: prometheus.NewDesc(
			prefix+"component_count",
			"The number of components in this process group.",
			[]string{"group", "status"},
			labels,
		),

		inFlowFiles5mCount: prometheus.NewDesc(
			prefix+"in_flow_files_5m_count",
			"The number of FlowFiles that have come into this ProcessGroup in the last 5 minutes",
			statLabels,
			labels,
		),
		inBytes5mCount: prometheus.NewDesc(
			prefix+"in_bytes_5m_count",
			"The number of bytes that have come into this ProcessGroup in the last 5 minutes",
			statLabels,
			labels,
		),
		queuedFlowFilesCount: prometheus.NewDesc(
			prefix+"queued_flow_files_count",
			"The number of FlowFiles that are queued up in this ProcessGroup right now",
			statLabels,
			labels,
		),
		queuedBytes: prometheus.NewDesc(
			prefix+"queued_bytes",
			"The number of bytes that are queued up in this ProcessGroup right now",
			statLabels,
			labels,
		),
		readBytes5mCount: prometheus.NewDesc(
			prefix+"read_bytes_5m_count",
			"The number of bytes read by components in this ProcessGroup in the last 5 minutes",
			statLabels,
			labels,
		),
		writtenBytes5mCount: prometheus.NewDesc(
			prefix+"written_bytes_5m_count",
			"The number of bytes written by components in this ProcessGroup in the last 5 minutes",
			statLabels,
			labels,
		),
		outFlowFiles5mCount: prometheus.NewDesc(
			prefix+"out_flow_files_5m_count",
			"The number of FlowFiles transferred out of this ProcessGroup in the last 5 minutes",
			statLabels,
			labels,
		),
		outBytes5mCount: prometheus.NewDesc(
			prefix+"out_bytes_5m_count",
			"The number of bytes transferred out of this ProcessGroup in the last 5 minutes",
			statLabels,
			labels,
		),
		transferredFlowFiles5mCount: prometheus.NewDesc(
			prefix+"transferred_flow_files_5m_count",
			"The number of FlowFiles transferred in this ProcessGroup in the last 5 minutes",
			statLabels,
			labels,
		),
		transferredBytes5mCount: prometheus.NewDesc(
			prefix+"transferred_bytes_5m_count",
			"The number of bytes transferred in this ProcessGroup in the last 5 minutes",
			statLabels,
			labels,
		),
		receivedBytes5mCount: prometheus.NewDesc(
			prefix+"received_bytes_5m_count",
			"The number of bytes received from external sources by components within this ProcessGroup in the last 5 minutes",
			statLabels,
			labels,
		),
		receivedFlowFiles5mCount: prometheus.NewDesc(
			prefix+"received_flow_files_5m_count",
			"The number of FlowFiles received from external sources by components within this ProcessGroup in the last 5 minutes",
			statLabels,
			labels,
		),
		sentBytes5mCount: prometheus.NewDesc(
			prefix+"sent_bytes_5m_count",
			"The number of bytes sent to an external sink by components within this ProcessGroup in the last 5 minutes",
			statLabels,
			labels,
		),
		sentFlowFiles5mCount: prometheus.NewDesc(
			prefix+"sent_flow_files_5m_count",
			"The number of FlowFiles sent to an external sink by components within this ProcessGroup in the last 5 minutes",
			statLabels,
			labels,
		),
		activeThreadCount: prometheus.NewDesc(
			prefix+"active_thread_count",
			"The active thread count for this process group.",
			statLabels,
			labels,
		),
	}
}

func (c *ProcessGroupsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.bulletin5mCount
	ch <- c.componentCount

	ch <- c.inFlowFiles5mCount
	ch <- c.inBytes5mCount
	ch <- c.queuedFlowFilesCount
	ch <- c.queuedBytes
	ch <- c.readBytes5mCount
	ch <- c.writtenBytes5mCount
	ch <- c.outFlowFiles5mCount
	ch <- c.outBytes5mCount
	ch <- c.transferredFlowFiles5mCount
	ch <- c.transferredBytes5mCount
	ch <- c.receivedBytes5mCount
	ch <- c.receivedFlowFiles5mCount
	ch <- c.sentBytes5mCount
	ch <- c.sentFlowFiles5mCount
	ch <- c.activeThreadCount
}

func (c *ProcessGroupsCollector) Collect(ch chan<- prometheus.Metric) {
	/*
		 Basic algorithm explanation:

		 We can't collect metrics following the graph
		 		"root" > "child 1" > "grandchild 1" > "child 2" (error here since we changed one level back) > ...)
		 To work properly we must collect entities metrics "level by level"
		 	  "root" > "child 1"..."child n" > "grandchild 1"..."grandchild n" > ...
	 */

	// First build the graph including all entities
	var data = make([]*list.List, c.maxDeep);
	deepCollect(ch, c, rootProcessGroupID, 0, data)

	// Now we call collect metris level by level
	level := 1;
	for i := range data {
		item := 1;
		for e := data[i].Front(); e != nil; e = e.Next() {
			// Must pass a prefix to avoid repeated metrics
			c.collect(ch, e.Value.(*client.ProcessGroupEntity),  "[" + strconv.Itoa(level) + "][" + strconv.Itoa(item) + "] - ")
			item++
		}
		level++
	}
}

func (c *ProcessGroupsCollector) collect(ch chan<- prometheus.Metric, entity *client.ProcessGroupEntity, prefix string) {
	bulletinCount := map[string]int{
		"INFO":    0,
		"WARNING": 0,
		"ERROR":   0,
	}
	for i := range entity.Bulletins {
		bulletinCount[entity.Bulletins[i].Bulletin.Level]++
	}

	// Use this "hack" to avoid duplicated metric errors
	// since different Nifi Process Groups may have the same name
	prefixedCompName := 	prefix + entity.Component.Name

	for level, count := range bulletinCount {
		ch <- prometheus.MustNewConstMetric(
			c.bulletin5mCount,
			prometheus.GaugeValue,
			float64(count),
			prefixedCompName,
			level,
		)
	}

	nodes := make(map[string]*client.ProcessGroupStatusSnapshotDTO)
	if len(entity.Status.NodeSnapshots) > 0 {
		for i := range entity.Status.NodeSnapshots {
			snapshot := &entity.Status.NodeSnapshots[i]
			nodes[snapshot.NodeID] = &snapshot.StatusSnapshot
		}
	} else if entity.Status.AggregateSnapshot != nil {
		nodes[AggregateNodeID] = entity.Status.AggregateSnapshot
	}

	ch <- prometheus.MustNewConstMetric(
		c.componentCount,
		prometheus.GaugeValue,
		float64(entity.RunningCount),
		prefixedCompName,
		"running",
	)
	ch <- prometheus.MustNewConstMetric(
		c.componentCount,
		prometheus.GaugeValue,
		float64(entity.StoppedCount),
		prefixedCompName,
		"stopped",
	)
	ch <- prometheus.MustNewConstMetric(
		c.componentCount,
		prometheus.GaugeValue,
		float64(entity.InvalidCount),
		prefixedCompName,
		"invalid",
	)
	ch <- prometheus.MustNewConstMetric(
		c.componentCount,
		prometheus.GaugeValue,
		float64(entity.DisabledCount),
		prefixedCompName,
		"disabled",
	)

	for nodeID, snapshot := range nodes {

		// Use this "hack" to avoid duplicated metric errors
		// since different Nifi Process Groups may have the same name
		prefixedSnapshotName := prefix + snapshot.Name

		ch <- prometheus.MustNewConstMetric(
			c.inFlowFiles5mCount,
			prometheus.GaugeValue,
			float64(snapshot.FlowFilesIn),
			nodeID,
			prefixedSnapshotName,
		)
		ch <- prometheus.MustNewConstMetric(
			c.inBytes5mCount,
			prometheus.GaugeValue,
			float64(snapshot.BytesIn),
			nodeID,
			prefixedSnapshotName,
		)
		ch <- prometheus.MustNewConstMetric(
			c.queuedFlowFilesCount,
			prometheus.GaugeValue,
			float64(snapshot.FlowFilesQueued),
			nodeID,
			prefixedSnapshotName,
		)
		ch <- prometheus.MustNewConstMetric(
			c.queuedBytes,
			prometheus.GaugeValue,
			float64(snapshot.BytesQueued),
			nodeID,
			prefixedSnapshotName,
		)
		ch <- prometheus.MustNewConstMetric(
			c.readBytes5mCount,
			prometheus.GaugeValue,
			float64(snapshot.BytesRead),
			nodeID,
			prefixedSnapshotName,
		)
		ch <- prometheus.MustNewConstMetric(
			c.writtenBytes5mCount,
			prometheus.GaugeValue,
			float64(snapshot.BytesWritten),
			nodeID,
			prefixedSnapshotName,
		)
		ch <- prometheus.MustNewConstMetric(
			c.outFlowFiles5mCount,
			prometheus.GaugeValue,
			float64(snapshot.FlowFilesOut),
			nodeID,
			prefixedSnapshotName,
		)
		ch <- prometheus.MustNewConstMetric(
			c.outBytes5mCount,
			prometheus.GaugeValue,
			float64(snapshot.BytesOut),
			nodeID,
			prefixedSnapshotName,
		)
		ch <- prometheus.MustNewConstMetric(
			c.transferredFlowFiles5mCount,
			prometheus.GaugeValue,
			float64(snapshot.FlowFilesTransferred),
			nodeID,
			prefixedSnapshotName,
		)
		ch <- prometheus.MustNewConstMetric(
			c.transferredBytes5mCount,
			prometheus.GaugeValue,
			float64(snapshot.BytesTransferred),
			nodeID,
			prefixedSnapshotName,
		)
		ch <- prometheus.MustNewConstMetric(
			c.receivedBytes5mCount,
			prometheus.GaugeValue,
			float64(snapshot.BytesReceived),
			nodeID,
			prefixedSnapshotName,
		)
		ch <- prometheus.MustNewConstMetric(
			c.receivedFlowFiles5mCount,
			prometheus.GaugeValue,
			float64(snapshot.FlowFilesReceived),
			nodeID,
			prefixedSnapshotName,
		)
		ch <- prometheus.MustNewConstMetric(
			c.sentBytes5mCount,
			prometheus.GaugeValue,
			float64(snapshot.BytesSent),
			nodeID,
			prefixedSnapshotName,
		)
		ch <- prometheus.MustNewConstMetric(
			c.sentFlowFiles5mCount,
			prometheus.GaugeValue,
			float64(snapshot.FlowFilesSent),
			nodeID,
			prefixedSnapshotName,
		)
		ch <- prometheus.MustNewConstMetric(
			c.activeThreadCount,
			prometheus.GaugeValue,
			float64(snapshot.ActiveThreadCount),
			nodeID,
			prefixedSnapshotName,
		)
	}
}

func deepCollect(ch chan<- prometheus.Metric, c *ProcessGroupsCollector, parent string, deep int, data []*list.List) {
	// Check stop condition
	if deep < len(data) {
		entities, err := c.api.GetProcessGroups(parent)
		if err != nil {
			ch <- prometheus.NewInvalidMetric(c.componentCount, err)
			return;
		}

			// Deep level list
			if(data[deep] == nil) {
				data[deep] = list.New()
			}

			// Deep level entries
			for i := range entities {
					data[deep].PushBack(&entities[i])
					// Deel level loop
					deepCollect(ch, c, entities[i].Component.ID, deep + 1, data)
			}
	}
}
