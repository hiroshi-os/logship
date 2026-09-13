// Package assignor implements consumer-group partition assignment strategies.
package assignor

import "sort"

// RangeAssign is Kafka's range assignor, applied independently per topic.
// Members and partitions are sorted; each member gets a contiguous slice.
// The first (partitions % members) members receive one extra partition.
func RangeAssign(members []string, topicPartitions map[string][]int) map[string]map[string][]int {
	ms := append([]string(nil), members...)
	sort.Strings(ms)
	out := make(map[string]map[string][]int, len(ms))
	for _, m := range ms {
		out[m] = map[string][]int{}
	}
	if len(ms) == 0 {
		return out
	}
	topics := make([]string, 0, len(topicPartitions))
	for t := range topicPartitions {
		topics = append(topics, t)
	}
	sort.Strings(topics)
	for _, topic := range topics {
		parts := append([]int(nil), topicPartitions[topic]...)
		sort.Ints(parts)
		assignRange(ms, topic, parts, out)
	}
	return out
}

func assignRange(members []string, topic string, parts []int, out map[string]map[string][]int) {
	n := len(members)
	p := len(parts)
	if n == 0 || p == 0 {
		return
	}
	per := p / n
	extra := p % n
	idx := 0
	for i, m := range members {
		count := per
		if i < extra {
			count++
		}
		if count == 0 {
			continue
		}
		out[m][topic] = append(out[m][topic], parts[idx:idx+count]...)
		idx += count
	}
}
