// Copyright 2018 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package opencensus

import (
	"fmt"
	"log"
	"time"

	info "github.com/google/cadvisor/info/v1"
	"github.com/google/cadvisor/storage"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
)

var (
	containerNameKey = newKey("container_name")
	containerIDKey   = newKey("container_id")

	cpuTypeKey = newKey("cpu_type")
	memTypeKey = newKey("mem_type")

	cpuLoadAverageView = newView("cpu_load_average", "Smoothed average of number of runnable threads x 1000.", stats.MeanAggregation{}, stats.Cumulative{})
	cpuUsageView       = newView("cpu_usage_total", "Total CPU usage in nanoseconds", stats.CountAggregation{}, stats.Cumulative{}, cpuTypeKey)
	memView            = newView("mem_view", "Memeory stats", stats.CountAggregation{}, stats.Cumulative{}, memTypeKey)
)

func init() {
	storage.RegisterStorageDriver("opencensus", newStorage)
}

func newStorage() (storage.StorageDriver, error) {
	// TODO(jbd): Read from configuration file.
	return &opencensus{}, nil
}

type opencensus struct {
	e stats.Exporter
}

func (o *opencensus) AddStats(ref info.ContainerReference, s *info.ContainerStats) error {
	vd := &stats.ViewData{}
	o.e.Export(vd)

	now := time.Now()

	tags := []tag.Tag{
		{Key: containerNameKey, Value: ref.Namespace + "/" + ref.Name},
		{Key: containerIDKey, Value: ref.Id},
	}

	// TODO(jbd): Handle dynamic labels.

	o.e.Export(&stats.ViewData{
		View:  cpuLoadAverageView,
		Start: s.Timestamp,
		End:   now,
		Rows: []*stats.Row{{
			Tags: tags,
			Data: &stats.MeanData{
				Mean:  float64(s.Cpu.LoadAverage),
				Count: 1,
			},
		}},
	})

	total := stats.CountData(int64(s.Cpu.Usage.Total))
	o.e.Export(&stats.ViewData{
		View:  cpuUsageView,
		Start: s.Timestamp,
		End:   now,
		Rows: []*stats.Row{{
			Tags: append(tags, tag.Tag{Key: cpuTypeKey, Value: "total"}),
			Data: &total,
		}},
	})

	sys := stats.CountData(int64(s.Cpu.Usage.System))
	o.e.Export(&stats.ViewData{
		View:  cpuUsageView,
		Start: s.Timestamp,
		End:   now,
		Rows: []*stats.Row{{
			Tags: append(tags, tag.Tag{Key: cpuTypeKey, Value: "system"}),
			Data: &sys,
		}},
	})

	user := stats.CountData(int64(s.Cpu.Usage.User))
	o.e.Export(&stats.ViewData{
		View:  cpuUsageView,
		Start: s.Timestamp,
		End:   now,
		Rows: []*stats.Row{{
			Tags: append(tags, tag.Tag{Key: cpuTypeKey, Value: "user"}),
			Data: &user,
		}},
	})

	for i, c := range s.Cpu.Usage.PerCpu {
		data := stats.CountData(int64(c))
		o.e.Export(&stats.ViewData{
			View:  cpuUsageView,
			Start: s.Timestamp,
			End:   now,
			Rows: []*stats.Row{{
				Tags: append(tags, tag.Tag{Key: cpuTypeKey, Value: fmt.Sprintf("cpu%d", i)}),
				Data: &data,
			}},
		})
	}

	memUsage := stats.CountData(int64(s.Memory.Usage))
	o.e.Export(&stats.ViewData{
		View:  memView,
		Start: s.Timestamp,
		End:   now,
		Rows: []*stats.Row{{
			Tags: append(tags, tag.Tag{Key: memTypeKey, Value: "usage"}),
			Data: &memUsage,
		}},
	})

	maxMemUsage := stats.CountData(int64(s.Memory.MaxUsage))
	o.e.Export(&stats.ViewData{
		View:  memView,
		Start: s.Timestamp,
		End:   now,
		Rows: []*stats.Row{{
			Tags: append(tags, tag.Tag{Key: memTypeKey, Value: "max_usage"}),
			Data: &maxMemUsage,
		}},
	})

	cacheMemUsage := stats.CountData(int64(s.Memory.Cache))
	o.e.Export(&stats.ViewData{
		View:  memView,
		Start: s.Timestamp,
		End:   now,
		Rows: []*stats.Row{{
			Tags: append(tags, tag.Tag{Key: memTypeKey, Value: "cache"}),
			Data: &cacheMemUsage,
		}},
	})

	swapMemUsage := stats.CountData(int64(s.Memory.Swap))
	o.e.Export(&stats.ViewData{
		View:  memView,
		Start: s.Timestamp,
		End:   now,
		Rows: []*stats.Row{{
			Tags: append(tags, tag.Tag{Key: memTypeKey, Value: "swap"}),
			Data: &swapMemUsage,
		}},
	})

	// Add RSS, working set.
	return nil
}

func (o *opencensus) Close() error {
	return nil
}

func newView(name string, desc string, agg stats.Aggregation, w stats.Window, k ...tag.Key) *stats.View {
	keys := []tag.Key{containerNameKey, containerIDKey}
	keys = append(keys, k...)
	view, err := stats.NewView("cadvisor/"+name, desc, keys, nil, agg, w)
	if err != nil {
		log.Printf("Cannot initialize OpenCensus view: %v", err)
	}
	return view
}

func newKey(name string) tag.Key {
	k, err := tag.NewKey(name)
	if err != nil {
		log.Printf("Cannot create OpenCensus key: %v", err)
	}
	return k
}
