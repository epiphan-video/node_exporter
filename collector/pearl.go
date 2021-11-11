// Copyright 2021 Epiphan Video Inc. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package collector

import (
	"fmt"

	"bufio"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/soundcloud/go-runit/runit"

	"github.com/go-kit/log"
)

var (
	knownMetrics = []struct {
		StatusKey      string
		MetricName     string
		Description    string
		NumberOfParams int
		Multiplier     int
	}{
		{"VmRSS", "service_mem_vmrss", "Virtual memory resident set size in bytes", 2, 1024},
		{"Threads", "service_threads", "Number of threads", 1, 1},
		{"voluntary_ctxt_switches", "service_voluntary_ctxt_switches", "Number of voluntary context switches", 1, 1},
		{"nonvoluntary_ctxt_switches", "service_nonvoluntary_ctxt_switches", "Number of nonvoluntary context switches", 1, 1},
	}
)

func init() {
	registerCollector("pearl", defaultEnabled, NewPearlCollector)
}

type pearlCollector struct {
	logger log.Logger
}

func NewPearlCollector(logger log.Logger) (Collector, error) {
	return &pearlCollector{
		logger: logger,
	}, nil
}

func (c *pearlCollector) Update(ch chan<- prometheus.Metric) error {
	services, err := getAllServices()
	if err != nil {
		return err
	}

	for _, svc := range services {
		labels := svc.GetLabels()
		metrics, err := svc.GetStatusFileMetrics()
		if err != nil {
			c.logger.Log("msg", "failed to get metrics for service", "service", svc.Name, "err", err)
			continue
		}

		labelKeys := make([]string, 0, len(labels))
		labelValues := make([]string, 0, len(labels))
		for k, v := range labels {
			labelKeys = append(labelKeys, k)
			labelValues = append(labelValues, v)
		}

		for _, metric := range knownMetrics {
			if value, ok := metrics[metric.MetricName]; ok {
				ch <- prometheus.MustNewConstMetric(
					prometheus.NewDesc(
						metric.MetricName,
						metric.Description,
						labelKeys,
						nil),
					prometheus.GaugeValue,
					float64(value),
					labelValues...,
				)
			}
		}
	}

	return nil
}

func isServiceDir(path string) (bool, error) {
	if _, err := os.Stat(fmt.Sprintf(path + "/supervise")); err == nil {
		return true, nil
	} else if os.IsNotExist(err) {
		return false, nil
	} else {
		return false, err
	}
}

type RunitService struct {
	Name string
	Path string
}

func (rs *RunitService) Status() (*runit.SvStatus, error) {
	svc := runit.GetService(rs.Name, rs.Path)
	return svc.Status()
}

func (rs *RunitService) getChannelID() string {
	file, err := os.OpenFile(fmt.Sprintf("%v/%v/channel_id", rs.Path, rs.Name), os.O_RDONLY, 0)
	if err != nil {
		return ""
	}
	defer file.Close()

	buf := make([]byte, 8)
	n, err := file.Read(buf)
	if err != nil {
		return ""
	}

	return strings.TrimSpace(string(buf[:n]))
}

func (rs *RunitService) GetStatusFileMetrics() (map[string]int, error) {
	status, err := rs.Status()
	if err != nil {
		return nil, err
	}

	return getMetricsFromStatusFile(status.Pid)
}

func strippedName(name string) string {
	if strings.Contains(name, ".") {
		parts := strings.Split(name, ".")
		return parts[len(parts)-1]
	}

	return name
}

func (rs *RunitService) GetLabels() map[string]string {
	labels := make(map[string]string)
	labels["service"] = strippedName(rs.Name)

	if channelID := rs.getChannelID(); channelID != "" {
		labels["channel_id"] = channelID
	}

	return labels
}

func getMetricsFromStatusFile(pid int) (map[string]int, error) {
	file := fmt.Sprintf("/proc/%d/status", pid)
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	metrics := make(map[string]int)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()

		for _, metric := range knownMetrics {
			if strings.HasPrefix(line, metric.StatusKey) {
				parts := strings.Fields(line)
				if len(parts) != metric.NumberOfParams+1 {
					return nil, fmt.Errorf("invalid number of params for %s: '%s'", metric.StatusKey, line)
				}

				value, err := strconv.Atoi(parts[1])
				if err != nil {
					return nil, fmt.Errorf("invalid value for %s: '%s'", metric.StatusKey, parts[1])
				}

				metrics[metric.MetricName] = value * metric.Multiplier
			}
		}
	}

	return metrics, nil

}

func getServicesInDir(dir string) ([]RunitService, error) {
	var services []RunitService

	walkFn := func(path string, dir os.DirEntry, err error) error {
		if s, _ := isServiceDir(path); s {
			svc := RunitService{
				Name: filepath.Base(path),
				Path: filepath.Dir(path),
			}
			services = append(services, svc)
		}
		if err != nil {
			return err
		}

		return nil
	}

	if err := filepath.WalkDir(dir, walkFn); err != nil {
		return services, err
	}

	return services, nil
}

func getAllServices() ([]RunitService, error) {
	var services []RunitService
	svcRoots := []string{"/service/", "/tmp/service/"}
	for _, root := range svcRoots {
		d, err := getServicesInDir(root)
		if err != nil {
			fmt.Println(err)
			continue
		}
		services = append(services, d...)
	}

	return services, nil
}
