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
		vmRss, err := svc.GetVMRSS()
		if err != nil {
			c.logger.Log("msg", "failed to get VMRSS", "service", svc.Name, "err", err)
			continue
		}

		labelKeys := make([]string, 0, len(labels))
		labelValues := make([]string, 0, len(labels))
		for k, v := range labels {
			labelKeys = append(labelKeys, k)
			labelValues = append(labelValues, v)
		}

		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(
				"service_mem_vmrss",
				"Virtual memory resident set size in bytes",
				labelKeys,
				nil),
			prometheus.GaugeValue,
			float64(vmRss),
			labelValues...,
		)
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

func (rs *RunitService) GetVMRSS() (int, error) {
	status, err := rs.Status()
	if err != nil {
		return 0, err
	}
	return getVMRSSFromStatusFile(status.Pid)
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

func getVMRSSFromStatusFile(pid int) (int, error) {
	file := fmt.Sprintf("/proc/%d/status", pid)
	f, err := os.Open(file)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	var vmRss int
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "VmRSS:") {
			fields := strings.Fields(line)
			if len(fields) != 3 {
				return 0, fmt.Errorf("invalid VmRSS line: %v", fields)
			}
			vmRss, err = strconv.Atoi(fields[1])
			if err != nil {
				return 0, nil
			}
			break
		}
	}
	return vmRss * 1024, nil
}

func getBasename(path string) string {
	return filepath.Base(path)
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
