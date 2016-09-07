package main

import (
	"fmt"
	"strings"
	"encoding/json"
	"regexp"
	"time"

	"github.com/parnurzeal/gorequest"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	mesos_agent = kingpin.Arg("url", "mesos agent URL, eg.: 'http://localhost:5051'").Default("http://localhost:5051").String()
	timeout = kingpin.Flag("timeout", "timeout for HTTP requests").Default("10").Short('t').Int()
)

type Task struct {
	Statistics struct {
		MemUnevictableBytes interface{} `json:"mem_unevictable_bytes,omitempty"`
		MemTotalMemswBytes interface{} `json:"mem_total_memsw_bytes,omitempty"`
		MemCriticalPressureCounter interface{} `json:"mem_critical_pressure_counter,omitempty"`
		MemCacheBytes interface{} `json:"mem_cache_bytes,omitempty"`
		MemAnonBytes interface{} `json:"mem_anon_bytes,omitempty"`
		DiskUsedBytes interface{} `json:"disk_used_bytes,omitempty"`
		DiskLimitBytes interface{} `json:"disk_limit_bytes,omitempty"`
		CpusUserTimeSecs interface{} `json:"cpus_user_time_secs,omitempty"`
		CpusSystemTimeSecs interface{} `json:"cpus_system_time_secs,omitempty"`
		CpusLimit interface{} `json:"cpus_limit,omitempty"`
		MemFileBytes interface{} `json:"mem_file_bytes,omitempty"`
		MemLimitBytes interface{} `json:"mem_limit_bytes,omitempty"`
		MemLowPressureCounter interface{} `json:"mem_low_pressure_counter,omitempty"`
		MemMappedFileBytes interface{} `json:"mem_mapped_file_bytes,omitempty"`
		MemMediumPressureCounter interface{} `json:"mem_medium_pressure_counter,omitempty"`
		MemRssBytes interface{} `json:"mem_rss_bytes,omitempty"`
		MemSwapBytes interface{} `json:"mem_swap_bytes,omitempty"`
		MemTotalBytes interface{} `json:"mem_total_bytes,omitempty"`
	} `json:"statistics"`
	ExecutorId string `json:"executor_id"`
}

func getTasksData(mesos_agent string, timeout int) (string, error) {
	request := gorequest.New()
	resp, body, errs := request.Get(fmt.Sprintf("%s/monitor/statistics.json", mesos_agent)).Timeout(time.Duration(timeout) * time.Second).End()

	if errs != nil {
		errs_str := make([]string, 0)
		for _, e := range errs {
			errs_str = append(errs_str, fmt.Sprintf("%s", e))
		}
		return "", fmt.Errorf("%s", strings.Join(errs_str, ", "))
	}
	if resp.StatusCode == 200 {
		return body, nil
	} else {
		return "", fmt.Errorf("HTTP response code: %s", resp.Status)
	}
}

func flattenData(tasks []Task) map[string][]map[string]float64 {
	tasks_map := make(map[string][]map[string]float64)
	r := regexp.MustCompile(`(\S+)\.[0-9A-Fa-f-]{36}`)
	for _, task := range tasks {
		if matches := r.FindStringSubmatch(task.ExecutorId); matches != nil {
			a := make(map[string]float64)
			if task.Statistics.MemUnevictableBytes != nil {
				a["mem_unevictable_bytes"] = task.Statistics.MemUnevictableBytes.(float64)
			}
			if task.Statistics.MemTotalMemswBytes != nil {
				a["mem_total_memsw_bytes"] = task.Statistics.MemTotalMemswBytes.(float64)
			}
			if task.Statistics.MemCriticalPressureCounter != nil {
				a["mem_critical_pressure_counter"] = task.Statistics.MemCriticalPressureCounter.(float64)
			}
			if task.Statistics.MemCacheBytes != nil {
				a["mem_cache_bytes"] = task.Statistics.MemCacheBytes.(float64)
			}
			if task.Statistics.MemAnonBytes != nil {
				a["mem_anon_bytes"] = task.Statistics.MemAnonBytes.(float64)
			}
			if task.Statistics.DiskUsedBytes != nil {
				a["disk_used_bytes"] = task.Statistics.DiskUsedBytes.(float64)
			}
			if task.Statistics.DiskLimitBytes != nil {
				a["disk_limit_bytes"] = task.Statistics.DiskLimitBytes.(float64)
			}
			if task.Statistics.CpusUserTimeSecs != nil {
				a["cpus_user_time_secs"] = task.Statistics.CpusUserTimeSecs.(float64)
			}
			if task.Statistics.CpusSystemTimeSecs != nil {
				a["cpus_system_time_secs"] = task.Statistics.CpusSystemTimeSecs.(float64)
			}
			if task.Statistics.CpusLimit != nil {
				a["cpus_limit"] = task.Statistics.CpusLimit.(float64)
			}
			if task.Statistics.MemFileBytes != nil {
				a["mem_file_bytes"] = task.Statistics.MemFileBytes.(float64)
			}
			if task.Statistics.MemLimitBytes != nil {
				a["mem_limit_bytes"] = task.Statistics.MemLimitBytes.(float64)
			}
			if task.Statistics.MemLowPressureCounter != nil {
				a["mem_low_pressure_counter"] = task.Statistics.MemLowPressureCounter.(float64)
			}
			if task.Statistics.MemMappedFileBytes != nil {
				a["mem_mapped_file_bytes"] = task.Statistics.MemMappedFileBytes.(float64)
			}
			if task.Statistics.MemMediumPressureCounter != nil {
				a["mem_medium_pressure_counter"] = task.Statistics.MemMediumPressureCounter.(float64)
			}
			if task.Statistics.MemRssBytes != nil {
				a["mem_rss_bytes"] = task.Statistics.MemRssBytes.(float64)
			}
			if task.Statistics.MemSwapBytes != nil {
				a["mem_swap_bytes"] = task.Statistics.MemSwapBytes.(float64)
			}
			if task.Statistics.MemTotalBytes != nil {
				a["mem_total_bytes"] = task.Statistics.MemTotalBytes.(float64)
			}

			tasks_map[matches[1]] = append(tasks_map[matches[1]], a)
		}
	}
	return tasks_map
}

func parseTasks(data string) ([]Task, error) {
	var tasks []Task
	err := json.Unmarshal([]byte(data), &tasks)
	if err != nil {
		return tasks, fmt.Errorf("JSON parse failed")
	}
	return tasks, nil
}

func avgMetrics(data map[string][]map[string]float64) map[string]map[string]float64 {
	result := make(map[string]map[string]float64)
	for task_name, list := range data {
		lenght := float64(len(list))
		result[task_name] = make(map[string]float64)
		for _, task := range list {
			for key, value := range task {
				result[task_name][key] += value
			}
		}
		for key, value := range result[task_name] {
			result[task_name][key] = value / lenght
		}
		result[task_name]["instances"] = lenght
	}

	for task_name, _ := range result {
		if _, ok := result[task_name]["mem_rss_bytes"]; ok {
			if _, ok := result[task_name]["mem_limit_bytes"]; ok {
				if result[task_name]["mem_limit_bytes"] != 0 {
					result[task_name]["mem_perc"] = result[task_name]["mem_rss_bytes"] / result[task_name]["mem_limit_bytes"] * 100
				}
			}
		}

		if _, ok := result[task_name]["disk_used_bytes"]; ok {
			if _, ok := result[task_name]["disk_limit_bytes"]; ok {
				if result[task_name]["disk_limit_bytes"] != 0 {
					result[task_name]["disk_perc"] = result[task_name]["disk_used_bytes"] / result[task_name]["disk_limit_bytes"] * 100
				}
			}
		}
	}
	return result
}

func genLineProto(data map[string]map[string]float64) string {
	output := make([]string, 0)
	for task_name, value := range data {
		list := make([]string, 0)
		for key, value := range value {
			list = append(list, fmt.Sprintf("%s=%.3f", key, value))
		}
		line := fmt.Sprintf("mesos_tasks,task_name=%s %s", task_name, strings.Join(list, ","))
		output = append(output, line)
	}
	return strings.Join(output, "\n")
}

func main() {
	kingpin.Parse()

	data, err := getTasksData(*mesos_agent, *timeout)
	if err != nil {
		panic(err)
	}

	tasks, err := parseTasks(data)
	if err != nil {
		panic(err)
	}

	fmt.Println(genLineProto(avgMetrics(flattenData(tasks))))
}
