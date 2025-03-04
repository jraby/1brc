package brc

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"sort"
	"strconv"
	"strings"
)

type Station struct {
	Min   float64
	Max   float64
	Total float64
	N     int64
}

func (s *Station) NewMeasurement(m float64) {
	if s.N != 0 {
		if m < s.Min {
			s.Min = m
		}
		if m > s.Max {
			s.Max = m
		}
		s.Total += m
		s.N++
		return
	}

	s.Min = m
	s.Max = m
	s.Total = m
	s.N++
}

func NewStation(m float64) *Station {
	return &Station{
		Min:   m,
		Max:   m,
		Total: m,
		N:     1,
	}
}

func Baseline(input io.Reader) string {
	stations := make(map[string]*Station)
	scanner := bufio.NewScanner(input)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, ";")
		m, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			log.Fatal(err)
		}

		name := strings.TrimSpace(fields[0])
		station, ok := stations[name]
		if ok {
			station.NewMeasurement(m)
		} else {
			stations[name] = NewStation(m)
		}
	}

	keys := make([]string, 0, len(stations))
	for k := range stations {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	out := make([]string, 0, len(stations)+2)
	out = append(out, "{")
	for i, k := range keys {
		if i == len(keys)-1 {
			out = append(out, fmt.Sprintf("%s=%0.1f/%0.1f/%0.1f", k, stations[k].Min, stations[k].Total/float64(stations[k].N), stations[k].Max))
		} else {
			out = append(out, fmt.Sprintf("%s=%0.1f/%0.1f/%0.1f, ", k, stations[k].Min, stations[k].Total/float64(stations[k].N), stations[k].Max))
		}
	}
	out = append(out, "}")
	return strings.Join(out, "")
}
