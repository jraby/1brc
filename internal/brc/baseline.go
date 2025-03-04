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

type StationInt16 struct {
	Min   int16
	Max   int16
	Total int32
	N     int32
}

func num2str(i int16) string {
	u, d := i/10, i%10

	if i >= 0 {
		return fmt.Sprintf("%d.%d", u, d)
	}

	u = -u
	d = -d
	return fmt.Sprintf("-%d.%d", u, d)
}

func (s *StationInt16) FancyPrint() string {
	avg := float64(s.Total) / 10 / float64(s.N)
	return num2str(s.Min) + "/" + fmt.Sprintf("%.1f", avg) + "/" + num2str(s.Max)
}

func (s *StationInt16) NewMeasurement(m int16) {
	s.N += 1
	s.Total += int32(m)
	if m < s.Min {
		s.Min = m
	}
	if m > s.Max {
		s.Max = m
	}
}

func NewStationInt16(m int16) *StationInt16 {
	return &StationInt16{
		Min:   m,
		Max:   m,
		Total: int32(m),
		N:     1,
	}
}

type StationInt struct {
	Min   int32
	Max   int32
	Total int32
	N     int32
}

func (s *StationInt) NewMeasurement(m float64) {
	mFixed := int32(m * 10)
	s.N += 1
	s.Total += mFixed
	if mFixed < s.Min {
		s.Min = mFixed
	}
	if mFixed > s.Max {
		s.Max = mFixed
	}
}

func NewStationInt(m float64) *StationInt {
	mFixed := int32(m * 10)
	return &StationInt{
		Min:   mFixed,
		Max:   mFixed,
		Total: mFixed,
		N:     1,
	}
}

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

func (s *Station) NewMeasurement2(m float64) {
	s.N += 1
	s.Total += m
	if m < s.Min {
		s.Min = m
	}
	if m > s.Max {
		s.Max = m
	}
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
