package fastbrc

import "fmt"

type StationInt16 struct {
	Min   int16
	Max   int16
	Total int32
	N     int32
	Name  []byte
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

func (s *StationInt16) NewMeasurementNoBranch(m int16) {
	s.N += 1
	s.Total += int32(m)
	s.Min = min(m, s.Min)
	s.Max = max(m, s.Max)
}
