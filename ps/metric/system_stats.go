package metric

import (
	"runtime"
	"time"

	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/mem"
	"github.com/shirou/gopsutil/net"

	"github.com/tiglabs/baudengine/proto/masterpb"
	"github.com/tiglabs/baudengine/util/log"
	"github.com/tiglabs/baudengine/util/routine"
)

// SystemMetric system metric gather
type SystemMetric struct {
	diskPath    string
	diskQuota   uint64
	lastTime    time.Time
	minInterval time.Duration

	// Memory
	MemoryTotal           uint64  `json:"memory_total,omitempty"`
	MemoryUsedRss         uint64  `json:"memory_used_rss,omitempty"`
	MemoryUsed            uint64  `json:"memory_used,omitempty"`
	MemoryFree            uint64  `json:"memory_free,omitempty"`
	MemoryUsedPercent     float64 `json:"memory_used_percent,omitempty"`
	SwapMemoryTotal       uint64  `json:"swap_memory_total,omitempty"`
	SwapMemoryUsed        uint64  `json:"swap_memory_used,omitempty"`
	SwapMemoryFree        uint64  `json:"swap_memory_free,omitempty"`
	SwapMemoryUsedPercent float64 `json:"swap_memory_used_percent,omitempty"`
	// CPU
	CPUCount    uint32  `json:"cpu_count,omitempty"`
	CPUProcRate float64 `json:"cpu_proc_rate,omitempty"`
	// Disk
	DiskTotal uint64 `json:"disk_total,omitempty"`
	DiskUsed  uint64 `json:"disk_used,omitempty"`
	DiskFree  uint64 `json:"disk_free,omitempty"`
	// Net
	netIoInBytes            uint64
	netIoOutBytes           uint64
	netIoInPackage          uint64
	netIoOutPackage         uint64
	NetIoInBytePerSec       uint64 `json:"net_io_in_flow_per_sec,omitempty"`
	NetIoOutBytePerSec      uint64 `json:"net_io_out_flow_per_sec,omitempty"`
	netTCPActiveOpens       uint64
	NetTCPConnections       uint32 `json:"net_tcp_connections,omitempty"`
	NetTCPActiveOpensPerSec uint64 `json:"net_tcp_active_opens_per_sec,omitempty"`
}

// NewSystemMetric create SystemMetric object
func NewSystemMetric(diskPath string, diskQuota uint64) *SystemMetric {
	return &SystemMetric{
		diskPath:    diskPath,
		diskQuota:   diskQuota,
		CPUCount:    uint32(runtime.NumCPU()),
		minInterval: 10 * time.Second,
	}
}

// Export gather system stats and export
func (s *SystemMetric) Export() (*masterpb.NodeSysStats, error) {
	stats := new(masterpb.NodeSysStats)
	if s.lastTime.IsZero() || time.Now().Sub(s.lastTime) >= s.minInterval {
		s.cpuMetric()
		s.memMetric()
		s.diskMetric()
		s.netMetric()
		s.lastTime = time.Now()
	}

	stats.MemoryTotal = s.MemoryTotal
	stats.MemoryUsedRss = s.MemoryUsedRss
	stats.MemoryUsed = s.MemoryUsed
	stats.MemoryFree = s.MemoryFree
	stats.SwapMemoryTotal = s.SwapMemoryTotal
	stats.SwapMemoryUsed = s.SwapMemoryUsed
	stats.SwapMemoryFree = s.SwapMemoryFree

	stats.CpuProcRate = s.CPUProcRate
	stats.CpuCount = s.CPUCount

	stats.DiskTotal = s.DiskTotal
	stats.DiskUsed = s.DiskUsed
	stats.DiskFree = s.DiskFree

	stats.NetIoInBytePerSec = s.NetIoInBytePerSec
	stats.NetIoOutBytePerSec = s.NetIoOutBytePerSec
	stats.NetTcpConnections = s.NetTCPConnections
	stats.NetTcpActiveOpensPerSec = s.NetTCPActiveOpensPerSec

	return stats, nil
}

func (s *SystemMetric) cpuMetric() {
	routine.RunWork("SystemMetric-CPU", func() error {
		cpuLoads, err := cpu.Percent(time.Second, false)
		if err != nil {
			log.Error("SystemMetric get cpu info error: %v", err)
			return err
		}

		var cpuPercent float64
		for _, load := range cpuLoads {
			cpuPercent += load
		}
		if len(cpuLoads) > 0 {
			s.CPUProcRate = cpuPercent / float64(len(cpuLoads))
		}

		return nil
	}, routine.LogPanic(false))
}

func (s *SystemMetric) memMetric() {
	routine.RunWork("SystemMetric-MEM", func() error {
		memoryStat, err := mem.VirtualMemory()
		if err != nil {
			log.Error("SystemMetric get memory info error: %v", err)
			return err
		}

		if memoryStat.Total > 0 && memoryStat.Used > 0 {
			s.MemoryUsed = memoryStat.Used
			s.MemoryFree = memoryStat.Free
			s.MemoryTotal = memoryStat.Total
			s.MemoryUsedPercent = memoryStat.UsedPercent
		}

		swapStat, err := mem.SwapMemory()
		if err != nil {
			log.Error("SystemMetric get swap info error: %v", err)
			return err
		}

		s.SwapMemoryTotal = swapStat.Total
		s.SwapMemoryUsed = swapStat.Used
		s.SwapMemoryFree = swapStat.Free
		s.SwapMemoryUsedPercent = swapStat.UsedPercent

		return nil
	}, routine.LogPanic(false))
}

func (s *SystemMetric) netMetric() {
	routine.RunWork("SystemMetric-NET", func() error {
		ioStat, err := net.IOCounters(false)
		if err != nil {
			log.Error("SystemMetric get net info error: %v", err)
			return err
		}
		if len(ioStat) == 0 {
			log.Error("invalid net IO stat")
			return nil
		}

		tcpProto, err := net.ProtoCounters([]string{"tcp"})
		if err != nil {
			log.Error("SystemMetric get net proto cpunter failed, err[%v]", err)
			return err
		}

		netIoInBytes := ioStat[0].BytesRecv
		netIoOutBytes := ioStat[0].BytesSent
		netIoInPackage := ioStat[0].PacketsRecv
		netIoOutPackage := ioStat[0].PacketsSent
		tcpConnections := uint64(tcpProto[0].Stats["CurrEstab"])
		tcpActiveOpens := uint64(tcpProto[0].Stats["ActiveOpens"])
		if !s.lastTime.IsZero() {
			s.NetIoInBytePerSec = uint64(float64(netIoInBytes-s.netIoInBytes) / time.Since(s.lastTime).Seconds())
			s.NetIoOutBytePerSec = uint64(float64(netIoOutBytes-s.netIoOutBytes) / time.Since(s.lastTime).Seconds())
			s.NetTCPActiveOpensPerSec = uint64(float64(tcpActiveOpens-s.netTCPActiveOpens) / time.Since(s.lastTime).Seconds())
		}
		s.NetTCPConnections = uint32(tcpConnections)
		s.netIoInBytes = netIoInBytes
		s.netIoOutBytes = netIoOutBytes
		s.netIoInPackage = netIoInPackage
		s.netIoOutPackage = netIoOutPackage
		s.netTCPActiveOpens = tcpActiveOpens
		return nil
	}, routine.LogPanic(false))
}

func (s *SystemMetric) diskMetric() {
	routine.RunWork("SystemMetric-DISK", func() error {
		diskStat, err := disk.Usage(s.diskPath)
		if err != nil {
			log.Error("SystemMetric get disk info error: %v", err)
			return err
		}
		s.DiskUsed = diskStat.Used
		if s.diskQuota == 0 {
			s.DiskTotal = diskStat.Total
			s.DiskFree = diskStat.Free
		} else {
			s.DiskTotal = s.diskQuota
			s.DiskFree = s.DiskTotal - s.DiskUsed
		}

		return nil
	}, routine.LogPanic(false))
}
