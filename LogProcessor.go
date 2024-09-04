package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/influxdb/client/v2"
)

const (
	TypeHandle  = 0
	ErrorHandle = 1
)

// 中转channel，防止协程冲突,数据不准确
var TypeMonitorChan = make(chan int, 200)

type Message struct {
	TimeLocal                    time.Time
	BytesSend                    int
	Path, Method, Status, Scheme string
	UpStreamTime, ResponseTime   float64
}

type Reader interface {
	Read(rc chan []byte)
}

type Writer interface {
	Write(wc chan *Message)
}

type logProcess struct {
	wc    chan *Message // 写通道
	rc    chan []byte   // 读通道
	read  Reader
	write Writer
}

type ReadFromFile struct {
	filePath string // 日志文件路径
}

type WriteToInfluxdb struct {
	influxdbDsn string // 数据库dsn
}

// 监控信息,通过http的方式暴露在外边
type SystemInfo struct {
	HandLine     int     `json:"handline"`     // 总处理日志行数
	Tps          float64 `json:"tps"`          // 吞吐量
	ReadChanLen  int     `json:"readchanlen"`  // 读通道长度
	WriteChanLen int     `json:"writechanlen"` // 写通道长度
	RunTime      string  `json:"runtime"`      // 运行时间
	Errnum       int     `json:"errnum"`       // 错误行数
}

// 监控模块
type Monitor struct {
	startTime time.Time
	data      SystemInfo
	tpsslic   []int
}

func (m *Monitor) Start(l *logProcess) {
	go func() {
		for n := range TypeMonitorChan {
			switch n {
			case TypeHandle:
				m.data.HandLine += 1
			case ErrorHandle:
				m.data.Errnum += 1
			}
		}
	}()

	ticker := time.NewTicker(time.Second * 5)
	go func() {
		for {
			<-ticker.C
			m.tpsslic = append(m.tpsslic, m.data.HandLine)
			if len(m.tpsslic) > 2 {
				m.tpsslic = m.tpsslic[1:]
			}
		}
	}()

	http.HandleFunc("/monitor", func(writer http.ResponseWriter, request *http.Request) {
		m.data.RunTime = time.Now().Sub(m.startTime).String()
		m.data.ReadChanLen = len(l.rc)
		m.data.WriteChanLen = len(l.wc)
		if len(m.tpsslic) >= 2 {
			m.data.Tps = float64(m.tpsslic[1]-m.tpsslic[0]) / 5 // 吞吐量计算
		}
		ret, err := json.MarshalIndent(m.data, "", "\t")
		if err != nil {
			log.Fatal("json marshal error", err.Error())
			return
		}

		_, err = io.WriteString(writer, string(ret))
		if err != nil {
			log.Fatal("write string error", err.Error())
		}
	})
	err := http.ListenAndServe(":9193", nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}

// 读取模块
func (r *ReadFromFile) Read(rc chan []byte) {
	file, err := os.Open(r.filePath)
	if err != nil {
		panic(fmt.Sprintf("open file error:%s", err.Error()))
	}
	file.Seek(0, 2) // 定位到文件末尾
	reader := bufio.NewReader(file)
	for {
		line, err := reader.ReadBytes('\n') // 读取一行内容
		if err == io.EOF {
			time.Sleep(500 * time.Millisecond) // 等待500ms，新的日志产生
			continue
		} else if err != nil {
			panic(fmt.Sprintf("read file error:%s", err.Error()))
		}
		TypeMonitorChan <- TypeHandle // 读取完一条数据
		rc <- line[:len(line)-1]
	}
}

// 分析模块
func (l *logProcess) process() {
	// 匹配日志消息的正则
	r := regexp.MustCompile(`([\d\.]+)\s+([^ \[]+)\s+([^ \[]+)\s+\[([^\]]+)\]\s+([a-z]+)\s+\"([^"]+)\"\s+(\d{3})\s+(\d+)\s+\"([^"]+)\"\s+\"(.*?)\"\s+\"([\d\.-]+)\"\s+([\d\.-]+)\s+([\d\.-]+)`)
	loc, _ := time.LoadLocation("Asia/Shanghai")
	for v := range l.rc {
		ret := r.FindStringSubmatch(string(v))
		// 如果分割的消息长度不等于13，说明这是一条无效的日志,跳过处理下一条
		if len(ret) != 14 {
			TypeMonitorChan <- ErrorHandle
			log.Println("ParseInLocation error:", string(v))
			continue
		}
		message := Message{}
		// 获取时间
		t, err := time.ParseInLocation("02/Jan/2006:15:04:05 +0000", ret[4], loc)
		if err != nil {
			TypeMonitorChan <- ErrorHandle
			log.Println("parse location error", err.Error())
			continue
		}
		message.TimeLocal = t
		// 获取字节数
		bytes, _ := strconv.Atoi(ret[8])
		message.BytesSend = bytes
		// 获取method url
		s := strings.Split(ret[6], " ")
		if len(s) != 3 {
			log.Println("split url error", ret[6])
			continue
		}
		message.Method = s[0]
		u, err := url.Parse(s[1])
		if err != nil {
			TypeMonitorChan <- ErrorHandle
			log.Println("parse url error", err.Error())
			continue
		}
		message.Path = u.Path
		// 获取协议
		message.Status = ret[5]
		// 获取状态码
		message.Status = ret[7]
		// 获取最后两个
		ut, err := strconv.ParseFloat(ret[12], 64)
		if err != nil {
			TypeMonitorChan <- ErrorHandle
			log.Println("parse upstream time error", err.Error())
			continue
		}
		message.UpStreamTime = ut
		rt, err := strconv.ParseFloat(ret[13], 64)
		if err != nil {
			log.Println("parse response time error", err.Error())
			continue
		}
		message.ResponseTime = rt
		l.wc <- &message
	}
}

// 写入模块
func (w *WriteToInfluxdb) Write(wc chan *Message) {
	dsnsli := strings.Split(w.influxdbDsn, "@")
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     dsnsli[0],
		Username: dsnsli[1],
		Password: dsnsli[2],
	})
	if err != nil {
		log.Fatal("create client error", err.Error())
	}

	for v := range wc {
		bp, err := client.NewBatchPoints(client.BatchPointsConfig{
			Database:  dsnsli[3],
			Precision: dsnsli[4],
		})
		if err != nil {
			log.Fatal("create batch points error", err.Error())
		}
		tags := map[string]string{"Path": v.Path, "Method": v.Method, "Scheme": v.Scheme, "Status": v.Status}
		fields := map[string]interface{}{"BytesSend": v.BytesSend, "UpStreamTime": v.UpStreamTime, "ResponseTime": v.ResponseTime}
		pt, er := client.NewPoint("nginx_log", tags, fields, v.TimeLocal)
		if er != nil {
			log.Fatal("create point error", er.Error())
		}
		bp.AddPoint(pt)
		if err := c.Write(bp); err != nil {
			log.Fatal("write error", err.Error())
		}
	}
}

func main() {
	var filepath, influxdbDsn string
	flag.StringVar(&filepath, "path", "", "日志文件路径")
	flag.StringVar(&influxdbDsn, "dsn", "", "influxdb dsn")
	r := &ReadFromFile{filePath: filepath}
	w := &WriteToInfluxdb{influxdbDsn: influxdbDsn}

	l := logProcess{
		rc:    make(chan []byte, 20),
		wc:    make(chan *Message, 20),
		read:  r,
		write: w,
	}
	go l.read.Read(l.rc)
	// 处理模块和写入模块的速度比较慢，多开几个协程增加处理速度
	for i := 0; i < 2; i++ {
		go l.process()
	}
	for i := 0; i < 4; i++ {
		go l.write.Write(l.wc)
	}

	m := Monitor{
		startTime: time.Now(),
		data:      SystemInfo{},
	}
	m.Start(&l)

}
