package main

import "fmt"
import "flag"
import "time"
import "strconv"
import "net"
import "os"
import "io/ioutil"
import "strings"
import "math"

func CheckError(err error) {
	if err != nil {
		fmt.Println("Error: ", err)
		os.Exit(0)
	}
}

type Master struct {
	address     string
	time        int
	d           int
	slaves_file string
	log_file    string
}

type Slave struct {
	address  string
	time     int
	log_file string
}

func (m *Master) updateTime() {
	m.time = m.time + 1
	fmt.Println("Master new time: ", m.time)
}

func (m *Master) getSlaves() []string {
	content, err := ioutil.ReadFile(m.slaves_file)
	if err != nil {
		panic(err)
	}
	lines := strings.Split(string(content), "\n")
	// remove last element because it's empty, no idea why.
	lines = lines[:len(lines)-1]
	return lines
}

func (m *Master) Start() {
	c := time.Tick(1500 * time.Millisecond)

	// period to the master query its slaves
	var query_period = 5
	var current_period = 0

	slaves := m.getSlaves()
	fmt.Println("Slaves ip: ", slaves)

	for now := range c {
		fmt.Printf("%v\n", now)

		go m.updateTime()

		if current_period == query_period {
			slaves_time := make(map[string]int)
			fmt.Println("Time to query my slaves")

			result := make(chan string)

			for _, slave := range slaves {

				go m.querySlave(result, slave, "time_request", 0)

				slave_current_time := <-result

				slave_current_time_int, _ := strconv.Atoi(slave_current_time)

				slaves_time[slave] = slave_current_time_int
			}
			current_period = 0
			fmt.Println("Slave times: ", slaves_time)

			deltas := m.getDeltas(slaves_time)

			fmt.Println("Deltas: ", deltas)

			fault_tolerant_avg := m.faultTolerantAverage(deltas)

			fmt.Println(fault_tolerant_avg)

			for slave, delta := range deltas {
				fmt.Println("Slave and delta: ", slave, delta)
				correction_value := fault_tolerant_avg - delta
				fmt.Println("correction for slave ", slave, ": ", correction_value)
				go m.querySlave(result, slave, "clock_correction", correction_value)
			}
		}
		current_period += 1
	}
}

func (m *Master) faultTolerantAverage(original_deltas map[string]int) int {
	deltas := make(map[string]int)
	for k, v := range original_deltas {
		deltas[k] = v
	}
	for _, time := range deltas {
		for s, t := range deltas {
			if int(math.Abs(float64(time)-float64(t))) > m.d {
				delete(deltas, s)
			}
		}
	}
	fmt.Println("non faulty deltas: ", deltas)

	var times []int
	for _, time := range deltas {
		times = append(times, time)
	}
	fmt.Println(times)

	return average(times)
}

func average(xs []int) int {
	total := 0
	for _, v := range xs {
		total += v
	}
	return total / len(xs)

}

func (m *Master) getDeltas(times map[string]int) map[string]int {
	deltas := make(map[string]int)

	for slave, time := range times {
		deltas[slave] = int(math.Abs(float64(m.time) - float64(time)))
	}
	return deltas
}

func (m *Master) querySlave(ch chan string, slave_ip string, action string, value int) {
	SlaveAddr, err := net.ResolveUDPAddr("udp", slave_ip)
	CheckError(err)

	MasterAddr, err := net.ResolveUDPAddr("udp", m.address)
	CheckError(err)

	Conn, err := net.DialUDP("udp", MasterAddr, SlaveAddr)
	CheckError(err)

	defer Conn.Close()

	if action == "time_request" {
		// request slave time
		buf := []byte("time_request")
		ret, err := Conn.Write(buf)
		if err != nil {
			fmt.Println(ret, err)
		}

		buffer := make([]byte, 1024)
		n, addr, err := Conn.ReadFromUDP(buffer)
		response := string(buffer[:n])

		fmt.Println("Slave: ", addr)
		fmt.Println("Received from slave: ", response)

		ch <- response
	}
	if action == "clock_correction" {
		// request slave time
		var buf []byte
		if value > 0 {
			string_value := strconv.Itoa(int(math.Abs(float64(value))))
			buf = []byte("clock_correction," + string_value + ",positive")
		} else if value < 0 {
			string_value := strconv.Itoa(int(math.Abs(float64(value))))
			buf = []byte("clock_correction," + string_value + ",negative")
		} else if value == 0 {
			buf = []byte("clock_correction,0,zero")
		}
		ret, err := Conn.Write(buf)
		if err != nil {
			fmt.Println(ret, err)
		}
	}
}

func (s *Slave) updateTime() {
	s.time = s.time + 1
	fmt.Println("Slave new time: ", s.time)
}

func (s *Slave) Listen() {
	go s.startClock()

	LocalAddr, err := net.ResolveUDPAddr("udp", s.address)
	CheckError(err)

	/* Now listen at selected port */
	LocalConn, err := net.ListenUDP("udp", LocalAddr)
	CheckError(err)
	defer LocalConn.Close()

	fmt.Println("Slave is listening at: ", LocalAddr)

	for {
		slaveHandleUDPConnection(s, LocalConn)
	}
}

func slaveHandleUDPConnection(s *Slave, conn *net.UDPConn) {

	buffer := make([]byte, 1024)

	n, addr, err := conn.ReadFromUDP(buffer)
	request := string(buffer[:n])
	fmt.Println("Master: ", addr)
	fmt.Println("Received from master:  ", request)

	if err != nil {
		panic(err)
	}
	splitted_request := strings.Split(request, ",")

	if splitted_request[0] == "time_request" {
		// send slave's time to master
		message := []byte(strconv.Itoa(s.time))
		_, err = conn.WriteToUDP(message, addr)

		if err != nil {
			panic(err)
		}
	} else if splitted_request[0] == "clock_correction" {
		fmt.Println("Got clock correction: " + splitted_request[1] + splitted_request[2])
		if splitted_request[2] == "positive" {
			clock_correction, _ := strconv.Atoi(splitted_request[1])
			s.time += clock_correction
		} else if splitted_request[1] == "negative" {
			clock_correction, _ := strconv.Atoi(splitted_request[1])
			s.time -= clock_correction
		}
	}
}

func (s *Slave) startClock() {
	c := time.Tick(1500 * time.Millisecond)

	for now := range c {
		fmt.Printf("%v\n", now)
		go s.updateTime()
	}
}

func main() {

	var master_flag = flag.Bool("m", false, "master flag")
	var slave_flag = flag.Bool("s", false, "slave flag")

	flag.Parse()

	if *master_flag && *slave_flag {
		fmt.Println("ERROR :: you're using both slave/master flag, pick one!")
		return
	}

	if *master_flag {

		// expected args: ip:port, time, d, slavesfile, log file

		fmt.Println("START :: intializing as master")

		initial_time, err := strconv.Atoi(flag.Arg(1))

		if err != nil {
			panic(err)
		}

		d, _ := strconv.Atoi(flag.Arg(2))

		master := Master{flag.Arg(0), initial_time, d,
			flag.Arg(3), flag.Arg(4)}

		fmt.Println("Master address:", master.address,
			"\nMaster initial time:", master.time,
			"\nd constant:", master.d,
			"\nslaves file:", master.slaves_file,
			"\nlog_file:", master.log_file)

		master.Start()

	} else if *slave_flag {
		// expected args: ip:port, time, logfile
		fmt.Println("START :: initializing as slave")

		initial_time, err := strconv.Atoi(flag.Arg(1))

		if err != nil {
			panic(err)
		}

		slave := Slave{flag.Arg(0), initial_time, flag.Arg(2)}

		fmt.Println("Slave address:", slave.address,
			"\nSlave initial time:", slave.time,
			"\nlog_file:", slave.log_file)

		slave.Listen()
	}

}
