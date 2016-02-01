package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
	"time"

	"encoding/json"

	"strings"

	"github.com/coreos/etcd/client"
	"golang.org/x/net/context"
)

const (
	CLUSTER_MASTER_KEY = "cluster_master"
	CLUSTER_INFO       = "cluster_info"
	MASTER_ALIVE       = time.Second * 30
	NODE_ALIVE         = time.Second * 20
)

type (
	Task struct {
		URLs []string `json:"urls"`
	}

	Node struct {
		sync.RWMutex
		UUID        string
		IsAlive     bool
		State       string
		CommandPort string
		Tasks       map[string]Task
		MasterDone  chan struct{}
		SlaveDone   chan struct{}
		etcd        client.KeysAPI
	}
)

func (self *Node) Init() {
	cfg := client.Config{
		Endpoints: []string{"http://127.0.0.1:2379"},
		Transport: client.DefaultTransport,
		// set timeout per request to fail fast when the target endpoint is unavailable
		HeaderTimeoutPerRequest: time.Second,
	}
	c, err := client.New(cfg)
	if err != nil {
		log.Fatal(err)
	}

	self.etcd = client.NewKeysAPI(c)

	self.Tasks = make(map[string]Task)

	self.MasterDone = make(chan struct{}, 2)
	self.SlaveDone = make(chan struct{}, 2)

	go self.StartCommandServer()
	self.Election()
}

func (self *Node) Election() {
	for {
		options := client.SetOptions{PrevExist: client.PrevExistType("false"), TTL: MASTER_ALIVE}
		_, err := self.etcd.Set(context.Background(), "/"+CLUSTER_MASTER_KEY, self.UUID, &options)
		if err != nil && err.(client.Error).Code == client.ErrorCodeNodeExist {
			self.SlaveAlive()
		} else {
			self.MasterAlive()
		}

		fmt.Println(self.State)
	}

}

func (self *Node) MasterAlive() {
	log.Printf("Start master process %v\n", self.UUID)

	if self.State == "slave" {
		self.SlaveDone <- struct{}{}
		time.Sleep(MASTER_ALIVE / 4)
	}

	self.State = "master"
	go self.MasterWorker()

	for {
		options := client.SetOptions{PrevValue: self.UUID, TTL: MASTER_ALIVE}
		_, err := self.etcd.Set(context.Background(), "/"+CLUSTER_MASTER_KEY, self.UUID, &options)

		if err != nil && err.(client.Error).Code == client.ErrorCodeTestFailed {
			log.Printf("Split brain detected %v\n", self.UUID)

			//Stop master worker
			self.MasterDone <- struct{}{}

			//Need sleep to wait until master worker stops
			time.Sleep(MASTER_ALIVE / 4)

			return
		}

		time.Sleep(MASTER_ALIVE / 4)
	}

	log.Printf("Stop master process %v\n", self.UUID)
}

func (self *Node) SlaveAlive() {
	log.Printf("Start slave process %v", self.UUID)

	self.State = "slave"
	go self.SlaveWorker()

	for {
		resp, err := self.etcd.Watcher(CLUSTER_MASTER_KEY, nil).Next(context.Background())
		if err != nil {
			log.Fatal(err)
		}

		if resp.Action == "expire" {
			log.Printf("Failed master detected %v\n", self.UUID)
			return
		}

		time.Sleep(MASTER_ALIVE / 2)
	}

}

func (self *Node) MasterWorker() {
	log.Printf("Start master worker %v", self.UUID)

	for {
		select {
		case <-self.MasterDone:
			log.Printf("Stop master worker %v\n", self.UUID)
			return
		default:
			servers := map[string]string{}

			options := client.GetOptions{Recursive: true}
			resp, err := self.etcd.Get(context.Background(), CLUSTER_INFO, &options)
			if err != nil {
				log.Fatal(err)
			}

			for _, node := range resp.Node.Nodes {
				resp, err := self.etcd.Get(context.Background(), node.Key, nil)
				if err != nil {
					log.Fatal(err)
				}

				vals := strings.Split(resp.Node.Value, ":")

				if vals[1] != self.CommandPort {
					servers[vals[0]] = "127.0.0.1:" + vals[1]
				}

			}

			if len(servers) > 0 {
				client := &http.Client{}
				for uuid, addr := range servers {
					client.Post(fmt.Sprintf("http://%s/add_task?uuid=%s", addr, uuid), "text/plain", bytes.NewBufferString(`{"urls": ["123","345"]}`))
					log.Printf("Send task to %s - %s\n", uuid, addr)
				}
			}

			log.Printf("Master worker tick")

		}

		time.Sleep(MASTER_ALIVE / 4)
	}

}

func (self *Node) SlaveWorker() {
	log.Printf("Start slave worker process %v", self.UUID)

	for {
		select {
		case <-self.SlaveDone:
			log.Printf("Stop slave worker %v\n", self.UUID)
			return
		default:
			self.RLock()
			if len(self.Tasks) > 0 {
				log.Printf("New task detected %v\n", self.UUID)
				fmt.Printf("%+v\n", self.Tasks)
			}
			self.RUnlock()
			log.Printf("Slave worker tick")
		}

		time.Sleep(MASTER_ALIVE / 4)
	}

}

func (self *Node) KeepAlive() {
	log.Printf("Start keep-alive process %v", self.UUID)

	val := self.UUID + ":" + self.CommandPort
	options := client.CreateInOrderOptions{TTL: NODE_ALIVE}
	resp, err := self.etcd.CreateInOrder(context.Background(), "/"+CLUSTER_INFO, val, &options)
	if err != nil {
		log.Fatal(err)
	}

	for {
		options := client.SetOptions{PrevValue: val, TTL: NODE_ALIVE}
		_, err := self.etcd.Set(context.Background(), resp.Node.Key, val, &options)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func (self *Node) AddTaskHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Fatal(err)
	}

	task := Task{}
	err = json.Unmarshal(body, &task)

	self.Lock()
	defer self.Unlock()

	uuid := r.URL.Query().Get("uuid")
	if uuid != "" {
		self.Tasks[uuid] = task
	}
}

func (self *Node) StartCommandServer() {
	log.Printf("Start command server %v", self.UUID)

	go self.KeepAlive()

	http.HandleFunc("/add_task", self.AddTaskHandler)
	http.ListenAndServe(fmt.Sprintf(":%s", self.CommandPort), nil)
}
