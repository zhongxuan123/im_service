package main
import "encoding/json"
import (
	log "github.com/golang/glog"
)

func (client *GroupClient) PublishSaveGroupMsgQueue(im *IMMessage) {
	conn := redis_pool.Get()
	defer conn.Close()
	v := make(map[string]interface{})
	v["sender"] = im.sender
	v["receiver"] = im.receiver
	v["content"] = im.content
	v["timestamp"] = nowTime
	b, _ := json.Marshal(v)
	var queue_name string = "save_group_queue"
	_, err := conn.Do("RPUSH", queue_name, b)
	if err != nil {
		log.Info("rpush error:", err)
	}
}

func (client *PeerClient) PublishSavePeerMsgQueue(im *IMMessage) {
	conn := redis_pool.Get()
	defer conn.Close()
	v := make(map[string]interface{})
	v["sender"] = im.sender
	v["receiver"] = im.receiver
	v["content"] = im.content
	v["timestamp"] = nowTime
	b, _ := json.Marshal(v)
	var queue_name string = "save_peer_queue"
	_, err := conn.Do("RPUSH", queue_name, b)
	if err != nil {
		log.Info("rpush error:", err)
	}
}