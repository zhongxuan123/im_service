/**
 * Copyright (c) 2014-2015, GoBelieve     
 * All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

package main
import "time"
import "fmt"
import log "github.com/golang/glog"
import "github.com/gomodule/redigo/redis"



type RoomMessageDeliver struct {
	wt chan *RoomMessage
}

func NewRoomMessageDeliver() *RoomMessageDeliver {
	usd := &RoomMessageDeliver{}
	usd.wt = make(chan *RoomMessage, 10000)
	return usd
}

func (usd *RoomMessageDeliver) SaveRoomMessage(msg *RoomMessage) bool {
	select {
	case usd.wt <- msg:
		return true
	case <- time.After(60*time.Second):
		log.Infof("save room message to wt timed out:%d, %d", msg.sender, msg.receiver)
		return false
	}
}

func (usd *RoomMessageDeliver) deliver(messages []*RoomMessage) {
	conn := redis_pool.Get()
	defer conn.Close()
	
	begin := time.Now()	
	conn.Send("MULTI")	
	for _, msg := range(messages) {
		content := fmt.Sprintf("%d\n%d\n%s", msg.sender, msg.receiver, msg.content)
		queue_name := fmt.Sprintf("rooms_%d", msg.receiver)
		conn.Send("LPUSH", queue_name, content)
	}
	res, err := redis.Values(conn.Do("EXEC"))
	
	end := time.Now()
	duration := end.Sub(begin)
	if err != nil {
		log.Info("multi lpush error:", err)
		return
	}
	log.Infof("mmulti lpush:%d time:%s success", len(messages), duration)


	rooms := make(map[int64]struct{})
	for index, v := range res {
		count, ok := v.(int64)

		if !ok {
			continue
		}

		//*2 for reduce call ltrim times
		if count <= int64(config.room_message_limit*2) {
			continue
		}
		
		if index >= len(messages) {
			log.Error("index out of bound")
			continue
		}
		msg := messages[index]
		rooms[msg.receiver] = struct{}{}
	}

	if len(rooms) == 0 {
		return
	}
	
	conn.Send("MULTI")
	for room_id, _ := range rooms {
		queue_name := fmt.Sprintf("rooms_%d", room_id)
		conn.Send("LTRIM", queue_name, 0, config.room_message_limit - 1)
	}

	_, err = conn.Do("EXEC")
	if err != nil {
		log.Warning("ltrim room list err:", err)
	}
}

func (usd *RoomMessageDeliver) run() {
	messages := make([]*RoomMessage, 0, 10000)	
	for {
		messages = messages[:0]
		
		m := <- usd.wt
		messages = append(messages, m)

	Loop:
		for {
			select {
			case m = <- usd.wt:
				messages = append(messages, m)
			default:
				break Loop
			}
		}
		usd.deliver(messages)
	}
}

func (usd *RoomMessageDeliver) Start() {
	go usd.run()
}
