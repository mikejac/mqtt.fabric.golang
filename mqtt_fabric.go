/* 
 * The MIT License (MIT)
 * 
 * MQTT Infrastructure
 * Copyright (c) 2016 Michael Jacobsen (github.com/mikejac)
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 */

package mqttfabric

import (
    "log"
    "time"
    "strconv"
    "os"
    "strings"
    "unsafe"
    "encoding/json"
    MQTT "github.com/mikejac/paho.mqtt.golang"  // import the Paho Go MQTT library
)

// OnConnectHandler ...
type OnConnectHandler func(mqtt *MqttFabric)
// OnDisconnectHandler ...
type OnDisconnectHandler func(mqtt *MqttFabric)
// OnOnrampHandler ...
type OnOnrampHandler func(  mqtt            *MqttFabric,
                            nodename        string,
                            platformID      string,
                            serviceID       string,
                            feedID          string,
                            msg             string)
// OnOfframpHandler ...
type OnOfframpHandler func( mqtt            *MqttFabric,
                            nodename        string,
                            actorID         string,
                            actorPlatformID string,
                            taskID          string,
                            platformID      string,
                            serviceID       string,
                            feedID          string,
                            msg             string)
                            
// MqttFabric ...
//
type MqttFabric struct {
    Options         *MQTT.ClientOptions
    Mqtt            *MQTT.Client
    F               *Fabric
    StartTime       time.Time
    OnConnect       OnConnectHandler
    OnDisconnect    OnDisconnectHandler
    OnOnramp        OnOnrampHandler
    OnOfframp       OnOfframpHandler
}

// Initialize ...
//
func MqttFabricInitialize(broker string, port int, keepalive int, rootTopic string, nodename string, platformID string, classType ClassType) *MqttFabric {
    m := &MqttFabric{}

    m.StartTime     = time.Now()
    m.OnConnect     = nil
    m.OnDisconnect  = nil
    m.OnOnramp      = nil
    m.OnOfframp     = nil
    
    m.F = FabricInitialize(rootTopic, nodename, platformID, classType)
    var lwtTopic, lwtMsg = m.F.StatusMessage(FABRIC_DISCONNECTED, 0)
    
    log.Println(lwtTopic)
    log.Println(lwtMsg)
    
    hostname, _ := os.Hostname()
    clientid    := hostname + strconv.Itoa(time.Now().Second())
    
    log.Printf("Initialize(): clientid = %s\n", clientid)
    
  	// create a ClientOptions struct setting the broker address, clientid, turn
  	// off trace output and set the default message handler
  	opts := MQTT.NewClientOptions().AddBroker("tcp://" + broker + ":" + strconv.Itoa(port))
  	opts.SetClientID(clientid)
    opts.SetCleanSession(true)
    opts.SetDefaultPublishHandler(onMessage)
    opts.SetOnConnectHandler(onConnect)
    opts.SetConnectionLostHandler(onDisconnect)
    opts.SetKeepAlive(time.Duration(keepalive) * time.Second)
    opts.SetWill(lwtTopic, lwtMsg, 2, true)
    opts.SetUserData((unsafe.Pointer)(m))
    
    m.Options = opts
    
    return m
}

// SetOnConnectHandler ...
//
func (m *MqttFabric) SetOnConnectHandler(handler OnConnectHandler) *MqttFabric {
    m.OnConnect = handler
	return m
}

// SetOnDisconnectHandler ...
//
func (m *MqttFabric) SetOnDisconnectHandler(handler OnDisconnectHandler) *MqttFabric {
    m.OnDisconnect = handler
	return m
}

// SetOnOnrampHandler ...
//
func (m *MqttFabric) SetOnOnrampHandler(handler OnOnrampHandler) *MqttFabric {
    m.OnOnramp = handler
	return m
}

// SetOnOfframpHandler ...
//
func (m *MqttFabric) SetOnOfframpHandler(handler OnOfframpHandler) *MqttFabric {
    m.OnOfframp = handler
	return m
}

// Start ...
//
func (m *MqttFabric) Start() (bool) {
    // create and start a client
    m.Mqtt = MQTT.NewClient(m.Options)

    if token := m.Mqtt.Connect(); token.Wait() && token.Error() != nil {
        panic(token.Error())
  	}
      
    return true
}

// Stop ...
//
func (m *MqttFabric) Stop() {
    var topic, msg = m.F.StatusMessage(FABRIC_OFFLINE, time.Now().Unix() - m.StartTime.Unix())
    
    m.Mqtt.Publish(topic, 2, true, msg)
    
    log.Println(topic)
    log.Println(msg)
    
    m.Mqtt.Disconnect(250)
}

// Run ...
//
func (m *MqttFabric) Run() {
    m.Start()
    
    go func() {
	    for {
            
        }
    }()
}

// CtrlPubText ...
//
func (m *MqttFabric) CtrlPubText(nodename string, platformID string, feedID string, data string, qos byte, retain bool) {
    topic := m.F.CtrlOfframpTopic(nodename, TASK_ID_RAW, platformID, SERVICE_ID_TEXT, feedID)
    
    log.Println(topic)
    
    type Data struct {
        Type        string `json:"_type"`
        FeedID      string `json:"feed_id"`
        Value       string `json:"value"`
    }
    
    type D struct {
        Data Data `json:"d"`
    }
    
	jsonMsg := D{
		Data: Data{
			Type:       SERVICE_ID_TEXT,
            FeedID:     feedID,
            Value:      data,
		},
	}
    
    msg, err := json.Marshal(jsonMsg)
    
	if err != nil {
		log.Println("CtrlPubText(): err = ", err)
		return
	}
    
    log.Println(string(msg))
    
    m.Mqtt.Publish(topic, qos, retain, msg)
}

// DevicePubText ...
//
func (m *MqttFabric) DevicePubText(feedID string, data string, qos byte, retain bool) {
    topic := m.F.DeviceOnrampTopic(SERVICE_ID_TEXT, feedID)
    
    log.Println(topic)
    
    type Data struct {
        Type        string `json:"_type"`
        FeedID      string `json:"feed_id"`
        Value       string `json:"value"`
    }
    
    type D struct {
        Data Data `json:"d"`
    }
    
	jsonMsg := D{
		Data: Data{
			Type:       SERVICE_ID_TEXT,
            FeedID:     feedID,
            Value:      data,
		},
	}
    
    msg, err := json.Marshal(jsonMsg)
    
	if err != nil {
		log.Println("DevicePubText(): err = ", err)
		return
	}
    
    log.Println(string(msg))
    
    m.Mqtt.Publish(topic, qos, retain, msg)
}

// define a function for the default message handler
//
var onMessage MQTT.MessageHandler = func(client *MQTT.Client, msg MQTT.Message) {
    //log.Printf("onMessage(): Topic   = %s\n", msg.Topic())
    //log.Printf("onMessage(): Payload = %s\n", msg.Payload())
    defer func() {
        if r := recover(); r != nil {
            log.Println("onMessage(): panic recovered; ", r)
        }
    }()
    
    m := (*MqttFabric)(client.GetUserData())

    tokenizer := strings.Split(msg.Topic(), "/")
    count     := len(tokenizer)
    //log.Printf("onMessage(): count = %d\n", count)
    
    if tokenizer[2] == "$commands" {
        nodename   := tokenizer[1]
        actorID    := tokenizer[4]
        platformID := tokenizer[5]
        cmd        := tokenizer[6]
        
        log.Printf("onMessage(): $commands\n")
        log.Printf("onMessage(): nodename   = %s\n", nodename)
        log.Printf("onMessage(): actorID    = %s\n", actorID)
        log.Printf("onMessage(): platformID = %s\n", platformID)
        log.Printf("onMessage(): cmd        = %s\n", cmd)
        
    } else if tokenizer[2] == "$feeds" {
        if count > 6 && tokenizer[3] == "$onramp" {
            nodename   := tokenizer[1]
            platformID := tokenizer[4]
            serviceID  := tokenizer[5]
            feedID     := tokenizer[6]

            //fmt.Printf("onMessage(): $onramp\n")
            //fmt.Printf("onMessage(): nodename        = %s\n", nodename)
            //fmt.Printf("onMessage(): platformID      = %s\n", platformID)
            //fmt.Printf("onMessage(): serviceID       = %s\n", serviceID)
            //fmt.Printf("onMessage(): feedID          = %s\n", feedID)
            
            if m.OnOnramp != nil {
                if m.F.ClassType == CONTROLLER {
                    m.OnOnramp(m, nodename, platformID, serviceID, feedID, string(msg.Payload()))
                } else if m.F.ClassType == DEVICE && nodename != m.F.NodeName {
                    m.OnOnramp(m, nodename, platformID, serviceID, feedID, string(msg.Payload()))                
                }
            }
            
        } else if count >=10 && tokenizer[3] == "$offramp" {
            nodename        := tokenizer[1]
            actorID         := tokenizer[4]
            actorPlatformID := tokenizer[5]
            taskID          := tokenizer[6]
            platformID      := tokenizer[7]
            serviceID       := tokenizer[8]
            feedID          := tokenizer[9]

            //fmt.Printf("onMessage(): $offramp\n")
            //fmt.Printf("onMessage(): nodename        = %s\n", nodename)
            //fmt.Printf("onMessage(): actorID         = %s\n", actorID)
            //fmt.Printf("onMessage(): actorPlatformID = %s\n", actorPlatformID)
            //fmt.Printf("onMessage(): taskID          = %s\n", taskID)
            //fmt.Printf("onMessage(): platformID      = %s\n", platformID)
            //fmt.Printf("onMessage(): serviceID       = %s\n", serviceID)
            //fmt.Printf("onMessage(): feedID          = %s\n", feedID)

            if m.OnOfframp != nil {
                if m.F.ClassType == DEVICE {
                    m.OnOfframp(m, nodename, actorID, actorPlatformID, taskID, platformID, serviceID, feedID, string(msg.Payload()))
                } else if m.F.ClassType == CONTROLLER && nodename != m.F.NodeName {
                    m.OnOfframp(m, nodename, actorID, actorPlatformID, taskID, platformID, serviceID, feedID, string(msg.Payload()))                
                }
            }
        } else {
            // invalid
            log.Printf("onMessage(): invalid feed\n")
        }
    } else {
        // other
        log.Printf("onMessage(): other\n")

    }
}

// define a function for the 
//
var onConnect MQTT.OnConnectHandler = func(client *MQTT.Client) {
    log.Printf("onConnect():\n")
    
    defer func() {
        if r := recover(); r != nil {
            log.Println("onConnect(): panic recovered; ", r)
        }
    }()
    
    // get pointer to our MqttFabric data
    m := (*MqttFabric)(client.GetUserData())
    
    var topic, msg = m.F.StatusMessage(FABRIC_ONLINE, time.Now().Unix() - m.StartTime.Unix())
    
    m.Mqtt.Publish(topic, 2, true, msg)
    
    log.Println(topic)
    log.Println(msg)
    
    if(m.OnConnect != nil) {
        m.OnConnect(m)
    }
}

// define a function for the 
//
var onDisconnect MQTT.ConnectionLostHandler = func(client *MQTT.Client, err error) {
    log.Printf("onDisconnect():\n")
    
    defer func() {
        if r := recover(); r != nil {
            log.Println("onDisconnect(): panic recovered; ", r)
        }
    }()
    
    // get pointer to our MqttFabric data
    m := (*MqttFabric)(client.GetUserData())
    
    if(m.OnDisconnect != nil) {
        m.OnDisconnect(m)
    }
}

/*
    #
    #
    #
    def on_command(self, node_name, actor_id, platform_id, cmd, msg):
        print("mqtt_fabric_t::on_command()\r")
        print("mqtt_fabric_t::on_command(): node_name         = " + node_name + "\r")
        print("mqtt_fabric_t::on_command(): actor_id          = " + actor_id + "\r")
        print("mqtt_fabric_t::on_command(): platform_id       = " + platform_id + "\r")
        print("mqtt_fabric_t::on_command(): cmd               = " + cmd + "\r")
*/