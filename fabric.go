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
    "encoding/json"
)

const (
    FABRIC_TOPIC_ANY                        = "+"
    NODENAME_BROADCAST                      = "broadcast"

    FABRIC_SYS                              = "sysctl"
    FABRIC_CMD_STATUS                       = "status"
    
    /******************************************************************************************************************
    * common platform id's
    *
    */
    PLATFORM_ID_CHRONOS                     = "chronos"
    
    /******************************************************************************************************************
    * common service id's
    *
    */
    SERVICE_ID_DIGITAL                      = "digital"
    SERVICE_ID_DIGITAL_IN                   = "digital_in"
    SERVICE_ID_DIGITAL_OUT                  = "digital_out"
    SERVICE_ID_ANALOG                       = "analog"
    SERVICE_ID_ANALOG_IN                    = "analog_in"
    SERVICE_ID_ANALOG_OUT                   = "analog_out"
    SERVICE_ID_TIME                         = "time"
    SERVICE_ID_TEXT                         = "text"
    
    /******************************************************************************************************************
    * common task id's
    *
    */
    TASK_ID_DIGITAL_WRITE_MOMENTARY         = "digital_write_momentary"
    TASK_ID_DIGITAL_WRITE_MOMENTARY_EX      = "digital_write_momentary_ex"
    TASK_ID_DIGITAL_WRITE                   = "digital_write"
    TASK_ID_DIGITAL_WRITE_EX                = "digital_write_ex"
    TASK_ID_ANALOG_WRITE                    = "analog_write"
    TASK_ID_ANALOG_WRITE_EX                 = "analog_write_ex"
    TASK_ID_RAW                             = "raw"
    
    /******************************************************************************************************************
    * common feed id's
    *
    */
    FEED_ID_SECONDS                         = "seconds"
)

type ClassType int

const (
    DEVICE              ClassType = 1
    CONTROLLER          ClassType = 2
)

// Fabric ...
//
type Fabric struct {
    RootTopic           string
    NodeName            string
    PlatformID          string
    ActorID             string
    ActorPlatformID     string
    TaskID              string
    
    ClassType           ClassType
}

type Status int

const (
    FABRIC_ONLINE       Status  = 1
    FABRIC_OFFLINE      Status  = 2
    FABRIC_DISCONNECTED Status  = 3
)

// FabricInitialize ...
//
func FabricInitialize(rootTopic string, nodename string, platformID string, classType ClassType) (*Fabric) {
    f := &Fabric{}
    
    f.ClassType         = classType
    f.RootTopic         = rootTopic
    f.NodeName          = nodename
    f.PlatformID        = platformID
    
    f.ActorID           = nodename    
    f.ActorPlatformID   = platformID
    f.TaskID            = "task_id"
    
    return f
}

// StatusMessage ...
//
func (f *Fabric) StatusMessage(fabricStatus Status, seconds int64) (string, string) {
    var topic = f.RootTopic + "/" + f.NodeName + "/$commands/$clients/" + FABRIC_SYS + "/" + f.PlatformID + "/" + FABRIC_CMD_STATUS
    
    type Data struct {
        Type        string `json:"_type"`
        Status      string `json:"status"`
        Uptime     *int64  `json:"uptime"`
        Nodename    string `json:"nodename"`
        PlatformID  string `json:"platform_id"`
        Class       string `json:"class"`
    }
    
    type D struct {
        Data Data `json:"d"`
    }

	jsonMsg := D{
		Data: Data{
			Type:       "status",
            Nodename:   f.NodeName,
            PlatformID: f.PlatformID,
		},
	}

    switch f.ClassType {
        case DEVICE:
            jsonMsg.Data.Class = "device"
            
        case CONTROLLER:
            jsonMsg.Data.Class = "controller"
            
        default:
            return "", ""
    }
    
    switch fabricStatus {
        case FABRIC_ONLINE:
            jsonMsg.Data.Status = "online"
            jsonMsg.Data.Uptime = &seconds
            
        case FABRIC_OFFLINE:
            jsonMsg.Data.Status = "offline"
            jsonMsg.Data.Uptime = &seconds
            
        case FABRIC_DISCONNECTED:
            jsonMsg.Data.Status = "disconnected"
            jsonMsg.Data.Uptime = nil
            
        default:
            return "", ""
    }
        
    msg, err := json.Marshal(jsonMsg)
    
	if err != nil {
		log.Println("StatusMessage(): err = ", err)
		return "", ""
	}
    
    return topic, string(msg)
}

// DeviceOnrampTopic ...
//
func (f *Fabric) DeviceOnrampTopic(serviceID string, feedID string) (string) {
    return f.RootTopic + "/" + f.NodeName + "/$feeds/$onramp/" + f.PlatformID + "/" + serviceID + "/" + feedID
}

// DeviceOfframpSubscription ...
//
func (f *Fabric) DeviceOfframpSubscription(nodename string, actorID string, actorPlatformID string, taskID string, platformID string, serviceID string, feedID string) (string) {
    return f.RootTopic + "/" + nodename + "/$feeds/$offramp/" + actorID + "/" + actorPlatformID + "/" + taskID + "/" + platformID + "/" + serviceID + "/" + feedID 
}

// CtrlOfframpTopic ...
//
func (f *Fabric) CtrlOfframpTopic(nodename string, taskID string, platformID string, serviceID string, feedID string) (string) {
    return f.RootTopic + "/" + nodename + "/$feeds/$offramp/" + f.ActorID + "/" + f.ActorPlatformID + "/" + taskID + "/" + platformID + "/" + serviceID + "/" + feedID 
}

// CtrlOnrampSubscription ...
//
func (f *Fabric) CtrlOnrampSubscription(nodename string, platformID string, serviceID string, feedID string) (string) {
    return f.RootTopic + "/" + nodename + "/$feeds/$onramp/" + platformID + "/" + serviceID + "/" + feedID
}

