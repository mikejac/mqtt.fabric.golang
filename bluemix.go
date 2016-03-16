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
    "errors"
    "encoding/json"
)

type BlueMixObject struct {
    Type            string
    FeedID          string
    
    T               interface{}         // the value
}

//
//
func BlueMixParse(msg string) (*BlueMixObject, error) {
    b := NewBlueMixObject()
    
    var objmap map[string]*json.RawMessage
    
    if err := json.Unmarshal([]byte(msg), &objmap); err != nil {
        log.Fatal(err)
        return nil, errors.New("BlueMixParse: cannot parse JSON top-level object")
    }
    
    gotType   := false
    gotFeedID := false
    gotValue  := false
    
    for k := range objmap {
        if k == "d" {
            //log.Println("BlueMixParse(): found 'd'")
            
            var parsed map[string]interface{}
            
            if err := json.Unmarshal(*objmap["d"], &parsed); err != nil {
                log.Fatal(err)
                return nil, errors.New("BlueMixParse: cannot parse JSON 'd' object")
            }
            
            for key, value := range parsed {
                //log.Println("Key:", key, "Value:", value)
                
                if key == "_type" && isString(value) {
                    //log.Println("BlueMixParse(): found '_type'")
                    b.SetType(value.(string))
                    gotType = true
                } else if key == "feed_id" && isString(value) {
                    //log.Println("BlueMixParse(): found 'feed_id'")
                    b.SetFeedID(value.(string))
                    gotFeedID = true
                } else if key == "value" {
                    //log.Println("BlueMixParse(): found 'value'")
                    b.SetValue(value)
                    gotValue = true
                }
            }
        }
    }
    
    if gotType && gotFeedID && gotValue {
        return b, nil
    }
    
    return nil, errors.New("BlueMixParse: missing one or more fields in JSON object")
}


func NewBlueMixObject() (*BlueMixObject) {
    return &BlueMixObject{T: nil}
}

func (o *BlueMixObject) SetType(valueType string) (*BlueMixObject) {
    o.Type = valueType
    return o
}

func (o *BlueMixObject) SetFeedID(feedID string) (*BlueMixObject) {
    o.FeedID = feedID
    return o
}

func (o *BlueMixObject) SetValue(v interface{}) (*BlueMixObject) {
    switch t := v.(type) {
        case int:
            //log.Println("SetValue(): int")
            o.T = v.(int)
            break
        case float64:
            //log.Println("SetValue(): float64")
            o.T = int(v.(float64))
            break
        case bool:
            //log.Println("SetValue(): bool")
            o.T = v.(bool)
            break
        case string:
            //log.Println("SetValue(): string")
            o.T = v.(string)
            break
        case nil:
            //log.Println("SetValue(): nil")
            o.T = nil
            break
        //case []interface{}:
        //    return "array"
        //case map[string]interface{}:
        //    return "object"
        default:
            log.Println("SetValue(): unknown")
            _ = t
            return nil
    }

    return o
}

func (o *BlueMixObject) GetType() (string, error) {
    if o.Type == "" {
        return "", errors.New("GetType: type has not been set")
    }
    
    return o.Type, nil
}

func (o *BlueMixObject) GetFeedID() (string, error) {
    if o.FeedID == "" {
        return "", errors.New("GetType: feed id has not been set")
    }
    
    return o.FeedID, nil
}

func (o *BlueMixObject) GetValueInt() (int, error) {
    switch t := o.T.(type) {
        case int:
            return o.T.(int), nil
        default:
            log.Println("GetValueInt(): not 'int'")
            _ = t
            return 0, errors.New("GetValueInt: value is not of type 'int'")
    }
}

func (o *BlueMixObject) GetValueBool() (bool, error) {
    switch t := o.T.(type) {
        case bool:
            return o.T.(bool), nil
        default:
            log.Println("GetValueBool(): not 'bool'")
            _ = t
            return false, errors.New("GetValueBool: value is not of type 'bool'")
    }
}

func (o *BlueMixObject) GetValueString() (string, error) {
    switch t := o.T.(type) {
        case string:
            return o.T.(string), nil
        default:
            log.Println("GetValueString(): not 'string'")
            _ = t
            return "", errors.New("GetValueString: value is not of type 'string'")
    }
}

func isObject(v interface{}) bool {
    switch t := v.(type) {
        case map[string]interface{}:
            return true
        default:
            _ = t
            return false
    }
}
func isString(v interface{}) bool {
    switch t := v.(type) {
        case string:
            return true
        default:
            _ = t
            return false
    }
}
func isBool(v interface{}) bool {
    switch t := v.(type) {
        case bool:
            return true
        default:
            _ = t
            return false
    }
}
func isNumeric(v interface{}) bool {
    switch t := v.(type) {
        case float64:
            return true
        default:
            _ = t
            return false
    }
}

/*********************************************************************/

/*func typeof(v interface{}) string {
    switch t := v.(type) {
        case int:
            return "int"
        case float64:
            return "float64"
        case bool:
            return "bool"
        case string:
            return "string"
        case nil:
            return "nil"
        case []interface{}:
            return "array"
        case map[string]interface{}:
            return "object"
        default:
            _ = t
            return "unknown"
    }
}*/