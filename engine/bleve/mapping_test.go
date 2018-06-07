package bleve

import (
	"testing"
	"fmt"
)

func TestMapping(t *testing.T) {
	schema := `{
  "mappings": {
    "user": { 
      "_all":       { "enabled": false  }, 
      "properties": { 
        "title":    { "type": "string"  }, 
        "name":     { "type": "string"  }, 
        "age":      { "type": "integer" }  
      }
    },
    "blogpost": { 
      "properties": { 
        "title":    { "type": "string"  }, 
        "body":     { "type": "string"  }, 
        "user_id":  {
          "type":   "string", 
          "index":  "not_analyzed"
        },
        "created":  {
          "type":   "date", 
          "format": "strict_date_optional_time||epoch_millis"
        },
       "baud": {
      "properties": { 
        "title":    { "type": "string"  }, 
        "name":     { "type": "string"  }, 
        "age":      { "type": "integer" }  
      }
    }
      }
    }
  }
}`
    dms, err := ParseSchema([]byte(schema))
    if err != nil {
    	t.Fatal(err)
    }
    for _, dm := range dms {
    	fmt.Println(dm.Dynamic, dm.Enabled, dm.DefaultAnalyzer, dm.StructTagKey)
    	for n, d := range dm.Properties {
    		fmt.Println("doc ", n, d)
	    }
	    for _,f := range dm.Fields {
	    	fmt.Println("field ", f)
	    }
    }
}
