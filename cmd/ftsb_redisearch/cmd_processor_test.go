package main

import (
	"reflect"
	"testing"
)

func Test_preProcessCmd(t *testing.T) {
	type args struct {
		row string
	}
	tests := []struct {
		name           string
		args           args
		wantCmdType    string
		wantCmdQueryId string
		wantCmd        string
		wantArgs       []string
		wantBytelen    uint64
		wantErr        bool
	}{
		{"empty", args{"WRITE,W1,HSET,doc:00cd782a6797464ea579429dbd921d60:0,pickup_location_long_lat,\"-73.993896484375,40.750110626220703\""},
			"WRITE", "W1", "FT.ADD", []string{"doc:00cd782a6797464ea579429dbd921d60:0", "pickup_location_long_lat", "\"-73.993896484375,40.750110626220703\""},
			100,
			false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotCmdType, gotCmdQueryId, gotCmd, gotArgs, gotBytelen, err := preProcessCmd(tt.args.row)
			if (err != nil) != tt.wantErr {
				t.Errorf("preProcessCmd() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotCmdType != tt.wantCmdType {
				t.Errorf("preProcessCmd() gotCmdType = %v, want %v", gotCmdType, tt.wantCmdType)
			}
			if gotCmdQueryId != tt.wantCmdQueryId {
				t.Errorf("preProcessCmd() gotCmdQueryId = %v, want %v", gotCmdQueryId, tt.wantCmdQueryId)
			}
			if gotCmd != tt.wantCmd {
				t.Errorf("preProcessCmd() gotCmd = %v, want %v", gotCmd, tt.wantCmd)
			}
			if !reflect.DeepEqual(gotArgs, tt.wantArgs) {
				t.Errorf("preProcessCmd() gotArgs = %v, want %v", gotArgs, tt.wantArgs)
			}
			if gotBytelen != tt.wantBytelen {
				t.Errorf("preProcessCmd() gotBytelen = %v, want %v", gotBytelen, tt.wantBytelen)
			}
		})
	}
}
