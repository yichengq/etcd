// Copyright 2015 CoreOS, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"flag"
	"strings"
	"time"
)

func main() {
	endpointStr := flag.String("endpoints", "", "HTTP endpoints of members")
	stressKeySize := flag.Int("stress-key-size", 100, "the size of each key written into etcd")
	stressKeySuffixRange := flag.Int("stress-key-count", 100000, "the count of key range written into etcd")
	flag.Parse()

	endpoints := strings.Split(*endpointStr, ",")
	for _, e := range endpoints {
		stress := &stresser{
			Endpoint:       e,
			KeySize:        *stressKeySize,
			KeySuffixRange: *stressKeySuffixRange,
			N:              100,
		}
		stress.Stress()
	}
	time.Sleep(time.Hour)
}
