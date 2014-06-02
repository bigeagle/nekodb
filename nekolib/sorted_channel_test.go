package nekolib

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

type mynode struct {
	key   int
	value string
}

func (m mynode) Key() int64 {
	return int64(m.key)
}

func (m mynode) String() string {
	return fmt.Sprintf("{%d, %s}", m.key, m.value)
}

func TestSortedBuffer(t *testing.T) {
	Convey("Subject: Test Sorted Buffer", t, func() {
		nodes := map[string]([]mynode){
			"t1": []mynode{{1, "a"}, {3, "d"}, {4, "c"}, {8, "h"}, {9, "i"}, {14, "n"}},
			"t2": []mynode{{2, "b"}, {5, "e"}, {6, "f"}, {10, "j"}, {12, "l"}},
			"t3": []mynode{{0, "_"}, {7, "g"}, {11, "k"}, {13, "m"}, {15, "o"}},
		}

		Convey("Things should work", func() {
			outChan := make(chan SCNode, 16)
			sb := NewSortedChannel(3, outChan)
			for k, _ := range nodes {
				sb.AddPublisher(k)
			}

			for k, list := range nodes {
				go func(key string, buf []mynode) {
					time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
					// fmt.Println(sb.npubs)
					for _, n := range buf {
						sb.Pub(key, n)
					}
					sb.RemovePublisher(key)
				}(k, list)
			}

			last := (<-outChan).(mynode)
			i := 1
			for n := range outChan {
				i++
				So(last.Key(), ShouldBeLessThan, n.Key())
			}
			So(i, ShouldEqual, 16)

		})
	})
}
