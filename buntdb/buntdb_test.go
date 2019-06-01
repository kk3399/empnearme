package buntdb

import (
	"testing"
	"time"

	domain "github.com/kdamarla/empnearme/domain"
	log "github.com/kdamarla/empnearme/log"
)

const testingdbFileName = "C:\\Users\\kdamarla\\go\\src\\github.com\\kdamarla\\empnearme\\data.db"

func BenchmarkGet(b *testing.B) {
	lcaRepo := Init(log.Writer{}, testingdbFileName, "")
	d, _ := time.Parse("20060102", "20180101")
	searchCriteria := domain.SearchCriteria{Radius: 5, Zipcode: "60523", H1FiledAfter: d, MinimumPay: 150000}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		k, _ := lcaRepo.Get(searchCriteria)
		if len(k) == 0 {
			b.Errorf("got %d; want something", 0)
		}
	}

}
