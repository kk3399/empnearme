package buntdb

import (
	"testing"

	domain "github.com/kdamarla/empnearme/domain"
	log "github.com/kdamarla/empnearme/log"
)

func BenchmarkGet(b *testing.B) {
	lcaRepo := Init(log.Writer{})
	searchCriteria := domain.SearchCriteria{Radius: 20, Zipcode: "60563"}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		k, _ := lcaRepo.Get(searchCriteria)
		if len(k) == 0 {
			b.Errorf("got %d; want something", 0)
		}
	}

}
