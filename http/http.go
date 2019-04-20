package http

import (
	"encoding/json"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	logWriter "github.com/kdamarla/empnearme/log"

	domain "github.com/kdamarla/empnearme/domain"
)

//LcaHandler handles all car http requests
type LcaHandler struct {
	LcaRepo domain.LcaRepo
	Log     logWriter.Writer
}

//Handler for all incoming http requests
type Handler struct {
	LcaHandler LcaHandler
}

//Serve http at predecided port
func Serve(handler Handler) error {
	return http.ListenAndServe(":8080", handler)
}

func (h Handler) ServeHTTP(res http.ResponseWriter, req *http.Request) {

	var head string

	head, req.URL.Path = shiftPath(req.URL.Path)
	if head == "lca" {
		h.LcaHandler.ServeHTTP(res, req)
		return
	}
	http.Error(res, "Not Found", http.StatusNotFound)
}

func (lcaHandler LcaHandler) ServeHTTP(res http.ResponseWriter, req *http.Request) {

	p := req.URL.Query()
	zip := p.Get("z")
	radius, _ := strconv.Atoi(p.Get("r"))
	emp := strings.ToLower(p.Get("e"))
	minPay, _ := strconv.Atoi(p.Get("p"))
	h1After, _ := time.Parse("20060102", p.Get("d"))

	filter := domain.SearchCriteria{Radius: radius, Zipcode: zip, Employer: emp, MinimumPay: minPay, H1FiledAfter: h1After}

	lcas, err := lcaHandler.LcaRepo.Get(filter)
	if err != nil {
		lcaHandler.Log.Write(err)
	}

	//i think we need a vue js site served from server
	res.Header().Set("Content-Type", "application/json")
	res.WriteHeader(http.StatusOK)
	//todo - use html\template and render HTML on server side

	json.NewEncoder(res).Encode(lcas)
}

// shiftPath splits off the first component of p, which will be cleaned of
// relative components before processing. head will never contain a slash and
// tail will always be a rooted path without trailing slash.
func shiftPath(p string) (head, tail string) {
	p = path.Clean("/" + p)
	i := strings.Index(p[1:], "/") + 1
	if i <= 0 {
		return p[1:], "/"
	}
	return p[1:i], p[i:]
}
